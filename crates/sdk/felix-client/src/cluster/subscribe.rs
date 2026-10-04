//! Subscribing through a [`ClusterClient`], following each shard to the
//! broker that owns it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::follow::ClusterSubscription;
use super::sharded::{ShardOffsets, ShardedGroup, ShardedSubscription};
use super::{Attempt, ClusterClient, MAX_REDIRECTS, Next, Retrying};
use crate::client::Client;

impl ClusterClient {
    /// Subscribe, following the cluster to whichever broker owns the shard,
    /// and following the shard again whenever it moves.
    ///
    /// A broker that does not own it answers `NotLeader` naming the one that
    /// does; this connects there and asks again. See [`ClusterSubscription`]
    /// for what happens when the shard later moves.
    ///
    /// The client this wrapper holds is **not** replaced. A redirect is about
    /// one shard, not about which broker is generally worth talking to, and
    /// moving every future publish because one stream lives elsewhere would be
    /// a much larger claim than the answer supports.
    pub async fn subscribe(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<ClusterSubscription> {
        self.subscribe_from(tenant_id, namespace, stream, None)
            .await
    }

    /// Like [`ClusterClient::subscribe`], but starting where the caller says.
    ///
    /// `None` is the tail, identical to `subscribe`. An offset is the first
    /// record the caller has *not* seen, so a client resuming after a
    /// disconnect passes the offset it last handled plus one.
    pub async fn subscribe_from(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<felix_wire::StartPosition>,
    ) -> Result<ClusterSubscription> {
        self.subscribe_shard(tenant_id, namespace, stream, 0, start)
            .await
    }

    /// Like [`ClusterClient::subscribe_from`], for one chosen shard of a
    /// stream rather than shard 0.
    ///
    /// The subscription follows that shard to its owner and again whenever
    /// it moves, exactly as [`ClusterSubscription`] describes. It reads that
    /// shard only; [`Self::subscribe_sharded`] reads them all.
    ///
    /// A shard past the stream's width is refused by the broker, not
    /// checked here.
    pub async fn subscribe_shard(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        start: Option<felix_wire::StartPosition>,
    ) -> Result<ClusterSubscription> {
        let (client, subscription) = self
            .open_shard(tenant_id, namespace, stream, shard, start)
            .await?;
        Ok(ClusterSubscription::new(
            Arc::clone(self),
            tenant_id,
            namespace,
            stream,
            shard,
            client,
            subscription,
        ))
    }

    /// Subscribe to **every** shard of a stream, merged into one channel.
    ///
    /// A subscription reads one shard, so this opens one per shard and follows
    /// each shard's own redirect to its owner. What comes back states its
    /// ordering guarantee rather than letting a caller infer one: see
    /// [`ShardedSubscription`].
    ///
    /// The shard count is asked of the broker, so this needs one that advertises
    /// `FEATURE_STREAM_SHARDS`. A broker that does not know the stream reports
    /// zero shards and this fails, rather than reading shard 0 and calling it
    /// the stream.
    ///
    /// Fails if **any** shard cannot be opened. A partial subscription looks
    /// exactly like a complete one to everything downstream, which makes quiet
    /// incompleteness the worst of the available answers.
    pub async fn subscribe_sharded(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<felix_wire::StartPosition>,
    ) -> Result<ShardedSubscription> {
        self.subscribe_sharded_inner(tenant_id, namespace, stream, start, None)
            .await
    }

    /// Resume a sharded subscription from where each shard had reached.
    ///
    /// `offsets` comes from [`ShardedSubscription::positions`]. Each listed
    /// shard resumes at `offset + 1`; a shard not listed starts wherever
    /// `start` says, which is what a shard that had delivered nothing should do.
    pub async fn resubscribe_sharded(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        offsets: ShardOffsets,
        start: Option<felix_wire::StartPosition>,
    ) -> Result<ShardedSubscription> {
        self.subscribe_sharded_inner(tenant_id, namespace, stream, start, Some(offsets))
            .await
    }

    /// One consumer group read across **every** shard of a stream.
    ///
    /// A group is bound to one shard, and only that shard's leader serves it,
    /// so this keeps one group per shard and follows each shard's own redirect
    /// to its leader. See [`ShardedGroup`] for how shards are visited
    /// and what ordering is promised.
    ///
    /// Needs a broker that advertises `FEATURE_STREAM_SHARDS`, to learn the
    /// shard count, and brokers that answer a group request for a shard they do
    /// not lead with a redirect rather than a refusal.
    pub async fn group_sharded(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        group: &str,
    ) -> Result<ShardedGroup> {
        let shards = self
            .stream_shard_count(tenant_id, namespace, stream)
            .await?;
        Ok(ShardedGroup::new(
            Arc::clone(self),
            tenant_id,
            namespace,
            stream,
            group,
            shards,
        ))
    }

    /// Open one shard for a new subscription, waiting out a refusal that
    /// clears by itself.
    ///
    /// An owner that was just placed or promoted answers `not_ready` until it
    /// has opened the shard, which for a replicated leader includes fencing a
    /// majority. That is a moment, not an answer about the stream, so it and
    /// the other retryable refusals get the policy's attempts and backoff, as
    /// a publish does. A fatal refusal is returned at once.
    pub(crate) async fn open_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        start: Option<felix_wire::StartPosition>,
    ) -> Result<(Arc<Client>, crate::Subscription)> {
        let started = Instant::now();
        let mut retrying = Retrying::default();
        let attempts = self.policy.attempts.max(1);
        let mut attempt = 0;
        loop {
            let error = match self
                .subscribe_shard_following_redirects(tenant_id, namespace, stream, shard, start)
                .await
            {
                Ok(opened) => return Ok(opened),
                Err(err) => err,
            };
            let at_least = match retrying.next(&error, Attempt::default()) {
                Next::Fail => return Err(error),
                Next::Reroute => Duration::ZERO,
                Next::Backoff { at_least } => at_least,
            };
            attempt += 1;
            if attempt >= attempts {
                return Err(error);
            }
            let delay = self.policy.delay_before(attempt - 1).max(at_least);
            if let Some(budget) = self.policy.deadline
                && started.elapsed() + delay >= budget
            {
                return Err(error);
            }
            tokio::time::sleep(delay).await;
        }
    }

    /// One shard, following the cluster to whichever broker owns *that shard*.
    ///
    /// The redirect loop is per shard because ownership is: two shards of one
    /// stream can live on two brokers, and an answer about one says nothing
    /// about the other.
    pub(crate) async fn subscribe_shard_following_redirects(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        start: Option<felix_wire::StartPosition>,
    ) -> Result<(Arc<Client>, crate::Subscription)> {
        let entry = self.client().await;
        self.subscribe_shard_via(entry, tenant_id, namespace, stream, shard, start)
            .await
    }

    /// [`Self::subscribe_shard_following_redirects`], asking `first` before
    /// anyone else.
    pub(crate) async fn subscribe_shard_via(
        &self,
        first: Arc<Client>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        start: Option<felix_wire::StartPosition>,
    ) -> Result<(Arc<Client>, crate::Subscription)> {
        let mut client = first;
        // Every broker this attempt has already asked. A cluster mid-rebalance
        // can name an owner that names another, and two brokers that disagree
        // would otherwise bounce a client between them until its deadline.
        let mut visited: Vec<String> = Vec::new();
        let mut went_back = false;

        for _ in 0..=MAX_REDIRECTS {
            let error = match client
                .subscribe_shard(tenant_id, namespace, stream, shard, start)
                .await
            {
                Ok(subscription) => return Ok((client, subscription)),
                Err(err) => err,
            };
            // An owner reached by redirect that answers `shard_unavailable` or
            // `draining` has lost the shard since it was named. The entry broker
            // routes by the current assignment, so ask it again, once.
            if !visited.is_empty() && !went_back && super::route_went_stale(&error) {
                went_back = true;
                visited.clear();
                client = self.client().await;
                continue;
            }
            let Some(redirect) = error.downcast_ref::<crate::NotLeaderError>().cloned() else {
                return Err(error);
            };
            if visited.iter().any(|seen| seen == &redirect.node_id) {
                return Err(error.context(format!(
                    "redirected back to {}, which has already been asked",
                    redirect.node_id
                )));
            }
            let Some(addr) = redirect.addr.clone() else {
                return Err(error.context(
                    "the owner's client address is not published, so there is nowhere to follow to",
                ));
            };
            let addr = self
                .resolve(&addr)
                .await
                .with_context(|| format!("the owner's address {addr:?} is not usable"))?;
            visited.push(redirect.node_id.clone());
            client = self
                .connect_to(addr)
                .await
                .with_context(|| format!("connect to the shard owner at {addr}"))?;
        }

        Err(anyhow::anyhow!(
            "still being redirected after {MAX_REDIRECTS} hops; the cluster has not settled on an owner"
        ))
    }

    async fn subscribe_sharded_inner(
        self: &Arc<Self>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<felix_wire::StartPosition>,
        resume: Option<ShardOffsets>,
    ) -> Result<ShardedSubscription> {
        let shards = self
            .stream_shard_count(tenant_id, namespace, stream)
            .await?;
        super::sharded::subscribe_sharded(self, tenant_id, namespace, stream, shards, start, resume)
            .await
    }

    /// How many shards a stream has, reconnecting once if the broker in hand
    /// cannot answer.
    pub(crate) async fn stream_shard_count(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<u32> {
        // Asking costs a round trip to whichever broker this client holds, and
        // that broker can be the one that just died. Reconnect and ask again
        // rather than reporting its death as an answer about the stream —
        // reconnecting is the whole reason this wrapper exists.
        let shards = match self.stream_shards_once(tenant_id, namespace, stream).await {
            Ok(shards) => shards,
            Err(first) => {
                self.reconnect().await.map_err(|reconnect_err| {
                    first.context(format!(
                        "and no other broker answered either: {reconnect_err:#}"
                    ))
                })?;
                self.stream_shards_once(tenant_id, namespace, stream)
                    .await
                    .with_context(|| format!("ask how many shards {stream} has"))?
            }
        };
        anyhow::ensure!(
            shards > 0,
            "the broker knows of no stream {stream} in {tenant_id}/{namespace}"
        );
        Ok(shards)
    }

    /// Ask the broker in hand how many shards a stream has, once.
    async fn stream_shards_once(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<u32> {
        let client = self.client().await;
        anyhow::ensure!(
            client.supports_stream_shards(),
            "this broker does not report stream shard counts, so the number of shards to \
             subscribe to cannot be established"
        );
        client
            .stream_shards(tenant_id, namespace, stream)
            .await
            .with_context(|| format!("ask how many shards {stream} has"))
    }
}

//! Following a subscription's or cache watch's shard when it moves to another
//! broker.
//!
//! A broker that stops serving a shard ends its subscriptions and cache
//! watches with a `shard_moved` frame saying where the shard went and where to
//! resume. Each is bound to the old broker's connection, so following means a
//! new one on the new owner, started where the old one left off.

mod cache_watch;

pub use cache_watch::ClusterCacheWatch;
pub(crate) use cache_watch::{WatchProgress, WatchTarget};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::StartPosition;
use tokio::time::Instant;

use super::{Attempt, ClusterClient, Next, next_step};
use crate::SubscriptionLost;
use crate::client::Client;
use crate::subscribe::{Event, ShardMoved, Subscription};

/// How long to keep trying the new owner when the policy sets no deadline.
///
/// The broker says the shard moved at the fence, before the new owner takes
/// over, so the first attempts can be refused. The cut-over takes a fraction
/// of a second; this is room for a slow one, not a wait anyone should hit.
const FOLLOW_DEADLINE: Duration = Duration::from_secs(30);

/// A subscription to one shard that follows the shard when it moves.
///
/// When the shard's owner hands it to another broker, the old owner ends the
/// subscription and says where to resume; [`Self::next_event`] subscribes on
/// the new owner and carries on, so a move shows up as a pause rather than an
/// end. On a durable stream the resume is exact: nothing is repeated and
/// nothing is skipped that the subscriber's own queue did not drop. An
/// in-memory stream has no offsets to resume from, so it resumes at the tail,
/// as a resubscribe would.
///
/// A lost connection is followed the same way when there is an offset to
/// resume from: the last one delivered plus one, or where the subscription
/// started if it delivered nothing. Without one (an in-memory stream, or a
/// broker that sends no offsets) resuming would skip whatever was published
/// meanwhile without saying so, so the [`crate::SubscriptionLost`] error is
/// returned instead.
pub struct ClusterSubscription {
    cluster: Arc<ClusterClient>,
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
    /// Held because dropping it closes the connection the events arrive on.
    client: Arc<Client>,
    subscription: Subscription,
    /// The highest offset handed out, so a move resumes after it.
    last_offset: Option<u64>,
    moves: u64,
    reconnects: u64,
    /// A loss that could not be resumed from yet, retried on the next call.
    lost: Option<anyhow::Error>,
}

impl ClusterSubscription {
    pub(crate) fn new(
        cluster: Arc<ClusterClient>,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        client: Arc<Client>,
        subscription: Subscription,
    ) -> Self {
        Self {
            cluster,
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
            client,
            subscription,
            last_offset: None,
            moves: 0,
            reconnects: 0,
            lost: None,
        }
    }

    /// The client connected to the broker currently serving this
    /// subscription. It changes when the shard moves.
    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }

    /// [`Subscription::start_offset`] of the current subscription, which
    /// after a move is where it resumed.
    pub fn start_offset(&self) -> Option<u64> {
        self.subscription.start_offset()
    }

    /// [`Subscription::live_offset`] of the current subscription.
    pub fn live_offset(&self) -> Option<u64> {
        self.subscription.live_offset()
    }

    /// How many times this subscription has followed its shard to a new owner.
    pub fn moves(&self) -> u64 {
        self.moves
    }

    /// The next event, or `None` once the broker has closed the event stream
    /// for a reason other than the shard moving.
    ///
    /// Following a move or a lost connection happens inside this call. If no
    /// broker can be subscribed to within the client's reconnect deadline, the
    /// error is returned; calling again retries.
    pub async fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            let next = match self.lost.take() {
                Some(lost) => Err(lost),
                None => self.subscription.next_event().await,
            };
            let next = match next {
                Ok(next) => next,
                Err(err) if err.chain().any(|cause| cause.is::<SubscriptionLost>()) => {
                    let Some(start) = self.resume_point() else {
                        return Err(err);
                    };
                    match self.resume_after_loss(start).await {
                        Ok(()) => continue,
                        Err(resume_err) => {
                            // Kept so the next call tries again rather than
                            // reading the dead subscription's closed queue as a
                            // clean end.
                            self.lost = Some(err);
                            return Err(resume_err);
                        }
                    }
                }
                Err(err) => return Err(err),
            };
            if let Some(event) = next {
                if let Some(offset) = event.offset {
                    self.last_offset = Some(self.last_offset.map_or(offset, |at| at.max(offset)));
                }
                return Ok(Some(event));
            }
            let Some(moved) = self.subscription.shard_moved().cloned() else {
                return Ok(None);
            };
            let (client, subscription) = self
                .cluster
                .follow_moved_shard(
                    &self.tenant_id,
                    &self.namespace,
                    &self.stream,
                    self.shard,
                    &moved,
                    self.last_offset,
                )
                .await?;
            self.subscription = subscription;
            self.client = client;
            self.moves += 1;
        }
    }

    /// How many times this subscription has resubscribed after losing its
    /// connection.
    pub fn reconnects(&self) -> u64 {
        self.reconnects
    }

    /// Where to resume after a lost connection, if anywhere is exact.
    fn resume_point(&self) -> Option<StartPosition> {
        self.last_offset
            .map(|last| last.saturating_add(1))
            .or(self.subscription.start_offset())
            .or(self.subscription.live_offset())
            .map(StartPosition::Offset)
    }

    /// Resubscribe from `start` on whichever broker owns the shard now,
    /// reconnecting the cluster client when the broker it holds is the one
    /// that was lost.
    async fn resume_after_loss(&mut self, start: StartPosition) -> Result<()> {
        let deadline = Instant::now() + self.cluster.policy.deadline.unwrap_or(FOLLOW_DEADLINE);
        let mut attempt = 0usize;
        loop {
            // The entry broker may be the dead one, and after a failed attempt
            // it may have died too; `connect_any` moves on to one that answers.
            if (attempt > 0 || Arc::ptr_eq(&self.cluster.client().await, &self.client))
                && let Err(err) = self.cluster.reconnect().await
            {
                tracing::debug!(error = %err, "no broker answered while resubscribing");
            }
            let error = match self
                .cluster
                .subscribe_shard_following_redirects(
                    &self.tenant_id,
                    &self.namespace,
                    &self.stream,
                    self.shard,
                    Some(start),
                )
                .await
            {
                Ok((client, subscription)) => {
                    self.client = client;
                    self.subscription = subscription;
                    self.reconnects += 1;
                    return Ok(());
                }
                Err(err) => err,
            };
            if next_step(&error, Attempt::default()) == Next::Fail {
                return Err(error.context(format!(
                    "resubscribe to shard {} of {} after losing the connection",
                    self.shard, self.stream
                )));
            }
            let delay = self.cluster.policy.delay_before(attempt);
            if Instant::now() + delay >= deadline {
                return Err(error.context(format!(
                    "no broker took shard {} of {} back before the deadline",
                    self.shard, self.stream
                )));
            }
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

impl ClusterClient {
    /// Subscribe to a shard on its new owner after `moved` ended the old
    /// subscription, resuming where that one left off.
    pub(crate) async fn follow_moved_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        moved: &ShardMoved,
        last_offset: Option<u64>,
    ) -> Result<(Arc<Client>, Subscription)> {
        let start = resume_position(last_offset, moved.resume_from);
        self.on_new_owner(moved, |first| {
            self.subscribe_shard_via(first, tenant_id, namespace, stream, shard, Some(start))
        })
        .await
        .with_context(|| format!("follow shard {shard} of {stream} after it moved"))
    }

    /// Open a reader of a moved shard on its new owner with `open`, given the
    /// broker to ask first.
    ///
    /// The broker named in `moved` is asked first, since it is usually right;
    /// otherwise the entry broker, which redirects. The new owner can refuse
    /// until it has taken over, so this retries with backoff until the
    /// policy's deadline.
    pub(crate) async fn on_new_owner<T, F, Fut>(&self, moved: &ShardMoved, open: F) -> Result<T>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let deadline = Instant::now() + self.policy.deadline.unwrap_or(FOLLOW_DEADLINE);
        let mut hint: Option<SocketAddr> = moved.addr.as_deref().and_then(|a| a.parse().ok());
        let mut hinted: Option<Arc<Client>> = None;
        let mut attempt = 0usize;
        loop {
            let first = match (&hinted, hint) {
                (Some(client), _) => Arc::clone(client),
                (None, Some(addr)) => match self.connect_to(addr).await {
                    Ok(client) => Arc::clone(hinted.insert(Arc::new(client))),
                    Err(err) => {
                        tracing::debug!(error = %err, %addr, "new owner unreachable; asking the entry broker");
                        hint = None;
                        self.client().await
                    }
                },
                (None, None) => self.client().await,
            };
            let error = match open(first).await {
                Ok(opened) => return Ok(opened),
                Err(err) => err,
            };
            if next_step(&error, Attempt::default()) == Next::Fail {
                return Err(error);
            }
            let delay = self.policy.delay_before(attempt);
            if Instant::now() + delay >= deadline {
                return Err(error.context("the new owner did not take it before the deadline"));
            }
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

/// Where to resume a subscription whose shard moved, given the last offset it
/// delivered and the broker's `resume_from`.
///
/// `resume_from` is the stream position: every record below it was offered to
/// the subscriber. Taking the max with `last + 1` matters because a record
/// fanned out just before the move can still reach the subscriber, putting
/// `last + 1` past it. Records the subscriber's queue dropped stay dropped, as
/// they would without a move. With neither, there is nothing to resume from,
/// so the tail.
pub(crate) fn resume_position(last_offset: Option<u64>, resume_from: Option<u64>) -> StartPosition {
    let after_last = last_offset.map(|last| last.saturating_add(1));
    match (after_last, resume_from) {
        (Some(next), Some(from)) => StartPosition::Offset(next.max(from)),
        (None, Some(from)) => StartPosition::Offset(from),
        (Some(next), None) => StartPosition::Offset(next),
        (None, None) => StartPosition::Latest,
    }
}

#[cfg(test)]
mod tests;

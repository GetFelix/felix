//! Consumer groups on a Raft stream shard. See `docs/per-shard-raft.md`,
//! "Consumer groups".
//!
//! Every operation first finds this broker leading the shard (routing),
//! then the reader confirms the term by a read index before it reads the
//! group table, and proposes each group write at that term, answering only
//! once it is applied. A cluster serves groups on Raft only once it
//! finalizes `raft_consumer_groups`: an older member refuses the entry.
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_broker::{Broker, BrokerError, GroupKey};
use felix_wire::{GroupRecord, StartPosition};

use super::{Settle, group_key, storage};
use felix_replication::engine::Engine;

use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::PublishContext;
use crate::shards::raft::RaftGroups;
use crate::shards::routing::{Dispatch, dispatch, dispatch_write};
use crate::shards::{ShardKey, ShardKind};

/// A group operation on a shard a Raft group runs here.
pub(super) struct OnRaft {
    groups: Arc<RaftGroups>,
    key: GroupKey,
    log: felix_broker::StreamLog,
    shard: ShardKey,
    timeout: Duration,
}

/// `Ok(None)` when the hybrid runs the shard here. Refused before the fleet
/// finalizes `raft_consumer_groups`, and by whoever leads it otherwise.
#[allow(clippy::too_many_arguments)]
pub(super) async fn on_raft(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    writes: bool,
) -> Result<Option<OnRaft>, ClientError> {
    let key = ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    };
    let Some(groups) = publish_ctx.engine.raft_groups(&key) else {
        // A durable stream on a Raft broker lives only on its group. Before
        // the member opens, the hybrid path would open a stream log and group
        // state of its own that nothing serves.
        if publish_ctx.engine.runs_raft()
            && broker
                .stream_metadata(tenant_id, namespace, stream)
                .await
                .is_some_and(|metadata| metadata.durable)
        {
            return Err(ClientError::unavailable(
                &crate::shards::routing::Reason::NotReady,
                format!(
                    "the Raft group of {tenant_id}/{namespace}/{stream} shard {shard} is not open on this broker yet; retry"
                ),
            ));
        }
        return Ok(None);
    };
    if publish_ctx.ingress.is_some()
        && !publish_ctx
            .engine
            .marks()
            .is_some_and(|marks| marks.fleet_supports(felix_common::fleet::RAFT_CONSUMER_GROUPS))
    {
        return Err(ClientError::new(
            felix_wire::ErrorCode::InvalidRequest,
            "consumer groups on Raft shards are not enabled for this fleet (finalize `raft_consumer_groups`)",
        )
        .with_retry(felix_wire::RetryClass::Fatal));
    }
    // A write is routed as a publish is, at the term it would be proposed
    // at; the reader then proposes only at a term a read index confirmed.
    let dispatched = if writes {
        dispatch_write(publish_ctx.ingress.as_deref(), &publish_ctx.engine, &key)
            .await
            .0
    } else {
        dispatch(publish_ctx.ingress.as_deref(), &publish_ctx.engine, &key)
    };
    match dispatched {
        Dispatch::Local { .. } => {}
        Dispatch::Forward { node_id, .. } => {
            return Err(ClientError::new(
                felix_wire::ErrorCode::NotLeader,
                format!("shard {shard} of {stream} is served by {node_id}"),
            ));
        }
        Dispatch::Unavailable(reason) => {
            return Err(ClientError::unavailable(&reason, reason.to_string()));
        }
    }
    // The member's own log, which the stream's handle reads.
    let log = broker
        .resolve_stream_handle(tenant_id, namespace, stream, shard)
        .await
        .map_err(|err| ClientError::from_broker(&err, "consumer group not served"))?
        .log()
        .cloned()
        .ok_or_else(|| super::no_storage("this stream is not durable"))?;
    Ok(Some(OnRaft {
        groups: Arc::clone(groups),
        key: group_key(tenant_id, namespace, stream, shard, group),
        log,
        shard: key,
        timeout: publish_ctx.quorum_timeout,
    }))
}

impl OnRaft {
    fn deadline(&self) -> Instant {
        Instant::now() + self.timeout
    }

    /// One past the last committed record: nothing above the commit index
    /// is handed out or sought to. Read it only after a read index, or it
    /// misses a publish acknowledged before its commit.
    fn committed(&self, publish_ctx: &PublishContext) -> Result<u64, ClientError> {
        publish_ctx
            .engine
            .read_bound(None, &self.shard)
            .map(|bound| bound.unwrap_or(u64::MAX))
            .map_err(|_| crate::shards::lifecycle::fence::Fenced::NotServing.into())
    }

    /// The tail, held to what this member has applied. Call it after a read
    /// index ([`Self::confirmed_end`] or a group read), or a publish
    /// acknowledged before the call can be above it.
    async fn read_end(&self, publish_ctx: &PublishContext) -> Result<u64, ClientError> {
        let committed = self.committed(publish_ctx)?;
        let tail = self.log.tail_offset().await.map_err(storage)?;
        Ok(tail.min(committed))
    }

    /// [`Self::read_end`] after a read index, so it covers every publish
    /// acknowledged before the call. Fails, retryably, when no majority
    /// confirms this leader in time.
    async fn confirmed_end(&self, publish_ctx: &PublishContext) -> Result<u64, ClientError> {
        self.groups
            .confirm(&self.key, self.deadline())
            .await
            .map_err(broker)?;
        self.read_end(publish_ctx).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn poll(
        &self,
        publish_ctx: &PublishContext,
        max_records: usize,
        wait: Duration,
        appended: Option<Arc<tokio::sync::Notify>>,
        recheck: Duration,
        consumer: Option<&felix_broker::GroupConsumer>,
        visibility: Option<Duration>,
    ) -> Result<Vec<GroupRecord>, ClientError> {
        let until = Instant::now() + wait;
        let mut first = true;
        loop {
            // Armed before the read, so a commit landing between the read and
            // the wait still wakes it. The stream's commit follower signals
            // it each time the shard's commit index moves here; `recheck`
            // covers a claim lapsing or a nack, which nothing signals.
            let mut on_append = super::armed(appended.as_ref());
            let polled = async {
                // The reader asks for the bound once its read index is
                // applied, so a record acknowledged before this poll is
                // below it, as it is for a stream read.
                let committed = || {
                    self.committed(publish_ctx).map_err(|_| {
                        BrokerError::StateNotReadable(felix_broker::NotReadable::Refused)
                    })
                };
                self.groups
                    .poll_below(
                        &self.key,
                        &self.log,
                        committed,
                        max_records,
                        Instant::now(),
                        consumer,
                        visibility,
                        self.deadline(),
                    )
                    .await
                    .map_err(|err| {
                        let moved = moved(&err);
                        (broker(err), moved)
                    })
            };
            let claimed = match polled.await {
                Ok(claimed) => claimed,
                // Leadership moved while the poll waited: nothing was
                // claimed, and the next poll goes to the new leader.
                Err((refused, moved)) => return failed(first, refused, moved),
            };
            first = false;
            if !claimed.is_empty() {
                return Ok(claimed.into_iter().map(super::record).collect());
            }
            let now = Instant::now();
            if now >= until {
                return Ok(Vec::new());
            }
            tokio::select! {
                () = tokio::time::sleep_until(until.min(now + recheck).into()) => {}
                () = super::fired(&mut on_append) => {}
            }
        }
    }

    pub(super) async fn settle(
        &self,
        offset: u64,
        action: Settle,
        group: &str,
    ) -> Result<(), ClientError> {
        let deadline = self.deadline();
        self.inherit(deadline).await?;
        super::refuse_non_record(Some(&self.log), offset).await?;
        let settled = match action {
            Settle::Ack => self.groups.ack(&self.key, offset, deadline).await,
            Settle::Nack { delay, attempts } => {
                self.groups
                    .nack_after(&self.key, offset, attempts, delay, Instant::now(), deadline)
                    .await
            }
            Settle::DeadLetter => {
                match self.groups.dead_letter(&self.key, offset, deadline).await {
                    Ok(true) => Ok(()),
                    Ok(false) => {
                        return Err(ClientError::invalid(format!(
                            "offset {offset} of {group} is already finished, so it was not dead-lettered"
                        )));
                    }
                    Err(err) => Err(err),
                }
            }
        };
        self.stale_or(settled).await
    }

    pub(super) async fn extend(
        &self,
        offset: u64,
        attempts: u32,
        by: Duration,
    ) -> Result<(), ClientError> {
        self.inherit(self.deadline()).await?;
        super::refuse_non_record(Some(&self.log), offset).await?;
        let extended = self
            .groups
            .extend(
                &self.key,
                offset,
                attempts,
                by,
                Instant::now(),
                self.deadline(),
            )
            .await;
        self.stale_or(extended).await
    }

    /// A claim handed out before this term is still the consumer's to
    /// settle, as on the hybrid.
    async fn inherit(&self, deadline: Instant) -> Result<(), ClientError> {
        let tail = self.log.tail_offset().await.map_err(storage)?;
        self.groups
            .inherit_below(&self.key, tail, deadline)
            .await
            .map_err(broker)
    }

    /// A claim this leader never handed out, on a record in the log, was
    /// handed out by an earlier leader: stale, and delivered again.
    async fn stale_or(&self, settled: felix_broker::Result<()>) -> Result<(), ClientError> {
        let Err(err) = settled else {
            return Ok(());
        };
        if let BrokerError::GroupOffsetNotHandedOut { offset, .. } = err
            && offset < self.log.tail_offset().await.map_err(storage)?
        {
            return Err(ClientError::new(
                felix_wire::ErrorCode::StaleClaim,
                format!("{err}; the claim is stale and the record will be delivered again"),
            ));
        }
        Err(broker(err))
    }

    pub(super) async fn dead_letters(&self) -> Result<Vec<u64>, ClientError> {
        self.groups
            .dead_letters(&self.key, self.deadline())
            .await
            .map_err(broker)
    }

    pub(super) async fn manage_dead_letter(
        &self,
        offset: u64,
        redrive: bool,
    ) -> Result<bool, ClientError> {
        let deadline = self.deadline();
        if redrive {
            self.groups.redrive(&self.key, offset, deadline).await
        } else {
            self.groups.discard(&self.key, offset, deadline).await
        }
        .map_err(broker)
    }

    pub(super) async fn seek(
        &self,
        publish_ctx: &PublishContext,
        start: StartPosition,
        if_new: bool,
    ) -> Result<felix_broker::Seek, ClientError> {
        let earliest = self.log.readable_base();
        // The tail a client sees is the records' end (see `describe`); the
        // group entries past it hold no record, so latest stops there too.
        let end = self.confirmed_end(publish_ctx).await?;
        let latest = publish_ctx.engine.records_end(&self.shard, end);
        let offset = super::seek_offset(start, earliest, latest, self.key.shard)?;
        self.groups
            .seek(&self.key, offset, if_new, self.deadline())
            .await
            .map_err(broker)
    }

    pub(super) async fn describe(
        &self,
        publish_ctx: &PublishContext,
    ) -> Result<(felix_broker::GroupSnapshot, u64), ClientError> {
        let mut snapshot = self
            .groups
            .describe(&self.key, self.deadline())
            .await
            .map_err(broker)?;
        // Group writes are entries of the shard's log but hold no record, so
        // the tail stops before the trailing ones, as on the hybrid. A poll
        // settles the trailing ones too, so the cursor can pass that point;
        // it then reports the records' end, which is still true of it. The
        // describe ran a read index, so the end covers every acknowledged
        // publish.
        let end = self.read_end(publish_ctx).await?;
        let records_end = publish_ctx.engine.records_end(&self.shard, end);
        if let Some(committed) = snapshot.committed.as_mut() {
            *committed = (*committed).min(records_end);
        }
        Ok((snapshot, records_end))
    }

    pub(super) async fn delete(&self) -> Result<bool, ClientError> {
        self.groups
            .delete(&self.key, self.deadline())
            .await
            .map_err(broker)
    }
}

/// A poll that failed. Only a later one whose leadership moved answers empty;
/// any other failure reaches the consumer.
pub(super) fn failed(
    first: bool,
    refused: ClientError,
    moved: bool,
) -> Result<Vec<GroupRecord>, ClientError> {
    if moved && !first {
        Ok(Vec::new())
    } else {
        Err(refused)
    }
}

/// Whether `err` says this broker stopped leading the shard.
pub(super) fn moved(err: &BrokerError) -> bool {
    matches!(
        err,
        BrokerError::StateNotReadable(felix_broker::NotReadable::Refused)
    )
}

fn broker(err: BrokerError) -> ClientError {
    ClientError::from_broker(&err, err.to_string())
}

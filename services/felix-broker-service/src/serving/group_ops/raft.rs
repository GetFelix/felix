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

/// How often a waiting poll looks again. Raft claims lapse and records
/// commit without waking it.
const RECHECK: Duration = Duration::from_millis(50);

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
        dispatch_write(publish_ctx.ingress.as_deref(), &key).await.0
    } else {
        dispatch(publish_ctx.ingress.as_deref(), &key)
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
    /// is handed out or sought to.
    fn committed(&self, publish_ctx: &PublishContext) -> Result<u64, ClientError> {
        publish_ctx
            .engine
            .read_bound(None, &self.shard)
            .map(|bound| bound.unwrap_or(u64::MAX))
            .map_err(|_| crate::shards::lifecycle::fence::Fenced::NotServing.into())
    }

    async fn read_end(&self, publish_ctx: &PublishContext) -> Result<u64, ClientError> {
        let committed = self.committed(publish_ctx)?;
        let tail = self.log.tail_offset().await.map_err(storage)?;
        Ok(tail.min(committed))
    }

    pub(super) async fn poll(
        &self,
        publish_ctx: &PublishContext,
        max_records: usize,
        wait: Duration,
        consumer: Option<&felix_broker::GroupConsumer>,
        visibility: Option<Duration>,
    ) -> Result<Vec<GroupRecord>, ClientError> {
        let until = Instant::now() + wait;
        let mut first = true;
        loop {
            let polled = async {
                let committed = self.committed(publish_ctx)?;
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
                    .map_err(broker)
            };
            let claimed = match polled.await {
                Ok(claimed) => claimed,
                // Leadership moved while the poll waited: nothing was
                // claimed, and the next poll goes to the new leader.
                Err(_) if !first => return Ok(Vec::new()),
                Err(refused) => return Err(refused),
            };
            first = false;
            if !claimed.is_empty() {
                return Ok(claimed.into_iter().map(super::record).collect());
            }
            let now = Instant::now();
            if now >= until {
                return Ok(Vec::new());
            }
            tokio::time::sleep_until(until.min(now + RECHECK).into()).await;
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
        let latest = self.read_end(publish_ctx).await?;
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
        let snapshot = self
            .groups
            .describe(&self.key, self.deadline())
            .await
            .map_err(broker)?;
        Ok((snapshot, self.read_end(publish_ctx).await?))
    }

    pub(super) async fn delete(&self) -> Result<bool, ClientError> {
        self.groups
            .delete(&self.key, self.deadline())
            .await
            .map_err(broker)
    }
}

fn broker(err: BrokerError) -> ClientError {
    ClientError::from_broker(&err, err.to_string())
}

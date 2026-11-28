//! Serving a consumer group, once ownership says this broker may.
//!
//! A group's in-flight state lives on whichever broker leads the shard. If two
//! brokers served the same group they would each hand out the same records —
//! the divergence cache routing exists to prevent, in a place where it would be
//! worse: a queue's whole promise is that one consumer holds a record at a time.
//!
//! So every operation here refuses unless this broker leads the shard. There is
//! no forwarding: unlike a cache operation, a poll returns records the consumer
//! then has to acknowledge, and relaying that through a second broker would put
//! the claim and the acknowledgement on different machines.
//!
//! A shard a Raft group runs here is served by [`raft`] instead: its group
//! state is entries in the shard's own log.
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_broker::{Broker, GroupKey};
use felix_replication::engine::Engine;
use felix_replication::engine::Unreadable;
use felix_wire::{GroupRecord, StartPosition};
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

use crate::serving::commit_ops;
use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::PublishContext;
use crate::shards::lifecycle::fence::{self, FenceGuard};
use crate::shards::routing::{Dispatch, Reason, dispatch};
use crate::shards::{ShardKey, ShardKind};

/// Longest a waiting poll goes without looking again unprompted.
///
/// New records, settled claims and lapsing claims each wake a poll directly.
/// This covers what does not signal, such as the shard moving away mid-wait.
const WAIT_RECHECK: Duration = Duration::from_millis(100);

/// Take up to `max_records` for a group, waiting up to `wait` for work.
///
/// The wait happens on a stream of the client's own, so holding it open blocks
/// nothing else on that connection.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn poll(
    broker: &Broker,
    publish_ctx: &PublishContext,
    // The operation's place in the shard's write fence, taken when it was
    // admitted.
    admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    max_records: usize,
    wait: Duration,
    consumer: Option<&felix_broker::GroupConsumer>,
    visibility: Option<Duration>,
) -> Result<Vec<GroupRecord>, ClientError> {
    poll_rechecking(
        broker,
        publish_ctx,
        admitted,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        max_records,
        wait,
        WAIT_RECHECK,
        consumer,
        visibility,
    )
    .await
}

/// [`poll`], looking again unprompted every `recheck` while it waits.
#[allow(clippy::too_many_arguments)]
async fn poll_rechecking(
    broker: &Broker,
    publish_ctx: &PublishContext,
    mut admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    max_records: usize,
    wait: Duration,
    recheck: Duration,
    consumer: Option<&felix_broker::GroupConsumer>,
    visibility: Option<Duration>,
) -> Result<Vec<GroupRecord>, ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        true,
    )
    .await?
    {
        let appended = match wait.is_zero() {
            true => None,
            false => broker
                .resolve_stream_handle(tenant_id, namespace, stream, shard)
                .await
                .ok()
                .map(|handle| handle.appended()),
        };
        return on_raft
            .poll(
                publish_ctx,
                max_records,
                wait,
                appended,
                recheck,
                consumer,
                visibility,
            )
            .await;
    }
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let deadline = Instant::now() + wait;
    let mut first = true;
    // Resolved only for a poll that may wait. A shard whose handle cannot be
    // had still waits, just on the recheck alone.
    let (appended, changed) = if wait.is_zero() {
        (None, None)
    } else {
        let appended = broker
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await
            .ok()
            .map(|handle| handle.appended());
        (appended, Some(reader.changed(&key)))
    };

    loop {
        // Registered before the poll reads, so a record or an ack landing
        // between the read and the wait still wakes it.
        let mut on_append = armed(appended.as_ref());
        let mut on_change = armed(changed.as_ref());
        // A poll writes: it records what it hands out, and dead-letters what
        // has run out of attempts.
        let fenced = match owned.enter(publish_ctx, &mut admitted) {
            Ok(fenced) => fenced,
            // The shard stopped serving here while this poll waited. Nothing
            // was claimed, and the consumer's next poll is held until the
            // move cuts over and then sent to the new owner.
            Err(_) if !first => return Ok(Vec::new()),
            Err(refused) => return Err(refused),
        };
        // Read each round: the mark moves while a poll waits.
        let committed = match committed_bound(broker, publish_ctx, &owned).await {
            Ok(committed) => committed,
            Err(_) if !first => return Ok(Vec::new()),
            Err(refused) => return Err(refused),
        };
        first = false;
        let claimed = match &log {
            Some(log) => reader
                .poll_below(
                    &key,
                    log,
                    committed.unwrap_or(u64::MAX),
                    max_records,
                    Instant::now(),
                    consumer,
                    visibility,
                )
                .await
                .map_err(storage),
            // Nothing to hand out: an in-memory stream keeps no records.
            None => Ok(Vec::new()),
        };
        drop(fenced);
        let claimed = claimed?;
        if !claimed.is_empty() {
            return Ok(claimed.into_iter().map(record).collect());
        }
        let now = Instant::now();
        if now >= deadline {
            // An empty answer, which is an answer: nothing was available in the
            // time the consumer was willing to wait for it.
            return Ok(Vec::new());
        }
        // Ownership is re-checked every round, because the shard can move while
        // a poll is waiting. Serving one after that would hand out records the
        // new owner is handing out too.
        if owned_here(publish_ctx, tenant_id, namespace, stream, shard).is_err() {
            return Ok(Vec::new());
        }
        let mut until = deadline.min(now + recheck);
        if let Some(lapse) = reader.next_lapse(&key).await {
            // That record is owed from then on, and nothing else says so.
            until = until.min(lapse.max(now));
        }
        tokio::select! {
            _ = tokio::time::sleep_until(until.into()) => {}
            _ = fired(&mut on_append) => {}
            _ = fired(&mut on_change) => {}
        }
    }
}

fn record(claimed: felix_broker::Claimed) -> GroupRecord {
    GroupRecord {
        offset: claimed.offset,
        payload: claimed.payload,
        attempts: claimed.attempts,
        skipped_before: claimed.skipped_before,
        publisher: claimed
            .publisher
            .map(|publisher| String::from_utf8_lossy(&publisher).into_owned()),
        timestamp_micros: Some(claimed.timestamp_micros),
    }
}

/// A notification future, enabled so it catches a signal sent before it is
/// awaited.
fn armed(notify: Option<&Arc<Notify>>) -> Option<Pin<Box<OwnedNotified>>> {
    notify.map(|notify| {
        let mut notified = Box::pin(Arc::clone(notify).notified_owned());
        notified.as_mut().enable();
        notified
    })
}

async fn fired(notified: &mut Option<Pin<Box<OwnedNotified>>>) {
    match notified {
        Some(notified) => notified.as_mut().await,
        None => std::future::pending().await,
    }
}

/// Offsets this group gave up on.
pub(crate) async fn dead_letters(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
) -> Result<Vec<u64>, ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        false,
    )
    .await?
    {
        return on_raft.dead_letters().await;
    }
    let (reader, _log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    commit_ops::refuse_read_without_lease(publish_ctx)?;
    let listed = reader.dead_lettered(&key).await.map_err(storage)?;
    commit_ops::confirm_read_leader(publish_ctx, &owned.key).await?;
    Ok(listed)
}

/// Drop a dead letter, or put it back in the queue.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn manage_dead_letter(
    broker: &Broker,
    publish_ctx: &PublishContext,
    // The operation's place in the shard's write fence, taken when it was
    // admitted.
    mut admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    offset: u64,
    redrive: bool,
) -> Result<(), ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        true,
    )
    .await?
    {
        return match on_raft.manage_dead_letter(offset, redrive).await? {
            true => Ok(()),
            false => Err(not_dead_lettered(offset, group)),
        };
    }
    let (reader, _log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let _fenced = owned.enter(publish_ctx, &mut admitted)?;
    let taken = if redrive {
        reader.redrive(&key, offset).await
    } else {
        reader.discard(&key, offset).await
    }
    .map_err(storage)?;
    if taken {
        return Ok(());
    }
    Err(not_dead_lettered(offset, group))
}

// Refused rather than silently accepted. An operator told a redrive
// succeeded when the offset was never dead-lettered would wait for a
// delivery that is not coming.
fn not_dead_lettered(offset: u64, group: &str) -> ClientError {
    ClientError::invalid(format!("offset {offset} is not a dead letter of {group}"))
}

/// Move a group to `start` on one shard, or with `if_new` create it there.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn seek(
    broker: &Broker,
    publish_ctx: &PublishContext,
    mut admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    start: StartPosition,
    if_new: bool,
) -> Result<felix_broker::Seek, ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        true,
    )
    .await?
    {
        return on_raft.seek(publish_ctx, start, if_new).await;
    }
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let _fenced = owned.enter(publish_ctx, &mut admitted)?;
    let earliest = log.as_ref().map_or(0, |log| log.readable_base());
    let latest = read_end(broker, publish_ctx, &owned, log.as_ref()).await?;
    let offset = seek_offset(start, earliest, latest, shard)?;
    let seek = reader.seek(&key, offset, if_new).await.map_err(storage)?;
    Ok(seek)
}

/// Where `start` puts a group on a shard holding `earliest..latest`.
fn seek_offset(
    start: StartPosition,
    earliest: u64,
    latest: u64,
    shard: u32,
) -> Result<u64, ClientError> {
    Ok(match start {
        StartPosition::Earliest => earliest,
        StartPosition::Latest => latest,
        // Below the head the records are gone, and the group would count each
        // as dropped work. Past the committed tail the offset may not hold the
        // record it will after a failover.
        StartPosition::Offset(offset) if offset < earliest => {
            return Err(ClientError::invalid(format!(
                "offset {offset} is below the oldest record shard {shard} holds, {earliest}"
            )));
        }
        StartPosition::Offset(offset) if offset > latest => {
            return Err(ClientError::invalid(format!(
                "offset {offset} is past the committed tail of shard {shard}, {latest}"
            )));
        }
        StartPosition::Offset(offset) => offset,
    })
}

/// Where a group stands on one shard, and the shard's committed tail.
pub(crate) async fn describe(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
) -> Result<(felix_broker::GroupSnapshot, u64), ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        false,
    )
    .await?
    {
        return on_raft.describe(publish_ctx).await;
    }
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    commit_ops::refuse_read_without_lease(publish_ctx)?;
    let snapshot = reader.describe(&key).await.map_err(storage)?;
    let tail = read_end(broker, publish_ctx, &owned, log.as_ref()).await?;
    commit_ops::confirm_read_leader(publish_ctx, &owned.key).await?;
    Ok((snapshot, tail))
}

/// Delete a group on one shard. Returns whether there was anything to delete.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn delete(
    broker: &Broker,
    publish_ctx: &PublishContext,
    mut admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
) -> Result<bool, ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        true,
    )
    .await?
    {
        return on_raft.delete().await;
    }
    let (reader, _log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let _fenced = owned.enter(publish_ctx, &mut admitted)?;
    let existed = reader.delete(&key).await.map_err(storage)?;
    Ok(existed)
}

/// The end of what a group may be handed on this shard: the tail, held to
/// the quorum mark on a shard that has one, as a poll is.
async fn read_end(
    broker: &Broker,
    publish_ctx: &PublishContext,
    owned: &Owned,
    log: Option<&felix_broker::StreamLog>,
) -> Result<u64, ClientError> {
    let committed = committed_bound(broker, publish_ctx, owned).await?;
    let tail = tail_of(log).await?;
    Ok(tail.min(committed.unwrap_or(u64::MAX)).min(
        log.and_then(|log| log.poisoned_read_end())
            .unwrap_or(u64::MAX),
    ))
}

/// The quorum mark a group may read to, `None` when the shard has none.
/// Refused once this broker no longer serves the shard, or while it has no
/// mark at its generation yet: zero there is not a position, and a seek to
/// `latest` would rewind the group to it.
async fn committed_bound(
    broker: &Broker,
    publish_ctx: &PublishContext,
    owned: &Owned,
) -> Result<Option<u64>, ClientError> {
    let key = &owned.key;
    publish_ctx
        .engine
        .read_bound(
            broker
                .stream_consistency(&key.tenant_id, &key.namespace, &key.stream)
                .await,
            key,
        )
        .map_err(|unreadable| match unreadable {
            Unreadable::NotServed => crate::shards::lifecycle::fence::Fenced::NotServing.into(),
            Unreadable::Settling => ClientError::unavailable(
                &Reason::NotReady,
                "this broker has no committed mark for the shard yet",
            ),
        })
}

/// What a consumer does with a record it was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settle {
    Ack,
    /// Hand it back, owed again after `delay`. With `attempts`, only while
    /// the claim that delivery made still stands.
    Nack {
        delay: Duration,
        attempts: Option<u32>,
    },
    /// Give up on it.
    DeadLetter,
}

/// Finish a record, hand it back, or give up on it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn settle(
    broker: &Broker,
    publish_ctx: &PublishContext,
    // The operation's place in the shard's write fence, taken when it was
    // admitted.
    mut admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    offset: u64,
    action: Settle,
) -> Result<(), ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        true,
    )
    .await?
    {
        return on_raft.settle(offset, action, group).await;
    }
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let _fenced = owned.enter(publish_ctx, &mut admitted)?;
    // A claim handed out before the shard moved here is still the consumer's
    // to settle.
    let tail = tail_of(log.as_ref()).await?;
    let claimed = reader
        .inherit_below(&key, tail, offset)
        .await
        .map_err(|err| ClientError::from_broker(&err, err.to_string()))?;
    refuse_non_record(log.as_ref(), offset, claimed).await?;
    let settled = match action {
        Settle::Ack => reader.ack(&key, offset).await,
        Settle::Nack { delay, attempts } => {
            reader
                .nack_after(&key, offset, attempts, delay, Instant::now())
                .await
        }
        Settle::DeadLetter => match reader.dead_letter(&key, offset).await {
            Ok(true) => Ok(()),
            // Refused rather than answered as done: the consumer would look
            // for a dead letter that is not there.
            Ok(false) => {
                return Err(ClientError::invalid(format!(
                    "offset {offset} of {group} is already finished, so it was not dead-lettered"
                )));
            }
            Err(err) => Err(err),
        },
    };
    let Err(err) = settled else {
        return Ok(());
    };
    // Past the tracker's inherited tail, but inside the log now: written
    // after the tracker was built and not yet polled. Told apart from past
    // the tail, which cannot have been handed out by anyone.
    if let felix_broker::BrokerError::GroupOffsetNotHandedOut { .. } = err
        && offset < tail_of(log.as_ref()).await?
    {
        return Err(ClientError::new(
            felix_wire::ErrorCode::StaleClaim,
            format!("{err}; the claim is stale and the record will be delivered again"),
        ));
    }
    Err(ClientError::from_broker(&err, err.to_string()))
}

/// Keep a claim standing for `by` from now.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn extend(
    broker: &Broker,
    publish_ctx: &PublishContext,
    mut admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    offset: u64,
    attempts: u32,
    by: Duration,
) -> Result<(), ClientError> {
    #[cfg(feature = "raft")]
    if let Some(on_raft) = raft::on_raft(
        broker,
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        shard,
        group,
        true,
    )
    .await?
    {
        return on_raft.extend(offset, attempts, by).await;
    }
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard).await?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    // Claims are leader memory, so this writes nothing durable. It is still
    // fenced: an extension landing after the shard moved would be lost
    // without the consumer hearing of it.
    let _fenced = owned.enter(publish_ctx, &mut admitted)?;
    let tail = tail_of(log.as_ref()).await?;
    let claimed = reader
        .inherit_below(&key, tail, offset)
        .await
        .map_err(|err| ClientError::from_broker(&err, err.to_string()))?;
    refuse_non_record(log.as_ref(), offset, claimed).await?;
    match reader
        .extend(&key, offset, attempts, by, Instant::now())
        .await
    {
        Ok(()) => Ok(()),
        // Handed out by a predecessor and not known here: not a claim this
        // broker can extend, and the record will be delivered again.
        Err(err @ felix_broker::BrokerError::GroupOffsetNotHandedOut { .. }) if offset < tail => {
            Err(ClientError::new(
                felix_wire::ErrorCode::StaleClaim,
                format!("{err}; the claim is stale and the record will be delivered again"),
            )
            .with_retry(felix_wire::RetryClass::Fatal))
        }
        Err(err) => Err(ClientError::from_broker(&err, err.to_string())),
    }
}

/// A shard this broker led when a group operation was admitted.
struct Owned {
    key: ShardKey,
    generation: u64,
}

impl Owned {
    /// Enter the shard's write fence, right before a group write, or keep the
    /// place `admitted` took. Group state moves with the shard, so a write
    /// landing after the shard stopped serving here would be left behind.
    fn enter(
        &self,
        publish_ctx: &PublishContext,
        admitted: &mut Option<FenceGuard>,
    ) -> Result<Option<FenceGuard>, ClientError> {
        let guard = fence::enter_or_keep(
            admitted,
            publish_ctx.ingress.as_deref(),
            Some(&self.key),
            self.generation,
        )
        .map_err(ClientError::from)?;
        // Group state is acknowledged on this broker alone, so it needs the
        // lease.
        if let Some(ingress) = publish_ctx.ingress.as_deref() {
            ingress.fence().require_lease().map_err(ClientError::from)?;
        }
        Ok(guard)
    }
}

/// Check that this broker leads the shard, and name the owner if it does not.
fn owned_here(
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Result<Owned, ClientError> {
    let key = ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    };
    publish_ctx
        .engine
        .refuse_on_raft(&key, "consumer groups")
        .map_err(|err| ClientError::from_broker(&err, err.to_string()))?;
    match dispatch(publish_ctx.ingress.as_deref(), &publish_ctx.engine, &key) {
        Dispatch::Local { generation } => Ok(Owned { key, generation }),
        Dispatch::Forward { node_id, .. } => Err(ClientError::new(
            felix_wire::ErrorCode::NotLeader,
            format!("shard {shard} of {stream} is served by {node_id}"),
        )),
        Dispatch::Unavailable(reason) => Err(ClientError::unavailable(&reason, reason.to_string())),
    }
}

/// The pieces a group operation needs, or why it cannot run.
///
/// No log for an in-memory stream: it has none, and opening one here would
/// leave the stream served as durable by whoever found it (#3136). Its group
/// operations run as they would over an empty log.
async fn reader_and_log<'a>(
    broker: &'a Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Result<
    (
        &'a std::sync::Arc<felix_broker::GroupReader>,
        Option<felix_broker::StreamLog>,
        Owned,
    ),
    ClientError,
> {
    let owned = owned_here(publish_ctx, tenant_id, namespace, stream, shard)?;
    let reader = broker.group_reader().ok_or_else(|| {
        no_storage("this broker has no durable storage, so it serves no consumer groups")
    })?;
    let logs = broker
        .durable_storage()
        .ok_or_else(|| no_storage("this broker has no durable storage"))?;
    if crate::shards::lifecycle::in_memory_stream(broker, &owned.key).await {
        return Ok((reader, None, owned));
    }
    let log = logs
        .open_stream(tenant_id, namespace, stream, shard)
        .map_err(storage)?;
    Ok((reader, Some(log), owned))
}

/// Refuse a settle or extension aimed at an offset that holds a group entry
/// or another log entry rather than a record. No group hands one out, so it is
/// answered as an offset never handed out. Left to the tracker, it would pass
/// its high-water check, which also covers the entries a poll skipped, or the
/// predecessor tail a new leader inherits, and be answered done.
///
/// An offset the group holds a claim on (`claimed`) is a record, since only a
/// poll claims and it claims only records, so the log is read only for an
/// offset that is not one. That keeps the read off a settle's usual path.
async fn refuse_non_record(
    log: Option<&felix_broker::StreamLog>,
    offset: u64,
    claimed: bool,
) -> Result<(), ClientError> {
    let Some(log) = log.filter(|_| !claimed) else {
        return Ok(());
    };
    match log.holds_no_client_record(offset).await {
        Ok(false) => Ok(()),
        Ok(true) => {
            // `next` only picks the answer's class; the message below replaces its text.
            let err = felix_broker::BrokerError::GroupOffsetNotHandedOut {
                offset,
                next: offset,
            };
            Err(ClientError::from_broker(
                &err,
                format!(
                    "offset {offset} holds a log entry, not a record, so no group handed it out"
                ),
            ))
        }
        Err(err) => Err(ClientError::from_broker(&err, err.to_string())),
    }
}

/// The log's tail, zero for a stream with no log.
async fn tail_of(log: Option<&felix_broker::StreamLog>) -> Result<u64, ClientError> {
    match log {
        Some(log) => log.tail_offset().await.map_err(storage),
        None => Ok(0),
    }
}

fn group_key(tenant_id: &str, namespace: &str, stream: &str, shard: u32, group: &str) -> GroupKey {
    GroupKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        group: group.to_string(),
    }
}

fn storage(err: impl std::fmt::Display) -> ClientError {
    ClientError::new(felix_wire::ErrorCode::Storage, err.to_string())
}

// Configuration, not state: asking again gets the same answer.
fn no_storage(message: &str) -> ClientError {
    ClientError::internal(message).with_retry(felix_wire::RetryClass::Fatal)
}

#[cfg(feature = "raft")]
mod raft;
#[cfg(test)]
mod tests;

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
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_broker::{Broker, GroupKey};
use felix_wire::GroupRecord;
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::PublishContext;
use crate::shards::lifecycle::fence::{self, FenceGuard};
use crate::shards::routing::{Dispatch, dispatch};
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
) -> Result<Vec<GroupRecord>, ClientError> {
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
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
        first = false;
        // Read each round: the mark moves while a poll waits.
        let committed = crate::replication::quorum::read_bound(
            broker
                .stream_consistency(tenant_id, namespace, stream)
                .await,
            &owned.key,
            publish_ctx.marks.as_deref(),
            publish_ctx.ingress.as_deref(),
        );
        let claimed = reader
            .poll_below(
                &key,
                &log,
                committed.unwrap_or(u64::MAX),
                max_records,
                Instant::now(),
            )
            .await
            .map_err(storage);
        drop(fenced);
        let claimed = claimed?;
        if !claimed.is_empty() {
            return Ok(claimed
                .into_iter()
                .map(|claimed| GroupRecord {
                    offset: claimed.offset,
                    payload: claimed.payload,
                    attempts: claimed.attempts,
                })
                .collect());
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
    let (reader, _log, _owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    reader.dead_lettered(&key).await.map_err(storage)
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
    let (reader, _log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
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
    // Refused rather than silently accepted. An operator told a redrive
    // succeeded when the offset was never dead-lettered would wait for a
    // delivery that is not coming.
    Err(ClientError::invalid(format!(
        "offset {offset} is not a dead letter of {group}"
    )))
}

/// Finish a record, or hand it back.
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
    finish: bool,
) -> Result<(), ClientError> {
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let _fenced = owned.enter(publish_ctx, &mut admitted)?;
    let settled = if finish {
        reader.ack(&key, offset).await
    } else {
        reader.nack(&key, offset).await
    };
    let Err(err) = settled else {
        return Ok(());
    };
    // The tracker is in memory, so eviction or a failover forgets what it
    // handed out. An offset the log holds may have been claimed from the
    // tracker before; the record is owed again either way, so the consumer
    // is told its claim is gone rather than that it made a mistake. Past the
    // tail it cannot have been handed out by anyone.
    if let felix_broker::BrokerError::GroupOffsetNotHandedOut { .. } = err
        && offset < log.tail_offset().await.map_err(storage)?
    {
        return Err(ClientError::new(
            felix_wire::ErrorCode::StaleClaim,
            format!("{err}; the claim is stale and the record will be delivered again"),
        ));
    }
    Err(ClientError::from_broker(&err, err.to_string()))
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
        fence::enter_or_keep(
            admitted,
            publish_ctx.ingress.as_deref(),
            Some(&self.key),
            self.generation,
        )
        .map_err(ClientError::from)
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
    match dispatch(publish_ctx.ingress.as_deref(), &key) {
        Dispatch::Local { generation } => Ok(Owned { key, generation }),
        Dispatch::Forward { node_id, .. } => Err(ClientError::new(
            felix_wire::ErrorCode::NotLeader,
            format!("shard {shard} of {stream} is served by {node_id}"),
        )),
        Dispatch::Unavailable(reason) => Err(ClientError::unavailable(&reason, reason.to_string())),
    }
}

/// The pieces a group operation needs, or why it cannot run.
fn reader_and_log<'a>(
    broker: &'a Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
) -> Result<
    (
        &'a std::sync::Arc<felix_broker::GroupReader>,
        felix_broker::StreamLog,
        Owned,
    ),
    ClientError,
> {
    let owned = owned_here(publish_ctx, tenant_id, namespace, stream, shard)?;
    let reader = broker.group_reader().ok_or_else(|| {
        no_storage("this broker has no durable storage, so it serves no consumer groups")
    })?;
    let log = broker
        .durable_storage()
        .ok_or_else(|| no_storage("this broker has no durable storage"))?
        .open_stream(tenant_id, namespace, stream, shard)
        .map_err(storage)?;
    Ok((reader, log, owned))
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

#[cfg(test)]
mod tests;

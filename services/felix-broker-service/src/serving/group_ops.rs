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
use std::time::{Duration, Instant};

use felix_broker::{Broker, GroupKey};
use felix_wire::GroupRecord;

use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::PublishContext;
use crate::shards::lifecycle::fence::{self, FenceGuard};
use crate::shards::routing::{Dispatch, dispatch};
use crate::shards::{ShardKey, ShardKind};

/// How often a waiting poll re-checks for work.
///
/// The check is a read lock and a field read — no I/O, no allocation — so the
/// cost lands on the consumer that chose to wait and never on a publisher. An
/// append notification would wake it sooner, but only by adding work to the
/// publish path on behalf of a consumer that is by definition idle.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(20);

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
    mut admitted: Option<FenceGuard>,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    shard: u32,
    group: &str,
    max_records: usize,
    wait: Duration,
) -> Result<Vec<GroupRecord>, ClientError> {
    let (reader, log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let deadline = Instant::now() + wait;
    let mut first = true;

    loop {
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
        let claimed = reader
            .poll(&key, &log, max_records, Instant::now())
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
        tokio::time::sleep(WAIT_POLL_INTERVAL.min(deadline - now)).await;
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
    let (reader, _log, owned) =
        reader_and_log(broker, publish_ctx, tenant_id, namespace, stream, shard)?;
    let key = group_key(tenant_id, namespace, stream, shard, group);
    let _fenced = owned.enter(publish_ctx, &mut admitted)?;
    // Classified rather than all reported as storage: an offset this group
    // never handed out is the consumer's mistake, and retrying will not fix it.
    if finish {
        reader.ack(&key, offset).await
    } else {
        reader.nack(&key, offset).await
    }
    .map_err(|err| ClientError::from_broker(&err, err.to_string()))
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

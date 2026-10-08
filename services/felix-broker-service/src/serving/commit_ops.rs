//! Serving an atomic commit, a conditional publish, and reads of the state
//! commits write.
//!
//! Each is a write to one stream shard's log, so it takes the same gates a
//! publish does: the shard dispatched here, a place in its write fence, and on
//! a `Quorum` stream a majority before the answer. There is no forwarding: a
//! broker that does not lead the shard answers `not_leader`, and the client
//! goes to the one that does. See `docs/atomic-commit.md`.

use std::future::Future;

use bytes::Bytes;
use felix_broker::{Broker, BrokerError, PublishOutcome, StateOp, StateRead, StreamHandle};
use felix_wire::StateChange;

use crate::serving::quic::client_error::ClientError;
use crate::serving::quic::handlers::publish::PublishContext;
use crate::shards::lifecycle::fence::{self, FenceGuard};
use crate::shards::routing::{Dispatch, dispatch, dispatch_write};
use crate::shards::{ShardKey, ShardKind};

/// Why a write was not made.
#[derive(Debug)]
pub(crate) enum NotWritten {
    /// It expected the shard's next offset to be something else; `tail` is
    /// what it is. Nothing was written.
    OffsetMismatch {
        tail: u64,
    },
    Refused(ClientError),
}

impl From<ClientError> for NotWritten {
    fn from(err: ClientError) -> Self {
        Self::Refused(err)
    }
}

impl NotWritten {
    fn from_broker(err: &BrokerError, context: &str) -> Self {
        match err {
            BrokerError::OffsetMismatch { tail, .. } => Self::OffsetMismatch { tail: *tail },
            err => Self::Refused(ClientError::from_broker(err, context)),
        }
    }
}

/// Commit `event` and `changes` to the shard of `stream` that `entity_key`
/// routes to, only at `expected_offset` when there is one, and return the
/// commit's offset.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    entity_key: &[u8],
    event: Bytes,
    changes: Vec<StateChange>,
    expected_offset: Option<u64>,
    publisher: Option<&Bytes>,
) -> Result<u64, NotWritten> {
    // A replica that predates the commit record would refuse it and stop
    // replicating, so a cluster member waits for the whole fleet. A single
    // broker has no replicas to disagree.
    if publish_ctx.ingress.is_some()
        && !publish_ctx
            .marks
            .as_deref()
            .is_some_and(|marks| marks.fleet_supports(felix_common::fleet::ATOMIC_COMMIT))
    {
        return Err(ClientError::new(
            felix_wire::ErrorCode::InvalidRequest,
            "atomic commits are not enabled for this fleet (finalize `atomic_commit`)",
        )
        .into());
    }
    let key = shard_key(publish_ctx, tenant_id, namespace, stream, Some(entity_key));
    let ops = changes.into_iter().map(state_op).collect();
    leader_write(
        broker,
        publish_ctx,
        &key,
        "commit not served",
        |handle| async move {
            broker
                .commit_to_handle_at(&handle, event, ops, expected_offset, publisher)
                .await
        },
    )
    .await
}

/// Append `payloads` to the shard of `stream` that `key` routes to, only if
/// the batch would start at exactly `expected_offset`, and return its first
/// offset.
///
/// Not forwarded, for the reason a commit is not: the check is against the
/// leader's tail, and a refusal has to name that tail.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn publish_at(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    key: Option<&[u8]>,
    payloads: Vec<Bytes>,
    expected_offset: u64,
    publisher: Option<&Bytes>,
) -> Result<u64, NotWritten> {
    let shard = shard_key(publish_ctx, tenant_id, namespace, stream, key);
    leader_write(
        broker,
        publish_ctx,
        &shard,
        "publish not served",
        |handle| async move {
            broker
                .publish_batch_at(&handle, &payloads, expected_offset, publisher)
                .await
        },
    )
    .await
}

/// Make `write` on the shard `key` names, if this broker leads it, inside the
/// shard's write fence, and wait for the quorum its stream asks for. Returns
/// the write's first offset.
async fn leader_write<W, F>(
    broker: &Broker,
    publish_ctx: &PublishContext,
    key: &ShardKey,
    context: &str,
    write: W,
) -> Result<u64, NotWritten>
where
    W: FnOnce(StreamHandle) -> F,
    F: Future<Output = felix_broker::Result<PublishOutcome>>,
{
    let (dispatched, mut fenced) = dispatch_write(publish_ctx.ingress.as_deref(), key).await;
    let generation = owned(dispatched, key)?;
    let handle = broker
        .resolve_stream_handle(&key.tenant_id, &key.namespace, &key.stream, key.shard)
        .await
        .map_err(|err| ClientError::from_broker(&err, context))?;
    let guard: Option<FenceGuard> = fence::enter_or_keep(
        &mut fenced,
        publish_ctx.ingress.as_deref(),
        Some(key),
        generation,
    )
    .map_err(ClientError::from)?;
    let outcome = write(handle.clone())
        .await
        .map_err(|err| NotWritten::from_broker(&err, context))?;
    drop(guard);
    let Some((offset, _)) = outcome.offsets else {
        return Err(ClientError::internal("a write reported no offset").into());
    };
    felix_replication::quorum::await_quorum(
        &handle,
        publish_ctx.ingress.as_ref().map(|_| key),
        &outcome,
        publish_ctx.marks.as_deref(),
        publish_ctx.ingress.as_deref(),
        publish_ctx.quorum_timeout,
    )
    .await
    .map_err(|err| ClientError::from_anyhow(&err))?;
    Ok(offset)
}

/// Read `key` in the state of the shard `entity_key` routes to.
pub(crate) async fn state_get(
    broker: &Broker,
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    entity_key: &[u8],
    key: &str,
) -> Result<StateRead, ClientError> {
    let shard = shard_key(publish_ctx, tenant_id, namespace, stream, Some(entity_key));
    owned(dispatch(publish_ctx.ingress.as_deref(), &shard), &shard)?;
    // The view holds only what the ring holds, which on a `Quorum` stream is
    // what the committed mark covers, so the answer is never one a failover
    // takes back. Whether this broker still leads is confirmed after the
    // value is taken, as for a cache read; on the lease path a lapsed lease
    // refuses up front as well.
    let marks = publish_ctx.marks.as_deref();
    let ingress = publish_ctx.ingress.as_deref();
    if marks.is_none_or(|marks| marks.reads_by_round().is_none())
        && ingress.is_some_and(|ingress| !ingress.fence().lease_valid())
    {
        return Err(ClientError::from(fence::Fenced::LeaseLapsed));
    }
    let handle = broker
        .resolve_stream_handle(tenant_id, namespace, stream, shard.shard)
        .await
        .map_err(|err| ClientError::from_broker(&err, "state read not served"))?;
    let read = broker
        .state_get(&handle, key)
        .await
        .map_err(|err| ClientError::from_broker(&err, "state read not served"))?;
    felix_replication::quorum::confirm_state_read(&shard, marks, ingress)
        .await
        .map_err(|err| ClientError::from_anyhow(&err))?;
    Ok(read)
}

fn shard_key(
    publish_ctx: &PublishContext,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    key: Option<&[u8]>,
) -> ShardKey {
    let shard = crate::serving::quic::handlers::publish::resolve_shard(
        publish_ctx,
        tenant_id,
        namespace,
        stream,
        key,
    );
    ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

/// The generation this broker leads `key` at, or who to ask instead.
fn owned(dispatched: Dispatch, key: &ShardKey) -> Result<u64, ClientError> {
    match dispatched {
        Dispatch::Local { generation } => Ok(generation),
        Dispatch::Forward { node_id, .. } => Err(ClientError::new(
            felix_wire::ErrorCode::NotLeader,
            format!(
                "shard {} of {} is served by {node_id}",
                key.shard, key.stream
            ),
        )),
        Dispatch::Unavailable(reason) => Err(ClientError::unavailable(&reason, reason.to_string())),
    }
}

fn state_op(change: StateChange) -> StateOp {
    match change {
        StateChange::Put { key, value } => StateOp::Put { key, value },
        StateChange::Delete { key } => StateOp::Delete { key },
    }
}

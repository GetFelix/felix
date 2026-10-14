//! Naming the stream incarnation a replication request is about.
//!
//! A leader names the incarnation its log belongs to on every request it
//! sends for an incarnated stream shard, and a follower holding another one,
//! or none yet, refuses it as a stale route. Without that, a follower whose
//! catalog still has the deleted stream would store the new one's records in
//! the deleted one's logs, and the delete would take them, after the leader
//! had counted them.
//!
//! Incarnation 0 is not named, so a frame for a stream from before
//! incarnations is what it always was. A stream has another incarnation only
//! once the fleet finalized them, when every peer reads the frame; no fleet
//! gate is read, so the rule holds from the first moment after a restart.

use felix_broker::Broker;
use felix_wire::internal::ShardRef;

/// The reference to `key` at `generation`, naming this broker's incarnation of
/// a stream shard when it is not 0.
pub(crate) fn shard_ref(
    broker: &Broker,
    key: &felix_router::ShardKey,
    generation: u64,
) -> ShardRef {
    let incarnation = (key.kind == felix_router::ShardKind::Stream)
        .then(|| broker.stream_incarnation(&key.tenant_id, &key.namespace, &key.stream))
        .flatten()
        .filter(|incarnation| *incarnation != 0);
    ShardRef {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        generation,
        incarnation,
    }
}

/// The incarnation this broker's own logs for `shard` may open at while it
/// acts under that reference, so the log it touches is the one the reference
/// names to its peers; `None` opens whatever is registered. Incarnation 0 is
/// named too once registration is required, as a follower reads a reference
/// without one.
pub(crate) fn expected(
    broker: &Broker,
    kind: felix_router::ShardKind,
    shard: &ShardRef,
) -> Option<u64> {
    if kind != felix_router::ShardKind::Stream {
        return None;
    }
    shard
        .incarnation
        .or_else(|| broker.registration_required().then_some(0))
}

/// Why a follower must not take a request for stream shard `key` that named
/// `sent` (none is incarnation 0), or `None` when it may. Only once this broker
/// requires registration: after it has seen a stream with an incarnation,
/// which a request naming one shows.
pub(crate) fn mismatch(
    broker: &Broker,
    key: &felix_router::ShardKey,
    sent: Option<u64>,
) -> Option<String> {
    if key.kind != felix_router::ShardKind::Stream {
        return None;
    }
    // A leader naming one is proof incarnations are in play.
    if sent.is_some_and(|sent| sent != 0) {
        broker.require_registered_streams();
    }
    if !broker.registration_required() {
        return None;
    }
    let sent = sent.unwrap_or(0);
    match broker.stream_incarnation(&key.tenant_id, &key.namespace, &key.stream) {
        None => {
            // The catalog is behind; ask for it now rather than at its tick.
            broker.request_catalog_refresh();
            Some("this broker has not registered the stream yet".to_string())
        }
        Some(held) if held != sent => Some(format!(
            "this broker holds incarnation {held} of the stream, the sender {sent}"
        )),
        Some(_) => None,
    }
}

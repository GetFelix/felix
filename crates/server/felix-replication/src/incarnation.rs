//! Naming the stream or cache incarnation a replication request is about.
//!
//! A leader names the incarnation its log belongs to on every request it
//! sends for an incarnated shard, and a follower holding another one, or none
//! yet, refuses it as a stale route. Without that, a follower whose catalog
//! still has the deleted stream would store the new one's records in the
//! deleted one's logs, and the delete would take them, after the leader had
//! counted them.
//!
//! A cache shard is checked the same way, for both its entry log and its
//! counter log, against the cache's incarnation and the broker's cache
//! registration flag, which is separate from the streams' one.
//!
//! Incarnation 0 is not named, so a frame for a stream or cache from before
//! incarnations is what it always was. One has another incarnation only once
//! the fleet finalized them, when every peer reads the frame; no fleet gate is
//! read, so the rule holds from the first moment after a restart.

use felix_broker::Broker;
use felix_router::{ShardKey, ShardKind};
use felix_wire::internal::ShardRef;

/// The incarnation a shard's logs open at on this broker now: the registered
/// one, or 0 for a stream or cache from before incarnations or one not
/// registered. Replication state (cursors, quorum marks) describes one log,
/// so it is kept per incarnation as well as per generation.
pub(crate) fn current(
    broker: &Broker,
    kind: ShardKind,
    tenant_id: &str,
    namespace: &str,
    name: &str,
) -> u64 {
    held(broker, kind, tenant_id, namespace, name).unwrap_or(0)
}

/// [`current`], for a routing key.
pub(crate) fn of(broker: &Broker, key: &ShardKey) -> u64 {
    current(
        broker,
        key.kind,
        &key.tenant_id,
        &key.namespace,
        &key.stream,
    )
}

/// The reference to `key` at `generation`, naming this broker's incarnation of
/// the shard's stream or cache when it is not 0.
pub(crate) fn shard_ref(broker: &Broker, key: &ShardKey, generation: u64) -> ShardRef {
    let incarnation = held(
        broker,
        key.kind,
        &key.tenant_id,
        &key.namespace,
        &key.stream,
    )
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
pub(crate) fn expected(broker: &Broker, kind: ShardKind, shard: &ShardRef) -> Option<u64> {
    shard
        .incarnation
        .or_else(|| registration_required(broker, kind).then_some(0))
}

/// Whether logs of `kind` open only for a registered stream or cache on this
/// broker. Streams and caches are flagged apart: each turns on when this
/// broker first sees one of its own kind with an incarnation.
pub(crate) fn registration_required(broker: &Broker, kind: ShardKind) -> bool {
    match kind {
        ShardKind::Stream => broker.registration_required(),
        ShardKind::Cache => broker.cache_registration_required(),
    }
}

/// Why a follower must not take a request for shard `key` that named `sent`
/// (none is incarnation 0), or `None` when it may. Only once this broker
/// requires registration for the shard's kind: after it has seen one with an
/// incarnation, which a request naming one shows.
pub(crate) fn mismatch(broker: &Broker, key: &ShardKey, sent: Option<u64>) -> Option<String> {
    // A leader naming one is proof incarnations are in play.
    if sent.is_some_and(|sent| sent != 0) {
        match key.kind {
            ShardKind::Stream => broker.require_registered_streams(),
            ShardKind::Cache => broker.require_registered_caches(),
        }
    }
    if !registration_required(broker, key.kind) {
        return None;
    }
    let what = match key.kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    };
    let sent = sent.unwrap_or(0);
    match held(
        broker,
        key.kind,
        &key.tenant_id,
        &key.namespace,
        &key.stream,
    ) {
        None => {
            // The catalog is behind; ask for it now rather than at its tick.
            broker.request_catalog_refresh();
            Some(format!("this broker has not registered the {what} yet"))
        }
        Some(held) if held != sent => Some(format!(
            "this broker holds incarnation {held} of the {what}, the sender {sent}"
        )),
        Some(_) => None,
    }
}

fn held(
    broker: &Broker,
    kind: ShardKind,
    tenant_id: &str,
    namespace: &str,
    name: &str,
) -> Option<u64> {
    match kind {
        ShardKind::Stream => broker.stream_incarnation(tenant_id, namespace, name),
        ShardKind::Cache => broker.cache_incarnation(tenant_id, namespace, name),
    }
}

#[cfg(test)]
mod tests;

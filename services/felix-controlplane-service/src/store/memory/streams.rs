//! Streams and their change log.
use super::{Decide, InMemoryStore, ShardCleanup};
use crate::model::{
    NamespaceKey, ShardKind, Stream, StreamChange, StreamChangeOp, StreamKey, StreamPatchRequest,
};
use crate::store::{ChangeSet, ControlPlaneStore, Snapshot, StoreError, StoreResult};

pub(super) async fn list_streams(
    store: &InMemoryStore,
    tenant_id: &str,
    namespace: &str,
) -> StoreResult<Vec<Stream>> {
    let items = store
        .streams
        .read()
        .await
        .values()
        .filter(|stream| stream.tenant_id == tenant_id && stream.namespace == namespace)
        .cloned()
        .collect();
    Ok(items)
}

pub(super) async fn get_stream(store: &InMemoryStore, key: &StreamKey) -> StoreResult<Stream> {
    store
        .streams
        .read()
        .await
        .get(key)
        .cloned()
        .ok_or_else(|| StoreError::NotFound("stream".into()))
}

pub(super) async fn create_stream(
    store: &InMemoryStore,
    stream: Stream,
    decide: Decide,
) -> StoreResult<Stream> {
    // Streams are scoped to a namespace; we reject creation if the parent namespace doesn't exist.
    if !store
        .namespace_exists(&NamespaceKey {
            tenant_id: stream.tenant_id.clone(),
            namespace: stream.namespace.clone(),
        })
        .await?
    {
        return Err(StoreError::NotFound("namespace".into()));
    }
    let key = StreamKey {
        tenant_id: stream.tenant_id.clone(),
        namespace: stream.namespace.clone(),
        stream: stream.stream.clone(),
    };
    let mut streams = store.streams.write().await;
    if streams.contains_key(&key) {
        return Err(StoreError::Conflict("stream exists".into()));
    }
    // Held until the stream is in, so a finalize cannot land between the
    // read and the write: nothing is created at 0 after it.
    let nodes = store.nodes.read().await;
    refuse_reused_name(store, &nodes, &key, decide).await?;
    let mut stream = stream;
    stream.incarnation = incarnation_for(store, &nodes, &key, stream.incarnation, decide).await;
    streams.insert(key.clone(), stream.clone());
    store
        .stream_changes
        .write()
        .await
        .record(|seq| StreamChange {
            seq,
            op: StreamChangeOp::Created,
            key,
            stream: Some(stream.clone()),
            deleted_through: None,
        });
    metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
    metrics::gauge!("felix_streams_total").set(streams.len() as f64);
    Ok(stream)
}

pub(super) async fn patch_stream(
    store: &InMemoryStore,
    key: &StreamKey,
    patch: StreamPatchRequest,
    fence: bool,
) -> StoreResult<Stream> {
    let mut streams = store.streams.write().await;
    let stream = streams
        .get_mut(key)
        .ok_or_else(|| StoreError::NotFound("stream".into()))?;
    let (consistency, durable) = (stream.consistency.clone(), stream.durable);
    if let Some(retention) = patch.retention {
        stream.retention = retention;
    }
    if let Some(consistency) = patch.consistency {
        stream.consistency = consistency;
    }
    if let Some(delivery) = patch.delivery {
        stream.delivery = delivery;
    }
    if let Some(durable) = patch.durable {
        stream.durable = durable;
    }
    // Placement decides how a shard fails over from these, so a pass that
    // read them before the change must not write after it (#1253).
    if fence && (stream.consistency != consistency || stream.durable != durable) {
        store.shards.write().await.placement.token += 1;
    }
    // After applying the patch, we emit an `Updated` change so watchers can reconcile.
    let updated = stream.clone();
    store
        .stream_changes
        .write()
        .await
        .record(|seq| StreamChange {
            seq,
            op: StreamChangeOp::Updated,
            key: key.clone(),
            stream: Some(updated.clone()),
            deleted_through: None,
        });
    metrics::counter!("felix_stream_changes_total", "op" => "updated").increment(1);
    Ok(updated)
}

pub(super) async fn delete_stream(
    store: &InMemoryStore,
    key: &StreamKey,
    cleanup: ShardCleanup,
    decide: Decide,
) -> StoreResult<()> {
    let mut streams = store.streams.write().await;
    let Some(removed) = streams.remove(key) else {
        return Err(StoreError::NotFound("stream".into()));
    };
    let nodes = store.nodes.read().await;
    let deleted_through = record_tombstone(store, &nodes, key, removed.incarnation, decide).await;
    store
        .drop_shard_assignments_for(
            ShardKind::Stream,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            cleanup,
        )
        .await;
    store
        .stream_changes
        .write()
        .await
        .record(|seq| StreamChange {
            seq,
            op: StreamChangeOp::Deleted,
            key: key.clone(),
            stream: crate::model::deleted_stream_payload(removed.clone()),
            deleted_through,
        });
    metrics::counter!("felix_stream_changes_total", "op" => "deleted").increment(1);
    metrics::gauge!("felix_streams_total").set(streams.len() as f64);
    Ok(())
}

pub(super) async fn stream_snapshot(store: &InMemoryStore) -> StoreResult<Snapshot<Stream>> {
    // `next_seq` is the checkpoint a consumer should use as `since` on its first changes poll.
    // The rows stay locked while `next_seq` is read: every writer records its
    // change while holding them, so a change counted in `next_seq` is in `items`.
    let rows = store.streams.read().await;
    let items = rows.values().cloned().collect();
    let next_seq = store.stream_changes.read().await.next_seq;
    drop(rows);
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn stream_changes(
    store: &InMemoryStore,
    since: u64,
) -> StoreResult<ChangeSet<StreamChange>> {
    // A `since` older than the retained window gets a partial history; the
    // caller has to re-bootstrap from the snapshot.
    Ok(store
        .stream_changes
        .read()
        .await
        .page(since, store.limit(), |change| change.seq))
}

pub(super) async fn stream_tombstones(store: &InMemoryStore) -> Vec<crate::model::StreamTombstone> {
    let mut tombstones: Vec<_> = store
        .stream_tombstones
        .read()
        .await
        .iter()
        .map(|(key, incarnation)| crate::model::StreamTombstone {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            incarnation: *incarnation,
        })
        .collect();
    tombstones.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.stream).cmp(&(&b.tenant_id, &b.namespace, &b.stream))
    });
    tombstones
}

/// Whether the fleet finalized `stream_incarnations`, from the node state the
/// caller holds: held across the write it decides, a finalize cannot land in
/// between.
pub(super) fn incarnations_finalized(nodes: &super::NodeState) -> bool {
    nodes
        .fleet_enabled
        .contains(felix_common::fleet::STREAM_INCARNATIONS.name())
}

/// The incarnation a stream created now under `key` gets, from the
/// `proposed` one its request carried. 0 until the finalize; after it, never
/// 0 and above every earlier incarnation of the name. A level-14 entry gets
/// one only if it proposed one, as every member decides from the entry.
pub(super) async fn incarnation_for(
    store: &InMemoryStore,
    nodes: &super::NodeState,
    key: &StreamKey,
    proposed: u64,
    decide: Decide,
) -> u64 {
    // An entry's 0 proposal stays 0: a member before incarnations applies
    // every create that way, even after a finalize it accepted unknowingly.
    if !incarnations_finalized(nodes) || (decide != Decide::Here && proposed == 0) {
        return 0;
    }
    let deleted = store.stream_tombstones.read().await.get(key).copied();
    crate::model::next_incarnation(proposed, deleted)
}

/// Record the delete of `incarnation` of `key` once incarnations are
/// finalized, returning what the `Deleted` change carries. Nothing before the
/// finalize: a stream created again at 0 then still reopens the deleted
/// one's logs, so its data must stay.
pub(super) async fn record_tombstone(
    store: &InMemoryStore,
    nodes: &super::NodeState,
    key: &StreamKey,
    incarnation: u64,
    decide: Decide,
) -> Option<u64> {
    // Only while a create could reuse it: after the finalize the set would
    // only grow. Read from the node state the caller holds, so every Raft
    // member decides alike.
    if decide.guards_names() && !incarnations_finalized(nodes) {
        store.deleted_stream_names.write().await.insert(key.clone());
    }
    // An older entry tombstones only an incarnated stream, which a member
    // before incarnations never holds.
    if !incarnations_finalized(nodes) || (decide == Decide::Legacy && incarnation == 0) {
        return None;
    }
    let mut tombstones = store.stream_tombstones.write().await;
    let through = tombstones.entry(key.clone()).or_default();
    *through = (*through).max(incarnation);
    Some(*through)
}

/// Refuse to create `key` again before the fleet finalized
/// `stream_incarnations` if a stream of that name was deleted. Without an
/// incarnation the new stream would open the deleted one's logs on every
/// broker, each holding what it had of them, and replicas could diverge
/// (#1447).
pub(super) async fn refuse_reused_name(
    store: &InMemoryStore,
    nodes: &super::NodeState,
    key: &StreamKey,
    decide: Decide,
) -> StoreResult<()> {
    if decide.guards_names()
        && !incarnations_finalized(nodes)
        && store.deleted_stream_names.read().await.contains(key)
    {
        return Err(crate::store::reused_name(
            "stream",
            &key.stream,
            felix_common::fleet::STREAM_INCARNATIONS,
        ));
    }
    Ok(())
}

/// Every deleted stream name, ordered.
pub(super) async fn deleted_stream_names(store: &InMemoryStore) -> Vec<StreamKey> {
    let mut names: Vec<StreamKey> = store
        .deleted_stream_names
        .read()
        .await
        .iter()
        .cloned()
        .collect();
    names.sort_by(|a, b| {
        (&a.tenant_id, &a.namespace, &a.stream).cmp(&(&b.tenant_id, &b.namespace, &b.stream))
    });
    names
}

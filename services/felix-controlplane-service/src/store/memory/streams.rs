//! Streams and their change log.
use super::InMemoryStore;
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

pub(super) async fn create_stream(store: &InMemoryStore, stream: Stream) -> StoreResult<Stream> {
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
        });
    metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
    metrics::gauge!("felix_streams_total").set(streams.len() as f64);
    Ok(stream)
}

pub(super) async fn patch_stream(
    store: &InMemoryStore,
    key: &StreamKey,
    patch: StreamPatchRequest,
) -> StoreResult<Stream> {
    let mut streams = store.streams.write().await;
    let stream = streams
        .get_mut(key)
        .ok_or_else(|| StoreError::NotFound("stream".into()))?;
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
        });
    metrics::counter!("felix_stream_changes_total", "op" => "updated").increment(1);
    Ok(updated)
}

pub(super) async fn delete_stream(store: &InMemoryStore, key: &StreamKey) -> StoreResult<()> {
    let mut streams = store.streams.write().await;
    let removed = streams.remove(key);
    if removed.is_none() {
        return Err(StoreError::NotFound("stream".into()));
    }
    store
        .drop_shard_assignments_for(
            ShardKind::Stream,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
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
            stream: None,
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

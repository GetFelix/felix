//! Streams and caches created together, all or none.
use std::collections::HashSet;

use super::InMemoryStore;
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, NamespaceKey, Stream, StreamChange,
    StreamChangeOp, StreamKey,
};
use crate::store::{StoreError, StoreResult};

pub(super) async fn create_resources(
    store: &InMemoryStore,
    streams: Vec<Stream>,
    caches: Vec<Cache>,
) -> StoreResult<()> {
    // Same lock order as the namespace and tenant cascades. Holding all three
    // while checking means nothing can land between the check and the insert.
    let namespaces = store.namespaces.read().await;
    let mut stream_map = store.streams.write().await;
    let mut cache_map = store.caches.write().await;

    let parents = streams
        .iter()
        .map(|s| (&s.tenant_id, &s.namespace))
        .chain(caches.iter().map(|c| (&c.tenant_id, &c.namespace)));
    for (tenant_id, namespace) in parents {
        let key = NamespaceKey {
            tenant_id: tenant_id.clone(),
            namespace: namespace.clone(),
        };
        if !namespaces.contains_key(&key) {
            return Err(StoreError::NotFound("namespace".into()));
        }
    }
    let mut seen = HashSet::new();
    for stream in &streams {
        let key = stream_key(stream);
        if stream_map.contains_key(&key) || !seen.insert(key) {
            return Err(StoreError::Conflict(format!(
                "stream {} exists",
                stream.stream
            )));
        }
    }
    let mut seen = HashSet::new();
    for cache in &caches {
        let key = cache_key(cache);
        if cache_map.contains_key(&key) || !seen.insert(key) {
            return Err(StoreError::Conflict(format!(
                "cache {} exists",
                cache.cache
            )));
        }
    }

    if !streams.is_empty() {
        let mut changes = store.stream_changes.write().await;
        for stream in streams {
            let key = stream_key(&stream);
            stream_map.insert(key.clone(), stream.clone());
            changes.record(|seq| StreamChange {
                seq,
                op: StreamChangeOp::Created,
                key,
                stream: Some(stream),
            });
            metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
        }
        metrics::gauge!("felix_streams_total").set(stream_map.len() as f64);
    }
    if !caches.is_empty() {
        let mut changes = store.cache_changes.write().await;
        for cache in caches {
            let key = cache_key(&cache);
            cache_map.insert(key.clone(), cache.clone());
            changes.record(|seq| CacheChange {
                seq,
                op: CacheChangeOp::Created,
                key,
                cache: Some(cache),
            });
            metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
        }
        metrics::gauge!("felix_caches_total").set(cache_map.len() as f64);
    }
    Ok(())
}

fn stream_key(stream: &Stream) -> StreamKey {
    StreamKey {
        tenant_id: stream.tenant_id.clone(),
        namespace: stream.namespace.clone(),
        stream: stream.stream.clone(),
    }
}

fn cache_key(cache: &Cache) -> CacheKey {
    CacheKey {
        tenant_id: cache.tenant_id.clone(),
        namespace: cache.namespace.clone(),
        cache: cache.cache.clone(),
    }
}

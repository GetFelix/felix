//! Streams and caches created together, all or none.
use std::collections::HashSet;

use super::{Decide, InMemoryStore};
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, NamespaceKey, Stream, StreamChange,
    StreamChangeOp, StreamKey,
};
use crate::store::{StoreError, StoreResult};

pub(super) async fn create_resources(
    store: &InMemoryStore,
    streams: Vec<Stream>,
    caches: Vec<Cache>,
    decide: Decide,
) -> StoreResult<(Vec<Stream>, Vec<Cache>)> {
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

    {
        // Every name before anything goes in, so a refusal leaves nothing.
        let nodes = store.nodes.read().await;
        for stream in &streams {
            super::streams::refuse_reused_name(store, &nodes, &stream_key(stream), decide).await?;
        }
        for cache in &caches {
            super::caches::refuse_reused_name(store, &nodes, &cache_key(cache), decide).await?;
        }
    }

    let mut stored = Vec::with_capacity(streams.len());
    let mut stored_caches = Vec::with_capacity(caches.len());
    if !streams.is_empty() {
        // Held until every stream is in, as for a single create.
        let nodes = store.nodes.read().await;
        let mut changes = store.stream_changes.write().await;
        for mut stream in streams {
            let key = stream_key(&stream);
            stream.incarnation =
                super::streams::incarnation_for(store, &nodes, &key, stream.incarnation, decide)
                    .await;
            stream_map.insert(key.clone(), stream.clone());
            stored.push(stream.clone());
            changes.record(|seq| StreamChange {
                seq,
                op: StreamChangeOp::Created,
                key,
                stream: Some(stream),
                deleted_through: None,
            });
            metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
        }
        metrics::gauge!("felix_streams_total").set(stream_map.len() as f64);
    }
    if !caches.is_empty() {
        let nodes = store.nodes.read().await;
        let mut changes = store.cache_changes.write().await;
        for mut cache in caches {
            let key = cache_key(&cache);
            cache.incarnation =
                super::caches::incarnation_for(store, &nodes, &key, cache.incarnation, decide)
                    .await;
            cache_map.insert(key.clone(), cache.clone());
            stored_caches.push(cache.clone());
            changes.record(|seq| CacheChange {
                seq,
                op: CacheChangeOp::Created,
                key,
                cache: Some(cache),
                deleted_through: None,
            });
            metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
        }
        metrics::gauge!("felix_caches_total").set(cache_map.len() as f64);
    }
    Ok((stored, stored_caches))
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

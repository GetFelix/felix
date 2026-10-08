//! Streams and caches created together, in one transaction.
use super::caches::insert_cache;
use super::streams::insert_stream;
use super::{PostgresStore, ensure_namespace};
use crate::model::{Cache, Stream};
use crate::store::StoreResult;

/// A unique violation on any row, including one a concurrent create commits
/// first, fails the whole transaction, so nothing from the batch is left.
pub(super) async fn create_resources(
    store: &PostgresStore,
    streams: Vec<Stream>,
    caches: Vec<Cache>,
) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;
    let mut parents: Vec<(&str, &str)> = streams
        .iter()
        .map(|s| (s.tenant_id.as_str(), s.namespace.as_str()))
        .chain(
            caches
                .iter()
                .map(|c| (c.tenant_id.as_str(), c.namespace.as_str())),
        )
        .collect();
    parents.sort_unstable();
    parents.dedup();
    for (tenant_id, namespace) in parents {
        ensure_namespace(&mut tx, tenant_id, namespace).await?;
    }
    for stream in &streams {
        insert_stream(&mut tx, stream).await?;
    }
    for cache in &caches {
        insert_cache(&mut tx, cache).await?;
    }
    tx.commit().await?;
    if !streams.is_empty() {
        metrics::counter!("felix_stream_changes_total", "op" => "created")
            .increment(streams.len() as u64);
    }
    if !caches.is_empty() {
        metrics::counter!("felix_cache_changes_total", "op" => "created")
            .increment(caches.len() as u64);
    }
    store.refresh_counts().await?;
    Ok(())
}

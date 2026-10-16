//! Caches and their change log.
use serde_json::Value;
use sqlx::FromRow;

use super::codec::{DbCache, cache_from_db, consistency_to_str};
use super::{
    CatalogFeed, PostgresStore, begin_consistent_read, begin_feed_write, feed_head,
    is_unique_violation, page_fetch,
};
use crate::model::{Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest};
use crate::store::{ChangeSet, Page, PageRequest, Snapshot, StoreError, StoreResult};

/// Row shape for the `cache_changes` table.
#[derive(Debug, Clone, FromRow)]
struct CacheChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    cache: String,
    payload: Option<Value>,
    deleted_through: Option<i64>,
}

pub(super) async fn list_caches(
    store: &PostgresStore,
    tenant_id: &str,
    namespace: &str,
) -> StoreResult<Vec<Cache>> {
    let rows = sqlx::query_as::<_, DbCache>(
        r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation FROM caches WHERE tenant_id = $1 AND namespace = $2 ORDER BY cache"#,
    )
    .bind(tenant_id)
    .bind(namespace)
    .fetch_all(&store.pool)
    .await
    ?;
    rows.into_iter().map(cache_from_db).collect()
}

pub(super) async fn list_caches_page(
    store: &PostgresStore,
    tenant_id: &str,
    namespace: &str,
    page: PageRequest<String>,
) -> StoreResult<Page<Cache>> {
    let fetch = page_fetch(page.limit);
    let rows = match &page.after {
        None => sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation FROM caches
               WHERE tenant_id = $1 AND namespace = $2 ORDER BY cache LIMIT $3"#,
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(fetch),
        Some(after) => sqlx::query_as::<_, DbCache>(
            r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation FROM caches
               WHERE tenant_id = $1 AND namespace = $2 AND cache > $3 ORDER BY cache LIMIT $4"#,
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(after)
        .bind(fetch),
    }
    .fetch_all(&store.pool)
    .await?;
    let items = rows
        .into_iter()
        .map(cache_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(Page::from_overfetch(items, page.limit))
}

pub(super) async fn get_cache(store: &PostgresStore, key: &CacheKey) -> StoreResult<Cache> {
    let row = sqlx::query_as::<_, DbCache>(
        r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.cache)
    .fetch_optional(&store.pool)
    .await
    ?;
    match row {
        Some(row) => cache_from_db(row),
        None => Err(StoreError::NotFound("cache".into())),
    }
}

pub(super) async fn create_cache(store: &PostgresStore, mut cache: Cache) -> StoreResult<Cache> {
    let (_gates, mut tx) = begin_feed_write(store, &[CatalogFeed::Caches]).await?;
    super::ensure_namespace(&mut tx, &cache.tenant_id, &cache.namespace).await?;
    insert_cache(&mut tx, &mut cache).await?;
    tx.commit().await?;
    metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
    store.refresh_counts().await?;
    Ok(cache)
}

/// The row and its `Created` change, inside the caller's transaction, which
/// must hold the feed lock (`begin_feed_write`).
/// `Conflict` when the cache exists.
///
/// The cache's incarnation is decided by the row's insert trigger (migration
/// 0032), against the finalize and the name's tombstone as they stand inside
/// this transaction, and read back into `cache`. This side only proposes one.
pub(super) async fn insert_cache(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    cache: &mut Cache,
) -> StoreResult<()> {
    if cache.incarnation == 0 {
        cache.incarnation = crate::model::proposed_incarnation();
    }
    let insert = sqlx::query_scalar::<_, i64>(
        r#"INSERT INTO caches (tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               RETURNING incarnation"#,
    )
    .bind(&cache.tenant_id)
    .bind(&cache.namespace)
    .bind(&cache.cache)
    .bind(&cache.display_name)
    .bind(cache.shards as i32)
    .bind(cache.replication_factor as i32)
    .bind(consistency_to_str(&cache.consistency))
    .bind(cache.incarnation as i64)
    .fetch_one(&mut **tx)
    .await;
    match insert {
        Ok(incarnation) => cache.incarnation = incarnation as u64,
        Err(err) if is_unique_violation(&err) => {
            return Err(StoreError::Conflict(format!(
                "cache {} exists",
                cache.cache
            )));
        }
        Err(err) => return Err(StoreError::Unexpected(err.into())),
    }

    sqlx::query(
        r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind("Created")
    .bind(&cache.tenant_id)
    .bind(&cache.namespace)
    .bind(&cache.cache)
    .bind(serde_json::to_value(&*cache).ok())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn patch_cache(
    store: &PostgresStore,
    key: &CacheKey,
    patch: CachePatchRequest,
) -> StoreResult<Cache> {
    let (_gates, mut tx) = begin_feed_write(store, &[CatalogFeed::Caches]).await?;
    let current = sqlx::query_as::<_, DbCache>(
        r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3 FOR UPDATE"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.cache)
    .fetch_optional(&mut *tx)
    .await
    ?;
    let mut cache = match current {
        Some(row) => cache_from_db(row)?,
        None => return Err(StoreError::NotFound("cache".into())),
    };

    if let Some(display_name) = patch.display_name {
        cache.display_name = display_name;
    }

    sqlx::query(
        r#"UPDATE caches SET display_name = $1, updated_at = now() WHERE tenant_id = $2 AND namespace = $3 AND cache = $4"#,
    )
    .bind(&cache.display_name)
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.cache)
    .execute(&mut *tx)
    .await
    ?;

    sqlx::query(
        r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind("Updated")
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.cache)
    .bind(serde_json::to_value(&cache).ok())
    .execute(&mut *tx)
    .await
    ?;

    tx.commit().await?;
    metrics::counter!("felix_cache_changes_total", "op" => "updated").increment(1);
    Ok(cache)
}

pub(super) async fn delete_cache(store: &PostgresStore, key: &CacheKey) -> StoreResult<()> {
    let (_gates, mut tx) = begin_feed_write(store, &[CatalogFeed::Caches]).await?;
    let removed = sqlx::query_as::<_, DbCache>(
        r#"DELETE FROM caches WHERE tenant_id = $1 AND namespace = $2 AND cache = $3
               RETURNING tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.cache)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(removed) = removed else {
        return Err(StoreError::NotFound("cache".into()));
    };
    let payload = crate::model::deleted_cache_payload(cache_from_db(removed)?)
        .and_then(|cache| serde_json::to_value(cache).ok());

    sqlx::query(
        r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind("Deleted")
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.cache)
    .bind(payload)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    metrics::counter!("felix_cache_changes_total", "op" => "deleted").increment(1);
    store.refresh_counts().await?;
    Ok(())
}

pub(super) async fn cache_snapshot(store: &PostgresStore) -> StoreResult<Snapshot<Cache>> {
    // One REPEATABLE READ snapshot for the rows and `next_seq`, or a change
    // committed between the two reads is counted but missing.
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, DbCache>(
        r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency, incarnation FROM caches ORDER BY tenant_id, namespace, cache"#,
    )
    .fetch_all(&mut *tx)
    .await
    ?;
    let items = rows
        .into_iter()
        .map(cache_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    let next_seq = feed_head(&mut tx, CatalogFeed::Caches).await?;
    tx.commit().await?;
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn cache_changes(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<CacheChange>> {
    // The page and `head` from one snapshot, so a short page means the feed
    // really had nothing more.
    let mut tx = store.pool.begin().await?;
    begin_consistent_read(&mut tx).await?;
    let rows = sqlx::query_as::<_, CacheChangeRow>(
        r#"SELECT seq, op, tenant_id, namespace, cache, payload, deleted_through FROM cache_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&mut *tx)
    .await
    ?;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let op = match row.op.as_str() {
            "Created" => CacheChangeOp::Created,
            "Updated" => CacheChangeOp::Updated,
            _ => CacheChangeOp::Deleted,
        };
        let cache = row
            .payload
            .and_then(|v| serde_json::from_value::<Cache>(v).ok());
        items.push(CacheChange {
            seq: row.seq as u64,
            op,
            key: CacheKey {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                cache: row.cache,
            },
            cache,
            deleted_through: row.deleted_through.map(|through| through as u64),
        });
    }
    let head = feed_head(&mut tx, CatalogFeed::Caches).await?;
    tx.commit().await?;
    Ok(ChangeSet::page(
        items,
        store.changes_limit(),
        head,
        |change| change.seq,
    ))
}

pub(super) async fn cache_tombstones(
    store: &PostgresStore,
) -> StoreResult<Vec<crate::model::CacheTombstone>> {
    let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
        r#"SELECT tenant_id, namespace, cache, incarnation FROM cache_tombstones
               ORDER BY tenant_id, namespace, cache"#,
    )
    .fetch_all(&store.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(tenant_id, namespace, cache, incarnation)| crate::model::CacheTombstone {
                tenant_id,
                namespace,
                cache,
                incarnation: incarnation as u64,
            },
        )
        .collect())
}

//! Tenants, and the cascade that deleting one sets off.
use serde_json::Value;
use sqlx::FromRow;

use super::codec::{
    DbCache, DbNamespace, DbStream, parse_consistency, parse_delivery, parse_stream_kind,
};
use super::{PostgresStore, is_unique_violation, page_fetch};
use crate::model::{
    Cache, CacheKey, Namespace, NamespaceKey, RetentionPolicy, Stream, StreamKey, Tenant,
    TenantChange, TenantChangeOp,
};
use crate::store::{
    ChangeSet, ControlPlaneStore, Page, PageRequest, Snapshot, StoreError, StoreResult,
};

/// Row shape for `tenants` table (minimal mapping needed by the API).
#[derive(Debug, Clone, FromRow)]
struct DbTenant {
    tenant_id: String,
    display_name: String,
}

/// Row shape for the `tenant_changes` table.
///
/// `seq` is a monotonic, append-only sequence number used for incremental sync.
/// `payload` is optional (e.g., deletions often store `NULL` payloads).
#[derive(Debug, Clone, FromRow)]
struct TenantChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    payload: Option<Value>,
}

/// Return all tenants (authoritative state).
pub(super) async fn list_tenants(store: &PostgresStore) -> StoreResult<Vec<Tenant>> {
    let rows = sqlx::query_as::<_, DbTenant>(
        "SELECT tenant_id, display_name FROM tenants ORDER BY tenant_id",
    )
    .fetch_all(&store.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Tenant {
            tenant_id: row.tenant_id,
            display_name: row.display_name,
        })
        .collect())
}

/// Create a tenant and emit a corresponding change-log entry in a single transaction.
///
/// Transactionality matters: we want the authoritative row and its change event to be consistent
/// (no “created row without change” or “change without row” states).
/// Keyset pages over the primary key, so each page is an index range scan.
pub(super) async fn list_tenants_page(
    store: &PostgresStore,
    page: PageRequest<String>,
) -> StoreResult<Page<Tenant>> {
    let fetch = page_fetch(page.limit);
    let rows = match &page.after {
        None => sqlx::query_as::<_, DbTenant>(
            "SELECT tenant_id, display_name FROM tenants ORDER BY tenant_id LIMIT $1",
        )
        .bind(fetch),
        Some(after) => sqlx::query_as::<_, DbTenant>(
            "SELECT tenant_id, display_name FROM tenants WHERE tenant_id > $1 \
             ORDER BY tenant_id LIMIT $2",
        )
        .bind(after)
        .bind(fetch),
    }
    .fetch_all(&store.pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|row| Tenant {
            tenant_id: row.tenant_id,
            display_name: row.display_name,
        })
        .collect();
    Ok(Page::from_overfetch(items, page.limit))
}

pub(super) async fn create_tenant(store: &PostgresStore, tenant: Tenant) -> StoreResult<Tenant> {
    let mut tx = store.pool.begin().await?;
    let insert = sqlx::query(r#"INSERT INTO tenants (tenant_id, display_name) VALUES ($1, $2)"#)
        .bind(&tenant.tenant_id)
        .bind(&tenant.display_name)
        .execute(&mut *tx)
        .await;
    if let Err(err) = insert {
        if is_unique_violation(&err) {
            return Err(StoreError::Conflict("tenant exists".into()));
        }
        return Err(StoreError::Unexpected(err.into()));
    }

    // Append to change log for incremental watchers.
    sqlx::query(r#"INSERT INTO tenant_changes (op, tenant_id, payload) VALUES ($1, $2, $3)"#)
        .bind("Created")
        .bind(&tenant.tenant_id)
        .bind(serde_json::to_value(&tenant).ok())
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(tenant)
}

/// Delete a tenant and all dependent resources, emitting “Deleted” change events.
///
/// Important: We fetch namespaces/streams/caches *before* deleting so we can emit “Deleted”
/// change events with the prior payload. This is useful for caches/watchers that want tombstones
/// with context (not just keys).
pub(super) async fn delete_tenant(store: &PostgresStore, tenant_id: &str) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;

    // Validate tenant exists to return a proper 404 semantics.
    let tenant_exists =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tenants WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_one(&mut *tx)
            .await?
            > 0;
    if !tenant_exists {
        return Err(StoreError::NotFound("tenant".into()));
    }

    // Prefetch dependents so we can produce Deleted payloads in change logs.
    let namespaces = sqlx::query_as::<_, DbNamespace>(
        r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1"#,
    )
    .bind(tenant_id)
    .fetch_all(&mut *tx)
    .await?;

    let streams = sqlx::query_as::<_, DbStream>(
        r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region
               FROM streams WHERE tenant_id = $1"#,
    )
    .bind(tenant_id)
    .fetch_all(&mut *tx)
    .await
    ?;

    let caches = sqlx::query_as::<_, DbCache>(
        r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency FROM caches WHERE tenant_id = $1"#,
    )
    .bind(tenant_id)
    .fetch_all(&mut *tx)
    .await?;

    // Delete tenant (schema should cascade or you rely on application-level deletes; either way
    // we emit explicit change events below based on the prefetched rows).
    sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await?;

    // Emit cache deletion events (payload included).
    for cache in caches {
        let key = CacheKey {
            tenant_id: cache.tenant_id.clone(),
            namespace: cache.namespace.clone(),
            cache: cache.cache.clone(),
        };
        let payload = Cache {
            tenant_id: cache.tenant_id,
            namespace: cache.namespace,
            cache: cache.cache,
            display_name: cache.display_name,
            shards: cache.shards as u32,
            replication_factor: cache.replication_factor as u32,
            consistency: parse_consistency(&cache.consistency)?,
        };
        sqlx::query(
            r#"INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Deleted")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.cache)
        .bind(serde_json::to_value(&payload).ok())
        .execute(&mut *tx)
        .await
        ?;
    }

    // Emit stream deletion events (payload included).
    for stream in streams {
        let key = StreamKey {
            tenant_id: stream.tenant_id.clone(),
            namespace: stream.namespace.clone(),
            stream: stream.stream.clone(),
        };
        let payload = Stream {
            tenant_id: stream.tenant_id,
            namespace: stream.namespace,
            stream: stream.stream,
            kind: parse_stream_kind(&stream.kind)?,
            shards: stream.shards as u32,
            replication_factor: stream.replication_factor as u32,
            retention: RetentionPolicy {
                max_age_seconds: stream.retention_max_age_seconds.map(|v| v as u64),
                max_size_bytes: stream.retention_max_size_bytes.map(|v| v as u64),
            },
            consistency: parse_consistency(&stream.consistency)?,
            delivery: parse_delivery(&stream.delivery)?,
            durable: stream.durable,
            region: stream.region,
        };
        sqlx::query(
            r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind("Deleted")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(&key.stream)
        .bind(serde_json::to_value(&payload).ok())
        .execute(&mut *tx)
        .await
        ?;
    }

    for namespace in namespaces {
        let key = NamespaceKey {
            tenant_id: namespace.tenant_id.clone(),
            namespace: namespace.namespace.clone(),
        };
        let payload = Namespace {
            tenant_id: namespace.tenant_id,
            namespace: namespace.namespace,
            display_name: namespace.display_name,
        };
        sqlx::query(
            r#"INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)"#,
        )
        .bind("Deleted")
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .bind(serde_json::to_value(&payload).ok())
        .execute(&mut *tx)
        .await
        ?;
    }

    sqlx::query(r#"INSERT INTO tenant_changes (op, tenant_id, payload) VALUES ($1, $2, $3)"#)
        .bind("Deleted")
        .bind(tenant_id)
        .bind(Option::<Value>::None)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    store.refresh_counts().await?;
    Ok(())
}

pub(super) async fn tenant_snapshot(store: &PostgresStore) -> StoreResult<Snapshot<Tenant>> {
    let items = store.list_tenants().await?;
    let next_seq =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM tenant_changes")
            .fetch_one(&store.pool)
            .await? as u64;
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn tenant_changes(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<TenantChange>> {
    let rows = sqlx::query_as::<_, TenantChangeRow>(
        r#"SELECT seq, op, tenant_id, payload FROM tenant_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&store.pool)
    .await
    ?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let op = match row.op.as_str() {
            "Created" => TenantChangeOp::Created,
            "Deleted" => TenantChangeOp::Deleted,
            _ => TenantChangeOp::Deleted,
        };
        let tenant = row
            .payload
            .and_then(|v| serde_json::from_value::<Tenant>(v).ok());
        items.push(TenantChange {
            seq: row.seq as u64,
            op,
            tenant_id: row.tenant_id,
            tenant,
        });
    }

    let next_seq =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM tenant_changes")
            .fetch_one(&store.pool)
            .await? as u64;

    Ok(ChangeSet { items, next_seq })
}

pub(super) async fn tenant_exists(store: &PostgresStore, tenant_id: &str) -> StoreResult<bool> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)")
            .bind(tenant_id)
            .fetch_one(&store.pool)
            .await?;
    Ok(exists)
}

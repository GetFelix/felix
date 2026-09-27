//! Namespaces, and the cascade that deleting one sets off.
use serde_json::Value;
use sqlx::FromRow;

use super::codec::{
    DbCache, DbNamespace, DbStream, parse_consistency, parse_delivery, parse_stream_kind,
};
use super::{PostgresStore, is_unique_violation, page_fetch};
use crate::model::{
    Cache, CacheKey, Namespace, NamespaceChange, NamespaceChangeOp, NamespaceKey, RetentionPolicy,
    Stream, StreamKey,
};
use crate::store::{ChangeSet, Page, PageRequest, Snapshot, StoreError, StoreResult};

/// Row shape for the `namespace_changes` table.
#[derive(Debug, Clone, FromRow)]
struct NamespaceChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    payload: Option<Value>,
}

pub(super) async fn list_namespaces(
    store: &PostgresStore,
    tenant_id: &str,
) -> StoreResult<Vec<Namespace>> {
    let rows = sqlx::query_as::<_, DbNamespace>(
        r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1 ORDER BY namespace"#,
    )
    .bind(tenant_id)
    .fetch_all(&store.pool)
    .await
    ?;

    Ok(rows
        .into_iter()
        .map(|row| Namespace {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            display_name: row.display_name,
        })
        .collect())
}

pub(super) async fn list_namespaces_page(
    store: &PostgresStore,
    tenant_id: &str,
    page: PageRequest<String>,
) -> StoreResult<Page<Namespace>> {
    let fetch = page_fetch(page.limit);
    let rows = match &page.after {
        None => sqlx::query_as::<_, DbNamespace>(
            "SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1 \
             ORDER BY namespace LIMIT $2",
        )
        .bind(tenant_id)
        .bind(fetch),
        Some(after) => sqlx::query_as::<_, DbNamespace>(
            "SELECT tenant_id, namespace, display_name FROM namespaces \
             WHERE tenant_id = $1 AND namespace > $2 ORDER BY namespace LIMIT $3",
        )
        .bind(tenant_id)
        .bind(after)
        .bind(fetch),
    }
    .fetch_all(&store.pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|row| Namespace {
            tenant_id: row.tenant_id,
            namespace: row.namespace,
            display_name: row.display_name,
        })
        .collect();
    Ok(Page::from_overfetch(items, page.limit))
}

pub(super) async fn create_namespace(
    store: &PostgresStore,
    namespace: Namespace,
) -> StoreResult<Namespace> {
    let mut tx = store.pool.begin().await?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)")
            .bind(&namespace.tenant_id)
            .fetch_one(&mut *tx)
            .await?;
    if !exists {
        return Err(StoreError::NotFound("tenant".into()));
    }

    let insert = sqlx::query(
        r#"INSERT INTO namespaces (tenant_id, namespace, display_name) VALUES ($1, $2, $3)"#,
    )
    .bind(&namespace.tenant_id)
    .bind(&namespace.namespace)
    .bind(&namespace.display_name)
    .execute(&mut *tx)
    .await;
    if let Err(err) = insert {
        if is_unique_violation(&err) {
            return Err(StoreError::Conflict("namespace exists".into()));
        }
        return Err(StoreError::Unexpected(err.into()));
    }

    sqlx::query(
        r#"INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)"#,
    )
    .bind("Created")
    .bind(&namespace.tenant_id)
    .bind(&namespace.namespace)
    .bind(serde_json::to_value(&namespace).ok())
    .execute(&mut *tx)
    .await
    ?;

    tx.commit().await?;
    Ok(namespace)
}

pub(super) async fn delete_namespace(store: &PostgresStore, key: &NamespaceKey) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;

    let ns_row = sqlx::query_as::<_, DbNamespace>(
        r#"SELECT tenant_id, namespace, display_name FROM namespaces WHERE tenant_id = $1 AND namespace = $2"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .fetch_optional(&mut *tx)
    .await
    ?;
    if ns_row.is_none() {
        return Err(StoreError::NotFound("namespace".into()));
    }
    let namespace_payload = ns_row.map(|row| Namespace {
        tenant_id: row.tenant_id,
        namespace: row.namespace,
        display_name: row.display_name,
    });

    let streams = sqlx::query_as::<_, DbStream>(
        r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region
               FROM streams WHERE tenant_id = $1 AND namespace = $2"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .fetch_all(&mut *tx)
    .await
    ?;

    let caches = sqlx::query_as::<_, DbCache>(
        r#"SELECT tenant_id, namespace, cache, display_name, shards, replication_factor, consistency FROM caches WHERE tenant_id = $1 AND namespace = $2"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .fetch_all(&mut *tx)
    .await
    ?;

    sqlx::query(r#"DELETE FROM namespaces WHERE tenant_id = $1 AND namespace = $2"#)
        .bind(&key.tenant_id)
        .bind(&key.namespace)
        .execute(&mut *tx)
        .await?;

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

    sqlx::query(
        r#"INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)"#,
    )
    .bind("Deleted")
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(namespace_payload.and_then(|ns| serde_json::to_value(&ns).ok()))
    .execute(&mut *tx)
    .await
    ?;

    tx.commit().await?;
    store.refresh_counts().await?;
    Ok(())
}

pub(super) async fn namespace_snapshot(store: &PostgresStore) -> StoreResult<Snapshot<Namespace>> {
    let items = sqlx::query_as::<_, DbNamespace>(
        r#"SELECT tenant_id, namespace, display_name FROM namespaces ORDER BY tenant_id, namespace"#,
    )
    .fetch_all(&store.pool)
    .await
    ?
    .into_iter()
    .map(|row| Namespace {
        tenant_id: row.tenant_id,
        namespace: row.namespace,
        display_name: row.display_name,
    })
    .collect();
    let next_seq =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM namespace_changes")
            .fetch_one(&store.pool)
            .await? as u64;
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn namespace_changes(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<NamespaceChange>> {
    let rows = sqlx::query_as::<_, NamespaceChangeRow>(
        r#"SELECT seq, op, tenant_id, namespace, payload FROM namespace_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&store.pool)
    .await
    ?;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let op = match row.op.as_str() {
            "Created" => NamespaceChangeOp::Created,
            _ => NamespaceChangeOp::Deleted,
        };
        let namespace = row
            .payload
            .and_then(|v| serde_json::from_value::<Namespace>(v).ok());
        items.push(NamespaceChange {
            seq: row.seq as u64,
            op,
            key: NamespaceKey {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
            },
            namespace,
        });
    }
    let next_seq =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM namespace_changes")
            .fetch_one(&store.pool)
            .await? as u64;
    Ok(ChangeSet { items, next_seq })
}

pub(super) async fn namespace_exists(
    store: &PostgresStore,
    key: &NamespaceKey,
) -> StoreResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM namespaces WHERE tenant_id = $1 AND namespace = $2)",
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .fetch_one(&store.pool)
    .await?;
    Ok(exists)
}

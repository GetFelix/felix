//! Streams and their change log.
use serde_json::Value;
use sqlx::FromRow;

use super::codec::{
    DbStream, consistency_to_str, delivery_to_str, parse_routing, stream_from_db,
    stream_kind_to_str,
};
use super::{PostgresStore, is_unique_violation, page_fetch};
use crate::model::{
    Stream, StreamChange, StreamChangeOp, StreamKey, StreamPatchRequest, StreamRouting,
};
use crate::store::{ChangeSet, Page, PageRequest, Snapshot, StoreError, StoreResult};

/// Row shape for the `stream_changes` table.
#[derive(Debug, Clone, FromRow)]
struct StreamChangeRow {
    seq: i64,
    op: String,
    tenant_id: String,
    namespace: String,
    stream: String,
    payload: Option<Value>,
}

pub(super) async fn list_streams(
    store: &PostgresStore,
    tenant_id: &str,
    namespace: &str,
) -> StoreResult<Vec<Stream>> {
    let rows = sqlx::query_as::<_, DbStream>(
        r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region, routing
               FROM streams WHERE tenant_id = $1 AND namespace = $2 ORDER BY stream"#,
    )
    .bind(tenant_id)
    .bind(namespace)
    .fetch_all(&store.pool)
    .await
    ?;
    rows.into_iter()
        .map(stream_from_db)
        .collect::<Result<Vec<_>, StoreError>>()
}

pub(super) async fn list_streams_page(
    store: &PostgresStore,
    tenant_id: &str,
    namespace: &str,
    page: PageRequest<String>,
) -> StoreResult<Page<Stream>> {
    let fetch = page_fetch(page.limit);
    let rows = match &page.after {
        None => sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region, routing
               FROM streams WHERE tenant_id = $1 AND namespace = $2 ORDER BY stream LIMIT $3"#,
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(fetch),
        Some(after) => sqlx::query_as::<_, DbStream>(
            r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region, routing
               FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream > $3
               ORDER BY stream LIMIT $4"#,
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
        .map(stream_from_db)
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(Page::from_overfetch(items, page.limit))
}

pub(super) async fn get_stream(store: &PostgresStore, key: &StreamKey) -> StoreResult<Stream> {
    let row = sqlx::query_as::<_, DbStream>(
        r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region, routing
               FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .fetch_optional(&store.pool)
    .await
    ?;
    match row {
        Some(row) => stream_from_db(row),
        None => Err(StoreError::NotFound("stream".into())),
    }
}

pub(super) async fn stream_routings(
    store: &PostgresStore,
    keys: &[StreamKey],
) -> StoreResult<std::collections::HashMap<StreamKey, StreamRouting>> {
    if keys.is_empty() {
        return Ok(Default::default());
    }
    let tenants: Vec<&str> = keys.iter().map(|key| key.tenant_id.as_str()).collect();
    let namespaces: Vec<&str> = keys.iter().map(|key| key.namespace.as_str()).collect();
    let names: Vec<&str> = keys.iter().map(|key| key.stream.as_str()).collect();
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        r#"SELECT s.tenant_id, s.namespace, s.stream, s.routing
               FROM streams s
               JOIN UNNEST($1::text[], $2::text[], $3::text[]) AS k(tenant_id, namespace, stream)
                 ON s.tenant_id = k.tenant_id AND s.namespace = k.namespace AND s.stream = k.stream
              WHERE s.routing IS NOT NULL"#,
    )
    .bind(&tenants)
    .bind(&namespaces)
    .bind(&names)
    .fetch_all(&store.pool)
    .await?;
    let mut routings = std::collections::HashMap::new();
    for (tenant_id, namespace, stream, routing) in rows {
        let routing = parse_routing(Some(&routing))?;
        if !routing.is_modulo() {
            let key = StreamKey {
                tenant_id,
                namespace,
                stream,
            };
            routings.insert(key, routing);
        }
    }
    Ok(routings)
}

pub(super) async fn create_stream(store: &PostgresStore, stream: Stream) -> StoreResult<Stream> {
    let mut tx = store.pool.begin().await?;
    super::ensure_namespace(&mut tx, &stream.tenant_id, &stream.namespace).await?;
    insert_stream(&mut tx, &stream).await?;
    tx.commit().await?;
    metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
    store.refresh_counts().await?;
    Ok(stream)
}

/// The row and its `Created` change, inside the caller's transaction.
/// `Conflict` when the stream exists.
pub(super) async fn insert_stream(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    stream: &Stream,
) -> StoreResult<()> {
    let insert = sqlx::query(
        r#"INSERT INTO streams (tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region, routing)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"#,
    )
    .bind(&stream.tenant_id)
    .bind(&stream.namespace)
    .bind(&stream.stream)
    .bind(stream_kind_to_str(&stream.kind))
    .bind(stream.shards as i32)
    .bind(stream.replication_factor as i32)
    .bind(stream.retention.max_age_seconds.map(|v| v as i64))
    .bind(stream.retention.max_size_bytes.map(|v| v as i64))
    .bind(consistency_to_str(&stream.consistency))
    .bind(delivery_to_str(&stream.delivery))
    .bind(stream.durable)
    .bind(stream.region.as_deref())
    .bind((!stream.routing.is_modulo()).then(|| stream.routing.as_str()))
    .execute(&mut **tx)
    .await;
    if let Err(err) = insert {
        if is_unique_violation(&err) {
            return Err(StoreError::Conflict(format!(
                "stream {} exists",
                stream.stream
            )));
        }
        return Err(StoreError::Unexpected(err.into()));
    }

    sqlx::query(
        r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind("Created")
    .bind(&stream.tenant_id)
    .bind(&stream.namespace)
    .bind(&stream.stream)
    .bind(serde_json::to_value(stream).ok())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn patch_stream(
    store: &PostgresStore,
    key: &StreamKey,
    patch: StreamPatchRequest,
) -> StoreResult<Stream> {
    let mut tx = store.pool.begin().await?;
    let current = sqlx::query_as::<_, DbStream>(
        r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region, routing
               FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3 FOR UPDATE"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .fetch_optional(&mut *tx)
    .await
    ?;
    let current = match current {
        Some(row) => row,
        None => return Err(StoreError::NotFound("stream".into())),
    };

    let mut updated = stream_from_db(current.clone())?;
    if let Some(retention) = patch.retention {
        updated.retention = retention;
    }
    if let Some(consistency) = patch.consistency {
        updated.consistency = consistency;
    }
    if let Some(delivery) = patch.delivery {
        updated.delivery = delivery;
    }
    if let Some(durable) = patch.durable {
        updated.durable = durable;
    }

    sqlx::query(
        r#"UPDATE streams SET kind = $1, shards = $2, replication_factor = $3, retention_max_age_seconds = $4, retention_max_size_bytes = $5, consistency = $6, delivery = $7, durable = $8, updated_at = now()
                WHERE tenant_id = $9 AND namespace = $10 AND stream = $11"#,
    )
    .bind(stream_kind_to_str(&updated.kind))
    .bind(updated.shards as i32)
    .bind(updated.replication_factor as i32)
    .bind(updated.retention.max_age_seconds.map(|v| v as i64))
    .bind(updated.retention.max_size_bytes.map(|v| v as i64))
    .bind(consistency_to_str(&updated.consistency))
    .bind(delivery_to_str(&updated.delivery))
    .bind(updated.durable)
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .execute(&mut *tx)
    .await
    ?;

    sqlx::query(
        r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind("Updated")
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(serde_json::to_value(&updated).ok())
    .execute(&mut *tx)
    .await
    ?;

    tx.commit().await?;
    metrics::counter!("felix_stream_changes_total", "op" => "updated").increment(1);
    Ok(updated)
}

pub(super) async fn delete_stream(store: &PostgresStore, key: &StreamKey) -> StoreResult<()> {
    let mut tx = store.pool.begin().await?;
    let removed = sqlx::query(
        r#"DELETE FROM streams WHERE tenant_id = $1 AND namespace = $2 AND stream = $3"#,
    )
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .execute(&mut *tx)
    .await?;
    if removed.rows_affected() == 0 {
        return Err(StoreError::NotFound("stream".into()));
    }

    sqlx::query(
        r#"INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind("Deleted")
    .bind(&key.tenant_id)
    .bind(&key.namespace)
    .bind(&key.stream)
    .bind(Option::<Value>::None)
    .execute(&mut *tx)
    .await
    ?;

    tx.commit().await?;
    metrics::counter!("felix_stream_changes_total", "op" => "deleted").increment(1);
    store.refresh_counts().await?;
    Ok(())
}

pub(super) async fn stream_snapshot(store: &PostgresStore) -> StoreResult<Snapshot<Stream>> {
    let rows = sqlx::query_as::<_, DbStream>(
        r#"SELECT tenant_id, namespace, stream, kind, shards, replication_factor, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable, region, routing FROM streams ORDER BY tenant_id, namespace, stream"#,
    )
    .fetch_all(&store.pool)
    .await
    ?;
    let items = rows
        .into_iter()
        .map(stream_from_db)
        .collect::<Result<Vec<_>, StoreError>>()?;
    let next_seq =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM stream_changes")
            .fetch_one(&store.pool)
            .await? as u64;
    Ok(Snapshot { items, next_seq })
}

pub(super) async fn stream_changes(
    store: &PostgresStore,
    since: u64,
) -> StoreResult<ChangeSet<StreamChange>> {
    let rows = sqlx::query_as::<_, StreamChangeRow>(
        r#"SELECT seq, op, tenant_id, namespace, stream, payload FROM stream_changes WHERE seq >= $1 ORDER BY seq ASC LIMIT $2"#,
    )
    .bind(since as i64)
    .bind(store.limit())
    .fetch_all(&store.pool)
    .await
    ?;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let op = match row.op.as_str() {
            "Created" => StreamChangeOp::Created,
            "Updated" => StreamChangeOp::Updated,
            _ => StreamChangeOp::Deleted,
        };
        let stream = row
            .payload
            .and_then(|v| serde_json::from_value::<Stream>(v).ok());
        items.push(StreamChange {
            seq: row.seq as u64,
            op,
            key: StreamKey {
                tenant_id: row.tenant_id,
                namespace: row.namespace,
                stream: row.stream,
            },
            stream,
        });
    }
    let next_seq =
        sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq) + 1, 0) FROM stream_changes")
            .fetch_one(&store.pool)
            .await? as u64;
    Ok(ChangeSet { items, next_seq })
}

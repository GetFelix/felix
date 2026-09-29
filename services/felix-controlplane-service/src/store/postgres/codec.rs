//! Rows shared across aggregates, and the enum encodings stored in text columns.
use anyhow::anyhow;
use sqlx::FromRow;

use crate::model::{RetentionPolicy, Stream, StreamKind, StreamRouting};
use crate::store::{StoreError, StoreResult};

/// Row shape for the `streams` authoritative table.
///
/// This is a direct mapping of the SQL schema into Rust types via `sqlx::FromRow`.
/// We keep these DB-facing structs separate from domain types (`Stream`, etc.) to:
/// - isolate schema details (column names, storage formats) from the API domain model
/// - make it explicit where parsing/validation occurs (e.g., string enums → domain enums)
/// - keep migration/schema evolution localized
#[derive(Debug, Clone, FromRow)]
pub(super) struct DbStream {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) stream: String,
    pub(super) kind: String,
    pub(super) shards: i32,
    pub(super) replication_factor: i32,
    pub(super) retention_max_age_seconds: Option<i64>,
    pub(super) retention_max_size_bytes: Option<i64>,
    pub(super) consistency: String,
    pub(super) delivery: String,
    pub(super) durable: bool,
    pub(super) region: Option<String>,
    pub(super) routing: Option<String>,
}

/// Row shape for `namespaces` table.
#[derive(Debug, Clone, FromRow)]
pub(super) struct DbNamespace {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) display_name: String,
}

/// Row shape for `caches` table.
#[derive(Debug, Clone, FromRow)]
pub(super) struct DbCache {
    pub(super) tenant_id: String,
    pub(super) namespace: String,
    pub(super) cache: String,
    pub(super) display_name: String,
    pub(super) shards: i32,
    pub(super) replication_factor: i32,
    pub(super) consistency: String,
}

pub(super) fn stream_from_db(row: DbStream) -> StoreResult<Stream> {
    Ok(Stream {
        tenant_id: row.tenant_id,
        namespace: row.namespace,
        stream: row.stream,
        kind: parse_stream_kind(&row.kind)?,
        shards: row.shards as u32,
        replication_factor: row.replication_factor as u32,
        retention: RetentionPolicy {
            max_age_seconds: row.retention_max_age_seconds.map(|v| v as u64),
            max_size_bytes: row.retention_max_size_bytes.map(|v| v as u64),
        },
        consistency: parse_consistency(&row.consistency)?,
        delivery: parse_delivery(&row.delivery)?,
        durable: row.durable,
        region: row.region,
        routing: parse_routing(row.routing.as_deref())?,
    })
}

pub(super) fn parse_routing(value: Option<&str>) -> StoreResult<StreamRouting> {
    match value {
        None => Ok(StreamRouting::Modulo),
        Some(value) => StreamRouting::parse(value)
            .ok_or_else(|| StoreError::Unexpected(anyhow!("unknown stream routing '{value}'"))),
    }
}

pub(super) fn parse_stream_kind(value: &str) -> StoreResult<StreamKind> {
    match value {
        "Stream" => Ok(StreamKind::Stream),
        "Queue" => Ok(StreamKind::Queue),
        "Cache" => Ok(StreamKind::Cache),
        _ => Err(StoreError::Unexpected(anyhow!(
            "invalid stream kind {value}"
        ))),
    }
}

pub(super) fn stream_kind_to_str(kind: &StreamKind) -> &'static str {
    match kind {
        StreamKind::Stream => "Stream",
        StreamKind::Queue => "Queue",
        StreamKind::Cache => "Cache",
    }
}

pub(super) fn parse_consistency(value: &str) -> StoreResult<crate::model::ConsistencyLevel> {
    match value {
        "Leader" => Ok(crate::model::ConsistencyLevel::Leader),
        "Quorum" => Ok(crate::model::ConsistencyLevel::Quorum),
        _ => Err(StoreError::Unexpected(anyhow!(
            "invalid consistency {value}"
        ))),
    }
}

pub(super) fn consistency_to_str(value: &crate::model::ConsistencyLevel) -> &'static str {
    match value {
        crate::model::ConsistencyLevel::Leader => "Leader",
        crate::model::ConsistencyLevel::Quorum => "Quorum",
    }
}

pub(super) fn parse_delivery(value: &str) -> StoreResult<crate::model::DeliveryGuarantee> {
    match value {
        "AtMostOnce" => Ok(crate::model::DeliveryGuarantee::AtMostOnce),
        "AtLeastOnce" => Ok(crate::model::DeliveryGuarantee::AtLeastOnce),
        _ => Err(StoreError::Unexpected(anyhow!("invalid delivery {value}"))),
    }
}

pub(super) fn delivery_to_str(value: &crate::model::DeliveryGuarantee) -> &'static str {
    match value {
        crate::model::DeliveryGuarantee::AtMostOnce => "AtMostOnce",
        crate::model::DeliveryGuarantee::AtLeastOnce => "AtLeastOnce",
    }
}

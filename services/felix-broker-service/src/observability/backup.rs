//! `GET /backup/offsets`: the committed offset of every log of every shard
//! this broker leads, for `felix-controlplane admin backup-point`.
//!
//! Read-only, like `/replication/halted` beside it, because this listener has
//! no authentication. It says how far each shard is committed and nothing
//! more; the records themselves are copied off the disk by the operator.
//! See `docs-site/src/content/docs/deployment/backup-and-restore.md`.

use std::sync::Arc;

use felix_broker::{Broker, BrokerError, ConsistencyLevel, LogKind, NotReadable, ReadBound};
use serde::Serialize;

use crate::shards::routing::IngressRouter;
use crate::shards::{ShardKey, ShardKind};
use felix_replication::quorum::QuorumMarks;

/// What the route reads from.
#[derive(Clone)]
pub(crate) struct BackupOffsets {
    pub(crate) node_id: Option<String>,
    pub(crate) broker: Arc<Broker>,
    /// `None` on a broker with no cluster: it has no assignments, so it leads
    /// nothing a backup point could name.
    pub(crate) ingress: Option<Arc<IngressRouter>>,
    pub(crate) marks: Arc<QuorumMarks>,
}

impl BackupOffsets {
    /// Every led shard's offsets, and the shards that have none to give yet.
    pub(crate) async fn collect(&self) -> OffsetsResponse {
        let mut response = OffsetsResponse {
            node_id: self.node_id.clone(),
            shards: Vec::new(),
            skipped: Vec::new(),
        };
        let Some(ingress) = &self.ingress else {
            return response;
        };
        let mut led = ingress.led_shards();
        led.sort();
        for (key, generation) in led {
            match self.shard(ingress, &key, generation).await {
                Ok(logs) => response.shards.push(ShardOffsets {
                    tenant_id: key.tenant_id,
                    namespace: key.namespace,
                    name: key.stream,
                    shard: key.shard,
                    kind: kind_name(key.kind),
                    generation,
                    logs,
                }),
                Err(reason) => response.skipped.push(SkippedShard {
                    tenant_id: key.tenant_id,
                    namespace: key.namespace,
                    name: key.stream,
                    shard: key.shard,
                    kind: kind_name(key.kind),
                    reason: reason.code(),
                    detail: match reason {
                        SkipReason::Error(detail) => Some(detail),
                        _ => None,
                    },
                }),
            }
        }
        response
    }

    async fn shard(
        &self,
        ingress: &IngressRouter,
        key: &ShardKey,
        generation: u64,
    ) -> Result<LogOffsets, SkipReason> {
        let quorum_cache = key.kind == ShardKind::Cache
            && self
                .broker
                .cache_consistency(&key.tenant_id, &key.namespace, &key.stream)
                .await
                == Some(ConsistencyLevel::Quorum)
            && ingress.replicated(key);
        // Pinned to `generation`: a mark from any other leadership says
        // nothing about this one. The re-check below catches a change after.
        let bound = |kind: LogKind| {
            let mark = match kind {
                LogKind::Cache if quorum_cache => self.marks.offset(key, generation),
                LogKind::Counters if quorum_cache => self.marks.counters().offset(key, generation),
                // Group state has no quorum mark: it is acknowledged on the
                // leader's own durability, which the acknowledged tail is.
                _ => return ReadBound::Unbounded,
            };
            if !ingress.fence().lease_valid() {
                return ReadBound::Refused;
            }
            mark.map_or(ReadBound::Settling, ReadBound::Committed)
        };
        let records = match key.kind {
            ShardKind::Stream => LogKind::Stream,
            ShardKind::Cache => LogKind::Cache,
        };
        let offsets = self
            .broker
            .committed_offsets(
                records,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                bound,
            )
            .await
            .map_err(|err| match err {
                BrokerError::NotReadable {
                    reason: NotReadable::Settling,
                    ..
                } => SkipReason::Settling,
                BrokerError::NotReadable {
                    reason: NotReadable::Refused,
                    ..
                } => SkipReason::Refused,
                other => SkipReason::Error(other.to_string()),
            })?
            .ok_or(SkipReason::NotDurable)?;
        // Offsets read while the leadership changed may come from a log a
        // new leader has already overtaken.
        if ingress.generation(key) != Some(generation) {
            return Err(SkipReason::Moved);
        }
        Ok(LogOffsets {
            records: offsets.records,
            group_cursors: offsets.group_cursors,
            group_dead_letters: offsets.group_dead_letters,
            counters: offsets.counters,
        })
    }
}

/// The answer to `GET /backup/offsets`.
#[derive(Debug, Serialize)]
pub(crate) struct OffsetsResponse {
    node_id: Option<String>,
    shards: Vec<ShardOffsets>,
    /// Shards this broker leads but has no committed answer for right now.
    skipped: Vec<SkippedShard>,
}

#[derive(Debug, Serialize)]
struct ShardOffsets {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: &'static str,
    generation: u64,
    logs: LogOffsets,
}

/// One past the last committed record of each log.
#[derive(Debug, Serialize)]
struct LogOffsets {
    records: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_cursors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_dead_letters: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    counters: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SkippedShard {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: &'static str,
    /// `settling`, `refused`, `moved`, `not_durable` or `error`. Every one but
    /// `not_durable` is worth asking again shortly.
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

/// Why a led shard has no offsets in the answer.
#[derive(Debug)]
enum SkipReason {
    /// Just taken; no quorum mark for this leadership yet.
    Settling,
    /// The lease lapsed, or the shard is no longer served here.
    Refused,
    /// The leadership changed while its offsets were read.
    Moved,
    /// An in-memory stream: nothing on disk to back up.
    NotDurable,
    Error(String),
}

impl SkipReason {
    fn code(&self) -> &'static str {
        match self {
            Self::Settling => "settling",
            Self::Refused => "refused",
            Self::Moved => "moved",
            Self::NotDurable => "not_durable",
            Self::Error(_) => "error",
        }
    }
}

/// `GET /backup/offsets` on `source`.
pub(crate) fn router(source: BackupOffsets) -> axum::Router {
    axum::Router::new().route(
        "/backup/offsets",
        axum::routing::get(move || {
            let source = source.clone();
            async move { axum::Json(source.collect().await) }
        }),
    )
}

fn kind_name(kind: ShardKind) -> &'static str {
    match kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    }
}

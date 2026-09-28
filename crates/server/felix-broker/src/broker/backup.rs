//! What a backup point records for one shard: the committed offset of each of
//! its logs, as this broker leads it now.
//!
//! See `docs-site/src/content/docs/deployment/backup-and-restore.md` for how
//! the offsets are collected across brokers and what the point guarantees.

use super::{Broker, LogKind};
use crate::error::{BrokerError, NotReadable, Result};
use crate::stream::ReadBound;

/// The committed offset of each log a shard has: one past the last record a
/// reader may see and a restore must keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedOffsets {
    /// The shard's own records, a stream's or a cache's.
    pub records: u64,
    /// A stream shard's consumer-group cursors, when this broker keeps them.
    pub group_cursors: Option<u64>,
    /// A stream shard's dead letters, when this broker keeps them.
    pub group_dead_letters: Option<u64>,
    /// A cache shard's counters, when this broker keeps them.
    pub counters: Option<u64>,
}

impl Broker {
    /// The committed offsets of one shard's logs, for a backup point.
    ///
    /// `records` is [`LogKind::Stream`] or [`LogKind::Cache`]. A stream's
    /// records are bounded exactly as [`Broker::cursor_tail`] bounds them;
    /// every other log by `bound`, which the caller answers from its own
    /// marks. `None` when the shard keeps no records on disk here.
    ///
    /// The sidecar logs are read before the records. A cursor or dead letter
    /// only ever names a record that already exists, so reading them first
    /// means nothing in the point refers past the point's records.
    ///
    /// [`BrokerError::NotReadable`] when any log has no committed answer here
    /// yet, or this broker may not serve it.
    pub async fn committed_offsets(
        &self,
        records: LogKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
        bound: impl Fn(LogKind) -> ReadBound,
    ) -> Result<Option<CommittedOffsets>> {
        let committed = async |kind: LogKind| -> Result<Option<u64>> {
            let Some(log) = self
                .shard_log(kind, tenant_id, namespace, name, shard)
                .await
            else {
                return Ok(None);
            };
            committed_offset(bound(kind), log.acknowledged_tail().await?, name, shard).map(Some)
        };
        match records {
            LogKind::Stream => {
                let group_cursors = committed(LogKind::GroupCursors).await?;
                let group_dead_letters = committed(LogKind::GroupDeadLetters).await?;
                let handle = self
                    .resolve_stream_handle(tenant_id, namespace, name, shard)
                    .await?;
                let Some(log) = &handle.state.durable else {
                    return Ok(None);
                };
                let records = committed_offset(
                    handle.state.read_bound(),
                    log.acknowledged_tail().await?,
                    name,
                    shard,
                )?;
                Ok(Some(CommittedOffsets {
                    records,
                    group_cursors,
                    group_dead_letters,
                    counters: None,
                }))
            }
            LogKind::Cache => {
                let counters = committed(LogKind::Counters).await?;
                let Some(records) = committed(LogKind::Cache).await? else {
                    return Ok(None);
                };
                Ok(Some(CommittedOffsets {
                    records,
                    group_cursors: None,
                    group_dead_letters: None,
                    counters,
                }))
            }
            other => Err(BrokerError::Storage(format!(
                "{other:?} is not a shard's records log"
            ))),
        }
    }
}

/// One past the last committed record of a log whose acknowledged tail is
/// `acknowledged`.
///
/// Both limits apply. A quorum mark can run ahead of the leader's own
/// acknowledged tail when the leader counts records it has written but not
/// yet synced, and a record is committed only once it is past both. This is
/// the whole of "the point includes nothing uncommitted".
fn committed_offset(bound: ReadBound, acknowledged: u64, name: &str, shard: u32) -> Result<u64> {
    match bound {
        ReadBound::Unbounded => Ok(acknowledged),
        ReadBound::Committed(mark) => Ok(mark.min(acknowledged)),
        ReadBound::Settling => Err(not_readable(name, shard, NotReadable::Settling)),
        ReadBound::Refused => Err(not_readable(name, shard, NotReadable::Refused)),
    }
}

fn not_readable(name: &str, shard: u32, reason: NotReadable) -> BrokerError {
    BrokerError::NotReadable {
        stream: name.to_string(),
        shard,
        reason,
    }
}

#[cfg(test)]
mod tests;

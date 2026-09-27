//! One cache shard: its log, and the index that maps each key to the record
//! currently defining it.
//!
//! The index is derived from the log and never trusted from disk, so it is
//! rebuilt by replay on first use and caught up whenever records reach the log
//! by another route.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::Mutex;

use super::CacheOp;
use crate::commit_order::CommitSequencer;
use crate::disk_log::DiskLog;
use crate::log::{AppendOnlyLog, LogConfig, Offset, ReadRange};
use crate::{Result, StorageError};

/// How much of the log one replay or compaction pass reads at a time.
///
/// Bounds peak memory during a rebuild: a cache far larger than this costs many
/// reads, not one enormous allocation.
const SCAN_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// One cache: its log, and the index derived from it.
pub(super) struct CacheShard {
    pub(super) dir: PathBuf,
    pub(super) label: String,
    pub(super) config: LogConfig,
    /// Guards the log handle and the index. A write holds it twice, briefly —
    /// once to stage (claim an offset, no fsync) and once to apply — never
    /// across the fsync, which is what lets concurrent writers share one
    /// group-committed flush instead of each paying a whole device flush.
    pub(super) state: Mutex<ShardState>,
    /// Re-serialises the post-durability half of writes into disk-offset
    /// order. Fsyncs overlap; what `get` and the watch observer see does not.
    pub(super) sequencer: Arc<CommitSequencer>,
}

impl CacheShard {
    /// The log this shard is writing to right now.
    ///
    /// Takes the state lock so it cannot observe compaction halfway through the
    /// directory swap.
    pub(super) async fn current_log(&self) -> DiskLog {
        self.state.lock().await.log.clone()
    }

    /// Rebuild the index by replaying the log.
    ///
    /// Run once, lazily, the first time a cache is touched after opening. The
    /// index is derived and never trusted from disk -- the same rule the segment
    /// indexes follow, and for the same reason: anything recomputable from the
    /// log must be, because then it cannot be stale in a way that matters.
    pub(super) async fn ensure_index(&self, state: &mut ShardState) -> Result<()> {
        if state.closed {
            return Err(StorageError::Closed(self.label.clone()));
        }
        let tail = state.log.tail_offset().await?;
        // Align the sequencer with records that did not come through the
        // write path — recovery on open, compaction, or a leader shipping
        // records to this shard as a follower. Without this, the next writer
        // would reserve its range past a gap nobody will ever resolve, and
        // wait on a turn that cannot arrive.
        match state.sequenced_through {
            None => {
                // First touch after open. Nothing can be in flight yet:
                // every writer passes through here, under this lock, before
                // it reserves anything.
                self.sequencer.reset(tail);
                state.sequenced_through = Some(tail);
            }
            Some(through) if tail > through => {
                // Out-of-band records hold [through, tail). Reserving and
                // immediately releasing the range resolves it, and the
                // resolution waits its turn behind any writer still in
                // flight below it.
                drop(self.sequencer.reserve(through, tail));
                state.sequenced_through = Some(tail);
            }
            _ => {}
        }
        // Fold nothing a writer has staged but not yet committed: staged
        // records become visible in their writers' apply step, after their
        // fsync, or a reader could see a put that a crash then loses. With
        // writers in flight the applied sequence is the visibility frontier;
        // idle, it equals the tail.
        let applied = self.sequencer.next_offset();
        let stop = if state.sequenced_through == Some(applied) {
            tail
        } else {
            applied
        };
        // Records can reach this log without going through the write path (see
        // above), so this catches up rather than building once and trusting
        // itself forever -- the same rule the segment indexes follow.
        let resume = state.index.covered_through;
        if resume == Some(stop) {
            return Ok(());
        }
        let (mut offset, mut index) = match resume {
            Some(covered) => (covered, std::mem::take(&mut state.index)),
            None => (state.log.base_offset(), Index::default()),
        };
        'scan: while offset < stop {
            let records = state
                .log
                .read_range(ReadRange {
                    start: offset,
                    max_bytes: SCAN_CHUNK_BYTES,
                })
                .await?;
            if records.is_empty() {
                break;
            }
            for record in &records {
                if record.offset >= stop {
                    // A read is bounded by bytes, not offset, so it can hand
                    // back staged records past the frontier.
                    break 'scan;
                }
                let bytes = record.payload.len() as u64;
                index.log_bytes += bytes;
                let op = CacheOp::decode(&record.payload)
                    .map_err(|err| StorageError::Corruption(err.in_shard(&self.label)))?;
                match op {
                    CacheOp::Put {
                        key,
                        expires_at_millis,
                        ..
                    } => {
                        if let Some(previous) = index.entries.insert(
                            key,
                            Entry {
                                offset: record.offset,
                                expires_at_millis,
                                bytes,
                            },
                        ) {
                            index.live_bytes -= previous.bytes;
                        }
                        index.live_bytes += bytes;
                    }
                    CacheOp::Delete { key } => {
                        if let Some(previous) = index.entries.remove(&key) {
                            index.live_bytes -= previous.bytes;
                        }
                    }
                }
                offset = record.offset + 1;
            }
        }
        index.covered_through = Some(stop.max(offset));
        state.index = index;
        Ok(())
    }

    /// Fold one committed record into the index.
    ///
    /// The caller holds the state lock and has waited its turn, so applies
    /// land strictly in disk-offset order — which is what keeps "a later
    /// offset wins" true for the index without the lock spanning the fsync.
    pub(super) fn apply_op(state: &mut ShardState, op: &CacheOp, offset: Offset, bytes: u64) {
        state.index.log_bytes += bytes;
        // Advance the watermark so the next `ensure_index` does not rescan
        // this record; never move it backwards, and never invent one, because
        // a watermark that skips unread history is worse than none.
        if state
            .index
            .covered_through
            .is_some_and(|covered| covered <= offset)
        {
            state.index.covered_through = Some(offset + 1);
        }
        match op {
            CacheOp::Put {
                key,
                expires_at_millis,
                ..
            } => {
                let entry = Entry {
                    offset,
                    expires_at_millis: *expires_at_millis,
                    bytes,
                };
                if let Some(previous) = state.index.entries.insert(key.clone(), entry) {
                    state.index.live_bytes -= previous.bytes;
                }
                state.index.live_bytes += bytes;
            }
            CacheOp::Delete { key } => {
                if let Some(previous) = state.index.entries.remove(key) {
                    state.index.live_bytes -= previous.bytes;
                }
            }
        }
    }

    /// Read the record the index points at, and hand back its value.
    pub(super) async fn read_value(
        &self,
        state: &ShardState,
        entry: Entry,
    ) -> Result<Option<Bytes>> {
        let records = state
            .log
            .read_range(ReadRange {
                start: entry.offset,
                // One record. The cap has to exceed it, and a record larger
                // than this could not have been appended in the first place.
                max_bytes: SCAN_CHUNK_BYTES,
            })
            .await?;
        let Some(found) = records.first() else {
            // The index points past the end of the log. Nothing can make that
            // right, and returning a miss would hide it.
            return Err(StorageError::NotFound);
        };
        match CacheOp::decode(&found.payload)
            .map_err(|err| StorageError::Corruption(err.in_shard(&self.label)))?
        {
            CacheOp::Put { value, .. } => Ok(Some(value)),
            // The index only ever points at a put; a tombstone here means the
            // index and the log disagree, which is a bug rather than a miss.
            CacheOp::Delete { .. } => Err(StorageError::NotFound),
        }
    }
}

impl std::fmt::Debug for CacheShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheShard")
            .field("label", &self.label)
            .finish()
    }
}

pub(super) struct ShardState {
    pub(super) log: DiskLog,
    pub(super) index: Index,
    /// Exclusive end of the last offset range a writer here has reserved with
    /// the sequencer. Offsets past it were appended by someone else — recovery
    /// on open, compaction, or a leader shipping records to this follower —
    /// and `ensure_index` resolves that gap so the sequence can walk past it.
    /// `None` until the first `ensure_index` aligns the sequencer to the tail.
    pub(super) sequenced_through: Option<u64>,
    /// Set by `LogCache::close_shard`. A caller that found this shard before
    /// the close must not touch the files: they may belong to a newer open.
    pub(super) closed: bool,
}

/// The index over one cache's log, and the accounting compaction needs.
#[derive(Debug, Default)]
pub(super) struct Index {
    pub(super) entries: HashMap<String, Entry>,
    /// Bytes held by records the index still points at.
    pub(super) live_bytes: u64,
    /// Bytes appended since the log was last compacted, live or not.
    pub(super) log_bytes: u64,
    /// The offset this index has read up to. Records at or past it are not
    /// reflected here yet.
    ///
    /// `None` means nothing has been read, which is not the same as having read
    /// an empty log: a shard whose log begins at a trimmed base has no offset
    /// zero to start from.
    pub(super) covered_through: Option<u64>,
}

/// Where one key's current value lives.
#[derive(Debug, Clone, Copy)]
pub(super) struct Entry {
    pub(super) offset: Offset,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub(super) expires_at_millis: u64,
    /// What this record costs on disk, for deciding when to compact.
    pub(super) bytes: u64,
}

impl Entry {
    pub(super) fn is_expired(&self, now_millis: u64) -> bool {
        self.expires_at_millis != 0 && self.expires_at_millis <= now_millis
    }
}

pub(super) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

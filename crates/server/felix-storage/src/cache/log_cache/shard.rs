//! One cache shard: its log, and the index that maps each key to the record
//! currently defining it.
//!
//! The index is derived from the log, so it is rebuilt by replay on first use
//! (from a checked snapshot when compaction left one) and caught up whenever
//! records reach the log by another route.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::{Mutex, MutexGuard, Notify};

use super::CacheOp;
use crate::commit_order::CommitSequencer;
use crate::compaction::Compactor;
use crate::disk_log::DiskLog;
use crate::index_snapshot::IndexEntry;
use crate::log::{AppendOnlyLog, Offset, ReadRange};
use crate::{Result, StorageError};

/// How much of the log one replay or compaction pass reads at a time.
///
/// Bounds peak memory during a rebuild: a cache far larger than this costs many
/// reads, not one enormous allocation.
pub(super) const SCAN_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// One cache: its log, and the index derived from it.
pub(super) struct CacheShard {
    pub(super) label: String,
    /// The log's directory, where the key index snapshot lives beside it.
    pub(super) dir: PathBuf,
    pub(super) compactor: Arc<Compactor>,
    /// Set while a background compaction pass runs, so only one does.
    pub(super) compacting: AtomicBool,
    /// Keys with a write staged but not yet applied, and how many. Compaction
    /// must not copy these forward; see `compaction::copy_forward`.
    pub(super) keys_in_flight: SyncMutex<HashMap<String, usize>>,
    /// Woken whenever a key leaves `keys_in_flight`, for a conditional write
    /// waiting to read the key's settled state.
    pub(super) key_settled: Notify,
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
    /// Takes the state lock so it cannot race a close.
    pub(super) async fn current_log(&self) -> DiskLog {
        self.state.lock().await.log.clone()
    }

    /// Rebuild the index by replaying the log.
    ///
    /// Run once, lazily, the first time a cache is touched after opening. The
    /// index is derived -- the same rule the segment indexes follow, and for
    /// the same reason: anything recomputable from the log must be, because
    /// then it cannot be stale in a way that matters. A snapshot left by the
    /// last compaction only moves where the replay starts, and only once it
    /// has been checked against the log.
    pub(super) async fn ensure_index(&self, state: &mut ShardState) -> Result<()> {
        if state.closed {
            return Err(StorageError::Closed(self.label.clone()));
        }
        let tail = state.log.tail_offset().await?;
        // Align the sequencer with records that did not come through the
        // write path — recovery on open, or a leader shipping
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
            None => match self.restore(state, stop).await {
                Some(index) => (index.covered_through.unwrap_or(stop), index),
                None => (state.log.base_offset(), Index::default()),
            },
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
                if record.mark.is_generation_start() {
                    // Replication's, not a cache op. None is written to a
                    // cache log today; decoding one would read as corruption.
                    offset = record.offset + 1;
                    continue;
                }
                let bytes = record.payload.len() as u64;
                index.log_bytes += bytes;
                let op = CacheOp::decode(&record.payload)
                    .map_err(|err| StorageError::Corruption(err.in_shard(&self.label)))?;
                match op {
                    CacheOp::Put {
                        key,
                        expires_at_millis,
                        version,
                        ..
                    } => {
                        if let Some(previous) = index.entries.insert(
                            key,
                            IndexEntry {
                                offset: record.offset,
                                version: version.unwrap_or(record.offset),
                                expires_at_millis,
                                bytes: bytes as u32,
                            },
                        ) {
                            index.live_bytes -= u64::from(previous.bytes);
                        }
                        index.live_bytes += bytes;
                    }
                    CacheOp::Delete { key } => {
                        if let Some(previous) = index.entries.remove(&key) {
                            index.live_bytes -= u64::from(previous.bytes);
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
                version,
                ..
            } => {
                let entry = IndexEntry {
                    offset,
                    version: version.unwrap_or(offset),
                    expires_at_millis: *expires_at_millis,
                    bytes: bytes as u32,
                };
                if let Some(previous) = state.index.entries.insert(key.clone(), entry) {
                    state.index.live_bytes -= u64::from(previous.bytes);
                }
                state.index.live_bytes += bytes;
            }
            CacheOp::Delete { key } => {
                if let Some(previous) = state.index.entries.remove(key) {
                    state.index.live_bytes -= u64::from(previous.bytes);
                }
            }
        }
    }

    /// Read the record the index points at, and hand back its value.
    pub(super) async fn read_value(
        &self,
        state: &ShardState,
        entry: IndexEntry,
    ) -> Result<Option<Bytes>> {
        Self::read_from(&state.log, &self.label, entry).await
    }

    /// [`CacheShard::read_value`] through a log handle, for a caller not
    /// holding the state lock.
    pub(super) async fn read_from(
        log: &DiskLog,
        label: &str,
        entry: IndexEntry,
    ) -> Result<Option<Bytes>> {
        let records = log
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
            .map_err(|err| StorageError::Corruption(err.in_shard(label)))?
        {
            CacheOp::Put { value, .. } => Ok(Some(value)),
            // The index only ever points at a put; a tombstone here means the
            // index and the log disagree, which is a bug rather than a miss.
            CacheOp::Delete { .. } => Err(StorageError::NotFound),
        }
    }

    /// The state lock, taken once no write to `key` is staged but unapplied,
    /// with the index caught up.
    ///
    /// The index lags a staged write until its fsync, so a condition checked
    /// against it while one is in flight could pass for two racers. Writers
    /// register in `keys_in_flight` under this same lock, so a key absent
    /// there means the index entry is the latest word on it.
    pub(super) async fn lock_settled(&self, key: &str) -> Result<MutexGuard<'_, ShardState>> {
        loop {
            let settled = self.key_settled.notified();
            tokio::pin!(settled);
            // Registered before the check, so a write settling in between
            // still wakes this one.
            settled.as_mut().enable();
            let mut state = self.state.lock().await;
            self.ensure_index(&mut state).await?;
            if !self.keys_in_flight.lock().contains_key(key) {
                return Ok(state);
            }
            drop(state);
            settled.await;
        }
    }

    /// Mark `key` as having a write in flight until the guard drops.
    pub(super) fn key_in_flight(self: &Arc<Self>, key: &str) -> KeyInFlight {
        *self
            .keys_in_flight
            .lock()
            .entry(key.to_string())
            .or_default() += 1;
        KeyInFlight {
            shard: Arc::clone(self),
            key: key.to_string(),
        }
    }
}

/// Held by a staged write until it has applied or failed.
pub(super) struct KeyInFlight {
    shard: Arc<CacheShard>,
    key: String,
}

impl Drop for KeyInFlight {
    fn drop(&mut self) {
        let mut keys = self.shard.keys_in_flight.lock();
        if let Some(count) = keys.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                keys.remove(&self.key);
                drop(keys);
                self.shard.key_settled.notify_waiters();
            }
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
    /// on open, or a leader shipping records to this follower —
    /// and `ensure_index` resolves that gap so the sequence can walk past it.
    /// `None` until the first `ensure_index` aligns the sequencer to the tail.
    pub(super) sequenced_through: Option<u64>,
    /// Set by `LogCache::close_shard`. A caller that found this shard before
    /// the close must not touch the files: they may belong to a newer open.
    pub(super) closed: bool,
    /// Bumped by `LogCache::forget_index`. A snapshot captured before the bump
    /// describes records that may be gone, so it is not installed.
    pub(super) index_epoch: u64,
}

/// The index over one cache's log, and the accounting compaction needs.
#[derive(Debug, Default)]
pub(super) struct Index {
    pub(super) entries: HashMap<String, IndexEntry>,
    /// Bytes held by records the index still points at.
    pub(super) live_bytes: u64,
    /// Payload bytes of every record still in the log, live or not.
    pub(super) log_bytes: u64,
    /// The offset this index has read up to. Records at or past it are not
    /// reflected here yet.
    ///
    /// `None` means nothing has been read, which is not the same as having read
    /// an empty log: a shard whose log begins at a trimmed base has no offset
    /// zero to start from.
    pub(super) covered_through: Option<u64>,
    /// Where replay started when this index was loaded from a snapshot.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) restored_through: Option<u64>,
}

pub(super) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

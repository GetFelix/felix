//! Reclaiming a cache shard's overwritten records, off the write path.
//!
//! A log that only ever grows makes "the cache is a log" a slow leak. A pass
//! seals the active segment, which fixes a cut: every record below it is in a
//! sealed segment. It then copies each live record below the cut to the tail
//! as an ordinary put, through the same staging and commit order as a write,
//! and finally deletes the sealed segments below the cut.
//!
//! Nothing is edited in place, and a crash at any point leaves a log whose
//! replay is the same cache: a copy restates a value the log already holds,
//! and the segments are deleted only once nothing live is left in them. The
//! pass runs on its own task and spends from the store's I/O budget; a write
//! only ever waits for the brief staging step of one batch of copies.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::CacheOp;
use super::shard::{CacheShard, Entry, Index, now_millis};
use crate::compaction::{COPY_BATCH, payload_bytes};
use crate::disk_log::DiskLog;
use crate::log::{AppendRecord, Offset};
use crate::{Result, StorageError};

/// How much larger than its live bytes a log may grow before it is compacted.
///
/// A multiple rather than a fixed size, so the cost is proportional to the
/// garbage: a cache that is mostly live is never compacted however big it is,
/// and one that is mostly overwrites is compacted however small.
const COMPACT_WHEN_TIMES_LIVE: u64 = 4;

/// Below this there is nothing worth reclaiming, whatever the ratio says. Stops
/// a cache holding a handful of keys from compacting on every other write.
const COMPACT_FLOOR_BYTES: u64 = 1024 * 1024;

/// Copy rounds before a pass gives up on keys that keep being rewritten under
/// it. Those keys land above the cut by themselves; the next pass trims.
const COPY_ROUNDS: usize = 3;

impl CacheShard {
    /// Start a background pass when the log holds enough garbage and none is
    /// running. Called after a write applies; never waits for the pass.
    pub(super) fn maybe_compact(self: &Arc<Self>, index: &Index) {
        if !Self::should_compact(index) || self.compacting.swap(true, Ordering::AcqRel) {
            return;
        }
        let shard = Arc::clone(self);
        let spawned = self.compactor.spawn(async move {
            match shard.compact().await {
                Ok(()) | Err(StorageError::Closed(_)) => {}
                Err(err) => tracing::warn!(
                    shard = %shard.label, error = %err,
                    "cache compaction failed; it is retried on a later write",
                ),
            }
            shard.compacting.store(false, Ordering::Release);
        });
        if !spawned {
            self.compacting.store(false, Ordering::Release);
        }
    }

    /// True when the log holds enough garbage to be worth rewriting.
    fn should_compact(index: &Index) -> bool {
        index.log_bytes > COMPACT_FLOOR_BYTES
            && index.log_bytes > index.live_bytes.saturating_mul(COMPACT_WHEN_TIMES_LIVE)
    }

    /// One compaction pass. Returns early, having changed nothing a replay
    /// would notice, when the shard closes or the store shuts down.
    pub(super) async fn compact(&self) -> Result<()> {
        let log = self.live_log().await?;
        let cut = log.roll_now().await?;

        // Every write below the cut must have applied before the index can say
        // what is live below it. Writers already hold their offsets, so this
        // is a wait of one fsync at most.
        loop {
            {
                let mut state = self.state.lock().await;
                self.ensure_index(&mut state).await?;
                if self.sequencer.next_offset() >= cut {
                    break;
                }
            }
            if self.compactor.stopping() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        for _ in 0..COPY_ROUNDS {
            let below = self.live_below(cut).await?;
            if below.is_empty() {
                break;
            }
            for batch in below.chunks(COPY_BATCH) {
                if !self.copy_forward(&log, batch).await? {
                    return Ok(());
                }
            }
        }

        {
            let mut state = self.state.lock().await;
            self.ensure_index(&mut state).await?;
            let now = now_millis();
            if state
                .index
                .entries
                .values()
                .any(|entry| entry.offset < cut && !entry.is_expired(now))
            {
                return Ok(());
            }
            // Expired entries below the cut are exactly what compaction is
            // for: reclaimed with their segments rather than copied.
            let index = &mut state.index;
            let expired: Vec<String> = index
                .entries
                .iter()
                .filter(|(_, entry)| entry.offset < cut)
                .map(|(key, _)| key.clone())
                .collect();
            for key in expired {
                if let Some(entry) = index.entries.remove(&key) {
                    index.live_bytes -= entry.bytes;
                }
            }
        }

        // The copies must be on the device before the originals leave it,
        // whatever the fsync mode says about ordinary writes.
        log.sync().await?;
        let removed = log.trim_before(cut).await?;
        let reclaimed: u64 = removed.iter().map(payload_bytes).sum();
        let mut state = self.state.lock().await;
        state.index.log_bytes = state.index.log_bytes.saturating_sub(reclaimed);
        Ok(())
    }

    async fn live_log(&self) -> Result<DiskLog> {
        let state = self.state.lock().await;
        if state.closed {
            return Err(StorageError::Closed(self.label.clone()));
        }
        Ok(state.log.clone())
    }

    /// Live, unexpired entries whose record sits below `cut`.
    pub(super) async fn live_below(&self, cut: Offset) -> Result<Vec<(String, Entry)>> {
        let mut state = self.state.lock().await;
        self.ensure_index(&mut state).await?;
        let now = now_millis();
        Ok(state
            .index
            .entries
            .iter()
            .filter(|(_, entry)| entry.offset < cut && !entry.is_expired(now))
            .map(|(key, entry)| (key.clone(), *entry))
            .collect())
    }

    /// Re-append one batch of live records at the tail. False when the pass
    /// should stop: the store is shutting down or the shard closed.
    pub(super) async fn copy_forward(
        &self,
        log: &DiskLog,
        batch: &[(String, Entry)],
    ) -> Result<bool> {
        // Read without the lock: records are never rewritten, so the bytes at
        // an offset cannot change, and the staging step below checks the
        // index still points there.
        let mut copies: Vec<(String, Entry, Bytes)> = Vec::with_capacity(batch.len());
        let mut cost = 0;
        for (key, entry) in batch {
            if let Some(value) = Self::read_from(log, &self.label, *entry).await? {
                cost += 2 * entry.bytes;
                copies.push((key.clone(), *entry, value));
            }
        }
        if copies.is_empty() {
            return Ok(true);
        }
        if !self.compactor.spend(cost).await {
            return Ok(false);
        }

        let (pending, turn, ops) = {
            let mut state = self.state.lock().await;
            if state.closed {
                return Ok(false);
            }
            self.ensure_index(&mut state).await?;
            // A key with a write staged but not applied is skipped: the copy
            // would land after that write and undo it on replay. The write
            // itself lands above the cut, so the key needs no copy.
            let kept: Vec<_> = {
                let in_flight = self.keys_in_flight.lock();
                copies
                    .into_iter()
                    .filter(|(key, entry, _)| {
                        let current = state.index.entries.get(key).map(|e| e.offset);
                        current == Some(entry.offset) && !in_flight.contains_key(key)
                    })
                    .collect()
            };
            let now = now_millis() * 1000;
            let mut ops = Vec::with_capacity(kept.len());
            let mut records = Vec::with_capacity(kept.len());
            for (key, entry, value) in kept {
                let op = CacheOp::Put {
                    key,
                    value,
                    expires_at_millis: entry.expires_at_millis,
                };
                let payload = op.encode();
                ops.push((op, payload.len() as u64));
                records.push(AppendRecord {
                    payload,
                    timestamp_micros: now,
                    mark: Default::default(),
                });
            }
            if records.is_empty() {
                return Ok(true);
            }
            let (pending, turn) = state.log.append_claimed(&records, &self.sequencer).await?;
            state.sequenced_through = Some(pending.last_offset() + 1);
            (pending, turn, ops)
        };

        log.commit(&pending).await?;
        let _ = turn.wait().await;
        let mut state = self.state.lock().await;
        // Not reported to the observer: a copy moves where a value lives, not
        // what it is, and a watcher told about it would see a phantom write.
        for (offset, (op, bytes)) in (pending.first_offset()..).zip(&ops) {
            Self::apply_op(&mut state, op, offset, *bytes);
        }
        drop(state);
        drop(turn);
        Ok(true)
    }
}

//! Collapsing a counter shard's deltas into checkpoints, off the write path.
//!
//! The same shape as the cache's pass: seal the active segment to fix a cut,
//! append a checkpoint at the tail for every key whose fold still reaches
//! below the cut, then delete the sealed segments below it. A checkpoint is
//! the fold restated, so a crash anywhere leaves a log that replays to the
//! same sums, and the offset space only ever grows.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::record::CounterOp;
use super::{CounterShard, Index, now_micros};
use crate::compaction::{COPY_BATCH, payload_bytes};
use crate::log::AppendRecord;
use crate::{Result, StorageError};

/// How much larger than its live bytes a log may grow before it is compacted.
/// The same proportional rule the cache uses, for the same reason: cost scales
/// with the garbage, and a mostly-live log is never compacted.
const COMPACT_WHEN_TIMES_LIVE: u64 = 4;

/// Below this there is nothing worth reclaiming, whatever the ratio says.
const COMPACT_FLOOR_BYTES: u64 = 64 * 1024;

impl CounterShard {
    /// Start a background pass when the log holds enough garbage and none is
    /// running. Never waits for the pass.
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
                    "counter compaction failed; it is retried on a later add",
                ),
            }
            shard.compacting.store(false, Ordering::Release);
        });
        if !spawned {
            self.compacting.store(false, Ordering::Release);
        }
    }

    fn should_compact(index: &Index) -> bool {
        index.log_bytes > COMPACT_FLOOR_BYTES
            && index.log_bytes > index.live_bytes.saturating_mul(COMPACT_WHEN_TIMES_LIVE)
    }

    /// One compaction pass. Returns early, having changed nothing a replay
    /// would notice, when the shard closes or the store shuts down.
    pub(super) async fn compact(&self) -> Result<()> {
        let log = {
            let state = self.state.lock().await;
            if state.closed {
                return Err(StorageError::Closed(self.label.clone()));
            }
            state.log.clone()
        };
        // An add holds the shard lock until its record is folded, so once the
        // roll returns every record below the cut is in the index.
        let cut = log.roll_now().await?;

        let behind: Vec<String> = {
            let mut state = self.state.lock().await;
            self.ensure_index(&mut state).await?;
            state
                .index
                .entries
                .iter()
                .filter(|(_, entry)| entry.since < cut)
                .map(|(key, _)| key.clone())
                .collect()
        };
        for batch in behind.chunks(COPY_BATCH) {
            // Priced before the lock is taken, so the lock is never held
            // across a wait for budget.
            let cost: u64 = batch.iter().map(|key| 2 * (key.len() as u64 + 16)).sum();
            if !self.compactor.spend(cost).await {
                return Ok(());
            }
            let pending = {
                let mut state = self.state.lock().await;
                if state.closed {
                    return Ok(());
                }
                self.ensure_index(&mut state).await?;
                let ops: Vec<CounterOp> = batch
                    .iter()
                    .filter_map(|key| {
                        let entry = state.index.entries.get(key)?;
                        (entry.since < cut).then(|| CounterOp::Checkpoint {
                            key: key.clone(),
                            sum: entry.sum,
                        })
                    })
                    .collect();
                if ops.is_empty() {
                    continue;
                }
                let now = now_micros();
                let records: Vec<AppendRecord> = ops
                    .iter()
                    .map(|op| AppendRecord {
                        payload: op.encode(),
                        timestamp_micros: now,
                        mark: Default::default(),
                    })
                    .collect();
                let bytes: u64 = records.iter().map(|r| r.payload.len() as u64).sum();
                // Staged under the lock and folded at once: the sum each
                // checkpoint states is the fold through every earlier record,
                // and an add after it lands after it. The flush is outside.
                let pending = state.log.append_pending(&records).await?;
                state.index.log_bytes += bytes;
                if state.index.covered_through.is_some() {
                    state.index.covered_through = Some(pending.last_offset() + 1);
                }
                for (offset, op) in (pending.first_offset()..).zip(ops) {
                    Self::fold(&mut state.index, op, offset);
                }
                pending
            };
            log.commit(&pending).await?;
        }

        {
            let mut state = self.state.lock().await;
            self.ensure_index(&mut state).await?;
            if state.index.entries.values().any(|entry| entry.since < cut) {
                return Ok(());
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
}

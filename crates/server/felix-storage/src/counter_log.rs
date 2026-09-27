//! A counter that is a log.
//!
//! `add` appends a signed delta record; an in-memory index holds the running
//! sum per key, rebuilt by folding the log; `get` reads the index. This is the
//! projection model the consumer-group cursors proved out — fold-over-log,
//! derived state never trusted — with addition as the fold and a checkpoint as
//! the compacted form. See `docs/projections.md` for the design.
//!
//! Deliberately beside the cache rather than inside it. A counter record is a
//! new durable shape, and mixing it into the cache's own logs would make every
//! one of those logs unreadable to a build that predates counters — including
//! a replica promoted mid-upgrade, whose *puts* would become collateral. In a
//! store of its own, the new shape's blast radius is the counters.
//!
//! Everything the cache's log bought, this inherits by construction: crash
//! safety, group commit, offsets that never rewind across compaction, and
//! replication that ships records at their offsets.

mod record;

use record::CounterOp;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::Mutex;

use crate::disk_log::{DiskLog, layout};
use crate::log::{AppendOnlyLog, AppendRecord, LogConfig, Offset, ReadRange, ShardKey};
use crate::log_swap::{recover_interrupted_swap, swap_in_compacted};
use crate::shard_slots::ShardSlots;
use crate::{Corruption, CorruptionKind, Result, StorageError};

/// How much larger than its live bytes a log may grow before it is compacted.
/// The same proportional rule the cache uses, for the same reason: cost scales
/// with the garbage, and a mostly-live log is never compacted.
const COMPACT_WHEN_TIMES_LIVE: u64 = 4;

/// Below this there is nothing worth reclaiming, whatever the ratio says.
const COMPACT_FLOOR_BYTES: u64 = 64 * 1024;

/// How much of the log one fold or compaction pass reads at a time.
const SCAN_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Every counter this broker holds, on a root of its own.
#[derive(Debug)]
pub struct CounterStore {
    root: PathBuf,
    config: LogConfig,
    shards: ShardSlots<CounterId, Arc<CounterShard>>,
}

impl CounterStore {
    /// Open the counter store rooted at `root`.
    ///
    /// Callers pass a root of the counters' own — not the cache's and not the
    /// streams' — so a counter scope and a cache of the same name can never
    /// interleave their records in one directory.
    pub fn open(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(StorageError::Io)?;
        Ok(Self {
            root,
            config,
            shards: ShardSlots::new(),
        })
    }

    /// Apply a signed delta and report the new sum and the record's offset.
    ///
    /// The read-fold-append runs under the shard lock, which is what makes
    /// the answer the sum *including* this delta rather than a racy neighbour
    /// of it.
    pub async fn add(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
        key: &str,
        delta: i64,
    ) -> Result<(i64, Offset)> {
        let shard = self.shard(tenant_id, namespace, scope, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let offset = shard
            .write(
                &mut state,
                CounterOp::Delta {
                    key: key.to_string(),
                    delta,
                },
            )
            .await?;
        let sum = state.index.entries.get(key).map_or(0, |entry| entry.sum);
        if CounterShard::should_compact(&state.index) {
            shard.compact(&mut state).await?;
        }
        Ok((sum, offset))
    }

    /// The current sum, or `None` for a counter nothing has ever touched.
    ///
    /// `None` rather than zero, because they are different answers: a counter
    /// whose deltas cancelled out to zero exists, and one that was never
    /// written does not.
    pub async fn get(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<i64>> {
        let shard = self.shard(tenant_id, namespace, scope, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        Ok(state.index.entries.get(key).map(|entry| entry.sum))
    }

    /// The log backing one counter shard, for replication.
    ///
    /// Same contract as the cache's: fetch it per pass rather than holding it,
    /// because compaction swaps the directory underneath a held handle.
    pub async fn shard_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
    ) -> Result<DiskLog> {
        Ok(self
            .shard(tenant_id, namespace, scope, shard)?
            .current_log()
            .await)
    }

    /// [`CounterStore::shard_log`], created at `base_offset` when absent — for
    /// a follower given a shard whose early history is already compacted away.
    pub async fn shard_log_at(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<DiskLog> {
        Ok(self
            .shard_with_base(tenant_id, namespace, scope, shard, Some(base_offset))?
            .current_log()
            .await)
    }

    /// Close one counter shard's log and forget it, for a shard this broker
    /// no longer holds. Same contract as `LogCache::close_shard`: flushed
    /// first, racing callers get [`StorageError::Closed`], and the next touch
    /// recovers it afresh.
    pub async fn close_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
    ) -> Result<()> {
        let id = (
            tenant_id.to_string(),
            namespace.to_string(),
            scope.to_string(),
            shard,
        );
        self.shards
            .close(&id, |shard: Arc<CounterShard>| async move {
                let mut state = shard.state.lock().await;
                state.closed = true;
                state.log.close().await
            })
            .await
    }

    /// Flush every open shard. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        let shards = self.shards.open_values();
        for shard in shards {
            shard.state.lock().await.log.shutdown().await?;
        }
        Ok(())
    }

    fn shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
    ) -> Result<Arc<CounterShard>> {
        self.shard_with_base(tenant_id, namespace, scope, shard, None)
    }

    fn shard_with_base(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
        base_offset: Option<u64>,
    ) -> Result<Arc<CounterShard>> {
        let id = (
            tenant_id.to_string(),
            namespace.to_string(),
            scope.to_string(),
            shard,
        );
        let key = || ShardKey {
            tenant: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: scope.to_string(),
            shard,
        };
        self.shards.get_or_open(
            &id,
            || {
                let key = key();
                let dir = layout::shard_dir(&self.root, &key);
                let label = layout::shard_label(&key);
                recover_interrupted_swap(&dir)?;
                let log = match base_offset {
                    Some(base) => {
                        DiskLog::open_at(dir.clone(), label.clone(), self.config.clone(), base)?
                    }
                    None => DiskLog::open(dir.clone(), label.clone(), self.config.clone())?,
                };
                Ok(Arc::new(CounterShard {
                    dir,
                    label,
                    config: self.config.clone(),
                    state: Mutex::new(ShardState {
                        log,
                        index: Index::default(),
                        closed: false,
                    }),
                }))
            },
            || StorageError::Closed(layout::shard_label(&key())),
        )
    }
}

/// Tenant, namespace, scope, shard — each one a separate log in a separate
/// directory, exactly as the cache lays its shards out.
type CounterId = (String, String, String, u32);

/// One shard's log and the sum folded from it.
struct CounterShard {
    dir: PathBuf,
    label: String,
    config: LogConfig,
    /// Held across a write and across compaction, exactly as the cache holds
    /// its shard lock: a counter write is serialised here anyway, and the
    /// read-fold-append of `add` has to be atomic or two adds could both fold
    /// from the same starting sum.
    state: Mutex<ShardState>,
}

impl CounterShard {
    async fn current_log(&self) -> DiskLog {
        self.state.lock().await.log.clone()
    }

    /// Rebuild or catch up the fold by replaying the log.
    ///
    /// Catches up to the tail rather than building once and trusting itself:
    /// records reach this log without going through `add` when a follower is
    /// shipped them, and it may later be promoted and asked for the sum — the
    /// same rule the cache index follows, for the same reason.
    async fn ensure_index(&self, state: &mut ShardState) -> Result<()> {
        // Every operation passes through here under the lock, compaction
        // included, so this is the one check a closed shard needs.
        if state.closed {
            return Err(StorageError::Closed(self.label.clone()));
        }
        let tail = state.log.tail_offset().await?;
        let resume = state.index.covered_through;
        if resume == Some(tail) {
            return Ok(());
        }
        let (mut offset, mut index) = match resume {
            Some(covered) => (covered, std::mem::take(&mut state.index)),
            None => (state.log.base_offset(), Index::default()),
        };
        while offset < tail {
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
                index.log_bytes += record.payload.len() as u64;
                let op = CounterOp::decode(&record.payload)
                    .map_err(|err| StorageError::Corruption(err.in_shard(&self.label)))?;
                Self::fold(&mut index, op);
                offset = record.offset + 1;
            }
        }
        index.covered_through = Some(tail.max(offset));
        state.index = index;
        Ok(())
    }

    /// Fold one record into the index. The whole semantic is this function.
    fn fold(index: &mut Index, op: CounterOp) {
        let (key, sum) = match op {
            CounterOp::Delta { key, delta } => {
                let current = index.entries.get(&key).map_or(0, |entry| entry.sum);
                (key, current.saturating_add(delta))
            }
            // A checkpoint replaces the fold so far: it *is* the collapsed
            // history, which is what lets compaction reclaim the deltas
            // without the sum moving.
            CounterOp::Checkpoint { key, sum } => (key, sum),
        };
        let checkpoint_bytes = CounterOp::Checkpoint {
            key: key.clone(),
            sum,
        }
        .encode()
        .len() as u64;
        if let Some(previous) = index.entries.insert(
            key,
            Entry {
                sum,
                checkpoint_bytes,
            },
        ) {
            index.live_bytes -= previous.checkpoint_bytes;
        }
        index.live_bytes += checkpoint_bytes;
    }

    /// Append one record and fold it, reporting its offset.
    async fn write(&self, state: &mut ShardState, op: CounterOp) -> Result<Offset> {
        let payload = op.encode();
        let bytes = payload.len() as u64;
        let appended = state
            .log
            .append(&[AppendRecord {
                payload,
                timestamp_micros: now_micros(),
                mark: Default::default(),
            }])
            .await?;
        state.index.log_bytes += bytes;
        if state.index.covered_through.is_some() {
            state.index.covered_through = Some(appended.first_offset + 1);
        }
        Self::fold(&mut state.index, op);
        Ok(appended.first_offset)
    }

    fn should_compact(index: &Index) -> bool {
        index.log_bytes > COMPACT_FLOOR_BYTES
            && index.log_bytes > index.live_bytes.saturating_mul(COMPACT_WHEN_TIMES_LIVE)
    }

    /// Collapse the applied deltas into one checkpoint per key and swap the
    /// fresh log in.
    ///
    /// **The offset space continues.** The checkpoints are appended starting
    /// at the current tail, never renumbered from zero: replication ships
    /// records at their offsets, so a leader that renumbered would make its
    /// offset 0 a different record from every follower's. The observable sum
    /// does not move either — a checkpoint is the fold, restated. Both are
    /// regression-tested.
    async fn compact(&self, state: &mut ShardState) -> Result<()> {
        let staging = self.dir.with_extension("compacting");
        if staging.exists() {
            // Left by a crash mid-compaction. Never swapped in, so it holds
            // nothing the current log does not.
            std::fs::remove_dir_all(&staging).map_err(StorageError::Io)?;
        }
        let resume_at = state.log.tail_offset().await?;
        let fresh = DiskLog::open_at(
            staging.clone(),
            self.label.clone(),
            self.config.clone(),
            resume_at,
        )?;

        let mut index = Index::default();
        for (key, entry) in &state.index.entries {
            let payload = CounterOp::Checkpoint {
                key: key.clone(),
                sum: entry.sum,
            }
            .encode();
            let bytes = payload.len() as u64;
            fresh
                .append(&[AppendRecord {
                    payload,
                    timestamp_micros: now_micros(),
                    mark: Default::default(),
                }])
                .await?;
            index.entries.insert(
                key.clone(),
                Entry {
                    sum: entry.sum,
                    checkpoint_bytes: bytes,
                },
            );
            index.live_bytes += bytes;
            index.log_bytes += bytes;
        }
        fresh.shutdown().await?;
        state.log.shutdown().await?;
        crate::disk_log::replica_state::copy_into(&self.dir, &staging)?;

        swap_in_compacted(&self.dir, &staging)?;

        state.log = DiskLog::open(self.dir.clone(), self.label.clone(), self.config.clone())?;
        index.covered_through = Some(state.log.tail_offset().await?);
        state.index = index;
        Ok(())
    }
}

impl std::fmt::Debug for CounterShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CounterShard")
            .field("label", &self.label)
            .finish()
    }
}

struct ShardState {
    log: DiskLog,
    index: Index,
    /// Set by `CounterStore::close_shard`. A caller that found this shard
    /// before the close must not touch the files: they may belong to a newer
    /// open.
    closed: bool,
}

/// The fold over one shard's log, and the accounting compaction needs.
#[derive(Debug, Default)]
struct Index {
    entries: HashMap<String, Entry>,
    /// Bytes one checkpoint per live key would occupy.
    live_bytes: u64,
    /// Bytes appended since the log was last compacted, live or not.
    log_bytes: u64,
    /// The offset this index has folded up to. `None` means nothing read yet.
    covered_through: Option<u64>,
}

/// One counter's state in the index.
#[derive(Debug, Clone, Copy)]
struct Entry {
    /// The fold over every record for this key up to `covered_through`.
    sum: i64,
    /// What one checkpoint for this key costs on disk, for deciding when to
    /// compact: the live set is exactly one checkpoint per key.
    checkpoint_bytes: u64,
}

/// The eight bytes a forwarded counter answer travels as.
///
/// Big-endian, matching the wire's integer convention; a sum crosses brokers
/// inside the cache-forward envelope's value bytes, and both ends must agree.
pub fn encode_sum(sum: i64) -> Bytes {
    Bytes::copy_from_slice(&sum.to_be_bytes())
}

/// Read back what [`encode_sum`] wrote, refusing anything else.
pub fn decode_sum(bytes: &[u8]) -> Result<i64> {
    let raw: [u8; 8] = bytes.try_into().map_err(|_| {
        StorageError::Corruption(Corruption::new(CorruptionKind::CounterRecord {
            detail: "forwarded sum is not eight bytes",
            found: bytes.len() as u64,
        }))
    })?;
    Ok(i64::from_be_bytes(raw))
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;

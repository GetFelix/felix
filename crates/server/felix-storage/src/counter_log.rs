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

mod compaction;
pub(crate) mod record;

use record::CounterOp;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::Mutex;

use crate::compaction::Compactor;
use crate::disk_log::{DiskLog, layout};
use crate::log::{AppendOnlyLog, AppendRecord, LogConfig, Offset, ReadRange, ShardKey};
use crate::shard_slots::ShardSlots;
use crate::{Corruption, CorruptionKind, Result, StorageError};

/// How much of the log one fold or compaction pass reads at a time.
const SCAN_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Every counter this broker holds, on a root of its own.
#[derive(Debug)]
pub struct CounterStore {
    root: PathBuf,
    config: LogConfig,
    shards: ShardSlots<CounterId, Arc<CounterShard>>,
    compactor: Arc<Compactor>,
    /// Which incarnation of each cache's counters to open, as the cache's.
    incarnations: crate::incarnation::Incarnations,
    dirs: Arc<crate::incarnation::DirIndex>,
}

impl CounterStore {
    /// Open the counter store rooted at `root`.
    ///
    /// Callers pass a root of the counters' own — not the cache's and not the
    /// streams' — so a counter scope and a cache of the same name can never
    /// interleave their records in one directory.
    pub fn open(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        let root = root.into();
        crate::io::create_dir_all_durable(&root).map_err(StorageError::Io)?;
        // Whatever a delete left before a crash or restart.
        crate::incarnation::purge_in_background(root.clone());
        Ok(Self {
            root,
            config,
            shards: ShardSlots::new(),
            compactor: Arc::new(Compactor::from_env()),
            incarnations: Default::default(),
            dirs: Default::default(),
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
        let key = key.to_string();
        // Run to the end even if the caller stops waiting: a delta in the log
        // must be in the sum too.
        crate::task::run_to_end(async move {
            let mut state = shard.state.lock().await;
            shard.ensure_index(&mut state).await?;
            let offset = shard
                .write(
                    &mut state,
                    CounterOp::Delta {
                        key: key.clone(),
                        delta,
                    },
                )
                .await?;
            let sum = state.index.entries.get(&key).map_or(0, |entry| entry.sum);
            shard.maybe_compact(&state.index);
            Ok((sum, offset))
        })
        .await
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
    /// because closing the shard replaces it, and compaction trims its head.
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
            .shard_with_base(tenant_id, namespace, scope, shard, Some(base_offset), None)?
            .current_log()
            .await)
    }

    /// [`CounterStore::shard_log`], or [`CounterStore::shard_log_at`] with a
    /// base, only when `incarnation` is the one set for `scope`: refused with
    /// [`StorageError::Closed`] otherwise, before anything is created.
    pub async fn shard_log_for(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
        base_offset: Option<u64>,
        incarnation: u64,
    ) -> Result<DiskLog> {
        Ok(self
            .shard_with_base(
                tenant_id,
                namespace,
                scope,
                shard,
                base_offset,
                Some(incarnation),
            )?
            .current_log()
            .await)
    }

    /// Open `scope`'s counters at `incarnation` from here on, or nothing with
    /// `None`. See [`crate::LogCache::set_incarnation`].
    pub fn set_incarnation(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        incarnation: Option<u64>,
    ) {
        match incarnation {
            Some(incarnation) => self
                .incarnations
                .set(tenant_id, namespace, scope, incarnation),
            None => self.incarnations.retire(tenant_id, namespace, scope),
        }
    }

    /// The incarnation set for `scope`, or `None` when none is set or it was
    /// retired.
    pub fn incarnation(&self, tenant_id: &str, namespace: &str, scope: &str) -> Option<u64> {
        self.incarnations.registered(tenant_id, namespace, scope)
    }

    /// Open no scope's counters until [`Self::set_incarnation`] names its
    /// incarnation.
    pub fn require_registration(&self) {
        self.incarnations.require_registration();
    }

    /// Close every shard of one incarnation of `scope` and move all its
    /// directories out of service. Safe to repeat.
    pub async fn retire(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        incarnation: u64,
    ) -> Result<()> {
        self.close_incarnation(tenant_id, namespace, scope, incarnation)
            .await?;
        self.dirs
            .retire_stream_off_runtime(&self.root, tenant_id, namespace, scope, incarnation)
            .await
    }

    /// Close every shard of one incarnation of `scope`, keeping its
    /// directories.
    pub async fn close_incarnation(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        incarnation: u64,
    ) -> Result<()> {
        for (id, _) in self.shards.open_entries() {
            if id.0 == tenant_id && id.1 == namespace && id.2 == scope && id.4 == incarnation {
                self.close_id(&id).await?;
            }
        }
        Ok(())
    }

    /// The shard and incarnation of every directory of `scope` here. Runs on
    /// the blocking pool: the first lookup reads the whole root.
    pub async fn dirs_on_disk(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
    ) -> Result<Vec<(u32, u64)>> {
        let (dirs, root) = (Arc::clone(&self.dirs), self.root.clone());
        let (tenant, namespace, scope) = (
            tenant_id.to_string(),
            namespace.to_string(),
            scope.to_string(),
        );
        crate::task::blocking(move || {
            Ok(dirs
                .stream_dirs(&root, &tenant, &namespace, &scope)?
                .into_iter()
                .map(|(shard, incarnation, _)| (shard, incarnation))
                .collect())
        })
        .await
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
        for (id, _) in self.shards.open_entries() {
            if id.0 == tenant_id && id.1 == namespace && id.2 == scope && id.3 == shard {
                self.close_id(&id).await?;
            }
        }
        Ok(())
    }

    async fn close_id(&self, id: &CounterId) -> Result<()> {
        self.shards
            .close(id, |shard: Arc<CounterShard>| async move {
                let mut state = shard.state.lock().await;
                state.closed = true;
                state.log.close().await
            })
            .await
    }

    /// Drop one shard's sums, so the next touch folds its log again. For
    /// replication after it cut records from the log, which folding on from
    /// the tail would never notice.
    pub async fn forget_index(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
    ) -> Result<()> {
        let shard = self.shard(tenant_id, namespace, scope, shard)?;
        shard.state.lock().await.index = Index::default();
        Ok(())
    }

    /// Flush every open shard. Call once during graceful shutdown. Compaction
    /// passes stop at their next step first.
    pub async fn shutdown(&self) -> Result<()> {
        self.compactor.shutdown().await;
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
        self.shard_with_base(tenant_id, namespace, scope, shard, None, None)
    }

    fn shard_with_base(
        &self,
        tenant_id: &str,
        namespace: &str,
        scope: &str,
        shard: u32,
        base_offset: Option<u64>,
        expected: Option<u64>,
    ) -> Result<Arc<CounterShard>> {
        let incarnation = self
            .incarnations
            .resolve(tenant_id, namespace, scope)
            .filter(|incarnation| expected.is_none_or(|expected| expected == *incarnation))
            .ok_or_else(|| {
                StorageError::Closed(format!("{tenant_id}/{namespace}/{scope}/{shard}"))
            })?;
        let id = (
            tenant_id.to_string(),
            namespace.to_string(),
            scope.to_string(),
            shard,
            incarnation,
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
                // Again under the slot's lock, so an open racing a delete
                // cannot recreate the directory the delete moved away.
                if self.incarnations.resolve(tenant_id, namespace, scope) != Some(incarnation) {
                    return Err(StorageError::Closed(layout::shard_label(&key)));
                }
                let dir = layout::incarnation_dir(&self.root, &key, incarnation);
                let label = layout::shard_label(&key);
                self.dirs
                    .created(&layout::incarnation_dir_name(&key, incarnation));
                crate::legacy_swap::recover_legacy_swap(&dir)?;
                let log = match base_offset {
                    Some(base) => {
                        DiskLog::open_at(dir.clone(), label.clone(), self.config.clone(), base)?
                    }
                    None => DiskLog::open(dir.clone(), label.clone(), self.config.clone())?,
                };
                Ok(Arc::new(CounterShard {
                    label,
                    compactor: Arc::clone(&self.compactor),
                    compacting: Default::default(),
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

/// Tenant, namespace, scope, shard and incarnation — each one a separate log
/// in a separate directory, exactly as the cache lays its shards out.
type CounterId = (String, String, String, u32, u64);

/// One shard's log and the sum folded from it.
struct CounterShard {
    label: String,
    compactor: Arc<Compactor>,
    /// Set while a background compaction pass runs, so only one does.
    compacting: AtomicBool,
    /// Held across a write: a counter write is serialised here anyway, and the
    /// read-fold-append of `add` has to be atomic or two adds could both fold
    /// from the same starting sum. Compaction takes it only to stage a batch.
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
                if record.mark.is_generation_start() {
                    // Replication's, not a counter op; see the cache index.
                    offset = record.offset + 1;
                    continue;
                }
                index.log_bytes += record.payload.len() as u64;
                let op = CounterOp::decode(&record.payload)
                    .map_err(|err| StorageError::Corruption(err.in_shard(&self.label)))?;
                Self::fold(&mut index, op, record.offset);
                offset = record.offset + 1;
            }
        }
        index.covered_through = Some(tail.max(offset));
        state.index = index;
        Ok(())
    }

    /// Fold one record into the index. The whole semantic is this function.
    fn fold(index: &mut Index, op: CounterOp, offset: Offset) {
        let (key, sum, since) = match op {
            CounterOp::Delta { key, delta } => {
                let (current, since) = index
                    .entries
                    .get(&key)
                    .map_or((0, offset), |entry| (entry.sum, entry.since));
                (key, current.saturating_add(delta), since)
            }
            // A checkpoint replaces the fold so far: it *is* the collapsed
            // history, which is what lets compaction reclaim the deltas
            // without the sum moving.
            CounterOp::Checkpoint { key, sum } => (key, sum, offset),
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
                since,
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
                publisher: None,
            }])
            .await?;
        state.index.log_bytes += bytes;
        if state.index.covered_through.is_some() {
            state.index.covered_through = Some(appended.first_offset + 1);
        }
        Self::fold(&mut state.index, op, appended.first_offset);
        Ok(appended.first_offset)
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
    /// Payload bytes of every record still in the log, live or not.
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
    /// The first record the fold needs: this key's latest checkpoint, or its
    /// first delta. Everything before it can be trimmed without moving `sum`.
    since: Offset,
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

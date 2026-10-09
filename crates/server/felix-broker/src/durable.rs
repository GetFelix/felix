//! Durable storage for streams marked `durable: true`.
//!
//! A durable stream keeps the in-memory ring buffer — cursor replay and fanout
//! still read from it, and it is what keeps non-durable performance intact — but
//! every publish is written to a disk-backed log *before* it is fanned out or
//! acknowledged.
//!
//! ## Ordering, and why it is this way
//!
//! ```text
//!   publish → append to the durable log → (fsync, if OnCommit) → fanout → ack
//! ```
//!
//! The append comes first because the alternative is unrecoverable: a record
//! delivered to subscribers and acknowledged to the publisher, but lost in a
//! crash, is a silent hole in a log that consumers believe they have read. Paying
//! the append latency before fanout means a failed write becomes a failed
//! publish, which the publisher can retry.
//!
//! The cost is real and deliberate: a durable publish carries the storage write
//! (and under `FsyncMode::OnCommit`, a device flush) inside its latency.
//! Non-durable streams never touch this path at all.

use std::sync::Arc;

use bytes::Bytes;
use felix_storage::disk_log::{DiskLog, PendingAppend, ProducerSequence};
use felix_storage::log::{
    AppendOnlyLog, AppendRecord, AppendResult, FsyncMode, LogConfig, LogRecord, Offset, ReadRange,
    RecordMark, ShardKey,
};
use felix_storage::{CommitSequencer, CommitTurn, DiskLogProvider};

use crate::error::{BrokerError, Result};

/// The broker's handle on durable storage: one provider, many stream logs.
#[derive(Debug, Clone)]
pub struct DurableStorage {
    provider: Arc<DiskLogProvider>,
}

impl DurableStorage {
    /// Open (and recover) durable storage rooted at `root`.
    pub fn open(root: impl Into<std::path::PathBuf>, config: LogConfig) -> Result<Self> {
        let provider = DiskLogProvider::new(root, config).map_err(BrokerError::from)?;
        Ok(Self {
            provider: Arc::new(provider),
        })
    }

    /// Wrap an already constructed provider, for callers that build their own.
    pub fn from_provider(provider: Arc<DiskLogProvider>) -> Self {
        Self { provider }
    }

    pub fn root(&self) -> &std::path::Path {
        self.provider.root()
    }

    /// Bound one stream's logs by `retention`, open and future, with the
    /// broker-wide bounds for any it leaves unset.
    pub fn set_stream_retention(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        retention: felix_storage::log::Retention,
    ) -> Result<()> {
        self.provider
            .set_stream_retention(tenant, namespace, stream, retention)
            .map_err(BrokerError::from)
    }

    /// Hold one stream's logs at the commit offset, open and future, or
    /// lift the hold. See `DiskLogProvider::set_stream_retention_hold`.
    pub async fn set_stream_retention_hold(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        hold: bool,
    ) -> Result<()> {
        self.provider
            .set_stream_retention_hold(tenant, namespace, stream, hold)
            .await
            .map_err(BrokerError::from)
    }

    pub fn config(&self) -> &LogConfig {
        self.provider.config()
    }

    /// Open the log for one stream shard, recovering whatever is on disk.
    ///
    /// Repeated calls for the same shard return the same log, so re-registering
    /// a stream — which the control-plane watcher does on every restart and
    /// resync — never opens a second writer over the same files.
    /// Open a shard's log, creating it to begin at `base_offset` if it is not
    /// there yet.
    ///
    /// For a replica receiving a shard whose early history has already been
    /// trimmed everywhere: its log begins where the surviving records do. An
    /// existing shard keeps the base recorded in its own first segment.
    pub fn open_stream_at(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        base_offset: felix_storage::log::Offset,
    ) -> Result<StreamLog> {
        let key = ShardKey {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
        };
        let log = self
            .provider
            .open_shard_at(&key, base_offset)
            .map_err(BrokerError::from)?;
        Ok(StreamLog { log })
    }

    pub fn open_stream(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<StreamLog> {
        let key = ShardKey {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
        };
        let log = self.provider.open_shard(&key).map_err(BrokerError::from)?;
        Ok(StreamLog { log })
    }

    /// One stream shard's log if this broker has it open, without opening it.
    pub fn opened_stream(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Option<StreamLog> {
        let key = ShardKey {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
        };
        let log = self.provider.opened_shard(&key)?;
        Some(StreamLog { log })
    }

    /// Close one stream shard's log, for a shard this broker no longer holds.
    ///
    /// Every [`StreamLog`] already handed out for it fails from here on; the
    /// next open recovers it afresh. A no-op when it is not open.
    pub async fn close_stream(
        &self,
        tenant: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<()> {
        let key = ShardKey {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            shard,
        };
        self.provider
            .close_shard(&key)
            .await
            .map_err(BrokerError::from)
    }

    /// Flush and stop every open log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.provider.shutdown().await.map_err(BrokerError::from)
    }
}

/// One durable stream shard's log, as the broker uses it.
#[derive(Debug, Clone)]
pub struct StreamLog {
    log: DiskLog,
}

impl StreamLog {
    /// Wrap a log this handle did not open.
    ///
    /// For a cache shard, whose log belongs to the cache store rather than to
    /// the stream provider. Everything replication does — tail, read, apply,
    /// divergence — is the same work on either, so it is the same type.
    pub fn from_log(log: DiskLog) -> Self {
        Self { log }
    }

    /// Write a publish batch and return its offsets *before* waiting for
    /// durability, with its range claimed in `order`.
    ///
    /// The caller must pair this with [`StreamLog::commit`]. The split exists
    /// so the broker can claim the batch's place in the stream's commit order
    /// the instant its offsets are consumed: from that point the records are on
    /// disk holding those offsets, and every later publish queues behind them
    /// whether this one goes on to succeed, fail, or be cancelled. The claim is
    /// made where the offsets are assigned, so even a caller cancelled before
    /// this returns releases its range.
    ///
    /// Every record is stamped `timestamp_micros`, which the caller takes from
    /// [`append_time_now`] so it can report the stored time without reading
    /// it back.
    pub async fn begin_append(
        &self,
        payloads: &[Bytes],
        publishers: &[Option<Bytes>],
        timestamp_micros: u64,
        order: &Arc<CommitSequencer>,
    ) -> Result<(PendingAppend, CommitTurn<'static>)> {
        self.begin_append_marked(payloads, &[], publishers, timestamp_micros, order)
            .await
    }

    /// [`StreamLog::begin_append`] with a producer mark per record, or none
    /// when `marks` is empty. `publishers` is likewise one per record, or
    /// empty when none is recorded.
    pub async fn begin_append_marked(
        &self,
        payloads: &[Bytes],
        marks: &[RecordMark],
        publishers: &[Option<Bytes>],
        timestamp_micros: u64,
        order: &Arc<CommitSequencer>,
    ) -> Result<(PendingAppend, CommitTurn<'static>)> {
        let records = records(payloads, marks, publishers, timestamp_micros)?;
        self.log
            .append_claimed(&records, order)
            .await
            .map_err(BrokerError::from)
    }

    /// [`Self::begin_append_marked`], only if the batch starts at exactly
    /// `expected`. `Err` with the log's tail, and nothing written or claimed,
    /// otherwise. The check is made where the offsets are assigned, so it is
    /// atomic with the claim.
    // One parameter per part of a record, as `begin_append_marked` has.
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_append_if(
        &self,
        expected: Offset,
        payloads: &[Bytes],
        marks: &[RecordMark],
        publishers: &[Option<Bytes>],
        timestamp_micros: u64,
        order: &Arc<CommitSequencer>,
    ) -> Result<std::result::Result<(PendingAppend, CommitTurn<'static>), Offset>> {
        let records = records(payloads, marks, publishers, timestamp_micros)?;
        self.log
            .append_claimed_at(expected, &records, order)
            .await
            .map_err(BrokerError::from)
    }

    /// [`Self::begin_append_marked`], only if the batch starts at exactly
    /// `first_offset`. `None`, and nothing written, otherwise.
    ///
    /// `times` is each record's time, as the leader that appended it stored
    /// it, or empty to stamp every record with this broker's clock.
    pub async fn begin_append_marked_at(
        &self,
        first_offset: Offset,
        payloads: &[Bytes],
        marks: &[RecordMark],
        publishers: &[Option<Bytes>],
        times: &[u64],
    ) -> Result<Option<PendingAppend>> {
        let mut records = records(payloads, marks, publishers, append_time_now())?;
        if times.len() == records.len() {
            for (record, time) in records.iter_mut().zip(times) {
                record.timestamp_micros = *time;
            }
        }
        self.log
            .append_pending_at(first_offset, &records)
            .await
            .map_err(BrokerError::from)
    }

    /// Write the rest of a producer batch the log holds only the start of,
    /// without waiting for durability. `None`, and nothing written, when the
    /// batch is no longer the last thing in the log. See
    /// `DiskLog::continue_claimed`.
    pub async fn continue_batch(
        &self,
        producer_id: u64,
        sequence: u64,
        payloads: &[Bytes],
        publishers: &[Option<Bytes>],
        timestamp_micros: u64,
        order: &Arc<CommitSequencer>,
    ) -> Result<Option<(PendingAppend, CommitTurn<'static>)>> {
        let marks = vec![RecordMark::Continues; payloads.len()];
        let records = records(payloads, &marks, publishers, timestamp_micros)?;
        self.log
            .continue_claimed(producer_id, sequence, &records, order)
            .await
            .map_err(BrokerError::from)
    }

    /// Where an idempotent producer's batch stands in this log.
    pub fn producer_sequence(&self, producer_id: u64, sequence: u64) -> ProducerSequence {
        self.log.producer_sequence(producer_id, sequence)
    }

    /// The sequence an idempotent producer owes next in this log, or `None`
    /// when the log holds nothing from it.
    pub fn producer_next_sequence(&self, producer_id: u64) -> Option<u64> {
        self.log.producer_next_sequence(producer_id)
    }

    /// Wait until every record below `offset` is as durable as a commit would
    /// have made it.
    pub async fn wait_durable(&self, offset: Offset) -> Result<()> {
        self.log
            .wait_durable(offset)
            .await
            .map_err(BrokerError::from)
    }

    /// Wait until a batch from [`StreamLog::begin_append`] satisfies the
    /// configured fsync policy.
    pub async fn commit(&self, pending: &PendingAppend) -> Result<()> {
        self.log.commit(pending).await.map_err(BrokerError::from)
    }

    /// Persist a publish batch, returning the offsets it was assigned.
    ///
    /// Returns only once the configured durability policy is satisfied: under
    /// `FsyncMode::OnCommit` the bytes are on the device before this resolves.
    pub async fn append(&self, payloads: &[Bytes]) -> Result<AppendResult> {
        let records = records(payloads, &[], &[], append_time_now())?;
        self.log.append(&records).await.map_err(BrokerError::from)
    }

    /// Replay persisted records from `start`, bounded by `max_bytes`.
    ///
    /// A read below the base offset is reported as `CursorTooOld`, not as an
    /// opaque storage failure: "those records existed and retention discarded
    /// them" is a resume outcome the caller can act on, and it is the same
    /// condition `subscribe_from` reports up front. Translating it here is what
    /// makes a trim landing *mid-replay* surface as well, rather than a short
    /// history that looks complete.
    ///
    /// Generation-start records are left out: they are the replication
    /// protocol's, not a client's. A page is empty only at the tail, never
    /// because every record it read was one of them.
    /// A commit record is returned as its event.
    pub async fn read_from(&self, start: Offset, max_bytes: usize) -> Result<Vec<LogRecord>> {
        let mut start = start;
        loop {
            let mut records = self.read_log_from(start, max_bytes).await?;
            let Some(last) = records.last().map(|record| record.offset) else {
                return Ok(records);
            };
            records.retain(|record| !record.mark.is_generation_start());
            if !records.is_empty() {
                return Ok(records
                    .into_iter()
                    .map(crate::commit::client_record)
                    .collect());
            }
            start = last + 1;
        }
    }

    /// The first client record below `until` appended at or after
    /// `at_micros`, as `(offset, append time)`. `None` when no record below
    /// `until` is that recent; a time older than the log answers with its
    /// oldest record.
    ///
    /// A binary search, so it relies on append times rising with the offset.
    /// They are the leader's wall clock: after the clock steps back the answer
    /// is near the first such record rather than exactly it.
    pub async fn offset_for_time(
        &self,
        at_micros: u64,
        until: Offset,
    ) -> Result<Option<(Offset, u64)>> {
        let (mut low, mut high) = (self.base_offset(), until);
        while low < high {
            let mid = low + (high - low) / 2;
            match self.record_time_at(mid, until).await? {
                Some((_, time)) if time < at_micros => low = mid + 1,
                _ => high = mid,
            }
        }
        if low >= until {
            return Ok(None);
        }
        self.record_time_at(low, until).await
    }

    /// The client record at or just after `offset` and below `until`. Past a
    /// generation-start record the next one can sit at or above `until`, and
    /// that is not an answer.
    async fn record_time_at(&self, offset: Offset, until: Offset) -> Result<Option<(Offset, u64)>> {
        // One byte asks for a single record; the log returns one whatever its
        // size.
        let records = self.read_from(offset, 1).await?;
        Ok(records
            .first()
            .filter(|record| record.offset < until)
            .map(|record| (record.offset, record.timestamp_micros)))
    }

    /// [`Self::read_from`] with every record the log holds, generation-start
    /// records included. For replication, which ships and compares the log
    /// exactly as it is.
    pub async fn read_log_from(&self, start: Offset, max_bytes: usize) -> Result<Vec<LogRecord>> {
        self.log
            .read_range(ReadRange { start, max_bytes })
            .await
            .map_err(|err| match err {
                felix_storage::StorageError::Trimmed { requested, oldest } => {
                    BrokerError::CursorTooOld { oldest, requested }
                }
                other => BrokerError::from(other),
            })
    }

    /// Just past the last client record below `tail`, stepping back over the
    /// generation-start records the log ends with, but never below `floor`.
    ///
    /// Those records hold offsets no reader is ever handed, so a reader
    /// waiting to reach `tail` itself would wait for an event that never
    /// comes.
    pub async fn event_end(&self, floor: Offset, tail: Offset) -> Result<Offset> {
        let floor = floor.max(self.base_offset());
        let mut end = tail;
        while end > floor {
            let records = self.read_log_from(end - 1, 1).await?;
            match records.first() {
                Some(record) if record.offset == end - 1 && record.mark.is_generation_start() => {
                    end -= 1;
                }
                _ => break,
            }
        }
        Ok(end)
    }

    /// Append the generation-start record for `generation` at the tail,
    /// durably, and return its offset.
    ///
    /// A promoted leader writes it before it serves. Until a majority holds a
    /// record of the leader's own generation, the quorum mark may not cover the
    /// records it inherited; this one gets there without waiting for a client.
    pub async fn append_generation_start(&self, generation: u64) -> Result<Offset> {
        let record = AppendRecord {
            payload: Bytes::copy_from_slice(&generation.to_be_bytes()),
            timestamp_micros: append_time_now(),
            mark: RecordMark::GenerationStart,
            publisher: None,
        };
        let appended = self
            .log
            .append(std::slice::from_ref(&record))
            .await
            .map_err(BrokerError::from)?;
        Ok(appended.first_offset)
    }

    /// Where readers must stop once the log is poisoned: its durable offset.
    /// Past it may be a batch whose publish failed, which no reader should
    /// see. `None` while the log is healthy.
    pub fn poisoned_read_end(&self) -> Option<Offset> {
        self.log.is_poisoned().then(|| self.durable_offset())
    }

    /// How far below `tail` a reader may go and see only records that will
    /// stay. Under `FsyncMode::OnCommit` a publish completes only once its
    /// record is synced, so a record written but not synced is not one yet.
    pub fn readable_end(&self, tail: Offset) -> Offset {
        match self.log.config().fsync_mode {
            FsyncMode::OnCommit => tail.min(self.durable_offset()),
            FsyncMode::None | FsyncMode::Periodic { .. } => tail,
        }
    }

    /// Offset the next published record will take.
    pub async fn tail_offset(&self) -> Result<Offset> {
        self.log.tail_offset().await.map_err(BrokerError::from)
    }

    /// [`StreamLog::tail_offset`], after every append already started has
    /// landed, including one whose caller was cancelled.
    pub async fn settled_tail_offset(&self) -> Result<Offset> {
        self.log
            .settled_tail_offset()
            .await
            .map_err(BrokerError::from)
    }

    /// Oldest offset still on disk.
    ///
    /// Rises as retention trims the head, so this is the floor a resuming
    /// subscriber can ask for: anything below it has been discarded and must be
    /// reported rather than silently skipped.
    /// Drop every record at or after `offset`.
    ///
    /// For replication's divergence repair, which is the only caller: a
    /// follower discards an uncommitted suffix left by a leader that is gone.
    /// Bounded by the generation history — see `docs/replication-design.md`.
    pub async fn truncate(&self, offset: Offset) -> Result<()> {
        self.log.truncate(offset).await.map_err(BrokerError::from)
    }

    /// The tail as far as every record below it has been acknowledged, or
    /// may be: what the log itself has committed, before any quorum.
    ///
    /// Under `FsyncMode::OnCommit` an append is acknowledged only once it is
    /// on the device, so a record past the durable offset is still in flight
    /// and may yet fail. Under the other modes Felix acknowledges before the
    /// sync, so everything written is as committed as it will get here.
    pub(crate) async fn acknowledged_tail(&self) -> Result<Offset> {
        let tail = self.tail_offset().await?;
        Ok(match self.log.config().fsync_mode {
            FsyncMode::OnCommit => tail.min(self.durable_offset()),
            _ => tail,
        })
    }

    /// Cut this log back to `offset`, below its commit offset if need be,
    /// to put a copy back as it stood at a backup point. Offline only; see
    /// `DiskLog::restore_to`.
    pub async fn restore_to(&self, offset: Offset) -> Result<()> {
        self.log.restore_to(offset).await.map_err(BrokerError::from)
    }

    /// Discard this log and start again, empty, at `base_offset`.
    ///
    /// For a follower rebuilding a diverged copy of a shard; see
    /// `DiskLog::reset_to`.
    pub async fn rebuild_at(&self, base_offset: Offset) -> Result<()> {
        self.log
            .reset_to(base_offset)
            .await
            .map_err(BrokerError::from)
    }

    /// Note that `generation` begins at `start_offset`.
    pub fn record_generation(&self, generation: u64, start_offset: Offset) -> Result<bool> {
        self.log
            .record_generation(generation, start_offset)
            .map_err(BrokerError::from)
    }

    /// Label the records from `from` on with the generations that wrote them.
    /// See `DiskLog::label_generations`.
    pub fn label_generations(
        &self,
        from: Offset,
        generations: &[felix_storage::log::Epoch],
    ) -> Result<()> {
        self.log
            .label_generations(from, generations)
            .map_err(BrokerError::from)
    }

    /// The highest generation a leader of this shard was accepted at here.
    pub fn accepted_generation(&self) -> u64 {
        self.log.accepted_generation()
    }

    /// The leader that generation was accepted from, if one was named.
    pub fn accepted_leader(&self) -> Option<std::sync::Arc<str>> {
        self.log.accepted_leader()
    }

    /// Accept `leader` at `generation`; a raised one is on disk on return.
    /// See `DiskLog::accept_generation`.
    pub async fn accept_generation(
        &self,
        generation: u64,
        leader: Option<&str>,
    ) -> Result<felix_storage::disk_log::GenerationCheck> {
        self.log
            .accept_generation(generation, leader)
            .await
            .map_err(BrokerError::from)
    }

    /// One past the last record known committed; truncation and rebuild
    /// refuse to cut below it.
    pub fn commit_offset(&self) -> Offset {
        self.log.commit_offset()
    }

    /// Record that every record below `offset` is committed.
    pub async fn advance_commit_offset(&self, offset: Offset) -> Result<()> {
        self.log
            .advance_commit_offset(offset)
            .await
            .map_err(BrokerError::from)
    }

    /// Keep retention and compaction below the commit offset, for a log
    /// replicated under `Quorum`. Survives a restart. See
    /// `DiskLog::hold_retention_at_commit`.
    pub async fn hold_retention_at_commit(&self, hold: bool) -> Result<()> {
        self.log
            .hold_retention_at_commit(hold)
            .await
            .map_err(BrokerError::from)
    }

    /// Where each leadership generation began here, oldest first.
    pub fn generations(&self) -> Vec<felix_storage::log::Epoch> {
        self.log.generations()
    }

    pub fn base_offset(&self) -> Offset {
        self.log.base_offset()
    }

    /// Whether the shard was closed under this handle; see
    /// [`DurableStorage::close_stream`].
    pub(crate) fn is_closed(&self) -> bool {
        self.log.is_closed()
    }

    /// Run one retention pass now rather than waiting for the timer.
    ///
    /// Returns the number of segments deleted. Retention normally runs on its
    /// own schedule; this exists so an operator can reclaim space immediately,
    /// and so tests can observe a trim deterministically.
    pub async fn enforce_retention_now(&self) -> Result<usize> {
        self.log
            .enforce_retention_now()
            .await
            .map(|outcome| outcome.segments_deleted)
            .map_err(BrokerError::from)
    }

    /// Exclusive bound on offsets that survive a crash right now.
    pub fn durable_offset(&self) -> Offset {
        self.log.durable_offset()
    }

    /// Bytes written but not yet flushed to the device.
    pub fn unsynced_bytes(&self) -> u64 {
        self.log.unsynced_bytes()
    }

    /// How many flushes this log has issued. Group commit shows up as far
    /// fewer flushes than appends when appends arrive together.
    pub fn flushes(&self) -> u64 {
        self.log.flushes()
    }

    /// Force a flush regardless of the configured policy.
    pub async fn sync(&self) -> Result<()> {
        self.log.sync().await.map_err(BrokerError::from)
    }
}

/// One timestamp for the batch: the records were published together, and
/// reading the clock per record costs more than the precision is worth.
fn records(
    payloads: &[Bytes],
    marks: &[RecordMark],
    publishers: &[Option<Bytes>],
    timestamp_micros: u64,
) -> Result<Vec<AppendRecord>> {
    if payloads.is_empty() {
        return Err(BrokerError::Storage(
            "cannot append an empty publish batch".to_string(),
        ));
    }
    let marks = marks
        .iter()
        .copied()
        .chain(std::iter::repeat(RecordMark::None));
    Ok(payloads
        .iter()
        .zip(marks)
        .enumerate()
        .map(|(index, (payload, mark))| AppendRecord {
            payload: payload.clone(),
            timestamp_micros,
            mark,
            publisher: publishers.get(index).cloned().flatten(),
        })
        .collect())
}

/// The time an append made now stores: microseconds since the Unix epoch on
/// this broker's clock.
pub fn append_time_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;

//! Metric names for the durable log, in one place.
//!
//! Storage performance is not something you tune by reading code — you tune it by
//! watching where time goes. The set below is chosen so that the three questions
//! that actually come up have an answer on the dashboard:
//!
//! * *Is durability the bottleneck?* Compare `append_duration_seconds` against
//!   `sync_duration_seconds`. If sync dominates, the fsync policy is the cost.
//! * *Is group commit working?* `sync_batch_appends` is the fan-in per device
//!   flush, in records. A value near 1 under concurrent single-record load
//!   means appends are serialising on the device instead of sharing a flush.
//! * *Is the periodic policy honouring its window?* `unsynced_bytes` is the data
//!   at risk right now; `sync_lag_seconds` is how long the oldest unsynced byte
//!   has been waiting.
//!
//! Names follow the `felix_` prefix already used by the broker so they land in
//! the same Prometheus namespace.

/// Records accepted into the log.
pub const APPEND_RECORDS_TOTAL: &str = "felix_storage_append_records_total";
/// Encoded bytes written, header included.
pub const APPEND_BYTES_TOTAL: &str = "felix_storage_append_bytes_total";
/// Records per append batch. Batch size is the main lever on syscall cost.
pub const APPEND_BATCH_RECORDS: &str = "felix_storage_append_batch_records";
/// End-to-end time for an append, including any durability wait.
pub const APPEND_DURATION_SECONDS: &str = "felix_storage_append_duration_seconds";

/// Device flushes performed.
pub const SYNC_TOTAL: &str = "felix_storage_sync_total";
/// Time spent inside a single flush.
pub const SYNC_DURATION_SECONDS: &str = "felix_storage_sync_duration_seconds";
/// Records made durable by one `OnCommit` flush: the group-commit fan-in. A
/// batch of N records counts N, whether one client sent it or the broker
/// merged N publishes into one append.
pub const SYNC_BATCH_APPENDS: &str = "felix_storage_sync_batch_appends";
/// Flushes that failed. Any non-zero value means acknowledged durability is in
/// doubt.
pub const SYNC_FAILURES_TOTAL: &str = "felix_storage_sync_failures_total";
/// Writes and file creations refused because the device or quota was full.
/// Nothing was written by any of them.
pub const STORAGE_FULL_TOTAL: &str = "felix_storage_full_total";
/// Bytes written but not yet flushed: the data a crash would lose right now.
pub const UNSYNCED_BYTES: &str = "felix_storage_unsynced_bytes";

/// Sparse index writes that failed. Non-fatal: the index is rebuilt on the next
/// open. A persistently non-zero value means every restart is paying for a
/// full rebuild scan.
pub const INDEX_WRITE_FAILURES_TOTAL: &str = "felix_storage_index_write_failures_total";

/// Segment rollovers.
pub const SEGMENT_ROLL_TOTAL: &str = "felix_storage_segment_roll_total";
/// Rollovers started in the background, off the append path.
pub const SEGMENT_ROLL_BACKGROUND_TOTAL: &str = "felix_storage_segment_roll_background_total";
/// Background rollovers that failed and degraded the log.
pub const SEGMENT_ROLL_FAILED_TOTAL: &str = "felix_storage_segment_roll_failed_total";
/// Extensions of an active segment's block reservation that failed. Appends
/// carry on and allocate as they write.
pub const SEGMENT_RESERVE_FAILED_TOTAL: &str = "felix_storage_segment_reserve_failed_total";
/// Prepared segments discarded because the tail moved on before the swap.
pub const SEGMENT_ROLL_DISCARDED_TOTAL: &str = "felix_storage_segment_roll_discarded_total";
/// Segments currently on disk for a shard.
pub const SEGMENT_COUNT: &str = "felix_storage_segment_count";
/// Sealed segments whose file and index are open, across every log. Bounded
/// by `LogConfig::max_open_sealed_segments` per storage root.
pub const OPEN_SEALED_SEGMENTS: &str = "felix_storage_open_sealed_segments";

/// Sealed segments deleted by retention.
pub const RETENTION_SEGMENTS_DELETED_TOTAL: &str = "felix_storage_retention_segments_deleted_total";
/// Bytes reclaimed by retention.
pub const RETENTION_BYTES_RECLAIMED_TOTAL: &str = "felix_storage_retention_bytes_reclaimed_total";
/// Oldest offset still readable. Rises only when retention deletes a segment,
/// and is what `Trimmed` and `CursorTooOld` report as `oldest`.
pub const RETENTION_BASE_OFFSET: &str = "felix_storage_retention_base_offset";
/// Retention passes that failed. Non-fatal — the next pass retries — but a
/// persistently non-zero value means the log is growing unbounded.
pub const RETENTION_FAILURES_TOTAL: &str = "felix_storage_retention_failures_total";

/// Records returned by range reads.
pub const READ_RECORDS_TOTAL: &str = "felix_storage_read_records_total";
/// Payload bytes returned by range reads.
pub const READ_BYTES_TOTAL: &str = "felix_storage_read_bytes_total";
/// Time to serve a range read.
pub const READ_DURATION_SECONDS: &str = "felix_storage_read_duration_seconds";

/// Time spent validating segments at startup.
pub const RECOVERY_DURATION_SECONDS: &str = "felix_storage_recovery_duration_seconds";
/// Bytes discarded from a torn tail during recovery.
pub const RECOVERY_TRUNCATED_BYTES: &str = "felix_storage_recovery_truncated_bytes";
/// Empty segments removed at startup because a rollover never installed them.
pub const RECOVERY_ABANDONED_ROLLS_TOTAL: &str = "felix_storage_recovery_abandoned_rolls_total";
/// Indexes rebuilt because they were missing or stale.
pub const RECOVERY_INDEX_REBUILDS_TOTAL: &str = "felix_storage_recovery_index_rebuilds_total";
/// Opens (or truncations) that rebuilt producer state by reading sealed
/// segments, because the producer snapshot was missing or out of date.
pub const PRODUCER_STATE_REBUILT_TOTAL: &str = "felix_storage_producer_state_rebuilt_total";

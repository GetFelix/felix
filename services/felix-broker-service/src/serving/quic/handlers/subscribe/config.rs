//! Tunables for the subscription event writer: batching limits and flush pacing.

use std::time::Duration;

/// Configuration for the subscription event writer.
///
/// Fields are chosen to make the event writer a pure “I/O + framing” component:
/// it doesn’t need the broker, only identifiers and batching policy.
///
/// Batching behavior:
/// Writer always coalesces into a single binary EventBatch per flush. A batch
/// takes whatever is already queued and flushes at once; it waits for more,
/// up to `flush_delay`, only after the previous batch found events queued.
/// Flush triggers:
/// - `max_events`
/// - `max_bytes`
/// - nothing queued (idle), or `flush_delay` (busy)
#[derive(Clone, Copy, Debug)]
pub(crate) struct EventWriterConfig {
    /// Stable identifier for this subscription; used by the client to route events.
    pub(super) subscription_id: u64,

    /// Max number of events per flush in batch mode.
    pub(super) max_events: usize,

    /// Max total payload bytes per flush in batch mode.
    pub(super) max_bytes: usize,

    /// The most a partially-filled batch waits for more events, and only while
    /// events arrive faster than the feeder drains them.
    pub(super) flush_delay: Duration,

    /// If true, encode each payload as its own one-item EventBatch frame.
    pub(super) single_event_mode: bool,

    /// Whether this subscriber negotiated `FLAG_EVENT_BATCH_OFFSETS`.
    ///
    /// Per-subscriber, because a stream can carry subscribers of both kinds and
    /// each must receive the frame shape it agreed to. The envelope caches both
    /// encodings, so the cost is at most two per batch however many subscribers
    /// there are -- the encode-once fanout property still holds.
    pub(super) offsets_enabled: bool,

    /// Whether this subscriber also negotiated `FLAG_EVENT_BATCH_SKIPPED`, and
    /// so is told when offsets before a batch hold no event. Only with
    /// `offsets_enabled`.
    pub(super) skip_enabled: bool,

    /// Whether this subscriber offered `FEATURE_SHARD_MOVED`, and so gets a
    /// `shard_moved` frame when its shard moves away.
    pub(super) shard_moved_enabled: bool,

    /// Max number of lane commands to gather per flush.
    pub(super) flush_max_items: usize,

    /// Max delay while filling a lane flush buffer.
    pub(super) flush_max_delay: Duration,

    /// Upper bound for coalesced bytes in one write.
    pub(super) max_bytes_per_write: usize,
}

//! Per-stream state: the subscriber registry, its lock-free publish snapshot,
//! and the bounded in-memory log that backs cursor replay.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use bytes::Bytes;
use felix_storage::CommitSequencer;
use felix_storage::log::LogRecord;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::delivery::{QueuedDelivery, SubQueuePolicy};
use super::producers::ProducerTable;
use super::subscription::SubscriptionReceiver;
use crate::ConsistencyLevel;
use crate::durable::StreamLog;
use crate::handoff::{ShardHandoff, ShardMoved};

/// One stream shard's live state, shared by every handle to it.
#[derive(Debug)]
pub(crate) struct StreamState {
    pub(crate) handle_id: u64,
    pub(crate) active: AtomicBool,
    // Snapshot used by publish hot path: lock-free read, no per-publish allocation.
    pub(crate) subscribers_snapshot: ArcSwap<Vec<SubscriberEntry>>,
    // Inner registry mutated only on subscribe/unsubscribe paths.
    pub(crate) subscribers: Mutex<SubscriberRegistry>,
    // In-memory log for cursor-based replay.
    pub(crate) log_state: Mutex<LogState>,
    // Per-subscriber bounded queue depth.
    pub(crate) subscriber_queue_capacity: usize,
    // Queue admission policy when subscriber queue is full.
    pub(crate) subscriber_queue_policy: SubQueuePolicy,
    // Approximate number of queued items across subscribers in this stream.
    pub(crate) queued_items: Arc<AtomicUsize>,
    // Disk-backed log, present only for streams registered as durable. The
    // in-memory ring above is kept either way: it is what cursor replay and
    // fanout read from, and dropping it for durable streams would put a file
    // read on the fanout path.
    pub(crate) durable: Option<StreamLog>,
    // Orders the post-durability half of a publish by disk offset, so cursor
    // replay and subscriber delivery agree with what is on disk. Only durable
    // streams use it; an ephemeral stream has no disk offsets to order by.
    /// `Arc` so a claim can outlive the call that made it: see
    /// `CommitSequencer::reserve_owned` and `Broker::claim_publish` (#535).
    pub(crate) commit_sequencer: Arc<CommitSequencer>,
    /// What an acknowledgement of a publish to this stream means. Carried on
    /// the state so the publish path reads it from the handle it already has,
    /// rather than looking the stream up again on the hot path.
    ///
    /// Atomic because it can change under a live stream: an operator raising a
    /// stream to `Quorum` must take effect on the next publish, not on the next
    /// broker restart. Read on the publish path, so an atomic rather than a
    /// lock.
    consistency: AtomicU8,
    /// The producers whose sequences this shard's leader remembers.
    pub(crate) producers: ProducerTable,
    /// Woken after each durable publish is committed, for readers that poll
    /// the log by offset (the Kafka listener's long-poll fetch) rather than
    /// holding a subscription.
    pub(crate) appended: Arc<tokio::sync::Notify>,
}

impl StreamState {
    pub(crate) fn new(
        handle_id: u64,
        subscriber_queue_capacity: usize,
        subscriber_queue_policy: SubQueuePolicy,
        durable: Option<StreamLog>,
        consistency: ConsistencyLevel,
    ) -> Self {
        Self {
            handle_id,
            consistency: AtomicU8::new(consistency.as_u8()),
            active: AtomicBool::new(true),
            subscribers_snapshot: ArcSwap::from_pointee(Vec::new()),
            subscribers: Mutex::new(SubscriberRegistry::default()),
            log_state: Mutex::new(LogState {
                log: VecDeque::new(),
                next_seq: 0,
            }),
            subscriber_queue_capacity,
            subscriber_queue_policy,
            queued_items: Arc::new(AtomicUsize::new(0)),
            durable,
            commit_sequencer: Arc::new(CommitSequencer::new(0)),
            producers: ProducerTable::default(),
            appended: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// What an acknowledgement of a publish to this stream currently means.
    pub(crate) fn consistency(&self) -> ConsistencyLevel {
        ConsistencyLevel::from_u8(self.consistency.load(Ordering::Acquire))
    }

    /// Apply a consistency change to a stream that is already live.
    ///
    /// Registering a stream that already exists takes a fast path that only
    /// refreshes the metadata map. Without this the live state kept whatever it
    /// was built with, so raising a stream to `Quorum` did nothing until the
    /// broker restarted — publishes carried on acknowledging on the leader
    /// alone while the catalog said a majority was required.
    pub(crate) fn set_consistency(&self, consistency: ConsistencyLevel) {
        self.consistency
            .store(consistency.as_u8(), Ordering::Release);
    }

    pub(crate) fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }

    pub(crate) fn tail_seq(&self) -> u64 {
        let state = self.log_state.lock();
        // The cursor tail points to the next sequence to be published.
        state.next_seq
    }

    /// Oldest sequence the in-memory replay ring can still serve.
    pub(crate) fn oldest_seq(&self) -> u64 {
        let state = self.log_state.lock();
        state
            .log
            .front()
            .map(|entry| entry.seq)
            .unwrap_or(state.next_seq)
    }

    /// How many subscriber slots are registered right now.
    ///
    /// `Broker::registered_subscribers` exposes it so a test can prove a
    /// *failed* subscribe left nothing behind. A stranded registration is
    /// otherwise invisible until some later publish reaps it.
    pub(crate) fn subscriber_count(&self) -> usize {
        let state = self.subscribers.lock();
        state.senders.len()
    }

    /// Refill the replay ring from a recovered durable log, and start the
    /// sequence at `next_seq`.
    ///
    /// A durable stream that recovered records from disk must not restart its
    /// cursor numbering at zero, or a subscriber resuming from a pre-restart
    /// cursor would silently receive the wrong records.
    ///
    ///
    /// Without this, a restart leaves the ring empty while `next_seq` jumps to
    /// the durable tail, so every cursor a client held from before the restart
    /// looks older than the oldest entry and `subscribe_with_cursor` answers
    /// `CursorTooOld` — even though the records are sitting on disk. Hydrating
    /// restores the same replay window the stream had before it stopped.
    ///
    /// `records` is already bounded by the caller to the ring's capacity; the
    /// ring is a fixed-size cache of the tail, not a second copy of the log.
    ///
    /// For durable streams a record's cursor sequence *is* its disk offset:
    /// both start at zero and advance by one per record, and this is what keeps
    /// them in step across a restart. The entries are therefore seeded with
    /// their on-disk offsets rather than renumbered.
    pub(crate) fn hydrate(&self, records: Vec<LogRecord>, next_seq: u64, capacity: usize) {
        let mut state = self.log_state.lock();
        // Only ever seeds a fresh stream. A re-registration of a live stream
        // must not disturb a ring that is already serving cursors.
        if !state.log.is_empty() {
            return;
        }
        for record in records {
            if record.offset >= next_seq {
                continue;
            }
            state.log.push_back(LogEntry {
                seq: record.offset,
                payload: record.payload,
            });
        }
        let overflow = state.log.len().saturating_sub(capacity);
        if overflow > 0 {
            state.log.drain(..overflow);
        }
        state.next_seq = next_seq;
        // The commit order is keyed on disk offsets, so it has to restart from
        // the recovered tail too, or the first publish after a restart would
        // wait for an offset that has already been written.
        self.commit_sequencer.reset(next_seq);
    }

    /// Move the stream's idea of its tail up to `next_seq`.
    ///
    /// For records that reached the log without passing through this state —
    /// replication writes the shard's log directly, so a follower's `next_seq`
    /// and commit order stay at zero while its disk fills.
    ///
    /// **The commit order has to move with it.** It is keyed on disk offsets, so
    /// a publish after those writes reserves a turn past them and then waits for
    /// turns that were never taken: the first write a promoted broker accepts
    /// would hang forever. Same reasoning as recovery after a restart, which is
    /// why `hydrate` does this too.
    ///
    /// The ring is emptied rather than refilled. Refilling it from disk on
    /// every replicated batch would read the whole window each time, and a
    /// reader is served from the log when the ring has nothing — see
    /// `Broker::subscribe_from`. Kept, it would end below the records just
    /// written, and a reader from before them would get the ring and then the
    /// live edge, skipping everything in between.
    pub(crate) fn advance_to(&self, next_seq: u64) {
        let mut state = self.log_state.lock();
        if next_seq <= state.next_seq {
            return;
        }
        state.log.clear();
        state.next_seq = next_seq;
        drop(state);
        self.commit_sequencer.reset(next_seq);
    }

    /// Forget everything and continue from `next_seq`, wherever that is.
    ///
    /// For a follower whose log was just rebuilt: the ring and the sequence
    /// described records that are gone, and `advance_to` only ever moves
    /// forward.
    pub(crate) fn reset_to(&self, next_seq: u64) {
        let mut state = self.log_state.lock();
        state.log.clear();
        state.next_seq = next_seq;
        drop(state);
        self.commit_sequencer.reset(next_seq);
    }

    /// Append to the replay ring, optionally pinning the sequence numbers.
    ///
    /// `first_seq` is `Some` for durable streams, carrying the offsets the log
    /// already assigned. That pinning is what keeps a cursor's sequence number
    /// and a record's disk offset the same value.
    ///
    /// Deriving the sequence from an independent counter instead let the two
    /// drift the moment a publish did not reach the ring — a cancelled publish
    /// consumes a disk offset, so the next record would take the *next* offset
    /// on disk but the *cancelled* record's sequence in memory. The same record
    /// then answered to one cursor before a restart and a different one after,
    /// because hydration rebuilds sequences from disk offsets. Pinning removes
    /// the possibility rather than relying on every path to keep two counters
    /// in step.
    ///
    /// Returns the subscriber list that must receive this batch. The append
    /// and the capture happen under one lock on purpose. A subscriber joining is only
    /// well defined relative to a point in the log: it takes everything before
    /// that point as backlog and everything after it live. If a publish could
    /// append *after* a joining subscriber captured its backlog and then fan
    /// out to a list captured *before* that subscriber registered, the record
    /// would be in neither — silently dropped from a replay the caller believes
    /// is contiguous. Pairing the append with the fanout list, and pairing
    /// registration with the backlog snapshot (see
    /// [`StreamState::register_with_backlog`]), removes the window.
    pub(crate) fn append_batch_at(
        &self,
        payloads: &[Bytes],
        first_seq: Option<u64>,
        log_capacity: usize,
    ) -> Arc<Vec<SubscriberEntry>> {
        if payloads.is_empty() {
            return self.subscribers_snapshot.load_full();
        }

        // Hot path: one lock per publish batch (instead of per payload).
        #[cfg(feature = "perf_debug")]
        let lock_wait_start = std::time::Instant::now();
        let mut state = self.log_state.lock();
        #[cfg(feature = "perf_debug")]
        {
            let wait_ns = lock_wait_start.elapsed().as_nanos() as u64;
            metrics::histogram!("felix_perf_log_lock_wait_ns").record(wait_ns as f64);
        }
        #[cfg(debug_assertions)]
        {
            // Debug-only invariant check kept outside the append loop.
            if let Some(last) = state.log.back() {
                debug_assert!(last.seq < state.next_seq);
            }
        }

        let mut seq = first_seq.unwrap_or(state.next_seq);
        for payload in payloads {
            state.log.push_back(LogEntry {
                seq,
                payload: payload.clone(),
            });
            seq = seq.checked_add(1).expect("log sequence overflow");
        }
        // A pinned batch may start beyond `next_seq` when an earlier publish
        // consumed offsets without reaching the ring; the tail is what the next
        // cursor must point at either way.
        state.next_seq = state.next_seq.max(seq);

        // Trim once after append to keep the newest `log_capacity` entries.
        let overflow = state.log.len().saturating_sub(log_capacity);
        if overflow > 0 {
            state.log.drain(..overflow);
        }

        // Captured while the log lock is still held, so any subscriber that
        // registers after this point takes these records as backlog instead.
        self.subscribers_snapshot.load_full()
    }

    /// Append with sequence numbers drawn from the ring's own counter.
    ///
    /// Only ephemeral streams use this; a durable stream pins its sequences to
    /// disk offsets via [`StreamState::append_batch_at`]. Kept for tests, which
    /// exercise the ring without a log behind it.
    #[cfg(test)]
    pub(crate) fn append_batch(&self, payloads: &[Bytes], log_capacity: usize) {
        self.append_batch_at(payloads, None, log_capacity);
    }

    pub(crate) fn register_subscriber(&self) -> (u64, SubscriptionReceiver) {
        let mut state = self.subscribers.lock();
        let (tx, rx) = mpsc::channel(self.subscriber_queue_capacity);
        let id = state.next_id;
        state.next_id += 1;
        state.senders.insert(id, tx);
        self.rebuild_subscriber_snapshot(&state);
        (id, SubscriptionReceiver::new(rx, Arc::clone(&state.moved)))
    }

    /// Register a subscriber and capture its backlog atomically.
    ///
    /// Returns `(oldest, backlog, subscriber_id, receiver)`. Holding the log
    /// lock across both halves is what makes the handoff lossless: a publish
    /// either completes before this and appears in the backlog, or happens
    /// after and is fanned out to a list that already contains this
    /// subscriber. It cannot fall between.
    pub(crate) fn register_with_backlog(
        &self,
        from_seq: u64,
    ) -> std::result::Result<(Vec<Bytes>, u64, SubscriptionReceiver), u64> {
        let state = self.log_state.lock();
        let oldest = state
            .log
            .front()
            .map(|entry| entry.seq)
            .unwrap_or(state.next_seq);
        // Checked before registering, so a rejected subscribe leaves no slot
        // behind for the publish path to discover and reap.
        if from_seq < oldest {
            return Err(oldest);
        }
        let backlog = state
            .log
            .iter()
            .filter(|entry| entry.seq >= from_seq)
            .map(|entry| entry.payload.clone())
            .collect();

        let (subscriber_id, receiver) = self.register_subscriber();
        drop(state);
        Ok((backlog, subscriber_id, receiver))
    }

    /// Register a subscriber starting as far back as the replay ring allows,
    /// reporting where that turned out to be.
    ///
    /// This is [`Self::register_with_backlog`] without the rejection. It cannot
    /// fail on a too-old cursor: it clamps to the oldest entry the ring still
    /// holds and returns that offset, so the caller learns exactly which range
    /// it must serve from disk to close the gap.
    ///
    /// The clamp has to happen *here*, under the one lock acquisition that also
    /// takes the backlog and registers the subscriber. Discovering `oldest` from
    /// a failed call and registering again would leave a window in which the
    /// ring evicts further, and the second registration would start later than
    /// the offset the caller was told to read up to -- which is precisely the
    /// gap this whole path exists to prevent.
    ///
    /// Returns `(backlog, backlog_start, subscriber_id, receiver)`, where
    /// `backlog_start` is the offset of the first backlog entry. Everything from
    /// `backlog_start` onward is either in `backlog` or will arrive on
    /// `receiver`; nothing can fall between them.
    pub(crate) fn register_clamped(
        &self,
        from_seq: u64,
    ) -> (Vec<(u64, Bytes)>, u64, u64, SubscriptionReceiver) {
        let state = self.log_state.lock();
        let oldest = state
            .log
            .front()
            .map(|entry| entry.seq)
            .unwrap_or(state.next_seq);
        let start = from_seq.max(oldest);
        // Each payload keeps its sequence number. The ring is *not* guaranteed
        // contiguous: a publish that consumed disk offsets and was cancelled
        // before reaching the ring leaves a hole. Returning bare payloads made
        // the caller assume `start, start+1, start+2, ...`, which both mislabels
        // every offset after a hole and hides the missing record entirely.
        let backlog: Vec<(u64, Bytes)> = state
            .log
            .iter()
            .filter(|entry| entry.seq >= start)
            .map(|entry| (entry.seq, entry.payload.clone()))
            .collect();
        // Where the backlog *actually* begins, which is not `start` when the
        // first surviving entry sits above it.
        let backlog_start = match backlog.first() {
            Some((seq, _)) => *seq,
            // An empty ring means the live edge is `next_seq`, and that is where
            // the backlog would have started had there been one.
            None => state.next_seq.max(start),
        };

        let (subscriber_id, receiver) = self.register_subscriber();
        drop(state);
        (backlog, backlog_start, subscriber_id, receiver)
    }

    pub(crate) fn remove_subscriber(&self, id: u64) {
        let mut state = self.subscribers.lock();
        if state.senders.remove(&id).is_some() {
            self.rebuild_subscriber_snapshot(&state);
        }
    }

    pub(crate) fn remove_subscribers(&self, subscriber_ids: &[u64]) {
        let mut state = self.subscribers.lock();
        let mut removed = false;
        for subscriber_id in subscriber_ids {
            removed |= state.senders.remove(subscriber_id).is_some();
        }
        if removed {
            self.rebuild_subscriber_snapshot(&state);
        }
    }

    /// Drop every subscriber's sender, so each receiver drains what is queued
    /// and then sees its channel close. Returns how many there were.
    ///
    /// The fanout snapshot holds clones of the senders, so it is emptied too;
    /// otherwise the channels would stay open until the next publish.
    ///
    /// With a `handoff`, each receiver also learns the shard moved and where
    /// to resume. `resume_from` is read under the log lock, the lock
    /// `append_batch_at` captures its fanout list under, so every batch below
    /// it was offered to these subscribers and none at or above it will be.
    /// That holds per subscriber whatever its queue dropped, which is why it
    /// is the stream's position and not what the subscriber received.
    pub(crate) fn end_subscribers(&self, handoff: Option<ShardHandoff>) -> usize {
        let log = self.log_state.lock();
        let mut state = self.subscribers.lock();
        let ended = state.senders.len();
        if let Some(to) = handoff {
            // An in-memory stream's sequence is local to this broker.
            let resume_from = self.durable.is_some().then_some(log.next_seq);
            let _ = state.moved.set(ShardMoved { resume_from, to });
            // A subscriber that registers after this, if the shard comes
            // back, must not inherit the old move.
            state.moved = Arc::default();
        }
        state.senders.clear();
        self.rebuild_subscriber_snapshot(&state);
        ended
    }

    /// The current fanout list.
    ///
    /// Test-only: the publish path takes the list from
    /// [`StreamState::append_batch_at`] under the log lock, which is what makes
    /// a joining subscriber's handoff lossless.
    #[cfg(test)]
    #[inline]
    pub(crate) fn subscriber_snapshot(&self) -> Arc<Vec<SubscriberEntry>> {
        self.subscribers_snapshot.load_full()
    }

    pub(crate) fn increment_queue_depth(&self, count: usize) {
        self.queued_items.fetch_add(count, Ordering::Relaxed);
        metrics::gauge!("felix_sub_queue_len").increment(count as f64);
        metrics::counter!("felix_sub_queue_enqueued_total").increment(count as u64);
    }

    fn rebuild_subscriber_snapshot(&self, state: &SubscriberRegistry) {
        let mut snapshot = Vec::with_capacity(state.senders.len());
        for (id, sender) in state.senders.iter() {
            snapshot.push(SubscriberEntry {
                id: *id,
                sender: sender.clone(),
            });
        }
        // The fanout reads this in order and a HashMap does not have one.
        // Delivery order across subscribers is not a guarantee, but an order
        // that reshuffles between publishes makes a fanout timing difference
        // look like a bug in whatever changed last.
        snapshot.sort_unstable_by_key(|entry| entry.id);
        self.subscribers_snapshot.store(Arc::new(snapshot));
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SubscriberEntry {
    pub(crate) id: u64,
    pub(crate) sender: mpsc::Sender<QueuedDelivery>,
}

/// The stream's live subscribers, keyed by an id that is never reused.
///
/// Reuse is the whole point of the counter. Both unregister paths work from an
/// id captured earlier — the publish fanout reaps senders it found closed, and
/// the subscription guard unregisters on drop — and neither can hold a lock
/// across that gap. If an id named a slot, a subscriber that registered in the
/// gap would inherit the slot and be unregistered in place of the one that
/// actually went away: silently unsubscribed, holding a live subscription that
/// never delivers again.
#[derive(Debug, Default)]
pub(crate) struct SubscriberRegistry {
    pub(crate) senders: HashMap<u64, mpsc::Sender<QueuedDelivery>>,
    next_id: u64,
    /// Shared with every receiver registered since the last move; set when
    /// the shard moves away.
    moved: Arc<OnceLock<ShardMoved>>,
}

#[derive(Debug)]
pub(crate) struct LogState {
    // Bounded log; oldest entries are dropped as new ones arrive.
    pub(crate) log: VecDeque<LogEntry>,
    // Next sequence number to assign.
    pub(crate) next_seq: u64,
}

#[derive(Debug)]
pub(crate) struct LogEntry {
    pub(crate) seq: u64,
    pub(crate) payload: Bytes,
}

#[cfg(test)]
mod tests;

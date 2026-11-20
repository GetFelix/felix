//! What the checker believes happened: which writes were acknowledged, which
//! are in doubt, and what the reader saw.
//!
//! A write is identified by `(producer, seq)`, numbered by the checker and
//! carried in the payload. That is separate from the broker's own producer id
//! and sequence, so the check does not depend on the mechanism it is checking.
//!
//! "Indeterminate" counts only errors that reach the checker. The SDK retries a
//! publish inside the call while it looks for the new leader, and it does not
//! report how often, so a failover usually shows as a slow publish and an
//! indeterminate count of 0. The count is not evidence that the in-doubt path
//! was exercised; the unit tests are what exercise it.
//!
//! The ledger takes every instant as an argument so tests can drive it
//! without sleeping.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// One write: the producer's index and its own sequence number.
pub(crate) type Key = (u16, u64);

/// A publish waiting this long counts as a stall.
pub(crate) const STALL_AFTER: Duration = Duration::from_millis(500);

/// An acknowledged write the reader has not delivered yet is only counted as
/// a gap once it has been acknowledged this long.
pub(crate) const READER_GRACE: Duration = Duration::from_secs(5);

/// A stretch where publishes could not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stall {
    /// Seconds into the run when the oldest waiting publish was sent.
    pub(crate) at_secs: u64,
    /// From that publish to the first acknowledgement after.
    pub(crate) blocked: Duration,
    /// From the same start to the moment no publish was left waiting.
    pub(crate) drained: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct Ledger {
    started: Instant,
    /// Publishes sent and not yet answered, by when their first attempt began.
    outstanding: HashMap<Key, Instant>,
    /// Keys whose latest attempt failed and whose outcome is unknown.
    in_doubt: HashSet<Key>,
    /// Keys that failed at least once, so an ack can be told from a recovery.
    errored: HashSet<Key>,
    acked: HashMap<Key, Instant>,
    /// Acknowledged and not yet seen by the reader.
    unread: HashMap<Key, Instant>,
    seen: HashMap<Key, u32>,
    sent: u64,
    recovered: u64,
    foreign: u64,
    stall_since: Option<Instant>,
    /// A stall was reported and the publishes behind it have not all drained,
    /// so the ones still waiting are not a second stall.
    latched: bool,
    last_stall_start: Option<Instant>,
    stalls: Vec<Stall>,
    /// Producers the run was meant to have, and how many have started.
    expected_producers: u16,
    started_producers: u16,
    /// Producers that stopped before the run did, and why.
    dead: Vec<(u16, String)>,
    /// Highest log offset any acknowledgement reported.
    max_offset: Option<u64>,
    reader_resubscribes: u64,
}

/// A frozen view of the writes at one moment, to check against a full re-read.
#[derive(Debug, Clone, Default)]
pub(crate) struct Snapshot {
    pub(crate) acked: HashSet<Key>,
    pub(crate) in_doubt: HashSet<Key>,
    pub(crate) in_flight: u64,
    pub(crate) sent: u64,
    pub(crate) recovered: u64,
    pub(crate) expected_producers: u16,
    pub(crate) started_producers: u16,
    pub(crate) dead: Vec<(u16, String)>,
    /// First offset past every acknowledged record, when acknowledgements
    /// reported offsets.
    pub(crate) read_to: Option<u64>,
}

/// What a re-read of the whole stream says about a snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Verdict {
    pub(crate) acked: u64,
    /// Acknowledged, absent from the stream.
    pub(crate) gaps: Vec<Key>,
    /// Present more than once, acknowledged or not.
    pub(crate) duplicates: Vec<Key>,
    /// In doubt at the snapshot, and in the stream.
    pub(crate) in_doubt_landed: u64,
    /// In doubt at the snapshot, and not in the stream.
    pub(crate) in_doubt_absent: u64,
    /// Of this run, never sent by any producer.
    pub(crate) unexpected: u64,
    pub(crate) expected_producers: u16,
    pub(crate) started_producers: u16,
    pub(crate) dead: Vec<(u16, String)>,
}

impl Verdict {
    /// The invariant: every acknowledged write exactly once, nothing invented.
    /// It also needs something to have been acknowledged and every producer to
    /// have started and stayed up, or a run that wrote nothing would pass.
    pub(crate) fn holds(&self) -> bool {
        self.violation().is_none() && self.failure().is_none()
    }

    /// The data was wrong.
    pub(crate) fn violation(&self) -> Option<&'static str> {
        (!self.gaps.is_empty() || !self.duplicates.is_empty() || self.unexpected > 0)
            .then_some("missing, repeated or unexpected records")
    }

    /// The run did not test anything, or stopped testing early.
    pub(crate) fn failure(&self) -> Option<String> {
        if self.started_producers < self.expected_producers {
            return Some(format!(
                "only {} of {} producers started",
                self.started_producers, self.expected_producers
            ));
        }
        if let Some((index, why)) = self.dead.first() {
            return Some(format!("producer {index} stopped early: {why}"));
        }
        (self.acked == 0).then(|| "nothing was acknowledged".to_string())
    }
}

/// The counters one status line prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Counters {
    pub(crate) acked: u64,
    pub(crate) in_flight: u64,
    pub(crate) in_doubt: u64,
    pub(crate) delivered: u64,
    pub(crate) gaps: u64,
    pub(crate) duplicates: u64,
    pub(crate) producers_up: u16,
    pub(crate) producers_expected: u16,
}

impl Ledger {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            started: now,
            outstanding: HashMap::new(),
            in_doubt: HashSet::new(),
            errored: HashSet::new(),
            acked: HashMap::new(),
            unread: HashMap::new(),
            seen: HashMap::new(),
            sent: 0,
            recovered: 0,
            foreign: 0,
            stall_since: None,
            latched: false,
            last_stall_start: None,
            stalls: Vec::new(),
            expected_producers: 0,
            started_producers: 0,
            dead: Vec::new(),
            max_offset: None,
            reader_resubscribes: 0,
        }
    }

    /// A producer is about to send `key` for the first time.
    pub(crate) fn begin(&mut self, key: Key, now: Instant) {
        self.sent += 1;
        self.outstanding.insert(key, now);
    }

    /// An attempt at `key` failed with an unknown outcome.
    pub(crate) fn failed(&mut self, key: Key) {
        self.errored.insert(key);
        self.in_doubt.insert(key);
    }

    /// `key` is being sent again, under the same sequence.
    pub(crate) fn retrying(&mut self, key: Key) {
        self.in_doubt.remove(&key);
    }

    /// The broker acknowledged `key`. Returns the stall this ended, if any.
    pub(crate) fn ack(&mut self, key: Key, now: Instant) -> Option<Stall> {
        self.outstanding.remove(&key);
        self.in_doubt.remove(&key);
        if self.errored.remove(&key) {
            self.recovered += 1;
        }
        self.acked.insert(key, now);
        if !self.seen.contains_key(&key) {
            self.unread.insert(key, now);
        }
        let since = self.stall_since.take()?;
        let stall = Stall {
            at_secs: since.saturating_duration_since(self.started).as_secs(),
            blocked: now.saturating_duration_since(since),
            drained: None,
        };
        self.last_stall_start = Some(since);
        self.stalls.push(stall.clone());
        Some(stall)
    }

    /// The run is meant to have this many producers.
    pub(crate) fn expect_producers(&mut self, n: u16) {
        self.expected_producers = n;
    }

    pub(crate) fn producer_started(&mut self) {
        self.started_producers += 1;
    }

    /// A producer ended before the run did. A write it left in doubt stays
    /// in doubt, but it is no longer waiting on anything.
    pub(crate) fn producer_died(&mut self, index: u16, why: String, in_doubt: Option<Key>) {
        if let Some(key) = in_doubt {
            self.failed(key);
            self.outstanding.remove(&key);
        }
        self.dead.push((index, why));
    }

    /// An acknowledgement reported the offset the record landed at.
    pub(crate) fn landed_at(&mut self, offset: u64) {
        self.max_offset = Some(self.max_offset.map_or(offset, |max| max.max(offset)));
    }

    pub(crate) fn reader_resubscribed(&mut self) {
        self.reader_resubscribes += 1;
    }

    /// The reader delivered a record carrying `key`, or `None` for one that is
    /// not this run's.
    pub(crate) fn delivered(&mut self, key: Option<Key>) {
        let Some(key) = key else {
            self.foreign += 1;
            return;
        };
        *self.seen.entry(key).or_insert(0) += 1;
        self.unread.remove(&key);
    }

    /// Called about every 100 ms. Starts a stall when a publish has waited too
    /// long, dated from the oldest one still waiting.
    pub(crate) fn watch(&mut self, now: Instant) -> Option<Instant> {
        let oldest = self.outstanding.values().min().copied();
        let waited = oldest.map(|at| now.saturating_duration_since(at));
        if waited.is_none_or(|waited| waited < STALL_AFTER) {
            if self.latched
                && self.stall_since.is_none()
                && let (Some(start), Some(stall)) = (self.last_stall_start, self.stalls.last_mut())
            {
                stall.drained = Some(now.saturating_duration_since(start));
            }
            self.latched = false;
        }
        if self.stall_since.is_some() || self.latched {
            return None;
        }
        if waited? >= STALL_AFTER {
            self.stall_since = oldest;
            self.latched = true;
            return oldest;
        }
        None
    }

    /// Sent, not answered, and not in doubt.
    fn in_flight(&self) -> u64 {
        self.outstanding
            .keys()
            .filter(|key| !self.in_doubt.contains(key))
            .count() as u64
    }

    pub(crate) fn counters(&self, now: Instant) -> Counters {
        let in_doubt = self.in_doubt.len() as u64;
        Counters {
            acked: self.acked.len() as u64,
            in_flight: self.in_flight(),
            in_doubt,
            delivered: self.seen.len() as u64,
            gaps: self
                .unread
                .values()
                .filter(|at| now.saturating_duration_since(**at) >= READER_GRACE)
                .count() as u64,
            producers_up: self
                .started_producers
                .saturating_sub(self.dead.len() as u16),
            producers_expected: self.expected_producers,
            duplicates: self
                .seen
                .values()
                .map(|n| u64::from(n.saturating_sub(1)))
                .sum(),
        }
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            acked: self.acked.keys().copied().collect(),
            in_doubt: self.in_doubt.clone(),
            in_flight: self.in_flight(),
            sent: self.sent,
            recovered: self.recovered,
            expected_producers: self.expected_producers,
            started_producers: self.started_producers,
            dead: self.dead.clone(),
            read_to: self.max_offset.map(|max| max + 1),
        }
    }

    pub(crate) fn stalls(&self) -> &[Stall] {
        &self.stalls
    }

    pub(crate) fn reader_resubscribes(&self) -> u64 {
        self.reader_resubscribes
    }

    pub(crate) fn foreign(&self) -> u64 {
        self.foreign
    }

    /// Whether `key` is one a producer has started.
    pub(crate) fn was_sent(&self, key: Key) -> bool {
        self.acked.contains_key(&key) || self.outstanding.contains_key(&key)
    }
}

/// Judge a snapshot against how many times each key appears in a full re-read
/// of the stream taken after the snapshot.
///
/// A write in doubt is reported as landed or absent and never as lost: the
/// producer was never told it succeeded.
pub(crate) fn judge(snapshot: &Snapshot, counts: &HashMap<Key, u32>, unexpected: u64) -> Verdict {
    let mut verdict = Verdict {
        acked: snapshot.acked.len() as u64,
        unexpected,
        expected_producers: snapshot.expected_producers,
        started_producers: snapshot.started_producers,
        dead: snapshot.dead.clone(),
        ..Verdict::default()
    };
    for key in &snapshot.acked {
        if !counts.contains_key(key) {
            verdict.gaps.push(*key);
        }
    }
    for (key, n) in counts {
        if *n > 1 {
            verdict.duplicates.push(*key);
        }
    }
    for key in &snapshot.in_doubt {
        if counts.contains_key(key) {
            verdict.in_doubt_landed += 1;
        } else {
            verdict.in_doubt_absent += 1;
        }
    }
    verdict.gaps.sort_unstable();
    verdict.duplicates.sort_unstable();
    verdict
}

#[cfg(test)]
mod tests;

//! The projection: what a cache group's applied entries add up to.

use bytes::Bytes;
use imbl::{OrdMap, OrdSet};

use super::{CacheCommand, Condition, Request};

/// A key's value. Its version is the index of the put that wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Entry {
    pub(super) value: Bytes,
    pub(super) version: u64,
    /// Zero is never.
    pub(super) expires_at: u64,
}

impl Entry {
    pub(super) fn live(&self, at: u64) -> bool {
        self.expires_at == 0 || at < self.expires_at
    }
}

/// What a write answered when it was first applied. A retry gets the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Answer {
    /// A put wrote this version.
    Written { version: u64 },
    /// A delete removed this version.
    Removed { version: u64 },
    /// A delete found no unexpired value.
    Absent,
    /// The condition did not hold. The version is the key's, if it has an
    /// unexpired value.
    Refused { version: Option<u64> },
    /// A counter's sum after the add.
    Sum(i64),
    /// The request is below its producer's floor and its answer is gone.
    /// The write was not applied by this entry. If the producer follows
    /// the floor rule (an entry's floor is the lowest sequence it has no
    /// answer for), a request below the floor was applied earlier.
    Forgotten,
}

/// What one producer's applied requests answered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Producer {
    /// Answers below this sequence were dropped.
    pub(super) floor: u64,
    pub(super) answers: OrdMap<u64, Answer>,
}

impl Producer {
    /// Raise the floor to `floor`, dropping the answers below it.
    fn raise(&mut self, floor: u64) {
        if floor > self.floor {
            self.floor = floor;
            let (_, at, mut kept) = self.answers.split_lookup(&floor);
            if let Some(answer) = at {
                kept.insert(floor, answer);
            }
            self.answers = kept;
        }
    }
}

/// What a read finds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Read {
    Value {
        value: Bytes,
        version: u64,
    },
    Absent,
    /// The leader's clock says this version expired and the log does not
    /// yet. The leader commits [`CacheCommand::Expire`] for it and reads
    /// again once that is applied. Answering "absent" without it would let
    /// a later leader with a slower clock find the key present.
    Expire {
        version: u64,
    },
}

// Ordered maps throughout: a snapshot walks them, and two members must
// write the same bytes. Persistent ones, so a clone shares every node and
// costs nothing until one side writes: a snapshot is built from a clone
// while apply goes on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CacheState {
    /// The log's time: the stamp of the last stamped entry applied.
    pub(super) time: u64,
    pub(super) entries: OrdMap<String, Entry>,
    /// Every entry that expires, by when.
    pub(super) expiring: OrdSet<(u64, String)>,
    pub(super) counters: OrdMap<String, i64>,
    pub(super) producers: OrdMap<u64, Producer>,
}

/// What the leader stamps its next entry with: its clock, held up to
/// `floor` so stamps never go back along the log.
pub(crate) fn stamp(clock: u64, floor: u64) -> u64 {
    clock.max(floor)
}

/// The stamp floor of a leader. `tail` is its log above its purge point,
/// newest first, the uncommitted entries it inherited included; a blank or
/// membership entry has no stamp and is `None`. With no stamped entry
/// there, the floor is its snapshot's time.
#[cfg(test)]
pub(crate) fn floor(tail: impl IntoIterator<Item = Option<u64>>, snapshot_time: u64) -> u64 {
    tail.into_iter().flatten().next().unwrap_or(snapshot_time)
}

impl CacheState {
    /// The log's time.
    pub(crate) fn time(&self) -> u64 {
        self.time
    }

    /// Apply the entry at `index`, stamped `stamp`. Returns what a write
    /// answered, the first time or on a retry.
    pub(crate) fn apply(
        &mut self,
        index: u64,
        stamp: u64,
        command: &CacheCommand,
    ) -> Option<Answer> {
        self.time = stamp;
        let request = match command {
            CacheCommand::Expire { key, version } => {
                // A key written again since the leader looked keeps its value.
                if self
                    .entries
                    .get(key)
                    .is_some_and(|entry| entry.version == *version)
                {
                    self.remove(key);
                }
                return None;
            }
            CacheCommand::Retire { producer, floor } => {
                // The floor stays behind as a tombstone: a copy of an
                // answered request can commit after the retire, and must
                // answer forgotten rather than apply again.
                self.producers.entry(*producer).or_default().raise(*floor);
                return None;
            }
            CacheCommand::Put { request, .. }
            | CacheCommand::Delete { request, .. }
            | CacheCommand::Add { request, .. } => *request,
        };
        let Request {
            producer,
            sequence,
            floor,
        } = request;
        if let Some(known) = self.producers.get(&producer) {
            if let Some(answer) = known.answers.get(&sequence) {
                return Some(*answer);
            }
            if sequence < known.floor {
                return Some(Answer::Forgotten);
            }
        }
        let answer = self.write(index, stamp, command);
        let known = self.producers.entry(producer).or_default();
        known.answers.insert(sequence, answer);
        // Never past the request itself, whose answer was just kept.
        known.raise(floor.min(sequence));
        Some(answer)
    }

    /// The version `key` holds, if its value is unexpired at `at`. An
    /// expired value is absent to every write, as it is to a read: a
    /// snapshot leaves it out, so a member that restored one could not
    /// answer otherwise.
    fn live_version(&self, key: &str, at: u64) -> Option<u64> {
        let entry = self.entries.get(key)?;
        entry.live(at).then_some(entry.version)
    }

    fn write(&mut self, index: u64, stamp: u64, command: &CacheCommand) -> Answer {
        match command {
            CacheCommand::Add { key, delta, .. } => {
                let sum = self.counters.entry(key.clone()).or_default();
                *sum = sum.saturating_add(*delta);
                Answer::Sum(*sum)
            }
            CacheCommand::Put {
                key,
                value,
                expires_at,
                cond,
                ..
            } => {
                let here = self.live_version(key, stamp);
                let holds = match cond {
                    Condition::None => true,
                    Condition::Absent => here.is_none(),
                    Condition::Version(version) => here == Some(*version),
                };
                if !holds {
                    return Answer::Refused { version: here };
                }
                self.remove(key);
                if *expires_at != 0 {
                    self.expiring.insert((*expires_at, key.clone()));
                }
                self.entries.insert(
                    key.clone(),
                    Entry {
                        value: value.clone(),
                        version: index,
                        expires_at: *expires_at,
                    },
                );
                Answer::Written { version: index }
            }
            CacheCommand::Delete { key, version, .. } => {
                match (self.live_version(key, stamp), version) {
                    (None, None) => Answer::Absent,
                    (Some(here), None) => {
                        self.remove(key);
                        Answer::Removed { version: here }
                    }
                    (Some(here), Some(named)) if here == *named => {
                        self.remove(key);
                        Answer::Removed { version: here }
                    }
                    (here, Some(_)) => Answer::Refused { version: here },
                }
            }
            CacheCommand::Expire { .. } | CacheCommand::Retire { .. } => {
                unreachable!("not a client write")
            }
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key)
            && entry.expires_at != 0
        {
            self.expiring.remove(&(entry.expires_at, key.to_string()));
        }
    }

    /// Read `key` on a leader whose clock says `clock`. The caller has
    /// confirmed it still leads and has applied its commit index.
    ///
    /// Expiry is judged at the later of the clock and the log's time, so an
    /// idle group never serves an expired key.
    pub(crate) fn read(&self, key: &str, clock: u64) -> Read {
        let Some(entry) = self.entries.get(key) else {
            return Read::Absent;
        };
        if entry.live(clock.max(self.time)) {
            Read::Value {
                value: entry.value.clone(),
                version: entry.version,
            }
        } else if entry.live(self.time) {
            Read::Expire {
                version: entry.version,
            }
        } else {
            Read::Absent
        }
    }

    pub(crate) fn sum(&self, key: &str) -> Option<i64> {
        self.counters.get(key).copied()
    }

    /// What `request` answered, if it was applied and its answer is kept.
    pub(crate) fn answer(&self, producer: u64, sequence: u64) -> Option<Answer> {
        self.producers
            .get(&producer)?
            .answers
            .get(&sequence)
            .copied()
    }

    /// When the first value expires, if any does.
    pub(crate) fn next_expiry(&self) -> Option<u64> {
        self.expiring.get_min().map(|(expires_at, _)| *expires_at)
    }

    /// Up to `max` keys whose value has expired on a leader whose clock
    /// says `clock`, each with the version to name in its
    /// [`CacheCommand::Expire`].
    pub(crate) fn due(&self, clock: u64, max: usize) -> Vec<(String, u64)> {
        let now = clock.max(self.time);
        self.expiring
            .iter()
            .take_while(|(expires_at, _)| *expires_at <= now)
            .take(max)
            .map(|(_, key)| (key.clone(), self.entries[key].version))
            .collect()
    }
}

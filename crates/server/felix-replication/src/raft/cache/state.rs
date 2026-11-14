//! The projection: what a cache group's applied entries add up to.

use std::collections::{BTreeMap, btree_map};
use std::sync::Arc;

use bytes::Bytes;

use super::layers::{Delta, Layers, Tables};
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
    pub(super) answers: BTreeMap<u64, Answer>,
}

impl Producer {
    /// Raise the floor to `floor`, dropping the answers below it.
    pub(super) fn raise(&mut self, floor: u64) {
        if floor > self.floor {
            self.floor = floor;
            self.answers = self.answers.split_off(&floor);
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

#[derive(Debug, Default)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct CacheState {
    /// The log's time: the stamp of the last stamped entry applied.
    pub(super) time: u64,
    /// The maps, unless `layered` holds them.
    tables: Tables,
    /// When the first value in `tables` expires, so a leader's expiry tick
    /// does not descend the set twice. Unused while layered.
    next_expiry: Option<u64>,
    /// Set while a snapshot cut holds the maps. Each op checks it once and
    /// takes an out-of-line path, so steady state runs the plain map code.
    layered: Option<Box<Layered>>,
}

/// A snapshot cut holds `base`, so apply writes to `delta` until the delta
/// is folded back. See `layers.rs`.
#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
struct Layered {
    base: Arc<Tables>,
    delta: Delta,
}

/// The state as of a snapshot's last entry, held while it is encoded.
/// Apply goes on meanwhile and does not change it.
pub(crate) struct Cut {
    time: u64,
    base: Arc<Tables>,
    /// Writes not yet folded into `base` when the cut was taken.
    delta: Option<Arc<Delta>>,
}

impl Cut {
    pub(crate) fn snapshot(&self) -> Bytes {
        let maps = Layers {
            base: &self.base,
            delta: self.delta.as_deref(),
        };
        super::snapshot::encode(self.time, maps)
    }
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
                    .entry(key)
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
                self.producer_mut(*producer).raise(*floor);
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
        if let Some(answer) = self.seen(producer, sequence) {
            return Some(answer);
        }
        let answer = self.write(index, stamp, command);
        let known = self.producer_mut(producer);
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
        let entry = self.entry(key)?;
        entry.live(at).then_some(entry.version)
    }

    /// What a request already answered: its kept answer, or forgotten
    /// when it is below its producer's floor.
    fn seen(&self, producer: u64, sequence: u64) -> Option<Answer> {
        if let Some(layered) = &self.layered {
            return layered.seen(producer, sequence);
        }
        let known = self.tables.producers.get(&producer)?;
        if let Some(answer) = known.answers.get(&sequence) {
            return Some(*answer);
        }
        (sequence < known.floor).then_some(Answer::Forgotten)
    }

    fn entry(&self, key: &str) -> Option<&Entry> {
        match &self.layered {
            None => self.tables.entries.get(key),
            Some(layered) => layered.view().entry(key),
        }
    }

    fn write(&mut self, index: u64, stamp: u64, command: &CacheCommand) -> Answer {
        match command {
            CacheCommand::Add { key, delta, .. } => {
                let sum = self.counter_mut(key);
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
                self.put(
                    key,
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

    /// Set `key` to `entry`, replacing any value it had, in one lookup.
    fn put(&mut self, key: &str, entry: Entry) {
        if let Some(layered) = &mut self.layered {
            return layered.put(key, entry);
        }
        let expires_at = entry.expires_at;
        let old = self.tables.entries.insert(key.to_owned(), entry);
        let old = old.map_or(0, |old| old.expires_at);
        if old != expires_at {
            let expiring = &mut self.tables.expiring;
            if old != 0 {
                expiring.remove(&(old, key.to_owned()));
            }
            if expires_at != 0 {
                expiring.insert((expires_at, key.to_owned()));
            }
            self.expiry_moved(old, expires_at);
        }
    }

    /// Keep `next_expiry` after a value's expiry went from `old` to `new`
    /// in `tables` (zero for none).
    fn expiry_moved(&mut self, old: u64, new: u64) {
        if old != 0 && self.next_expiry == Some(old) {
            self.next_expiry = self.tables.expiring.first().map(|(at, _)| *at);
        } else if new != 0 && self.next_expiry.is_none_or(|first| new < first) {
            self.next_expiry = Some(new);
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(layered) = &mut self.layered {
            return layered.remove(key);
        }
        if let Some(entry) = self.tables.entries.remove(key)
            && entry.expires_at != 0
        {
            self.tables
                .expiring
                .remove(&(entry.expires_at, key.to_string()));
            self.expiry_moved(entry.expires_at, 0);
        }
    }

    fn counter_mut(&mut self, key: &str) -> &mut i64 {
        match &mut self.layered {
            None => self.tables.counters.entry(key.to_owned()).or_default(),
            Some(layered) => layered.counter_mut(key),
        }
    }

    fn producer_mut(&mut self, producer: u64) -> &mut Producer {
        match &mut self.layered {
            None => self.tables.producers.entry(producer).or_default(),
            Some(layered) => layered.producer_mut(producer),
        }
    }

    pub(super) fn view(&self) -> Layers<'_> {
        match &self.layered {
            None => Layers {
                base: &self.tables,
                delta: None,
            },
            Some(layered) => layered.view(),
        }
    }

    pub(super) fn from_tables(time: u64, tables: Tables) -> Self {
        Self {
            time,
            next_expiry: tables.expiring.first().map(|(at, _)| *at),
            tables,
            layered: None,
        }
    }

    /// The state as it is now, for a snapshot. It takes no copy: the maps
    /// become a frozen base the cut shares, and writes go to a delta over
    /// it until [`CacheState::fold`] puts them back. Taken while a fold is
    /// unfinished, it finishes it first, or, while an earlier cut still
    /// holds the base, copies the delta: both in proportion to the delta,
    /// which is why a build that need not happen now is deferred while
    /// the maps are [`layered`](CacheState::layered).
    pub(crate) fn cut(&mut self) -> Cut {
        if let Some(layered) = &mut self.layered
            && !layered.delta.is_empty()
            && Arc::get_mut(&mut layered.base).is_some()
        {
            while self.fold(usize::MAX) {}
        }
        let time = self.time;
        match &self.layered {
            None => {
                let base = Arc::new(std::mem::take(&mut self.tables));
                self.next_expiry = None;
                let cut = Cut {
                    time,
                    base: Arc::clone(&base),
                    delta: None,
                };
                self.layered = Some(Box::new(Layered {
                    base,
                    delta: Delta::default(),
                }));
                cut
            }
            Some(layered) => Cut {
                time,
                base: Arc::clone(&layered.base),
                delta: (!layered.delta.is_empty()).then(|| Arc::new(layered.delta.clone())),
            },
        }
    }

    /// Fold up to about `budget` of the writes made since a cut back into
    /// the base. Does nothing while a cut still holds the base: the last
    /// one to let go folds. Returns whether there is more to fold.
    pub(crate) fn fold(&mut self, budget: usize) -> bool {
        let Some(layered) = &mut self.layered else {
            return false;
        };
        let Some(tables) = Arc::get_mut(&mut layered.base) else {
            return false;
        };
        if layered.delta.fold_into(tables, budget) {
            return true;
        }
        if let Some(layered) = self.layered.take() {
            // Unique: checked above, and a cut is only taken under `&mut self`.
            self.tables = Arc::try_unwrap(layered.base).unwrap_or_else(|base| Tables::clone(&base));
            self.next_expiry = self.tables.expiring.first().map(|(at, _)| *at);
        }
        false
    }

    /// Whether the maps are layered: a cut holds them or a fold is due.
    pub(crate) fn layered(&self) -> bool {
        self.layered.is_some()
    }

    /// Read `key` on a leader whose clock says `clock`. The caller has
    /// confirmed it still leads and has applied its commit index.
    ///
    /// Expiry is judged at the later of the clock and the log's time, so an
    /// idle group never serves an expired key.
    pub(crate) fn read(&self, key: &str, clock: u64) -> Read {
        let Some(entry) = self.entry(key) else {
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
        self.view().counter(key)
    }

    /// What `request` answered, if it was applied and its answer is kept.
    pub(crate) fn answer(&self, producer: u64, sequence: u64) -> Option<Answer> {
        self.view().producer(producer)?.answer(sequence)
    }

    /// When the first value expires, if any does.
    pub(crate) fn next_expiry(&self) -> Option<u64> {
        match &self.layered {
            None => {
                debug_assert_eq!(
                    self.next_expiry,
                    self.tables.expiring.first().map(|(at, _)| *at)
                );
                self.next_expiry
            }
            Some(layered) => layered.view().expiring().next().map(|(at, _)| *at),
        }
    }

    /// Up to `max` keys whose value has expired on a leader whose clock
    /// says `clock`, each with the version to name in its
    /// [`CacheCommand::Expire`].
    pub(crate) fn due(&self, clock: u64, max: usize) -> Vec<(String, u64)> {
        let now = clock.max(self.time);
        if let Some(layered) = &self.layered {
            return layered.due(now, max);
        }
        if self.next_expiry.is_none_or(|first| first > now) {
            return Vec::new();
        }
        self.tables
            .expiring
            .iter()
            .take_while(|(expires_at, _)| *expires_at <= now)
            .take(max)
            .map(|(_, key)| (key.clone(), self.tables.entries[key].version))
            .collect()
    }
}

// Out of line and cold: only apply during a snapshot build, and the reads
// beside it, come here.
impl Layered {
    fn view(&self) -> Layers<'_> {
        Layers {
            base: &self.base,
            delta: Some(&self.delta),
        }
    }

    #[cold]
    fn seen(&self, producer: u64, sequence: u64) -> Option<Answer> {
        let known = self.view().producer(producer)?;
        if let Some(answer) = known.answer(sequence) {
            return Some(answer);
        }
        (sequence < known.floor).then_some(Answer::Forgotten)
    }

    #[cold]
    fn put(&mut self, key: &str, entry: Entry) {
        let Self { base, delta } = self;
        let expires_at = entry.expires_at;
        // A slot already in the delta holds what the key has now; `None`
        // there means the base's value is already gone. Without one the key
        // still has the base's value.
        let (old, below) = match delta.entries.entry(key.to_owned()) {
            btree_map::Entry::Occupied(mut slot) => (
                slot.get_mut()
                    .replace(entry)
                    .map_or(0, |old| old.expires_at),
                None,
            ),
            btree_map::Entry::Vacant(slot) => {
                slot.insert(Some(entry));
                let below = base.entries.get(key).map_or(0, |below| below.expires_at);
                (below, Some(below))
            }
        };
        if old != expires_at {
            // A member is the base's exactly when the base's value expires then.
            let below =
                below.unwrap_or_else(|| base.entries.get(key).map_or(0, |below| below.expires_at));
            if old != 0 {
                delta.hide_expiring(base, (old, key.to_owned()), old == below);
            }
            if expires_at != 0 {
                delta.show_expiring(base, (expires_at, key.to_owned()), expires_at == below);
            }
        }
    }

    #[cold]
    fn remove(&mut self, key: &str) {
        let Self { base, delta } = self;
        let below = base.entries.get(key);
        let removed = match delta.entries.get_mut(key) {
            Some(above) if below.is_some() => above.take(),
            Some(_) => delta.entries.remove(key).flatten(),
            None => {
                if below.is_some() {
                    delta.entries.insert(key.to_owned(), None);
                }
                below.cloned()
            }
        };
        if let Some(entry) = removed
            && entry.expires_at != 0
        {
            let in_base = below.is_some_and(|below| below.expires_at == entry.expires_at);
            delta.hide_expiring(base, (entry.expires_at, key.to_string()), in_base);
        }
    }

    #[cold]
    fn counter_mut(&mut self, key: &str) -> &mut i64 {
        let Self { base, delta } = self;
        delta
            .counters
            .entry(key.to_owned())
            .or_insert_with(|| base.counters.get(key).copied().unwrap_or_default())
    }

    #[cold]
    fn producer_mut(&mut self, producer: u64) -> &mut Producer {
        let Self { base, delta } = self;
        // Only the floor is copied: the base's answers stay where they
        // are, and those below the floor stop counting.
        delta.producers.entry(producer).or_insert_with(|| Producer {
            floor: base.producers.get(&producer).map_or(0, |known| known.floor),
            answers: BTreeMap::new(),
        })
    }

    #[cold]
    fn due(&self, now: u64, max: usize) -> Vec<(String, u64)> {
        let maps = self.view();
        maps.expiring()
            .take_while(|(expires_at, _)| *expires_at <= now)
            .take(max)
            .map(|(_, key)| match maps.entry(key) {
                Some(entry) => (key.clone(), entry.version),
                None => unreachable!("an expiring key {key} has no value"),
            })
            .collect()
    }
}

impl PartialEq for CacheState {
    /// The same contents, however each is laid out.
    fn eq(&self, other: &Self) -> bool {
        let (ours, theirs) = (self.view(), other.view());
        let same_producers = || {
            let (mut ours, mut theirs) = (ours.producers(), theirs.producers());
            loop {
                match (ours.next(), theirs.next()) {
                    (None, None) => return true,
                    (Some((a, ours)), Some((b, theirs)))
                        if a == b
                            && ours.floor == theirs.floor
                            && ours.answers().eq(theirs.answers()) => {}
                    _ => return false,
                }
            }
        };
        self.time == other.time
            && ours.entries().eq(theirs.entries())
            && ours.expiring().eq(theirs.expiring())
            && ours.counters().eq(theirs.counters())
            && same_producers()
    }
}

impl Eq for CacheState {}

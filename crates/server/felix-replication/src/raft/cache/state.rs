//! The projection: what a cache group's applied entries add up to.

use std::collections::{BTreeMap, btree_map};
use std::sync::Arc;

use bytes::Bytes;

use super::layers::{Delta, Layers, Tables};
use super::recent::{Change, MARK_MICROS, Recent, WINDOW_MICROS};
use super::size::{self, Bound};
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
    /// A put or an add that would have taken the group's values and
    /// counters past their [`Bound`]. Nothing was written, and a retry
    /// gets this answer too. Deletes and expiries free value bytes;
    /// counter bytes are never freed (#3243).
    Full,
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

/// A key's value as a cache watch reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Watched {
    pub(crate) value: Bytes,
    /// The index of the put that wrote it.
    pub(crate) version: u64,
    /// In the log's microseconds; zero is never.
    pub(crate) expires_at: u64,
}

impl Watched {
    fn of(entry: &Entry) -> Self {
        Self {
            value: entry.value.clone(),
            version: entry.version,
            expires_at: entry.expires_at,
        }
    }
}

#[derive(Debug, Default)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct CacheState {
    /// The log's time: the stamp of the last stamped entry applied.
    pub(super) time: u64,
    /// The index of the last entry folded in, or the last a restored
    /// snapshot covers. Not in a snapshot: a cache watch reads it as its
    /// live edge, under the lock apply holds.
    applied: u64,
    /// The maps, unless `layered` holds them.
    tables: Tables,
    /// When the first value in `tables` expires, so a leader's expiry tick
    /// does not descend the set twice. Unused while layered.
    next_expiry: Option<u64>,
    /// Set while a snapshot cut holds the maps. Each op checks it once and
    /// takes an out-of-line path, so steady state runs the plain map code.
    layered: Option<Box<Layered>>,
    /// The latest changes, for a watch resuming from one, kept only while
    /// the shard is watched here and for a window after. Not in a snapshot.
    recent: Option<Box<Recent>>,
    /// Until when, in the log's time, `recent` keeps recording.
    watched_until: u64,
    /// Until when, in the log's time, the applied
    /// [`CacheCommand::Watched`] entries ask every member to record. A
    /// function of the applied entries, so it is in the snapshot: a member
    /// that opens from one inside a mark's life records from there.
    pub(super) marked_until: u64,
    /// The snapshot bytes of every value and counter held, expired values
    /// included.
    data: u64,
    /// The part of `data` that is values expired by `swept`.
    expired: u64,
    /// Values that expire at or before this are counted in `expired`.
    /// Stamps never go back, so each value is swept in once.
    swept: u64,
    bound: Bound,
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
    marked_until: u64,
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
        super::snapshot::encode(self.time, self.marked_until, maps)
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
        self.apply_reporting(index, stamp, command, None)
    }

    /// [`Self::apply`], handing the change the entry made to `report` and
    /// keeping it while the shard is watched. `report` is given only while
    /// some watch could take the change, and says whether one of this shard
    /// did. With neither a report nor recording, apply pays nothing extra.
    pub(crate) fn apply_reporting(
        &mut self,
        index: u64,
        stamp: u64,
        command: &CacheCommand,
        report: Option<&dyn Fn(&Change) -> bool>,
    ) -> Option<Answer> {
        // Exhaustive on purpose: a new command that changes a key must be
        // recorded, or a resume would be replayed with a gap.
        let key = match command {
            CacheCommand::Put { key, .. }
            | CacheCommand::Delete { key, .. }
            | CacheCommand::Expire { key, .. } => key,
            CacheCommand::Add { .. } | CacheCommand::Retire { .. } => {
                return self.fold_in(index, stamp, command);
            }
            CacheCommand::Watched => {
                let answer = self.fold_in(index, stamp, command);
                self.marked(index, stamp);
                return answer;
            }
        };
        if report.is_none() && self.recent.is_none() {
            return self.fold_in(index, stamp, command);
        }
        let was = self.entry(key).is_some();
        let answer = self.fold_in(index, stamp, command);
        // A refused write, a retry and an expiry of a key written since
        // change nothing.
        let (value, expires_at) = match self.entry(key) {
            Some(entry) if entry.version == index => (Some(entry.value.clone()), entry.expires_at),
            None if was => (None, 0),
            _ => return answer,
        };
        let change = Change {
            offset: index,
            key: key.clone(),
            value,
            expires_at,
        };
        if report.is_some_and(|report| report(&change)) {
            self.watched_until = stamp.saturating_add(WINDOW_MICROS);
            if self.recent.is_none() {
                self.recent = Some(Box::new(Recent::starting_at(index)));
            }
        } else if stamp > self.watched_until {
            self.recent = None;
        }
        if let Some(recent) = &mut self.recent {
            recent.push(change);
        }
        answer
    }

    fn fold_in(&mut self, index: u64, stamp: u64, command: &CacheCommand) -> Option<Answer> {
        self.time = stamp;
        self.applied = index;
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
            CacheCommand::Watched => return None,
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

    /// Whether a write that would add `grows` bytes of values or counters
    /// and drop `shrinks` takes them past the bound. Judged on what is
    /// unexpired at `stamp`, as every write sees it: a member that restored
    /// a snapshot that left an expired value out judges like one that
    /// still holds it.
    fn full(&mut self, stamp: u64, grows: u64, shrinks: u64) -> bool {
        if grows <= shrinks {
            return false;
        }
        // Most writes are far under the bound and need no sweep.
        if self.data + grows - shrinks <= self.bound.0 {
            return false;
        }
        self.sweep(stamp);
        self.data - self.expired + grows - shrinks > self.bound.0
    }

    /// Count in `expired` every value that expires after `swept` and by
    /// `stamp`.
    fn sweep(&mut self, stamp: u64) {
        if stamp <= self.swept {
            return;
        }
        let maps = self.view();
        let swept: u64 = maps
            .expiring_between(self.swept, stamp)
            .filter_map(|(_, key)| Some(size::entry(key, maps.entry(key)?.value.len())))
            .sum();
        self.expired += swept;
        self.swept = stamp;
    }

    /// The bytes of `key`'s value, if it has one unexpired at `at`.
    fn live_size(&self, key: &str, at: u64) -> u64 {
        self.entry(key)
            .filter(|entry| entry.live(at))
            .map_or(0, |entry| size::entry(key, entry.value.len()))
    }

    fn write(&mut self, index: u64, stamp: u64, command: &CacheCommand) -> Answer {
        match command {
            CacheCommand::Add { key, delta, .. } => {
                let grows = match self.view().counter(key) {
                    Some(_) => 0,
                    None => size::counter(key),
                };
                if self.full(stamp, grows, 0) {
                    return Answer::Full;
                }
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
                let shrinks = self.live_size(key, stamp);
                if self.full(stamp, size::entry(key, value.len()), shrinks) {
                    return Answer::Full;
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
            CacheCommand::Expire { .. } | CacheCommand::Retire { .. } | CacheCommand::Watched => {
                unreachable!("not a client write")
            }
        }
    }

    /// Count `cost` bytes of a value that expires at `at` coming or going.
    fn account(&mut self, cost: u64, at: u64, comes: bool) {
        let expired = at != 0 && at <= self.swept;
        if comes {
            self.data += cost;
            self.expired += if expired { cost } else { 0 };
        } else {
            self.data -= cost;
            self.expired -= if expired { cost } else { 0 };
        }
    }

    /// Set `key` to `entry`, replacing any value it had, in one lookup.
    fn put(&mut self, key: &str, entry: Entry) {
        if let Some(old) = self.entry(key) {
            let (cost, at) = (size::entry(key, old.value.len()), old.expires_at);
            self.account(cost, at, false);
        }
        self.account(size::entry(key, entry.value.len()), entry.expires_at, true);
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
        if let Some(old) = self.entry(key) {
            let (cost, at) = (size::entry(key, old.value.len()), old.expires_at);
            self.account(cost, at, false);
        }
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
        if self.view().counter(key).is_none() {
            self.data += size::counter(key);
        }
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
        let values = tables.entries.iter();
        let counters = tables.counters.keys();
        let data = values
            .map(|(key, entry)| size::entry(key, entry.value.len()))
            .chain(counters.map(|key| size::counter(key)))
            .sum();
        let mut state = Self {
            time,
            applied: 0,
            next_expiry: tables.expiring.first().map(|(at, _)| *at),
            tables,
            layered: None,
            recent: None,
            watched_until: 0,
            marked_until: 0,
            data,
            expired: 0,
            swept: 0,
            bound: Bound::default(),
        };
        state.sweep(time);
        state
    }

    /// This state judged by `bound` instead of the constant one, for tests
    /// that cannot fill a gigabyte.
    #[cfg(test)]
    pub(crate) fn bounded(mut self, bound: Bound) -> Self {
        self.bound = bound;
        self
    }

    /// The snapshot bytes of the values and counters held, expired values
    /// included.
    #[cfg(test)]
    pub(crate) fn data(&self) -> u64 {
        self.data
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
        let (time, marked_until) = (self.time, self.marked_until);
        match &self.layered {
            None => {
                let base = Arc::new(std::mem::take(&mut self.tables));
                self.next_expiry = None;
                let cut = Cut {
                    time,
                    marked_until,
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
                marked_until,
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

    /// See [`Self::applied`]'s field.
    pub(crate) fn applied(&self) -> u64 {
        self.applied
    }

    /// The state restored from a snapshot whose last entry is `index`.
    /// Inside a mark's life it records from past `index`: nothing at or
    /// below it can be replayed, everything above it is applied here.
    pub(crate) fn restored_through(&mut self, index: u64) {
        self.applied = index;
        if self.recent.is_none() && self.time < self.marked_until {
            self.watched_until = self.watched_until.max(self.marked_until);
            self.recent = Some(Box::default());
        }
        if let Some(recent) = &mut self.recent {
            recent.restart(index);
        }
    }

    /// Every kept change at or past `from` that `keep` takes, or `None` when
    /// some from `from` on were not kept.
    pub(crate) fn recent_since(
        &self,
        from: u64,
        keep: &dyn Fn(&str) -> bool,
    ) -> Option<Vec<Change>> {
        self.recent.as_ref()?.since(from, keep)
    }

    /// A watch of this shard here ended, or resumes, at `now`: record from
    /// the live edge on, for a window, so a resume can be replayed.
    pub(crate) fn watch_ended(&mut self, now: u64) {
        self.watched_until = self.watched_until.max(now.saturating_add(WINDOW_MICROS));
        if self.recent.is_none() {
            self.recent = Some(Box::new(Recent::starting_at(self.applied + 1)));
        }
    }

    /// A [`CacheCommand::Watched`] at `index`, stamped `stamp`: record from
    /// here on. The entry changes no key, so every change from `index` on
    /// is kept and that is the floor of a recording it starts.
    fn marked(&mut self, index: u64, stamp: u64) {
        let until = stamp.saturating_add(MARK_MICROS);
        self.marked_until = self.marked_until.max(until);
        self.watched_until = self.watched_until.max(until);
        if self.recent.is_none() {
            self.recent = Some(Box::new(Recent::starting_at(index)));
        }
    }

    /// Whether the shard's leader should append a [`CacheCommand::Watched`]
    /// at `now` for a watch that took a change: the last one applied keeps
    /// members recording for less than a window more. Renewed once a
    /// window at most while watched.
    pub(crate) fn wants_mark(&self, now: u64) -> bool {
        self.marked_until < now.saturating_add(WINDOW_MICROS)
    }

    /// Whether this state records changes no watch has wanted since `now`
    /// minus the window.
    pub(crate) fn recording_lapsed(&self, now: u64) -> bool {
        self.recent.is_some() && now > self.watched_until
    }

    /// Stop recording and free what was kept, if [`Self::recording_lapsed`].
    pub(crate) fn stop_lapsed_recording(&mut self, now: u64) {
        if self.recording_lapsed(now) {
            self.recent = None;
        }
    }

    /// Every key `keep` takes with a value unexpired at the log's time, for
    /// a watch's snapshot.
    pub(crate) fn watched_entries(&self, keep: &dyn Fn(&str) -> bool) -> Vec<(String, Watched)> {
        self.view()
            .entries()
            .filter(|(key, entry)| keep(key) && entry.live(self.time))
            .map(|(key, entry)| (key.clone(), Watched::of(entry)))
            .collect()
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
            && self.marked_until == other.marked_until
            && ours.entries().eq(theirs.entries())
            && ours.expiring().eq(theirs.expiring())
            && ours.counters().eq(theirs.counters())
            && same_producers()
    }
}

impl Eq for CacheState {}

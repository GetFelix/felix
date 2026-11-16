//! Consumer groups on a Raft stream shard.
//!
//! [`RaftGroups`] serves what [`GroupReader`](super::GroupReader) serves, but a
//! group's writes are entries in the shard's own Raft log ([`GroupEntry`]),
//! and its durable state is the [`ShardGroups`] table every member folds from
//! them. The leader still keeps what is in flight in a [`GroupTracker`], bound
//! to the term and the group incarnation it was hydrated for.
//!
//! The rules are those `docs/formal/FelixRaftGroups.tla` checks:
//!
//! - A write is answered only once its entry has applied.
//! - When the fold drops an entry a tracker decided, it forgets that tracker
//!   (in the apply path). The proposer sees its token gone after the wait,
//!   answers the claim lapsed, and the next poll hydrates afresh.
//! - A new group's creation is committed at its first hand-out, so a delete
//!   of another group never voids a group still closing its first run.
//! - A dead letter is written before the advance over it.
//! - Hydrate and every operation first wait for the shard's last proposed
//!   group entry, which a timed-out write leaves unanswered.
//! - A record that is not data is settled in the tracker alone. No entry is
//!   proposed for it; the next advance over a data record covers it.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_common::random::HashMap;
use felix_storage::log::ReadPage;
use parking_lot::RwLock;
use tokio::sync::{Mutex, OwnedMutexGuard};

use super::entry::{CreatedBy, CreationToken, GroupEntry, GroupOp};
use super::reader::{Claimed, GroupKey, GroupSnapshot, Seek};
use super::table::ShardGroups;
use super::tracker::{GroupConsumer, GroupTracker};
use crate::error::{BrokerError, Result};

/// What a Raft stream shard offers its consumer groups. The broker service
/// implements it over the shard's Raft group; every call is about the shard
/// `key` names (its `group` is not read).
pub trait GroupConsensus: Send + Sync {
    /// Wait, until `deadline`, for this member to lead the shard and to have
    /// applied every entry committed when the call began (a read index).
    fn fresh(
        &self,
        key: &GroupKey,
        deadline: Instant,
    ) -> impl Future<Output = Result<FreshGroups>> + Send;

    /// Append `entry` to the shard's log without waiting for it to commit.
    /// Refused unless this member still leads the shard in `term`: the entry
    /// was decided from state read in that term.
    ///
    /// Any error means nothing was appended in `term`. An implementation that
    /// cannot tell must stop leading `term` before it returns the error, so
    /// whatever it did append is settled by the next term's read index.
    fn propose(
        &self,
        key: &GroupKey,
        term: u64,
        entry: &GroupEntry,
    ) -> impl Future<Output = Result<Proposed>> + Send;

    /// Wait, until `deadline`, for `proposed` to commit and apply. An error
    /// leaves its outcome unknown.
    fn applied(
        &self,
        key: &GroupKey,
        proposed: Proposed,
        deadline: Instant,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// What [`GroupConsensus::fresh`] found.
#[derive(Debug, Clone)]
pub struct FreshGroups {
    /// The term this member leads the shard in.
    pub term: u64,
    /// The shard's folded groups, as of at least the read index.
    pub groups: Arc<RwLock<ShardGroups>>,
}

/// A proposed group entry's Raft log id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Proposed {
    /// The term the entry was appended in.
    pub term: u64,
    /// The entry's index in the shard's log.
    pub index: u64,
}

/// The consumer groups of this broker's Raft stream shards.
pub struct RaftGroups<C> {
    consensus: C,
    visibility: Duration,
    max_attempts: u32,
    max_in_flight: usize,
    slots: parking_lot::Mutex<Slots>,
    /// Per shard, the last group entry proposed from here.
    last_proposed: parking_lot::Mutex<HashMap<ShardId, Proposed>>,
}

type ShardId = (String, String, String, u32);

/// Every group's slot, and per shard the newest term an operation saw.
#[derive(Default)]
struct Slots {
    slots: HashMap<GroupKey, Slot>,
    terms: HashMap<ShardId, u64>,
    last_sweep: Option<Instant>,
}

struct Slot {
    cell: Arc<Mutex<Option<Tracked>>>,
    last_used: Instant,
}

/// A group's tracker and the incarnation it serves.
#[derive(Debug)]
struct Tracked {
    tracker: GroupTracker,
    cre: CreationToken,
    term: u64,
    /// The table holds the group under `cre`.
    created: bool,
    groups: Arc<RwLock<ShardGroups>>,
}

/// Bytes one raw log read asks for. A run of group or generation records is
/// settled a page at a time rather than one read per offset.
const PAGE_BYTES: usize = 64 * 1024;

/// How long a group goes untouched before its tracker may be dropped, as
/// on the hybrid reader.
const TRACKER_IDLE: Duration = Duration::from_secs(10 * 60);

/// How often an operation also sweeps for idle trackers.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

impl<C: GroupConsensus> RaftGroups<C> {
    pub fn new(consensus: C, visibility: Duration, max_attempts: u32) -> Self {
        Self {
            consensus,
            visibility,
            max_attempts,
            max_in_flight: super::reader::DEFAULT_MAX_IN_FLIGHT,
            slots: parking_lot::Mutex::default(),
            last_proposed: parking_lot::Mutex::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn consensus(&self) -> &C {
        &self.consensus
    }

    /// [`GroupReader::poll_below`](super::GroupReader::poll_below) on a Raft
    /// shard. Gives up waiting for the shard at `deadline`.
    #[allow(clippy::too_many_arguments)]
    pub async fn poll_below(
        &self,
        key: &GroupKey,
        log: &crate::durable::StreamLog,
        committed: u64,
        max: usize,
        now: Instant,
        consumer: Option<&GroupConsumer>,
        visibility: Option<Duration>,
        deadline: Instant,
    ) -> Result<Vec<Claimed>> {
        let poll = || {
            self.poll_once(
                key, log, committed, max, now, consumer, visibility, deadline,
            )
        };
        match poll().await {
            // A delete of another group voided this one's hydrate before its
            // creation committed. Nothing went out; a fresh hydrate is after
            // that delete.
            Err(BrokerError::GroupClaimLapsed { .. }) => poll().await,
            polled => polled,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn poll_once(
        &self,
        key: &GroupKey,
        log: &crate::durable::StreamLog,
        committed: u64,
        max: usize,
        now: Instant,
        consumer: Option<&GroupConsumer>,
        visibility: Option<Duration>,
        deadline: Instant,
    ) -> Result<Vec<Claimed>> {
        let (_fresh, mut guard) = self.current(key, deadline).await?;
        let log_tail = log.tail_offset().await?;
        let tail = log_tail
            .min(committed)
            .min(log.poisoned_read_end().unwrap_or(u64::MAX));
        let visibility = visibility.unwrap_or(self.visibility);
        let max_in_flight = self.max_in_flight;
        let held = guard.as_mut().expect("current hydrates");
        held.tracker.skip_below(log.readable_base());
        // A tracker hydrated for a group the table holds takes over its
        // predecessor's claims. One for an absent group already inherits none.
        held.tracker.inherit_below(log_tail);
        held.tracker.set_max_in_flight(max_in_flight);

        let mut claimed: Vec<Claimed> = Vec::new();
        let mut bytes = 0usize;
        let mut page: Option<(u64, ReadPage)> = None;
        loop {
            let held = guard.as_mut().expect("current hydrates");
            let claim = held
                .tracker
                .claim_as(tail, max - claimed.len(), now, visibility, consumer);
            for (i, dead) in claim.dead_lettered.iter().enumerate() {
                let written = self.dead_letter_settled(key, &mut guard, dead.offset, deadline);
                if let Err(err) = written.await {
                    if let Some(held) = guard.as_mut() {
                        for dead in &claim.dead_lettered[i..] {
                            held.tracker.owe(dead.offset);
                        }
                        for offset in claim
                            .offsets
                            .iter()
                            .chain(claimed.iter().map(|c| &c.offset))
                        {
                            held.tracker.unclaim(*offset);
                        }
                    }
                    return Err(err);
                }
            }
            let held = guard
                .as_mut()
                .expect("a dead letter that applied keeps the tracker");
            let mut skipped_any = false;
            for (i, &offset) in claim.offsets.iter().enumerate() {
                if bytes >= super::reader::MAX_POLL_BYTES {
                    for &offset in &claim.offsets[i..] {
                        held.tracker.unclaim(offset);
                    }
                    break;
                }
                if !page
                    .as_ref()
                    .is_some_and(|(start, page)| *start <= offset && offset < page.next)
                {
                    match log.read_log_from(offset, PAGE_BYTES).await {
                        Ok(read) => page = Some((offset, read)),
                        Err(BrokerError::CursorTooOld { .. }) => {
                            // Retention removed it: settled, nobody can deliver it.
                            held.tracker.skip(offset);
                            held.tracker.ack(offset);
                            skipped_any = true;
                            continue;
                        }
                        Err(err) => {
                            for offset in claimed
                                .iter()
                                .map(|c| c.offset)
                                .chain(claim.offsets[i..].iter().copied())
                            {
                                held.tracker.unclaim(offset);
                            }
                            return Err(err);
                        }
                    }
                }
                let (_, read) = page.as_ref().expect("read above");
                let found = read
                    .records
                    .binary_search_by_key(&offset, |record| record.offset)
                    .ok()
                    .map(|at| read.records[at].clone());
                match found {
                    Some(record) if record.mark.is_data() => {
                        let record = crate::commit::client_record(record);
                        bytes += record.payload.len();
                        claimed.push(Claimed {
                            offset,
                            payload: record.payload,
                            attempts: held.tracker.attempts(offset),
                            skipped_before: held.tracker.skipped_before(offset),
                            publisher: record.publisher,
                            timestamp_micros: record.timestamp_micros,
                        });
                    }
                    // A group, generation or trim record: nobody's work. Settled in
                    // the tracker alone; proposing an advance per record would cost
                    // a Raft entry for every group entry the shard holds.
                    Some(_) => {
                        held.tracker.skip(offset);
                        held.tracker.ack(offset);
                        skipped_any = true;
                    }
                    None if offset < read.next => {
                        held.tracker.skip(offset);
                        held.tracker.ack(offset);
                        skipped_any = true;
                    }
                    // Below the tail and yet nothing there: owed again.
                    None => held.tracker.nack(offset),
                }
            }
            // A claim that was only skipped records claims again past them, so a
            // long run of group records costs one poll, not one per claim.
            if !claimed.is_empty() || !skipped_any || bytes >= super::reader::MAX_POLL_BYTES {
                break;
            }
        }

        let held = guard.as_mut().expect("current hydrates");
        if !claimed.is_empty() && !held.created {
            // The group's first hand-out: commit its creation before any record
            // goes out, so a delete elsewhere in the shard cannot void it.
            let entry = GroupOp::Advance {
                cre: held.cre,
                to: held.tracker.committed(),
            };
            let first = claimed[0].offset;
            if let Err(err) = self
                .write_held(key, &mut guard, entry, first, deadline)
                .await
            {
                if let Some(held) = guard.as_mut() {
                    for c in &claimed {
                        held.tracker.unclaim(c.offset);
                    }
                }
                return Err(err);
            }
            guard.as_mut().expect("applied").created = true;
        }
        Ok(claimed)
    }

    /// Finish one record. Answered once the advance it closes has applied.
    pub async fn ack(&self, key: &GroupKey, offset: u64, deadline: Instant) -> Result<()> {
        let (_fresh, mut guard) = self.current(key, deadline).await?;
        check_handed_out(&guard.as_ref().expect("current hydrates").tracker, offset)?;
        self.settle(key, &mut guard, offset, deadline).await
    }

    /// See [`GroupReader::inherit_below`](super::GroupReader::inherit_below).
    pub async fn inherit_below(&self, key: &GroupKey, tail: u64, deadline: Instant) -> Result<()> {
        let (_fresh, mut guard) = self.current(key, deadline).await?;
        guard
            .as_mut()
            .expect("current hydrates")
            .tracker
            .inherit_below(tail);
        Ok(())
    }

    /// Give one record back, owed again at once. Leader memory only.
    pub async fn nack(&self, key: &GroupKey, offset: u64, deadline: Instant) -> Result<()> {
        let (_fresh, mut guard) = self.current(key, deadline).await?;
        let held = guard.as_mut().expect("current hydrates");
        check_handed_out(&held.tracker, offset)?;
        held.tracker.nack(offset);
        Ok(())
    }

    /// Keep the claim `attempts` names on `offset` standing until `by` after
    /// `now`. Leader memory only.
    pub async fn extend(
        &self,
        key: &GroupKey,
        offset: u64,
        attempts: u32,
        by: Duration,
        now: Instant,
        deadline: Instant,
    ) -> Result<()> {
        let (_fresh, mut guard) = self.current(key, deadline).await?;
        let held = guard.as_mut().expect("current hydrates");
        check_handed_out(&held.tracker, offset)?;
        if held.tracker.extend(offset, attempts, now, now + by) {
            return Ok(());
        }
        Err(BrokerError::GroupClaimLapsed { offset })
    }

    /// Give up on one record for a consumer. False, writing nothing, when it
    /// is already finished.
    pub async fn dead_letter(
        &self,
        key: &GroupKey,
        offset: u64,
        deadline: Instant,
    ) -> Result<bool> {
        let (_fresh, mut guard) = self.current(key, deadline).await?;
        let held = guard.as_ref().expect("current hydrates");
        check_handed_out(&held.tracker, offset)?;
        if !held.tracker.in_play(offset) {
            return Ok(false);
        }
        self.dead_letter_settled(key, &mut guard, offset, deadline)
            .await?;
        Ok(true)
    }

    /// The group's dead letters, ascending.
    pub async fn dead_letters(&self, key: &GroupKey, deadline: Instant) -> Result<Vec<u64>> {
        let fresh = self.ready(key, deadline).await?;
        let groups = fresh.groups.read();
        Ok(groups
            .table()
            .group(&key.group)
            .map(|state| state.dead.iter().copied().collect())
            .unwrap_or_default())
    }

    /// Put one dead letter back in the queue. Returns whether it was taken.
    pub async fn redrive(&self, key: &GroupKey, offset: u64, deadline: Instant) -> Result<bool> {
        let (fresh, mut guard) = self.current(key, deadline).await?;
        let dead = fresh
            .groups
            .read()
            .table()
            .group(&key.group)
            .is_some_and(|state| state.dead.contains(&offset));
        let held = guard.as_ref().expect("current hydrates");
        if !dead || !held.tracker.can_redrive(offset) {
            return Ok(false);
        }
        let entry = GroupOp::Redrive {
            cre: held.cre,
            offset,
        };
        // Owed before the write: a tracker that outlives an unanswered wait
        // serves only once the entry has applied (see `write_held`).
        let held = guard.as_mut().expect("current hydrates");
        held.tracker.redrive(offset);
        held.tracker.mark_redriven(offset);
        self.write_held(key, &mut guard, entry, offset, deadline)
            .await?;
        Ok(true)
    }

    /// Stop tracking one dead letter. Returns whether it was listed.
    pub async fn discard(&self, key: &GroupKey, offset: u64, deadline: Instant) -> Result<bool> {
        let (fresh, _guard) = self.locked(key, deadline).await?;
        let cre = fresh
            .groups
            .read()
            .table()
            .group(&key.group)
            .filter(|state| state.dead.contains(&offset))
            .map(|state| state.cre);
        let Some(cre) = cre else {
            return Ok(false);
        };
        let entry = GroupEntry {
            group: key.group.clone(),
            op: GroupOp::Discard { cre, offset },
        };
        // A failed write keeps the tracker: a discard decides nothing from it,
        // and `locked` drops it if the term ended.
        self.write(key, fresh.term, &entry, deadline).await?;
        // Dropped only if a seek or delete came between: then it is not listed.
        Ok(!fresh
            .groups
            .read()
            .table()
            .group(&key.group)
            .is_some_and(|state| state.dead.contains(&offset)))
    }

    /// Move the group's cursor to `offset`. With `if_new`, a group the table
    /// holds or this leader has handed records out from is left alone, and
    /// nothing is proposed.
    pub async fn seek(
        &self,
        key: &GroupKey,
        offset: u64,
        if_new: bool,
        deadline: Instant,
    ) -> Result<Seek> {
        let (fresh, mut guard) = self.locked(key, deadline).await?;
        if if_new {
            let committed = fresh
                .groups
                .read()
                .table()
                .group(&key.group)
                .map(|state| state.cursor);
            let current = committed.or_else(|| {
                guard
                    .as_ref()
                    .filter(|held| held.tracker.high_water() > 0)
                    .map(|held| held.tracker.committed())
            });
            if let Some(offset) = current {
                return Ok(Seek {
                    offset,
                    moved: false,
                });
            }
        }
        let entry = GroupEntry {
            group: key.group.clone(),
            op: GroupOp::Seek { to: offset, if_new },
        };
        if let Err(err) = self.write(key, fresh.term, &entry, deadline).await {
            self.forget(key, &fresh, &mut guard);
            return Err(err);
        }
        // The table's token, not one made from the proposal: the fold names
        // the incarnation, and it is what later writes are checked against.
        let (cre, tracker) = {
            let mut groups = fresh.groups.write();
            let Some(state) = groups.table().group(&key.group) else {
                // A delete applied after the seek. The next operation hydrates.
                drop(groups);
                self.forget(key, &fresh, &mut guard);
                return Ok(Seek {
                    offset,
                    moved: true,
                });
            };
            let cre = state.cre;
            let mut tracker = GroupTracker::moved_to(state.cursor, self.max_attempts);
            for &offset in &state.redriven {
                tracker.restore_redrive(offset);
            }
            groups.track(&key.group, cre);
            (cre, tracker)
        };
        *guard = Some(Tracked {
            tracker,
            cre,
            term: fresh.term,
            created: true,
            groups: Arc::clone(&fresh.groups),
        });
        Ok(Seek {
            offset,
            moved: true,
        })
    }

    /// Delete the group on this shard. Returns whether there was anything to
    /// delete. Only this group is touched: every other group's claims stand.
    pub async fn delete(&self, key: &GroupKey, deadline: Instant) -> Result<bool> {
        let (fresh, mut guard) = self.locked(key, deadline).await?;
        let existed = fresh.groups.read().table().group(&key.group).is_some()
            || guard
                .as_ref()
                .is_some_and(|held| held.tracker.high_water() > 0);
        let entry = GroupEntry {
            group: key.group.clone(),
            op: GroupOp::Delete,
        };
        let written = self.write(key, fresh.term, &entry, deadline).await;
        self.forget(key, &fresh, &mut guard);
        self.release(key, guard);
        written.map(|()| existed)
    }

    /// Where the group stands. The cursor and dead letters are the table's;
    /// in flight and owed are this leader's memory.
    pub async fn describe(&self, key: &GroupKey, deadline: Instant) -> Result<GroupSnapshot> {
        let (fresh, mut guard) = self.locked(key, deadline).await?;
        let (committed, dead_letters) = {
            let groups = fresh.groups.read();
            let state = groups.table().group(&key.group);
            (state.map(|s| s.cursor), state.map_or(0, |s| s.dead.len()))
        };
        let (in_flight, owed) = match guard.as_mut() {
            Some(held) => {
                held.tracker.expire(Instant::now());
                held.tracker.outstanding()
            }
            None => (0, 0),
        };
        Ok(GroupSnapshot {
            committed,
            in_flight: in_flight as u64,
            owed: owed as u64,
            dead_letters: dead_letters as u64,
        })
    }

    /// Record `offset` as a dead letter, then settle it.
    async fn dead_letter_settled(
        &self,
        key: &GroupKey,
        guard: &mut OwnedMutexGuard<Option<Tracked>>,
        offset: u64,
        deadline: Instant,
    ) -> Result<()> {
        let cre = guard.as_ref().expect("hydrated").cre;
        // Before the advance over it: an advance first would leave a record
        // the group never finished and never listed.
        let entry = GroupOp::DeadLetter { cre, offset };
        self.write_held(key, guard, entry, offset, deadline).await?;
        // The dead letter replaced any redrive.
        guard
            .as_mut()
            .expect("applied")
            .tracker
            .take_redriven(offset);
        self.settle(key, guard, offset, deadline).await
    }

    /// Settle `offset` in the tracker, then write what that closed.
    async fn settle(
        &self,
        key: &GroupKey,
        guard: &mut OwnedMutexGuard<Option<Tracked>>,
        offset: u64,
        deadline: Instant,
    ) -> Result<()> {
        let held = guard.as_mut().expect("hydrated");
        let advanced = held.tracker.ack(offset);
        let redriven = held.tracker.take_redriven(offset);
        let cre = held.cre;
        if redriven {
            let entry = GroupOp::FinishRedrive { cre, offset };
            self.write_held(key, guard, entry, offset, deadline).await?;
        }
        if let Some(to) = advanced {
            let entry = GroupOp::Advance { cre, to };
            self.write_held(key, guard, entry, offset, deadline).await?;
        }
        Ok(())
    }

    /// Write a tracker-decided entry. The tracker is forgotten when the
    /// proposal is refused or the fold dropped it; the claim on `offset` is
    /// then answered lapsed, and the next operation hydrates afresh.
    ///
    /// A wait that gives up keeps the tracker: the next operation's
    /// [`Self::ready`] waits for this entry in the same term, and
    /// [`Self::locked`] drops the tracker if the term ended or the fold
    /// dropped the entry. Dropping it here would hand standing claims out
    /// again and redeliver what it holds acked ahead of the cursor.
    async fn write_held(
        &self,
        key: &GroupKey,
        guard: &mut OwnedMutexGuard<Option<Tracked>>,
        op: GroupOp,
        offset: u64,
        deadline: Instant,
    ) -> Result<()> {
        let entry = GroupEntry {
            group: key.group.clone(),
            op,
        };
        let held = guard.as_ref().expect("hydrated");
        let (cre, term, groups) = (held.cre, held.term, Arc::clone(&held.groups));
        let proposed = match self.propose(key, term, &entry).await {
            Ok(proposed) => proposed,
            // Nothing was appended, or this member no longer leads `term`
            // (see `GroupConsensus::propose`): either way no entry of ours
            // is left for a later hydrate to miss.
            Err(err) => {
                guard.take();
                return Err(err);
            }
        };
        self.consensus.applied(key, proposed, deadline).await?;
        if groups.read().tracker(&key.group) != Some(cre) {
            // The apply path forgot the tracker: the table will take none of
            // its writes, so none of its claims stands.
            guard.take();
            return Err(BrokerError::GroupClaimLapsed { offset });
        }
        Ok(())
    }

    async fn write(
        &self,
        key: &GroupKey,
        term: u64,
        entry: &GroupEntry,
        deadline: Instant,
    ) -> Result<()> {
        let proposed = self.propose(key, term, entry).await?;
        self.consensus.applied(key, proposed, deadline).await
    }

    async fn propose(&self, key: &GroupKey, term: u64, entry: &GroupEntry) -> Result<Proposed> {
        let proposed = self.consensus.propose(key, term, entry).await?;
        let mut last = self.last_proposed.lock();
        let slot = last.entry(shard_id(key)).or_insert(proposed);
        *slot = (*slot).max(proposed);
        Ok(proposed)
    }

    /// The shard fresh, and this leader's own last proposed entry applied.
    async fn ready(&self, key: &GroupKey, deadline: Instant) -> Result<FreshGroups> {
        let fresh = self.consensus.fresh(key, deadline).await?;
        let last = self.last_proposed.lock().get(&shard_id(key)).copied();
        // An older term's entry is either committed, and so under the read
        // index, or never will be from here.
        if let Some(last) = last
            && last.term == fresh.term
        {
            self.consensus.applied(key, last, deadline).await?;
        }
        Ok(fresh)
    }

    /// The group's slot locked, the shard ready, and a tracker kept only if
    /// it is still the one the table serves the group from in this term.
    async fn locked(
        &self,
        key: &GroupKey,
        deadline: Instant,
    ) -> Result<(FreshGroups, OwnedMutexGuard<Option<Tracked>>)> {
        let now = Instant::now();
        let (slot, sweep) = {
            let mut slots = self.slots.lock();
            let sweep = slots
                .last_sweep
                .is_none_or(|last| now.saturating_duration_since(last) >= SWEEP_EVERY);
            let slot = slots.slots.entry(key.clone()).or_insert_with(|| Slot {
                cell: Arc::default(),
                last_used: now,
            });
            slot.last_used = now;
            (Arc::clone(&slot.cell), sweep)
        };
        if sweep {
            self.evict_idle(now);
        }
        let mut guard = slot.lock_owned().await;
        let fresh = self.ready(key, deadline).await?;
        self.saw_term(key, fresh.term);
        let current = guard.as_ref().is_some_and(|held| {
            held.term == fresh.term && fresh.groups.read().tracker(&key.group) == Some(held.cre)
        });
        if !current {
            *guard = None;
        }
        Ok((fresh, guard))
    }

    /// [`Self::locked`], hydrating the tracker from the table if there is none.
    async fn current(
        &self,
        key: &GroupKey,
        deadline: Instant,
    ) -> Result<(FreshGroups, OwnedMutexGuard<Option<Tracked>>)> {
        let (fresh, mut guard) = self.locked(key, deadline).await?;
        if guard.is_none() {
            let mut groups = fresh.groups.write();
            let table = groups.table();
            let (cursor, cre, created, redriven) = match table.group(&key.group) {
                Some(state) => (state.cursor, state.cre, true, state.redriven.clone()),
                // A group that has never written starts at the beginning of
                // what the log holds, under a token naming this hydrate.
                // It inherits no claims: req (c) puts every group with live
                // claims in the table, so an ack above 0 is from a deleted
                // incarnation and must not count here.
                None => (
                    0,
                    CreationToken {
                        term: fresh.term,
                        index: table.applied(),
                        by: CreatedBy::Hydrate,
                    },
                    false,
                    Default::default(),
                ),
            };
            let mut tracker = if created {
                GroupTracker::new(cursor, self.max_attempts)
            } else {
                GroupTracker::moved_to(cursor, self.max_attempts)
            };
            // A dead letter above the cursor is settled: the advance over it
            // may not have been written before the leader changed.
            if let Some(state) = table.group(&key.group) {
                for &offset in state.dead.range(cursor..) {
                    tracker.ack(offset);
                }
            }
            for offset in redriven {
                tracker.restore_redrive(offset);
            }
            groups.track(&key.group, cre);
            drop(groups);
            *guard = Some(Tracked {
                tracker,
                cre,
                term: fresh.term,
                created,
                groups: Arc::clone(&fresh.groups),
            });
        }
        Ok((fresh, guard))
    }

    /// Drop the trackers of `shard`'s groups (its `group` is not read): this
    /// member no longer leads it. A slot in use keeps its tracker until
    /// [`Self::locked`] finds it stale.
    pub fn step_down(&self, shard: &GroupKey) {
        let shard = shard_id(shard);
        self.evict(|key, _, _| shard_id(key) == shard);
    }

    /// Drop every tracker nobody is using that has been idle for
    /// [`TRACKER_IDLE`] (at least twice the visibility timeout) and holds no
    /// standing claim. Losing one costs what a leader change does. Returns
    /// how many went.
    pub(crate) fn evict_idle(&self, now: Instant) -> usize {
        let idle_after = TRACKER_IDLE.max(self.visibility * 2);
        let evicted = self.evict(|_, slot, held| {
            now.saturating_duration_since(slot.last_used) >= idle_after
                && held
                    .is_none_or(|held| held.tracker.next_lapse().is_none_or(|lapse| lapse <= now))
        });
        self.slots.lock().last_sweep = Some(now);
        evicted
    }

    /// How many groups hold a slot here.
    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.slots.lock().slots.len()
    }

    /// Note `term` for the shard. A newer one drops the trackers of every
    /// older term there: a former leader's are of no use.
    fn saw_term(&self, key: &GroupKey, term: u64) {
        let shard = shard_id(key);
        let newer = {
            let mut slots = self.slots.lock();
            let seen = slots.terms.entry(shard.clone()).or_insert(term);
            let newer = term > *seen;
            *seen = (*seen).max(term);
            newer
        };
        if newer {
            self.evict(|other, _, held| {
                shard_id(other) == shard && held.is_some_and(|held| held.term < term)
            });
        }
    }

    /// Remove every slot nobody else holds for which `stale` says so.
    fn evict(&self, stale: impl Fn(&GroupKey, &Slot, Option<&Tracked>) -> bool) -> usize {
        let mut slots = self.slots.lock();
        let before = slots.slots.len();
        // Only the map's count: anyone else holding the slot is mid
        // operation, and a new slot beside it would be a second tracker.
        slots.slots.retain(|key, slot| {
            Arc::strong_count(&slot.cell) > 1
                || !slot
                    .cell
                    .try_lock()
                    .is_ok_and(|held| stale(key, slot, held.as_ref()))
        });
        before - slots.slots.len()
    }

    /// Unlock the slot, and drop it if it holds no tracker and nobody else
    /// holds it.
    fn release(&self, key: &GroupKey, guard: OwnedMutexGuard<Option<Tracked>>) {
        let empty = guard.is_none();
        let mut slots = self.slots.lock();
        drop(guard);
        if empty
            && slots
                .slots
                .get(key)
                .is_some_and(|slot| Arc::strong_count(&slot.cell) == 1)
        {
            slots.slots.remove(key);
        }
    }

    /// Drop the group's tracker here and in the table's registry.
    fn forget(
        &self,
        key: &GroupKey,
        fresh: &FreshGroups,
        guard: &mut OwnedMutexGuard<Option<Tracked>>,
    ) {
        if guard.take().is_some() {
            fresh.groups.write().untrack(&key.group);
        }
    }
}

fn shard_id(key: &GroupKey) -> ShardId {
    (
        key.tenant_id.clone(),
        key.namespace.clone(),
        key.stream.clone(),
        key.shard,
    )
}

fn check_handed_out(tracker: &GroupTracker, offset: u64) -> Result<()> {
    if tracker.handed_out(offset) {
        return Ok(());
    }
    Err(BrokerError::GroupOffsetNotHandedOut {
        offset,
        next: tracker.high_water(),
    })
}

#[cfg(test)]
mod tests;

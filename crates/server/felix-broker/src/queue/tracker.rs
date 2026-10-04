//! What a consumer group has handed out, and what it owes.
//!
//! The durable cursor in [`ConsumerGroups`](crate::queue::ConsumerGroups) says where a group has
//! finished. This is everything between there and the tail: offsets handed to a
//! consumer and not yet settled, offsets whose consumer stopped answering, and
//! the bookkeeping that turns acknowledgements into cursor movement.
//!
//! **Deliberately in memory.** A leader that dies loses what was in flight, and
//! the group resumes from its durable cursor — so those records are delivered
//! again. That is at-least-once, which is the guarantee a queue offers anyway;
//! persisting the in-flight set would buy a smaller redelivery window at the
//! cost of a write per delivery, and would still not make it exactly-once.
//!
//! Pure logic with the clock passed in, so every rule here is testable without
//! waiting for one.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// One group's position on one shard.
#[derive(Debug)]
pub(crate) struct GroupTracker {
    /// Everything below this is acknowledged and will never be handed out
    /// again. Mirrors the durable cursor.
    committed: u64,
    /// The next offset never yet handed to anyone.
    high_water: u64,
    /// The log tail when this tracker was first used, once known. A tracker
    /// rebuilt after a move, failover or eviction never saw its predecessor's
    /// claims, and every one of them is below this, so a settle there is
    /// taken rather than refused as stale.
    inherited_below: Option<u64>,
    /// Handed out and unsettled, with the instant its claim lapses.
    in_flight: BTreeMap<u64, Instant>,
    /// The same claims ordered by when they lapse, so finding the lapsed ones
    /// costs what lapsed rather than everything in flight. Kept in step with
    /// `in_flight` by `hand_out` and `take_in_flight`, the only two writers.
    lapses: BTreeSet<(Instant, u64)>,
    /// Acknowledged, but above an offset that is not. Held until the run below
    /// them closes, because the cursor can only move over a contiguous prefix:
    /// advancing past a gap would drop a record nobody has finished.
    acked_ahead: BTreeSet<u64>,
    /// Owed again — nacked, or claimed by a consumer that stopped answering.
    redeliver: BTreeSet<u64>,
    /// How many times each unsettled offset has been handed out.
    ///
    /// Dropped as soon as an offset settles, so this holds only what is
    /// currently in play rather than growing with the log.
    attempts: BTreeMap<u64, u32>,
    /// Offsets an operator redrove whose redrive is recorded on disk and not
    /// yet finished. Settling one has to clear that record too, or every
    /// later leader would redeliver it again.
    redriven: BTreeSet<u64>,
    /// Which named member holds each claim, and on which connection. Kept in
    /// step with `in_flight`.
    holders: BTreeMap<u64, Holder>,
    /// Each named member with a claim standing or reserved. Also interns the
    /// member's key, so its claims share one allocation.
    members: HashMap<Arc<MemberKey>, MemberState>,
    /// Runs of offsets settled without being delivered (generation starts,
    /// trimmed records), keyed by the end of the run, which is exclusive, to
    /// the start. What a record at that end reports as skipped before it.
    /// Dropped once the cursor is past the record that follows the run.
    skipped: BTreeMap<u64, u64>,
    /// Most times a record is handed out before it is given up on.
    ///
    /// Without a bound a record that always fails is redelivered for ever and
    /// the group never gets past it — one poison record stops the queue.
    max_attempts: u32,
    /// Most offsets handed out and unsettled at once. A consumer that polls
    /// and never answers would otherwise pull the whole backlog into
    /// `in_flight`, holding memory and keeping every other consumer idle until
    /// its claims lapse.
    max_in_flight: usize,
}

impl GroupTracker {
    /// A group resuming at `committed`, giving up on a record after
    /// `max_attempts` deliveries.
    pub(crate) fn new(committed: u64, max_attempts: u32) -> Self {
        Self {
            committed,
            high_water: committed,
            inherited_below: None,
            in_flight: BTreeMap::new(),
            lapses: BTreeSet::new(),
            acked_ahead: BTreeSet::new(),
            redeliver: BTreeSet::new(),
            attempts: BTreeMap::new(),
            redriven: BTreeSet::new(),
            holders: BTreeMap::new(),
            members: HashMap::new(),
            skipped: BTreeMap::new(),
            max_attempts: max_attempts.max(1),
            max_in_flight: usize::MAX,
        }
    }

    /// A group just moved to `committed` by an operator. Every claim made
    /// before the move is void, so unlike [`Self::new`] it inherits none: a
    /// late settle from an old claim above `committed` is refused rather than
    /// taken as finishing a record the group now owes.
    pub(crate) fn moved_to(committed: u64, max_attempts: u32) -> Self {
        let mut tracker = Self::new(committed, max_attempts);
        tracker.inherited_below = Some(committed);
        tracker
    }

    /// How many offsets are handed out and unsettled, and how many are owed
    /// again after a nack or a lapsed claim.
    pub(crate) fn outstanding(&self) -> (usize, usize) {
        (self.in_flight.len(), self.redeliver.len())
    }

    /// Bound how many offsets may be handed out and unsettled at once.
    pub(crate) fn set_max_in_flight(&mut self, max_in_flight: usize) {
        self.max_in_flight = max_in_flight.max(1);
    }

    /// When the earliest standing claim lapses, if any does. A waiting poll
    /// sleeps no longer than this, since that record is owed from then on.
    pub(crate) fn next_lapse(&self) -> Option<Instant> {
        self.lapses.first().map(|(deadline, _)| *deadline)
    }

    /// Whether `offset` has been handed out by this tracker or a predecessor
    /// (see [`Self::inherit_below`]), or is below the cursor. Anything else is
    /// not the consumer's to settle: an ack there would finish a record nobody
    /// received, and a nack would make an offset owed that may not exist yet.
    pub(crate) fn handed_out(&self, offset: u64) -> bool {
        offset < self.high_water || self.inherited_below.is_some_and(|below| offset < below)
    }

    /// Record the log tail the first time this tracker sees one. Later calls
    /// do nothing: a record written after that was never a predecessor's.
    pub(crate) fn inherit_below(&mut self, tail: u64) {
        self.inherited_below.get_or_insert(tail);
    }

    /// The next offset never handed to anyone.
    pub(crate) fn high_water(&self) -> u64 {
        self.high_water
    }

    /// How many times `offset` has been handed out, if it is still in play.
    pub(crate) fn attempts(&self, offset: u64) -> u32 {
        self.attempts.get(&offset).copied().unwrap_or(0)
    }

    /// Everything below this is finished.
    pub(crate) fn committed(&self) -> u64 {
        self.committed
    }

    /// [`Self::claim_as`] for a consumer that did not name itself.
    #[cfg(test)]
    pub(crate) fn claim(
        &mut self,
        tail: u64,
        max: usize,
        now: Instant,
        visibility: Duration,
    ) -> Claim {
        self.claim_as(tail, max, now, visibility, None)
    }

    /// Take up to `max` offsets to deliver, claimed until `now + visibility`.
    ///
    /// Owed offsets come before new ones. A group that always preferred new
    /// records would starve the redeliveries behind a fast producer, and those
    /// are precisely the records a consumer already failed to finish once.
    ///
    /// `tail` is the shard's log tail: nothing at or above it exists yet.
    ///
    /// A `consumer` that named itself has its claims recorded as its own. Its
    /// first poll with `reclaim` on a connection newer than any that reclaimed
    /// before reserves the claims it holds from older connections, left by a
    /// process that restarted. Those go to that connection ahead of anything
    /// else, over as many polls as it takes, until each is delivered or its
    /// claim lapses.
    pub(crate) fn claim_as(
        &mut self,
        tail: u64,
        max: usize,
        now: Instant,
        visibility: Duration,
        consumer: Option<&GroupConsumer>,
    ) -> Claim {
        self.expire(now);
        let holder = consumer.map(|consumer| self.register(consumer));

        let wanted = max.min(MAX_CLAIM);
        let room = self.max_in_flight.saturating_sub(self.in_flight.len());
        let max = wanted.min(room);
        let deadline = now + visibility;
        let mut claim = Claim {
            offsets: Vec::with_capacity(max.min(16)),
            dead_lettered: Vec::new(),
            capped: false,
        };

        if let Some(holder) = &holder {
            self.take_reserved(holder, max, deadline, &mut claim);
        }
        while claim.offsets.len() < max
            && let Some(offset) = self.redeliver.iter().next().copied()
        {
            self.redeliver.remove(&offset);
            let attempts = self.attempts.get(&offset).copied().unwrap_or(0);
            if attempts >= self.max_attempts {
                // Given up on rather than handed out again. Reported so the
                // caller can record it, and left unsettled here -- the caller
                // settles it once that record is durable, or a crash in between
                // would lose the fact that it was ever tried.
                claim.dead_lettered.push(DeadLettered { offset, attempts });
                continue;
            }
            self.hand_out(offset, deadline, holder.as_ref());
            claim.offsets.push(offset);
        }

        while claim.offsets.len() < max && self.high_water < tail {
            let offset = self.high_water;
            self.high_water += 1;
            // Already settled or in play through a predecessor's claim.
            if self.acked_ahead.contains(&offset)
                || self.in_flight.contains_key(&offset)
                || self.redeliver.contains(&offset)
            {
                continue;
            }
            self.hand_out(offset, deadline, holder.as_ref());
            claim.offsets.push(offset);
        }

        if let Some(holder) = &holder
            && self
                .members
                .get(&holder.member)
                .is_some_and(|state| state.held == 0)
        {
            self.members.remove(&holder.member);
        }

        // Only when the cap is what stopped it: work was left behind that the
        // consumer asked for and would otherwise have had.
        claim.capped = max < wanted && (!self.redeliver.is_empty() || self.high_water < tail);
        claim
    }

    /// Settle one offset. Returns the new committed position if it moved.
    ///
    /// Acknowledging something already settled is not an error: a consumer that
    /// answered after its claim lapsed cannot tell the difference, and the
    /// record has since been handed to someone else who will answer too.
    pub(crate) fn ack(&mut self, offset: u64) -> Option<u64> {
        if offset < self.committed {
            // Below the cursor, so the run has already closed over it. That is
            // an ordinary duplicate — or a redriven record being finished, which
            // settles it without the cursor moving, since the cursor was never
            // waiting on it.
            self.take_in_flight(offset);
            self.redeliver.remove(&offset);
            self.attempts.remove(&offset);
            return None;
        }
        self.take_in_flight(offset);
        // No longer owed: it has been finished by whoever answered first.
        self.redeliver.remove(&offset);
        self.attempts.remove(&offset);
        self.acked_ahead.insert(offset);

        let before = self.committed;
        while self.acked_ahead.remove(&self.committed) {
            self.committed += 1;
        }
        let committed = self.committed;
        self.skipped.retain(|&end, _| end >= committed);
        // `high_water` can lag when a group is created above its acks.
        self.high_water = self.high_water.max(self.committed);
        (self.committed != before).then_some(self.committed)
    }

    /// Note that `offset` was settled without being delivered, before
    /// settling it.
    pub(crate) fn skip(&mut self, offset: u64) {
        let start = self.skipped.remove(&offset).unwrap_or(offset);
        self.skipped.insert(offset + 1, start);
    }

    /// How many offsets directly below `offset` were settled without being
    /// delivered.
    pub(crate) fn skipped_before(&self, offset: u64) -> u64 {
        self.skipped.get(&offset).map_or(0, |start| offset - start)
    }

    /// Give one offset back without finishing it. It is owed again at once,
    /// rather than after the visibility timeout: the consumer has said it
    /// cannot do the work, so waiting only delays someone else trying.
    pub(crate) fn nack(&mut self, offset: u64) {
        if offset < self.committed || self.acked_ahead.contains(&offset) {
            return;
        }
        self.take_in_flight(offset);
        self.redeliver.insert(offset);
    }

    /// Put a record the group gave up on back in play, its attempt count reset.
    ///
    /// The cursor is *not* moved backwards. It has already passed this offset,
    /// and rewinding it would redeliver everything the group finished since.
    /// The record is owed again instead, which reaches the same consumer
    /// without disturbing anything else — a queue's order was never a promise,
    /// and a redriven record is the clearest case of that.
    ///
    /// Returns whether it was taken. A record at or above the cursor is refused
    /// unless it was settled there by giving up on it: otherwise it is in play
    /// or owed already, and resetting its attempts would let it evade the bound
    /// for ever.
    pub(crate) fn redrive(&mut self, offset: u64) -> bool {
        if !self.can_redrive(offset) {
            return false;
        }
        // Given up on while a gap below held the cursor back: settled, but not
        // yet passed. Owed again instead, so the cursor now waits on it.
        self.acked_ahead.remove(&offset);
        self.attempts.remove(&offset);
        self.redeliver.insert(offset);
        true
    }

    /// Whether [`GroupTracker::redrive`] would take `offset`. Asked before the
    /// redrive is made durable, so a refusal writes nothing.
    pub(crate) fn can_redrive(&self, offset: u64) -> bool {
        offset < self.committed || self.acked_ahead.contains(&offset)
    }

    /// Remember that `offset` has a durable redrive record to clear when it
    /// settles. Rebuilding a tracker also makes it owed again if the cursor
    /// has already passed it; above the cursor it is delivered in turn.
    pub(crate) fn restore_redrive(&mut self, offset: u64) {
        self.redriven.insert(offset);
        if offset < self.committed {
            self.redeliver.insert(offset);
        }
    }

    /// Mark `offset` as having a durable redrive record.
    pub(crate) fn mark_redriven(&mut self, offset: u64) {
        self.redriven.insert(offset);
    }

    /// Forget the redrive record for `offset`, returning whether there was one.
    pub(crate) fn take_redriven(&mut self, offset: u64) -> bool {
        self.redriven.remove(&offset)
    }

    /// Make `offset` owed again without counting an attempt. For a dead
    /// letter whose record could not be written: it stays at its attempt
    /// bound, so the next claim tries to give up on it again.
    pub(crate) fn owe(&mut self, offset: u64) {
        if offset < self.committed || self.acked_ahead.contains(&offset) {
            return;
        }
        self.redeliver.insert(offset);
    }

    /// Take back a claim that never reached the consumer. Its attempt is not
    /// counted, since nobody tried the record.
    pub(crate) fn unclaim(&mut self, offset: u64) {
        if !self.take_in_flight(offset) {
            return;
        }
        if let Some(attempts) = self.attempts.get_mut(&offset) {
            *attempts = attempts.saturating_sub(1);
        }
        self.redeliver.insert(offset);
    }

    /// Move claims that have lapsed back to owed.
    ///
    /// This is what makes a consumer that stopped answering recoverable rather
    /// than a permanent hole in the group's progress.
    pub(crate) fn expire(&mut self, now: Instant) {
        while let Some(&(deadline, offset)) = self.lapses.first()
            && deadline <= now
        {
            self.lapses.pop_first();
            self.in_flight.remove(&offset);
            self.release_holder(offset);
            self.redeliver.insert(offset);
        }
    }

    /// How many offsets are handed out and unsettled.
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        debug_assert_eq!(self.in_flight.len(), self.lapses.len());
        self.in_flight.len()
    }

    fn hand_out(&mut self, offset: u64, deadline: Instant, holder: Option<&Holder>) {
        // Counted before the old claim is dropped, so a member taking back its
        // own claim never drops to holding nothing, which would forget it.
        if let Some(holder) = holder {
            self.members
                .entry(Arc::clone(&holder.member))
                .or_default()
                .held += 1;
        }
        // A redelivery replaces the claim it was owed under, if one stands.
        self.take_in_flight(offset);
        self.in_flight.insert(offset, deadline);
        self.lapses.insert((deadline, offset));
        if let Some(holder) = holder {
            self.holders.insert(offset, holder.clone());
        }
        *self.attempts.entry(offset).or_insert(0) += 1;
    }

    /// Record `consumer` as a member, and make its reservation if this poll
    /// is the reclaim. Returns what its claims are recorded under.
    fn register(&mut self, consumer: &GroupConsumer) -> Holder {
        let member = match self.members.get_key_value(&consumer.member) {
            Some((member, _)) => Arc::clone(member),
            None => {
                let member = Arc::clone(&consumer.member);
                self.members
                    .insert(Arc::clone(&member), MemberState::default());
                member
            }
        };
        let holder = Holder {
            member,
            connection: consumer.connection,
        };
        let state = self.members.get_mut(&holder.member).expect("registered");
        // Once per connection, and never by an older connection than the last
        // to reclaim: a client that sets `reclaim` on every poll, or a second
        // live process under the same name, would otherwise keep taking claims
        // still being worked on, an attempt each time.
        if consumer.reclaim && state.reclaimed_on < consumer.connection {
            state.reclaimed_on = consumer.connection;
            state.reserved = self
                .holders
                .iter()
                .filter(|(_, held)| {
                    held.member == holder.member && held.connection < consumer.connection
                })
                .map(|(offset, _)| *offset)
                .collect();
        }
        holder
    }

    /// Hand `holder` the claims its reclaim reserved, lowest offset first, as
    /// many as fit. A reserved claim that lapsed or settled since is skipped;
    /// a lapsed one is already owed to the whole group.
    fn take_reserved(&mut self, holder: &Holder, max: usize, deadline: Instant, claim: &mut Claim) {
        while claim.offsets.len() < max {
            let Some(state) = self.members.get_mut(&holder.member) else {
                return;
            };
            if state.reclaimed_on != holder.connection {
                return;
            }
            let Some(offset) = state.reserved.pop_first() else {
                return;
            };
            let still_held = self.holders.get(&offset).is_some_and(|held| {
                held.member == holder.member && held.connection != holder.connection
            });
            if !still_held {
                continue;
            }
            let attempts = self.attempts.get(&offset).copied().unwrap_or(0);
            if attempts >= self.max_attempts {
                // As for an owed record: the caller settles it once the dead
                // letter is durable.
                self.take_in_flight(offset);
                claim.dead_lettered.push(DeadLettered { offset, attempts });
                continue;
            }
            self.hand_out(offset, deadline, Some(holder));
            claim.offsets.push(offset);
        }
    }

    /// Forget who held `offset`. A member left holding nothing is forgotten
    /// too, along with anything still reserved for it, which by then has all
    /// lapsed or settled.
    fn release_holder(&mut self, offset: u64) {
        let Some(holder) = self.holders.remove(&offset) else {
            return;
        };
        if let Some(state) = self.members.get_mut(&holder.member) {
            state.held = state.held.saturating_sub(1);
            if state.held == 0 {
                self.members.remove(&holder.member);
            }
        }
    }

    /// Drop the claim on `offset`, returning whether there was one.
    fn take_in_flight(&mut self, offset: u64) -> bool {
        self.release_holder(offset);
        match self.in_flight.remove(&offset) {
            Some(deadline) => {
                self.lapses.remove(&(deadline, offset));
                true
            }
            None => false,
        }
    }
}

/// A group member that names itself when it polls.
///
/// A member is its name together with the principal it authenticated as, so
/// a name chosen by one principal never reaches the claims of another.
#[derive(Debug, Clone)]
pub struct GroupConsumer {
    member: Arc<MemberKey>,
    connection: u64,
    reclaim: bool,
}

impl GroupConsumer {
    /// `name`, polling as `principal` on `connection`. Connection ids must
    /// grow with each new connection: a reclaim takes only what older ones
    /// hold. `reclaim` asks for those claims back; it takes effect on the
    /// connection's first such poll and is ignored after.
    pub fn new(principal: &str, name: &str, connection: u64, reclaim: bool) -> Self {
        Self {
            member: Arc::new(MemberKey {
                principal: principal.into(),
                name: name.into(),
            }),
            connection,
            reclaim,
        }
    }
}

/// Who a named member is: the principal and the name it chose.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MemberKey {
    principal: Box<str>,
    name: Box<str>,
}

/// The member holding a claim, and the connection it claimed on.
#[derive(Debug, Clone)]
struct Holder {
    member: Arc<MemberKey>,
    connection: u64,
}

/// One named member's standing in a group.
#[derive(Debug, Default)]
struct MemberState {
    /// How many claims it holds.
    held: usize,
    /// The newest connection that reclaimed. An older one cannot.
    reclaimed_on: u64,
    /// Claims that connection's reclaim took from older ones, not yet handed
    /// back to it.
    reserved: BTreeSet<u64>,
}

/// Most offsets one claim hands out, whatever the client asked for. A request
/// for millions would otherwise claim them all at once and have the broker read
/// every one into a single answer.
pub(crate) const MAX_CLAIM: usize = 1_000;

/// What one `claim` produced.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Claim {
    /// Offsets handed to the caller, to deliver and then settle.
    pub(crate) offsets: Vec<u64>,
    /// Offsets given up on, having been delivered too many times. The caller
    /// records them and then settles them.
    pub(crate) dead_lettered: Vec<DeadLettered>,
    /// The in-flight cap held this claim short of what was available.
    pub(crate) capped: bool,
}

/// A record the group has given up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeadLettered {
    pub(crate) offset: u64,
    /// How many times it was handed out before being given up on.
    pub(crate) attempts: u32,
}

#[cfg(test)]
mod tests;

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
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

/// One group's position on one shard.
#[derive(Debug)]
pub(crate) struct GroupTracker {
    /// Everything below this is acknowledged and will never be handed out
    /// again. Mirrors the durable cursor.
    committed: u64,
    /// The next offset never yet handed to anyone.
    high_water: u64,
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
            in_flight: BTreeMap::new(),
            lapses: BTreeSet::new(),
            acked_ahead: BTreeSet::new(),
            redeliver: BTreeSet::new(),
            attempts: BTreeMap::new(),
            redriven: BTreeSet::new(),
            max_attempts: max_attempts.max(1),
            max_in_flight: usize::MAX,
        }
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

    /// Whether `offset` has been handed out by this tracker, or is below the
    /// cursor. Anything else is not the consumer's to settle: an ack there
    /// would finish a record nobody received, and a nack would make an offset
    /// owed that may not exist yet. A fresh tracker (after eviction or
    /// failover) says no to claims its predecessor made; the serving layer
    /// tells those apart by the log tail.
    pub(crate) fn handed_out(&self, offset: u64) -> bool {
        offset < self.high_water
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
    #[cfg(test)]
    pub(crate) fn committed(&self) -> u64 {
        self.committed
    }

    /// Take up to `max` offsets to deliver, claimed until `now + visibility`.
    ///
    /// Owed offsets come before new ones. A group that always preferred new
    /// records would starve the redeliveries behind a fast producer, and those
    /// are precisely the records a consumer already failed to finish once.
    ///
    /// `tail` is the shard's log tail: nothing at or above it exists yet.
    pub(crate) fn claim(
        &mut self,
        tail: u64,
        max: usize,
        now: Instant,
        visibility: Duration,
    ) -> Claim {
        self.expire(now);

        let wanted = max.min(MAX_CLAIM);
        let room = self.max_in_flight.saturating_sub(self.in_flight.len());
        let max = wanted.min(room);
        let deadline = now + visibility;
        let mut claim = Claim {
            offsets: Vec::with_capacity(max.min(16)),
            dead_lettered: Vec::new(),
            capped: false,
        };

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
            self.hand_out(offset, deadline);
            claim.offsets.push(offset);
        }

        while claim.offsets.len() < max && self.high_water < tail {
            let offset = self.high_water;
            self.high_water += 1;
            self.hand_out(offset, deadline);
            claim.offsets.push(offset);
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
        // `high_water` can lag when a group is created above its acks.
        self.high_water = self.high_water.max(self.committed);
        (self.committed != before).then_some(self.committed)
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
            self.redeliver.insert(offset);
        }
    }

    /// How many offsets are handed out and unsettled.
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        debug_assert_eq!(self.in_flight.len(), self.lapses.len());
        self.in_flight.len()
    }

    fn hand_out(&mut self, offset: u64, deadline: Instant) {
        // A redelivery replaces the claim it was owed under, if one stands.
        self.take_in_flight(offset);
        self.in_flight.insert(offset, deadline);
        self.lapses.insert((deadline, offset));
        *self.attempts.entry(offset).or_insert(0) += 1;
    }

    /// Drop the claim on `offset`, returning whether there was one.
    fn take_in_flight(&mut self, offset: u64) -> bool {
        match self.in_flight.remove(&offset) {
            Some(deadline) => {
                self.lapses.remove(&(deadline, offset));
                true
            }
            None => false,
        }
    }
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

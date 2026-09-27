//! One authoritative apply order per log.
//!
//! A durable write has two halves separated by an `await`:
//!
//!   1. the durable append, which assigns disk offsets under the segment lock
//!      and then waits for the fsync its policy requires, and
//!   2. everything the rest of the system observes — the broker's replay ring
//!      and fanout, or the cache's index and watch observer.
//!
//! Without coordination those halves can disagree. Two concurrent writes A and
//! B take disk offsets in that order, both wait on the same group-commit flush,
//! and then resume in whatever order the scheduler picks. B can apply first, so
//! the log on disk reads A, B while every reader sees B, A. Under
//! `FsyncMode::OnCommit` that window is a whole device flush wide —
//! milliseconds — so it is not a theoretical race.
//!
//! The sequencer closes it. Each writer waits until every lower offset has been
//! applied, then applies its own and releases the next in line. Disk order
//! becomes the single source of truth for what readers observe.
//!
//! What this deliberately does *not* do is serialise the durable append itself.
//! Offsets are still assigned concurrently and flushes are still shared, so
//! group commit keeps its fan-in; only the cheap post-flush half is ordered.
//! The broker's publish path and the cache's write path both rely on that.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::log::Offset;

/// Orders the post-durability half of writes by their disk offset.
pub struct CommitSequencer {
    state: Mutex<SequenceState>,
}

impl CommitSequencer {
    pub fn new(next: Offset) -> Self {
        Self {
            state: Mutex::new(SequenceState {
                next,
                resolved: BTreeMap::new(),
                generation: 0,
                waiters: BTreeMap::new(),
            }),
        }
    }

    /// The offset whose turn it is — everything below it has applied.
    ///
    /// Also how a caller that appends outside the reserve path (recovery,
    /// compaction, replication shipping) tells whether writers are in flight:
    /// with nothing reserved and unresolved, this equals the log's next offset.
    pub fn next_offset(&self) -> Offset {
        self.state.lock().next
    }

    /// Claim the offset range `[first_offset, next_offset)` in the commit order.
    ///
    /// Returns immediately and does **not** wait for a turn — call
    /// [`CommitTurn::wait`] for that. The split matters: the guard must be
    /// created the moment offsets are consumed, because from then on the range
    /// exists on disk and every later offset is queued behind it. If the caller
    /// then fails, or its future is cancelled part-way through the durability
    /// wait, the guard's `Drop` still releases the range and the stream keeps
    /// moving. Claiming the range only *after* a successful append is what
    /// stranded the stream: an abandoned range never released, and every
    /// subsequent publish waited on a turn that could not arrive.
    pub fn reserve(&self, first_offset: Offset, next_offset: Offset) -> CommitTurn<'_> {
        CommitTurn {
            sequencer: SequencerRef::Borrowed(self),
            first_offset,
            next_offset,
            generation: self.state.lock().generation,
        }
    }

    /// [`CommitSequencer::reserve`], but the turn owns its sequencer and so has
    /// no lifetime tying it to this call.
    ///
    /// Same guarantees, including the one that matters: the range is claimed
    /// the moment offsets are consumed, and `Drop` releases it however the
    /// caller exits. The difference is only that the claim can be moved into
    /// another task, which is what lets durability waits overlap.
    pub fn reserve_owned(
        self: &Arc<Self>,
        first_offset: Offset,
        next_offset: Offset,
    ) -> CommitTurn<'static> {
        CommitTurn {
            sequencer: SequencerRef::Owned(Arc::clone(self)),
            first_offset,
            next_offset,
            generation: self.state.lock().generation,
        }
    }

    /// Restart the sequence at `next`, used when a stream adopts a recovered
    /// log and its offsets resume from the durable tail.
    pub fn reset(&self, next: Offset) {
        let orphaned: Vec<oneshot::Sender<()>> = {
            let mut state = self.state.lock();
            state.next = next;
            // Pending resolutions describe a sequence that no longer exists.
            state.resolved.clear();
            state.generation += 1;
            // Wake everyone: a waiter parked on an offset the reset just
            // discarded would otherwise never be released. Its `wait` sees the
            // generation has moved and reports `Superseded`.
            state.waiters.split_off(&0).into_values().collect()
        };
        for waiter in orphaned {
            let _ = waiter.send(());
        }
    }
}

impl fmt::Debug for CommitSequencer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        f.debug_struct("CommitSequencer")
            .field("next", &state.next)
            .field("pending_resolutions", &state.resolved.len())
            .field("generation", &state.generation)
            .finish()
    }
}

#[derive(Debug)]
struct SequenceState {
    /// Offset whose turn it is. Publishers wait until this reaches their first
    /// offset.
    next: Offset,
    /// Ranges that have resolved out of order, keyed by their first offset.
    ///
    /// A range resolves when its guard drops — whether it applied cleanly or
    /// was abandoned. Resolving is *not* the same as being next: a range behind
    /// an unfinished predecessor has to wait its turn to be counted, or a
    /// cancelled range would hand its successors permission to overtake the
    /// range still in flight ahead of them, and the replay ring would receive
    /// offsets out of order. Entries live here only until the prefix catches
    /// up, so this holds at most one entry per publish in flight.
    resolved: BTreeMap<Offset, Offset>,
    /// Bumped by every reset. A turn acquired before a reset carries
    /// pre-reset offsets, so releasing it must not move the sequence: the
    /// reset is authoritative about where the log now ends.
    generation: u64,
    /// One sender per parked waiter, keyed by the offset it is waiting for.
    ///
    /// This is what makes a release wake *the* waiter rather than all of them.
    /// Broadcasting cost every parked publisher a wake-up, a lock acquisition
    /// and a re-park per commit — so the work per commit grew with the number
    /// of publishers in flight, and throughput fell as concurrency rose.
    ///
    /// At most one waiter becomes eligible per release, which is what lets this
    /// be a single wake rather than a scan: `next` only advances past a range
    /// once that range's own waiter has finished with it, so the waiter at
    /// `next` is the only one whose condition can have just become true.
    waiters: BTreeMap<Offset, oneshot::Sender<()>>,
}

impl SequenceState {
    /// Record a resolved range and advance through whatever contiguous prefix
    /// that completes.
    fn resolve(&mut self, first_offset: Offset, next_offset: Offset) {
        if next_offset > self.next {
            self.resolved.insert(first_offset, next_offset);
        }
        // Walk forward only while the next range in line has resolved. A gap
        // stops the walk, which is exactly the range still in flight.
        while let Some(end) = self.resolved.remove(&self.next) {
            self.next = self.next.max(end);
        }
    }

    /// The waiter whose turn it now is, removed from the registry.
    ///
    /// Returned rather than fired here so the send happens outside the lock:
    /// waking a task while holding the mutex it is about to want is how a
    /// wake-up turns into a hand-off stall.
    fn take_ready(&mut self) -> Option<oneshot::Sender<()>> {
        // Every waiter at or below `next` is eligible. Normally that is one —
        // a range's successor cannot be eligible until the range resolves — but
        // a reset can move `next` arbitrarily, and a waiter left parked behind
        // one would never be released.
        let first = *self.waiters.keys().next()?;
        if first <= self.next {
            self.waiters.remove(&first)
        } else {
            None
        }
    }
}

/// A claim on one offset range. Releasing it lets the next range proceed.
pub struct CommitTurn<'a> {
    sequencer: SequencerRef<'a>,
    first_offset: Offset,
    next_offset: Offset,
    /// Generation this range was claimed in.
    generation: u64,
}

impl CommitTurn<'_> {
    /// Wait until every lower offset has been released.
    ///
    /// Fails with [`Superseded`] when a reset lands before or during the wait:
    /// the caller must then apply nothing, since whatever it would publish
    /// sits at offsets the reset declared gone.
    ///
    /// Cancellation-safe: dropping the guard mid-wait releases this range too,
    /// so a cancelled publisher cannot block the ones behind it.
    pub async fn wait(&self) -> Result<(), Superseded> {
        // Registered and tested under one lock, so a release landing between
        // the two cannot be missed: whoever resolves next either sees this
        // waiter in the registry and wakes it, or has already moved `next` past
        // it and the test below returns.
        let receiver = {
            let mut state = self.sequencer.state.lock();
            if state.generation != self.generation {
                return Err(Superseded);
            }
            if state.next >= self.first_offset {
                return Ok(());
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters.insert(self.first_offset, sender);
            receiver
        };

        // A closed channel means the sender was dropped rather than fired --
        // only possible if this waiter's entry was replaced, which needs two
        // live turns on one offset. Either way the generation decides: a reset
        // wakes every waiter, and those must not go on to apply.
        let _ = receiver.await;
        if self.is_current() {
            Ok(())
        } else {
            Err(Superseded)
        }
    }

    /// False once a reset has superseded this turn.
    ///
    /// A caller that applies under a lock the resetter also holds while
    /// resetting can check this under that lock, which closes the gap between
    /// `wait` returning and the apply.
    pub fn is_current(&self) -> bool {
        self.sequencer.state.lock().generation == self.generation
    }
}

impl Drop for CommitTurn<'_> {
    fn drop(&mut self) {
        let ready = {
            let mut state = self.sequencer.state.lock();
            // Dropped mid-wait: take this range's own registration with it, or
            // the entry outlives the waiter and a later release wakes nobody.
            state.waiters.remove(&self.first_offset);
            // A reset while this range was held means the log was rewritten
            // underneath it. Its offsets describe a sequence that no longer
            // exists, so resolving on them would step the stream past records
            // that are gone.
            if state.generation == self.generation {
                state.resolve(self.first_offset, self.next_offset);
            }
            state.take_ready()
        };
        // Outside the lock: the woken task wants this mutex immediately.
        if let Some(waiter) = ready {
            let _ = waiter.send(());
        }
    }
}

/// A reset moved the sequence while a turn was held, so the turn's offsets
/// belong to a log that no longer exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superseded;

impl fmt::Display for Superseded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the log was reset while this write waited for its turn")
    }
}

impl std::error::Error for Superseded {}

/// How a turn holds the sequencer it will release into.
///
/// A borrowed turn is the common case and costs nothing. An owned one exists so
/// a claim can outlive the stack frame that made it -- which is what lets a
/// caller claim its offsets in order and then await the device flush
/// concurrently with other publishes, instead of holding the whole stream
/// behind one flush at a time (#535).
enum SequencerRef<'a> {
    Borrowed(&'a CommitSequencer),
    Owned(Arc<CommitSequencer>),
}

impl std::ops::Deref for SequencerRef<'_> {
    type Target = CommitSequencer;

    fn deref(&self) -> &CommitSequencer {
        match self {
            SequencerRef::Borrowed(sequencer) => sequencer,
            SequencerRef::Owned(sequencer) => sequencer,
        }
    }
}

#[cfg(test)]
mod tests;

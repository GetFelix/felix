//! What a reader of a stream shard may see, and the batches held back until
//! it may.
//!
//! On a replicated `Quorum` shard a record is committed when the quorum mark
//! passes it, not when the leader has it: a record past the mark can be lost
//! at failover and its offset reused for a different one. So a batch that is
//! durable here but past the mark is neither appended to the replay ring nor
//! fanned out. It waits in [`CommitHold`], in commit order, and is released
//! once the mark passes it -- ring append and fanout together, one envelope
//! for every subscriber, exactly as an unheld publish does. The ring therefore
//! only ever holds committed records, and every reader that starts from it
//! (cursor replay, resume, the live edge) inherits the bound.
//!
//! Whether a shard is bounded, and where, is the service's to say: it owns
//! the marks and the routes. It answers through [`ReadBounds`], asked only for
//! `Quorum` streams, so a `Leader` stream never pays for the question. A shard
//! whose engine commits every write (Raft) is bounded at every level instead.
//! See "Readers stop at the committed mark" in `docs/replication-design.md`.
//!
//! On such a shard the engine, not the broker, gives a batch its offsets.
//! [`ProposalOrder`] keeps the batches joining the hold in the order they
//! were handed to the engine, which is their offset order.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use felix_storage::{CommitSequencer, CommitTurn};
use parking_lot::Mutex;

/// How far a reader may see into a stream shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadBound {
    /// Everything durable here is committed: a `Leader` stream, a shard
    /// served here with no replicas, or a broker with no cluster.
    Unbounded,
    /// Offsets below this are committed; the rest may yet be lost.
    Committed(u64),
    /// This broker leads the shard but has no mark for its generation yet, so
    /// nothing here is known to be committed. Usually one replication pass
    /// after taking the shard.
    Settling,
    /// This broker must not serve reads of the shard: its lease lapsed, or it
    /// no longer leads it.
    Refused,
}

impl ReadBound {
    /// Whether every offset below `end` is committed.
    pub fn covers(&self, end: u64) -> bool {
        match self {
            Self::Unbounded => true,
            Self::Committed(mark) => end <= *mark,
            Self::Settling | Self::Refused => false,
        }
    }
}

/// The service's answer to "how far may readers see", per stream shard.
///
/// Asked for `Quorum` streams and for shards bounded at every level, on the
/// publish path among others, so it must be cheap and must not block.
pub trait ReadBounds: Send + Sync + std::fmt::Debug {
    fn stream_bound(&self, tenant_id: &str, namespace: &str, stream: &str, shard: u32)
    -> ReadBound;
}

/// Batches durable on this broker that readers may not see yet.
#[derive(Debug, Default)]
pub(crate) struct CommitHold {
    source: OnceLock<ReadSource>,
    /// Something is held or being released. While it is set, every durable
    /// publish joins the queue whatever its own bound says, or it would
    /// overtake the batches ahead of it.
    busy: AtomicBool,
    queue: Mutex<Queue>,
    /// Wakes a release waiting for the bound to cover the front batch.
    wake: tokio::sync::Notify,
    /// Wakes [`Self::settled`]: a release ended or a batch was held.
    settled: tokio::sync::Notify,
    /// Readers at every consistency level stop at the bound, not only
    /// `Quorum` ones.
    every_level: AtomicBool,
}

impl CommitHold {
    /// Tie the hold to its shard and to where the broker's bounds come from.
    /// Called once, as the stream state is built; the bounds themselves can
    /// be installed later, when the cluster side starts.
    pub(crate) fn bind(&self, source: ReadSource) {
        let _ = self.source.set(source);
    }

    /// The bound as the service states it now, `Unbounded` when nobody does.
    pub(crate) fn bound(&self) -> ReadBound {
        let Some(source) = self.source.get() else {
            return ReadBound::Unbounded;
        };
        let Some(bounds) = source.bounds.get() else {
            return ReadBound::Unbounded;
        };
        bounds.stream_bound(
            &source.tenant_id,
            &source.namespace,
            &source.stream,
            source.shard,
        )
    }

    /// Bound readers at every consistency level, for a shard whose engine
    /// acknowledges no write before it commits.
    pub(crate) fn bound_every_level(&self) {
        self.every_level.store(true, Ordering::Release);
    }

    /// Whether [`Self::bound_every_level`] was called.
    pub(crate) fn bounds_every_level(&self) -> bool {
        self.every_level.load(Ordering::Acquire)
    }

    /// The stream and shard this hold is bound to, for naming them in an
    /// error.
    pub(crate) fn shard(&self) -> Option<(&str, u32)> {
        self.source
            .get()
            .map(|source| (source.stream.as_str(), source.shard))
    }

    /// The replay ring's capacity, which a released batch is appended under.
    pub(crate) fn log_capacity(&self) -> Option<usize> {
        self.source.get().map(|source| source.log_capacity)
    }

    /// Whether anything is held or being released.
    pub(crate) fn is_busy(&self) -> bool {
        self.busy.load(Ordering::SeqCst)
    }

    /// Queue a batch behind the ones already held. True when the caller must
    /// start a release; otherwise the one running picks this up.
    pub(crate) fn push(&self, batch: HeldBatch) -> bool {
        let mut queue = self.queue.lock();
        queue.held.push_back(batch);
        self.busy.store(true, Ordering::SeqCst);
        let start = self.start(&mut queue);
        drop(queue);
        self.settled.notify_waiters();
        start
    }

    /// Ask for another look at the bound. True when the caller must start a
    /// release: something is held and no release is running.
    pub(crate) fn kick(&self) -> bool {
        let mut queue = self.queue.lock();
        if queue.held.is_empty() && !queue.releasing {
            return false;
        }
        self.start(&mut queue)
    }

    fn start(&self, queue: &mut Queue) -> bool {
        let start = queue.start();
        if !start {
            self.wake.notify_one();
        }
        start
    }

    /// Resolves on the next kick or push while a release is running.
    pub(crate) async fn woken(&self) {
        self.wake.notified().await;
    }

    /// Called by the release before it reads the bound, so a kick after the
    /// read is not lost.
    pub(crate) fn begin_pass(&self) {
        self.queue.lock().again = false;
    }

    /// The batches the bound now covers, oldest first.
    ///
    /// [`Pass::Wait`] while some stay held: the release keeps running and
    /// looks again, because the bound can come to cover them without the
    /// mark moving (the shard's route or lease changing back), and nothing
    /// kicks the hold then. [`Pass::Done`] once nothing is held, which ends
    /// the release.
    pub(crate) fn take_ready(&self, bound: ReadBound) -> Pass {
        let mut queue = self.queue.lock();
        let mut ready = Vec::new();
        while let Some(front) = queue.held.front()
            && bound.covers(front.end())
        {
            ready.extend(queue.held.pop_front());
        }
        if !ready.is_empty() || queue.again {
            return Pass::Ready(ready);
        }
        if !queue.held.is_empty() {
            return Pass::Wait;
        }
        queue.releasing = false;
        self.busy.store(false, Ordering::SeqCst);
        drop(queue);
        self.settled.notify_waiters();
        Pass::Done
    }

    /// Resolves once the hold is idle or holds a batch, not while a release
    /// runs over an empty queue.
    ///
    /// After [`Self::discard`] the hold stays busy until its release task
    /// next runs and sees nothing left. A caller deciding on
    /// [`Self::is_busy`] right after a discard would otherwise act on a
    /// release that only has to be scheduled to end.
    pub(crate) async fn settled(&self) {
        loop {
            let notified = self.settled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let queue = self.queue.lock();
                if !queue.releasing || !queue.held.is_empty() {
                    return;
                }
            }
            notified.await;
        }
    }

    /// Drop everything held. For a shard this broker stopped leading: what
    /// it holds was never committed under its leadership, and the next
    /// leader's log is the one readers follow.
    pub(crate) fn discard(&self) -> usize {
        let mut queue = self.queue.lock();
        let dropped = queue.held.len();
        queue.held.clear();
        self.busy.store(queue.releasing, Ordering::SeqCst);
        if queue.releasing {
            self.wake.notify_one();
        }
        dropped
    }
}

/// What a release does after one look at the bound.
#[derive(Debug)]
pub(crate) enum Pass {
    /// Append and fan these out, then look again.
    Ready(Vec<HeldBatch>),
    /// Something is held that the bound does not cover yet.
    Wait,
    /// Nothing is held; the release ends.
    Done,
}

/// Where a hold asks for its bound.
#[derive(Debug)]
pub(crate) struct ReadSource {
    pub(crate) bounds: Arc<OnceLock<Arc<dyn ReadBounds>>>,
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) stream: String,
    pub(crate) shard: u32,
    pub(crate) log_capacity: usize,
}

/// One publish batch, durable and waiting for the mark.
#[derive(Debug)]
pub(crate) struct HeldBatch {
    pub(crate) payloads: Vec<Bytes>,
    pub(crate) publisher: Option<Bytes>,
    /// Each record's headers block, when any has one.
    pub(crate) record_headers: Option<std::sync::Arc<[Option<Bytes>]>>,
    /// When the batch's records were appended.
    pub(crate) timestamp_micros: Option<u64>,
    pub(crate) first_offset: u64,
    /// A commit's state updates, applied when the event is released.
    pub(crate) commit: Option<std::sync::Arc<[crate::commit::StateOp]>>,
    /// The floor of a trim record, which is the batch's one record and is
    /// applied to the ring rather than appended to it.
    pub(crate) trim: Option<u64>,
    /// Told once the trim's floor reaches the ring.
    pub(crate) trim_applied: Option<tokio::sync::oneshot::Sender<()>>,
}

impl HeldBatch {
    /// One past the batch's last offset.
    pub(crate) fn end(&self) -> u64 {
        self.first_offset + self.payloads.len() as u64
    }
}

/// The order batches were handed to an engine that gives them their
/// offsets, so they join the hold in offset order.
///
/// The engine queues a batch when it is handed one and writes its queue in
/// order, so the order of hand-overs is the order of offsets. Publishers
/// learn their offsets in any order; each waits for the turn of the one
/// handed over before it. A turn is released however its publish ends, so
/// a failed or cancelled one never holds up the rest.
#[derive(Debug)]
pub(crate) struct ProposalOrder {
    /// The next hand-over's place. Locked across the hand-over itself.
    next: Mutex<u64>,
    turns: Arc<CommitSequencer>,
}

impl Default for ProposalOrder {
    fn default() -> Self {
        Self {
            next: Mutex::new(0),
            turns: Arc::new(CommitSequencer::new(0)),
        }
    }
}

impl ProposalOrder {
    /// Call `hand_over` with the next place taken. It must queue its batch
    /// with the engine before it returns.
    pub(crate) fn hand_over<T>(&self, hand_over: impl FnOnce() -> T) -> (T, CommitTurn<'static>) {
        let mut next = self.next.lock();
        let place = *next;
        *next += 1;
        let turn = self.turns.reserve_owned(place, place + 1);
        (hand_over(), turn)
    }
}

#[derive(Debug, Default)]
struct Queue {
    held: VecDeque<HeldBatch>,
    /// A release task is running. There is one per stream at most, which is
    /// what keeps released batches in offset order.
    releasing: bool,
    /// The bound may have moved since the running release last read it.
    again: bool,
}

impl Queue {
    fn start(&mut self) -> bool {
        if self.releasing {
            self.again = true;
            false
        } else {
            self.releasing = true;
            true
        }
    }
}

#[cfg(test)]
mod tests;

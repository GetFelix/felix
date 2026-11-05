//! Publish streams that each carry one shard, opened only for shards that stall.
//!
//! The broker answers a pipelining stream's publishes in the order the stream
//! carried them, and stops reading the stream once its window is full. On a
//! pooled stream shared by several shards, one shard stuck on a quorum wait
//! holds back answers the others have already committed. A stream per shard
//! avoids that but costs throughput when keys spread over many shards, so a
//! shard rides the pool until there is evidence it is holding its stream up,
//! and only then gets a stream of its own.
//!
//! The evidence is an overdue answer: the publish at the head of a stream, the
//! one every later answer waits behind, has waited longer than the stream
//! usually takes ([`HeadWatch::bound`]). The writer notes it against the
//! head's shard when the late answer arrives, and a publish of that shard
//! notes it while the answer is still outstanding. A publish of any other
//! shard that finds its pooled stream's head overdue moves too, rather than
//! queue behind it, and stays out until both it and the pool are calm.
//!
//! A shard moves only with nothing of its own in flight. Each publish holds a
//! [`ShardTicket`] until its answer is read, and the publish that moves the
//! shard waits for the count to reach zero while the shard's other publishes
//! wait for the move, so nothing of the shard is left on the old stream to be
//! overtaken. The way back is the same, once the shard has gone a calm period
//! without an overdue answer. A shard that stalls again soon after going back
//! stays out twice as long the next time, so a flapping shard settles on its
//! own stream instead of moving every few seconds.
//!
//! A shard that ever publishes without an ack stays where it is: such a
//! publish has no answer to drain, so one sent after it on another stream
//! could reach the broker first.
//!
//! See "Publish streams per shard" in docs/protocol.md.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use ahash::RandomState;
use anyhow::Result;
use hashbrown::HashMap;
use tokio::sync::Notify;
use tracing::debug;

use super::writer::PublishWorker;

/// Opens and authenticates one publish stream and starts its writer.
pub(crate) type OpenWorker =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<PublishWorker>> + Send>> + Send + Sync>;

/// An answer slower than this at the head of its stream is never ordinary:
/// well above a quorum ack's fsync and round trip, well below the seconds a
/// stalled quorum wait takes.
pub(crate) const STALL_FLOOR: Duration = Duration::from_millis(250);
/// How long a shard on its own stream must go without an overdue answer
/// before it rejoins the pool, the first time. Also how long a stall counts
/// as evidence for moving a pooled shard.
pub(crate) const CALM_FIRST: Duration = Duration::from_secs(5);
const CALM_MAX: Duration = Duration::from_secs(300);
/// How long an own stream must go unused before a sweep gives it back. Well
/// past the calm period: the sweep cannot see whether the shard's pool is
/// calm, and a shard still publishing here is better sent back by its own
/// route, which can.
const IDLE_RETURN: Duration = Duration::from_secs(30);
/// Cells kept before a sweep starts forgetting unused pooled ones.
const FORGET_ABOVE_MIN: usize = 4096;

const POOLED: u8 = 0;
const OWN: u8 = 1;
const MOVING: u8 = 2;

/// The per-shard publish streams of one client.
pub(crate) struct ShardStreams {
    /// Most shards on streams of their own at once.
    cap: usize,
    open: OpenWorker,
    cells: RwLock<HashMap<ShardStreamKey, Arc<Cell>, RandomState>>,
    own: AtomicUsize,
    /// When idle own streams and unused cells were last swept, as [`now_nanos`].
    swept_at: AtomicU64,
    /// Cell count past which a sweep also forgets unused pooled cells.
    forget_above: AtomicUsize,
    /// Set by `Publisher::finish`, so a finished client does not open new
    /// streams for shards whose writers it just ended.
    closed: AtomicBool,
}

/// One shard's place: the pool or its own stream.
struct Cell {
    id: u64,
    phase: AtomicU8,
    inflight: AtomicUsize,
    /// When the shard last held its stream up, as [`now_nanos`]. 0 for never.
    stalled_at: AtomicU64,
    /// No move out before this, as [`now_nanos`]: set when opening a stream
    /// failed, so a stall does not retry the open on every publish.
    retry_at: AtomicU64,
    pinned: AtomicBool,
    /// When a publish last went out on the shard's own stream, as [`now_nanos`].
    used_at: AtomicU64,
    drained: Notify,
    settled: Notify,
    /// Changed only by the publish that holds the cell in `MOVING`.
    moves: Mutex<Moves>,
}

struct Moves {
    own: Option<Arc<PublishWorker>>,
    since: u64,
    returned_at: u64,
    calm: Duration,
}

/// A claim on a shard's place, held by a routed publish and then by its
/// request until the answer is read or the request fails. While any is held
/// the shard does not move.
pub(crate) struct ShardTicket {
    cell: Arc<Cell>,
}

impl ShardTicket {
    /// Keeps the shard where it is from now on. For a publish sent without an
    /// ack, which has no answer to drain before a move.
    pub(crate) fn pin(&self) {
        self.cell.pinned.store(true, Ordering::Release);
    }

    /// Tells the shard its answer waited `waited` at the head of its stream,
    /// where `bound` is overdue for that stream.
    pub(crate) fn answered(&self, waited: Duration, bound: Duration) {
        if waited > bound {
            self.cell.stalled_at.store(now_nanos(), Ordering::Release);
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.cell.id
    }

    #[cfg(test)]
    pub(crate) fn pinned(&self) -> bool {
        self.cell.pinned.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn stalled(&self) -> bool {
        self.cell.stalled_at.load(Ordering::Acquire) != 0
    }
}

impl Clone for ShardTicket {
    fn clone(&self) -> Self {
        self.cell.inflight.fetch_add(1, Ordering::SeqCst);
        Self {
            cell: Arc::clone(&self.cell),
        }
    }
}

impl Drop for ShardTicket {
    fn drop(&mut self) {
        if self.cell.inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.cell.drained.notify_waiters();
        }
    }
}

/// Which shard a writer's head publish is and how long it has been the head.
/// Written by the writer task, read by routing.
#[derive(Default)]
pub(crate) struct HeadWatch {
    /// When the current head became the head, as [`now_nanos`]. 0 when idle.
    since: AtomicU64,
    shard: AtomicU64,
    /// Smoothed time an answer spends at the head, in nanoseconds.
    smoothed: AtomicU64,
}

impl HeadWatch {
    /// The head wait past which an answer on this stream is overdue: eight
    /// times the usual, never under [`STALL_FLOOR`]. A broker slow for every
    /// shard raises the bound rather than moving them all.
    pub(crate) fn bound(&self) -> Duration {
        let usual = Duration::from_nanos(self.smoothed.load(Ordering::Relaxed));
        (usual * 8).max(STALL_FLOOR)
    }

    /// The head is now the publish of `shard` (0 for none) from `at`, or
    /// nothing is unanswered.
    pub(crate) fn set(&self, head: Option<u64>, at: u64) {
        match head {
            Some(shard) => {
                self.shard.store(shard, Ordering::Relaxed);
                self.since.store(at.max(1), Ordering::Release);
            }
            None => self.since.store(0, Ordering::Release),
        }
    }

    /// The head that just left waited `waited`. Clamped to the bound, so one
    /// stall does not hide the next.
    pub(crate) fn sample(&self, waited: Duration) {
        let waited = waited.min(self.bound()).as_nanos() as u64;
        let old = self.smoothed.load(Ordering::Relaxed);
        self.smoothed
            .store(old - old / 8 + waited / 8, Ordering::Relaxed);
    }

    /// When the current head became the head, as [`now_nanos`].
    pub(crate) fn since(&self) -> Option<u64> {
        match self.since.load(Ordering::Acquire) {
            0 => None,
            since => Some(since),
        }
    }

    /// The shard of a head that has waited longer than `bound` by `now`.
    fn overdue(&self, now: u64, bound: Duration) -> Option<u64> {
        let since = self.since()?;
        let shard = self.shard.load(Ordering::Relaxed);
        (Duration::from_nanos(now.saturating_sub(since)) > bound).then_some(shard)
    }
}

/// Monotonic nanoseconds, never 0. Tokio's clock, so tests can pause it.
pub(crate) fn now_nanos() -> u64 {
    static EPOCH: OnceLock<tokio::time::Instant> = OnceLock::new();
    EPOCH
        .get_or_init(tokio::time::Instant::now)
        .elapsed()
        .as_nanos() as u64
        + 1
}

impl ShardStreams {
    pub(crate) fn new(cap: usize, open: OpenWorker) -> Self {
        Self {
            cap,
            open,
            cells: RwLock::new(HashMap::with_hasher(RandomState::new())),
            own: AtomicUsize::new(0),
            swept_at: AtomicU64::new(0),
            forget_above: AtomicUsize::new(FORGET_ABOVE_MIN),
            closed: AtomicBool::new(false),
        }
    }

    /// Where the shard's next publish goes: its own stream, or `None` for
    /// `pooled`, with the ticket the publish holds until it is answered.
    ///
    /// Moves the shard first when the evidence says to. A move waits for the
    /// shard's publishes in flight to be answered, and the shard's other
    /// publishes wait for the move.
    pub(crate) async fn route(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        pooled: &PublishWorker,
    ) -> (Option<Arc<PublishWorker>>, Option<ShardTicket>) {
        if self.cap == 0 {
            return (None, None);
        }
        self.maybe_sweep().await;
        let cell = self.cell(&ShardStreamKeyRef {
            tenant_id,
            namespace,
            stream,
            shard,
        });
        loop {
            match cell.phase.load(Ordering::SeqCst) {
                MOVING => {
                    let settled = cell.settled.notified();
                    tokio::pin!(settled);
                    settled.as_mut().enable();
                    if cell.phase.load(Ordering::SeqCst) == MOVING {
                        settled.await;
                    }
                }
                POOLED => {
                    let now = now_nanos();
                    let overdue = pooled.head.overdue(now, pooled.head.bound());
                    if overdue == Some(cell.id) {
                        cell.stalled_at.store(now, Ordering::Release);
                    }
                    // Another shard's overdue head would hold this publish
                    // up too, so this shard leaves as well.
                    if self.wants_own(&cell, now, overdue.is_some()) && cell.begin_move(POOLED) {
                        self.move_to_own(&cell).await;
                        continue;
                    }
                    if let Some(ticket) = cell.ticket(POOLED) {
                        return (None, Some(ticket));
                    }
                }
                _ => {
                    let now = now_nanos();
                    let worker = cell.moves.lock().expect("shard cell").own.clone();
                    let gone = worker.as_ref().is_none_or(|worker| worker.tx.is_closed());
                    if let Some(worker) = worker.as_ref().filter(|_| !gone)
                        && worker.head.overdue(now, worker.head.bound()).is_some()
                    {
                        cell.stalled_at.store(now, Ordering::Release);
                    }
                    // A writer that failed has failed everything it held, so
                    // the shard can leave it at once. A calm shard waits for
                    // the pool to be calm too.
                    let back = gone
                        || (cell.recovered(now)
                            && pooled.head.overdue(now, pooled.head.bound()).is_none());
                    if back && cell.begin_move(OWN) {
                        self.move_to_pool(&cell, gone).await;
                        continue;
                    }
                    if let Some(ticket) = cell.ticket(OWN) {
                        cell.used_at.store(now, Ordering::Relaxed);
                        return (worker, Some(ticket));
                    }
                }
            }
        }
    }

    /// Whether shards may get streams of their own at all.
    pub(crate) fn enabled(&self) -> bool {
        self.cap > 0
    }

    /// Stop opening streams, and return the writers that are open so they
    /// can be finished.
    pub(crate) fn close(&self) -> Vec<Arc<PublishWorker>> {
        self.closed.store(true, Ordering::Release);
        self.cells
            .read()
            .expect("shard streams")
            .values()
            .filter_map(|cell| cell.moves.lock().expect("shard cell").own.clone())
            .collect()
    }

    /// How many shards the client keeps a place for.
    #[cfg(test)]
    pub(crate) fn cell_count(&self) -> usize {
        self.cells.read().expect("shard streams").len()
    }

    /// How many shards are on streams of their own.
    #[cfg(test)]
    pub(crate) fn own_count(&self) -> usize {
        self.own.load(Ordering::Acquire)
    }

    fn cell(&self, key: &ShardStreamKeyRef<'_>) -> Arc<Cell> {
        if let Some(cell) = self.cells.read().expect("shard streams").get(key) {
            return Arc::clone(cell);
        }
        let mut cells = self.cells.write().expect("shard streams");
        if let Some(cell) = cells.get(key) {
            return Arc::clone(cell);
        }
        // Unique across clients: a writer's head names its shard by id.
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let cell = Arc::new(Cell::new(NEXT_ID.fetch_add(1, Ordering::Relaxed)));
        cells.insert(key.owned(), Arc::clone(&cell));
        cell
    }

    /// At most once per [`CALM_FIRST`]: send back own streams nothing has used
    /// for a while, and forget unused pooled cells once there are
    /// many. Without it a shard whose leader moved to another broker would keep
    /// its idle stream here, and its place under the cap, until close.
    async fn maybe_sweep(&self) {
        let now = now_nanos();
        let swept_at = self.swept_at.load(Ordering::Relaxed);
        if now.saturating_sub(swept_at) < CALM_FIRST.as_nanos() as u64
            || self
                .swept_at
                .compare_exchange(swept_at, now, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let idle: Vec<Arc<Cell>> = self
            .cells
            .read()
            .expect("shard streams")
            .values()
            .filter(|cell| {
                cell.phase.load(Ordering::Acquire) == OWN && cell.idle(now) && cell.recovered(now)
            })
            .cloned()
            .collect();
        for cell in idle {
            if !cell.begin_move(OWN) {
                continue;
            }
            // A publish that took a ticket just before the move began would
            // make the drain wait on it; leave that shard for its own route.
            if cell.inflight.load(Ordering::SeqCst) != 0 {
                cell.settle(OWN);
                continue;
            }
            self.move_to_pool(&cell, false).await;
        }
        let limit = self.forget_above.load(Ordering::Relaxed);
        if self.cells.read().expect("shard streams").len() > limit {
            let mut cells = self.cells.write().expect("shard streams");
            // Held only by the map: no publish has it, so a later lookup
            // making a fresh cell cannot split the shard across two. Pinned
            // and recently stalled cells keep what they know.
            cells.retain(|_, cell| {
                Arc::strong_count(cell) > 1
                    || cell.phase.load(Ordering::Acquire) != POOLED
                    || cell.pinned.load(Ordering::Acquire)
                    || cell.stalled_at.load(Ordering::Acquire) != 0
            });
            self.forget_above
                .store((cells.len() * 2).max(FORGET_ABOVE_MIN), Ordering::Relaxed);
        }
    }

    /// Whether a pooled shard should move: it stalled lately, or its pooled
    /// stream is `blocked` behind an overdue head.
    fn wants_own(&self, cell: &Cell, now: u64, blocked: bool) -> bool {
        let stalled_at = cell.stalled_at.load(Ordering::Acquire);
        let stalled =
            stalled_at != 0 && Duration::from_nanos(now.saturating_sub(stalled_at)) < CALM_FIRST;
        (stalled || blocked)
            && now >= cell.retry_at.load(Ordering::Acquire)
            && !cell.pinned.load(Ordering::Acquire)
            && !self.closed.load(Ordering::Acquire)
            && self.own.load(Ordering::Acquire) < self.cap
    }

    async fn move_to_own(&self, cell: &Cell) {
        // A caller may drop its publish mid-move; the guard puts the shard
        // back in the pool rather than leaving it moving for good.
        let mut undo = Undo {
            cell,
            back: POOLED,
            claimed: None,
        };
        cell.drain().await;
        if cell.pinned.load(Ordering::Acquire)
            || self.closed.load(Ordering::Acquire)
            || self
                .own
                .try_update(Ordering::AcqRel, Ordering::Acquire, |own| {
                    (own < self.cap).then_some(own + 1)
                })
                .is_err()
        {
            return;
        }
        undo.claimed = Some(&self.own);
        let worker = match (self.open)().await {
            Ok(worker) => Arc::new(worker),
            Err(err) => {
                debug!(error = %err, "could not open a shard publish stream; the shard stays pooled");
                return;
            }
        };
        // `close` may have collected the writers while this one opened; it
        // would never be finished.
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let now = now_nanos();
        {
            let mut moves = cell.moves.lock().expect("shard cell");
            // Out again soon after its last return: the shard is flapping.
            let flapping = moves.returned_at != 0
                && Duration::from_nanos(now.saturating_sub(moves.returned_at)) < moves.calm * 4;
            moves.calm = if flapping {
                (moves.calm * 2).min(CALM_MAX)
            } else {
                CALM_FIRST
            };
            moves.own = Some(worker);
            moves.since = now;
        }
        undo.done(OWN);
    }

    /// `gone` says the own writer has failed, so nothing of the shard is left
    /// on it to be overtaken.
    async fn move_to_pool(&self, cell: &Cell, gone: bool) {
        let undo = Undo {
            cell,
            back: OWN,
            claimed: None,
        };
        cell.drain().await;
        // An unacked publish written during the drain could still be overtaken
        // on another stream.
        if !gone && cell.pinned.load(Ordering::Acquire) {
            return;
        }
        let worker = {
            let mut moves = cell.moves.lock().expect("shard cell");
            moves.returned_at = now_nanos();
            moves.own.take()
        };
        if worker.is_some() {
            self.own.fetch_sub(1, Ordering::AcqRel);
        }
        cell.stalled_at.store(0, Ordering::Release);
        undo.done(POOLED);
        // The last clone ends the writer, which finishes the stream. Nothing
        // of the shard is left on it.
        drop(worker);
    }
}

/// Ends a move: in `back`, unless [`Self::done`] says where the shard went.
struct Undo<'a> {
    cell: &'a Cell,
    back: u8,
    /// A place claimed under the cap, given back if the move does not finish.
    claimed: Option<&'a AtomicUsize>,
}

impl Undo<'_> {
    fn done(mut self, phase: u8) {
        self.claimed = None;
        self.back = phase;
    }
}

impl Drop for Undo<'_> {
    fn drop(&mut self) {
        if let Some(own) = self.claimed {
            own.fetch_sub(1, Ordering::AcqRel);
            self.cell.stalled_at.store(0, Ordering::Release);
            self.cell.retry_at.store(
                now_nanos() + CALM_FIRST.as_nanos() as u64,
                Ordering::Release,
            );
        }
        self.cell.settle(self.back);
    }
}

impl Cell {
    fn new(id: u64) -> Self {
        Self {
            id,
            phase: AtomicU8::new(POOLED),
            inflight: AtomicUsize::new(0),
            stalled_at: AtomicU64::new(0),
            retry_at: AtomicU64::new(0),
            pinned: AtomicBool::new(false),
            used_at: AtomicU64::new(0),
            drained: Notify::new(),
            settled: Notify::new(),
            moves: Mutex::new(Moves {
                own: None,
                since: 0,
                returned_at: 0,
                calm: CALM_FIRST,
            }),
        }
    }

    /// A ticket for a publish that goes where the shard is in `phase`, unless
    /// a move started meanwhile.
    ///
    /// The count goes up before the phase is read again, and a move sets the
    /// phase before it reads the count, all `SeqCst`: either this sees the
    /// move, or the move waits for this publish.
    fn ticket(self: &Arc<Self>, phase: u8) -> Option<ShardTicket> {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        let ticket = ShardTicket {
            cell: Arc::clone(self),
        };
        (self.phase.load(Ordering::SeqCst) == phase).then_some(ticket)
    }

    fn begin_move(&self, from: u8) -> bool {
        self.phase
            .compare_exchange(from, MOVING, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Nothing in flight, and nothing sent on its own stream for
    /// [`IDLE_RETURN`] or its calm period, whichever is longer.
    fn idle(&self, now: u64) -> bool {
        let calm = self.moves.lock().expect("shard cell").calm;
        self.inflight.load(Ordering::SeqCst) == 0
            && Duration::from_nanos(now.saturating_sub(self.used_at.load(Ordering::Relaxed)))
                > calm.max(IDLE_RETURN)
    }

    fn recovered(&self, now: u64) -> bool {
        if self.pinned.load(Ordering::Acquire) {
            return false;
        }
        let (since, calm) = {
            let moves = self.moves.lock().expect("shard cell");
            (moves.since, moves.calm)
        };
        let last = self.stalled_at.load(Ordering::Acquire).max(since);
        Duration::from_nanos(now.saturating_sub(last)) > calm
    }

    async fn drain(&self) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.inflight.load(Ordering::SeqCst) == 0 {
                return;
            }
            drained.await;
        }
    }

    fn settle(&self, phase: u8) {
        self.phase.store(phase, Ordering::SeqCst);
        self.settled.notify_waiters();
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ShardStreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
    shard: u32,
}

/// Borrowed form of [`ShardStreamKey`], so a lookup does not allocate. Hashes
/// the same, field for field.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ShardStreamKeyRef<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    stream: &'a str,
    shard: u32,
}

impl ShardStreamKeyRef<'_> {
    fn owned(&self) -> ShardStreamKey {
        ShardStreamKey {
            tenant_id: self.tenant_id.to_string(),
            namespace: self.namespace.to_string(),
            stream: self.stream.to_string(),
            shard: self.shard,
        }
    }
}

impl hashbrown::Equivalent<ShardStreamKey> for ShardStreamKeyRef<'_> {
    fn equivalent(&self, key: &ShardStreamKey) -> bool {
        self.shard == key.shard
            && self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.stream == key.stream
    }
}

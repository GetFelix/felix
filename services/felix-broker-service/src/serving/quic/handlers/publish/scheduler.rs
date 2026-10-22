//! The publish scheduler: per-shard ordered lanes, fed fairly across tenants.
//!
//! Every queued publish belongs to a *lane*, the unit whose order matters: a
//! stream shard this broker writes, or one it forwards to another broker. A
//! lane runs one job at a time, in arrival order, which is what keeps offsets
//! claimed in the order publishes arrived. Different lanes run side by side
//! on a small set of executors, and which ready lane an executor takes next
//! is decided per tenant by deficit round robin (see [`fair_queue`]).
//!
//! An executor only holds a lane for the ordered part of a job. Anything that
//! waits on something outside the broker -- a forward's round trip, a quorum,
//! a device flush -- is handed to its own task, so one slow shard or peer
//! holds up its own lane and nothing else. See `worker` for what each kind of
//! job does on its lane.
//!
//! The queue is bounded. A publish that finds no room is answered with a
//! retryable `overloaded` when it has an ack, and counted against its tenant
//! either way: see `ingress::enqueue_publish`.
//!
//! With core shards on there is one partition per core, each with its own
//! queue and executors on that core, so a stream's claims stay on the core
//! that owns it.

mod fair_queue;

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::Notify;

use super::{PublishJob, PublishTarget};
use crate::serving::quic::GLOBAL_INGRESS_DEPTH;
use crate::serving::quic::telemetry::t_gauge;
use fair_queue::FairQueue;

/// What a tenant earns per turn, in bytes. A 64 × 1 KiB batch plus its job
/// cost lands just over this, but a backlogged tenant carries its deficit
/// forward, so tenants of similar size still alternate about job for job.
const QUANTUM_BYTES: usize = 64 * 1024;
/// Added to every job's byte cost, so a tenant sending empty or tiny batches
/// still pays something for each turn it takes.
const JOB_COST_BYTES: usize = 256;
/// How far the queue depth may drift from the gauge before it is set again.
const DEPTH_STEP: usize = 16;

/// The process-wide publish queue and its executors.
///
/// Held by every connection's `PublishContext`. When the last one goes the
/// queue closes: executors finish what is queued and exit, which is what the
/// shutdown drain waits for.
pub(crate) struct PublishScheduler {
    partitions: Vec<Arc<Partition>>,
}

impl PublishScheduler {
    /// A scheduler with `partitions` queues. `capacity` and `share` are per
    /// partition: see [`FairQueue::new`].
    ///
    /// Executors spin for `FELIX_INGRESS_SPIN_US` microseconds (default 0, off)
    /// before they park; see [`Self::with_spin`].
    pub(crate) fn new(partitions: usize, capacity: usize, share: usize) -> Self {
        let spin = std::env::var("FELIX_INGRESS_SPIN_US")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0);
        Self::with_spin(partitions, capacity, share, spin)
    }

    /// [`Self::new`] with an explicit spin: how many microseconds an executor
    /// that finds the queue empty keeps watching for work before it parks.
    /// Zero parks at once. Spinning saves the wake-up a push would otherwise
    /// pay, at the price of an executor burning CPU for the window each time
    /// it goes idle.
    pub(crate) fn with_spin(
        partitions: usize,
        capacity: usize,
        share: usize,
        spin_us: u64,
    ) -> Self {
        Self {
            partitions: (0..partitions.max(1))
                .map(|_| {
                    Arc::new(Partition::new(
                        capacity,
                        share,
                        Duration::from_micros(spin_us),
                    ))
                })
                .collect(),
        }
    }

    pub(crate) fn partitions(&self) -> &[Arc<Partition>] {
        &self.partitions
    }

    /// Queue `job` for `tenant`, waiting for room until `give_up` resolves.
    ///
    /// Pass a ready future to not wait at all.
    pub(crate) async fn submit<G>(
        &self,
        tenant: &str,
        job: PublishJob,
        give_up: G,
    ) -> Result<(), Rejected>
    where
        G: Future<Output = ()>,
    {
        let lane = LaneKey::of(&job.target);
        let partition = &self.partitions[lane.partition(self.partitions.len())];
        let cost = job
            .payloads
            .iter()
            .map(Bytes::len)
            .sum::<usize>()
            .saturating_add(JOB_COST_BYTES);
        let mut job = match partition.try_push(tenant, lane, job, cost) {
            Err(Rejected::Full(job)) => *job,
            done => return done,
        };
        tokio::pin!(give_up);
        loop {
            // Registered before the retry, so a pop between the two still
            // wakes this waiter.
            let _waiting = partition.register_waiter();
            let space = partition.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            job = match partition.try_push(tenant, lane, job, cost) {
                Err(Rejected::Full(job)) => *job,
                done => return done,
            };
            tokio::select! {
                biased;
                _ = &mut space => {}
                _ = &mut give_up => return Err(Rejected::Full(Box::new(job))),
            }
        }
    }

    /// Queue `job` as one tenant, waiting for room however long it takes.
    #[cfg(test)]
    pub(crate) async fn send(&self, job: PublishJob) {
        if self
            .submit("test", job, std::future::pending())
            .await
            .is_err()
        {
            panic!("the publish queue is closed");
        }
    }

    /// Stop taking jobs. What is queued still runs.
    pub(crate) fn close(&self) {
        for partition in &self.partitions {
            partition.close();
        }
    }
}

impl Drop for PublishScheduler {
    fn drop(&mut self) {
        self.close();
    }
}

/// Why a job was not queued. Carries the job back.
pub(crate) enum Rejected {
    /// No room for this tenant, even after any wait the caller allowed.
    Full(Box<PublishJob>),
    /// The broker is shutting down.
    Closed,
}

/// One queue and the executors that drain it.
pub(crate) struct Partition {
    queue: Mutex<FairQueue<LaneKey, PublishJob>>,
    /// Wakes executors: a lane became ready, or the queue closed.
    ///
    /// A lane that was already ready, or is running a job, is never announced:
    /// the first is already known to a woken or running executor, and the
    /// second is announced by `complete`.
    work: Notify,
    /// Executors inside [`Self::next`], from before they look at the queue
    /// until they hold a job. Running executors are not counted; they look at
    /// the queue again when their job ends. A push or `complete` only wakes
    /// one when this is above zero.
    idle: AtomicUsize,
    /// How long an executor with nothing to do watches before parking.
    spin: Duration,
    /// Idle executors currently spinning. They find work without a wake-up,
    /// so a push does not pay for one unless some idle executor is parked.
    spinning: AtomicUsize,
    /// Set when a lane became ready, cleared by a look that finds nothing.
    /// Lets a spinner skip the queue lock until there may be something.
    ready_hint: AtomicBool,
    /// `notify_one` calls made, for tests.
    #[cfg(test)]
    wakes: AtomicUsize,
    /// Test hook: an executor that has just found the queue empty tells the
    /// first, then waits for the second before it parks.
    #[cfg(test)]
    look_hook: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
    /// Wakes submitters waiting for room.
    space: Notify,
    /// Submitters waiting on `space`, so a pop only pays for a wake-up when
    /// someone is listening.
    waiting: AtomicUsize,
    closed: AtomicBool,
}

impl Partition {
    fn new(capacity: usize, share: usize, spin: Duration) -> Self {
        Self {
            spin,
            spinning: AtomicUsize::new(0),
            ready_hint: AtomicBool::new(false),
            queue: Mutex::new(FairQueue::new(capacity, share, QUANTUM_BYTES)),
            work: Notify::new(),
            idle: AtomicUsize::new(0),
            #[cfg(test)]
            wakes: AtomicUsize::new(0),
            #[cfg(test)]
            look_hook: Mutex::new(None),
            space: Notify::new(),
            waiting: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }

    /// The next job to run and the guard that holds its lane, or `None` once
    /// the queue is closed and empty.
    pub(crate) async fn next(self: &Arc<Self>) -> Option<(PublishJob, LaneGuard)> {
        // Counted before the first look at the queue and kept until this call
        // returns. A producer that pushes after our look took the queue lock
        // after we released it, so it reads the count after our increment and
        // wakes us; one that pushed before is seen by our look. A producer
        // that reads zero therefore has no executor that could miss the job:
        // each one is running and checks the queue when it finishes.
        let _idle = Counted::new(&self.idle);
        let mut spun = false;
        loop {
            let work = self.work.notified();
            tokio::pin!(work);
            work.as_mut().enable();
            {
                let mut queue = self.queue.lock();
                if let Some((lane, job)) = queue.pop() {
                    drop(queue);
                    self.popped(1);
                    let guard = LaneGuard {
                        partition: Arc::clone(self),
                        lane,
                    };
                    return Some((job, guard));
                }
                self.ready_hint.store(false, Ordering::Release);
                // Jobs behind a busy lane still count, so this waits for them.
                if self.closed.load(Ordering::Acquire) && queue.is_empty() {
                    return None;
                }
            }
            #[cfg(test)]
            self.pause_after_look().await;
            // Watch for a while before parking. The look after the spin is
            // what keeps this safe: a push that skipped its wake because we
            // were spinning happened before we stopped counting as spinning,
            // so that look sees it.
            if !spun && !self.spin.is_zero() {
                spun = true;
                self.spin_for_work().await;
                continue;
            }
            spun = false;
            work.await;
        }
    }

    /// Wait, counted as spinning, until work may have arrived, the queue
    /// closes, or the spin window ends.
    async fn spin_for_work(&self) {
        let _spinning = Counted::new(&self.spinning);
        let deadline = Instant::now() + self.spin;
        loop {
            for _ in 0..64 {
                if self.ready_hint.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
                    return;
                }
                std::hint::spin_loop();
            }
            if Instant::now() >= deadline {
                return;
            }
            // Lets other tasks on this thread, such as the producers of a
            // core shard, run.
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    async fn pause_after_look(&self) {
        let hook = self.look_hook.lock().take();
        if let Some((looked, resume)) = hook {
            looked.notify_one();
            resume.notified().await;
        }
    }

    fn try_push(
        self: &Arc<Self>,
        tenant: &str,
        lane: LaneKey,
        job: PublishJob,
        cost: usize,
    ) -> Result<(), Rejected> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Rejected::Closed);
        }
        let ready = self
            .queue
            .lock()
            .push(tenant, lane, job, cost)
            .map_err(|job| Rejected::Full(Box::new(job)))?;
        GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed);
        report_ingress_depth();
        if ready {
            self.ready_hint.store(true, Ordering::Release);
            if !defer_wake(self) {
                self.wake_one();
            }
        }
        Ok(())
    }

    fn popped(&self, jobs: usize) {
        let _ = GLOBAL_INGRESS_DEPTH.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
            Some(depth.saturating_sub(jobs))
        });
        report_ingress_depth();
        if self.waiting.load(Ordering::SeqCst) > 0 {
            self.space.notify_waiters();
        }
    }

    fn complete(&self, lane: &LaneKey) {
        let ready = self.queue.lock().complete(lane);
        // A closing queue may now be empty, which every idle executor needs to
        // hear to exit. Otherwise only a lane that is ready again needs a wake.
        if self.closed.load(Ordering::Acquire) {
            self.work.notify_waiters();
        } else if ready {
            self.ready_hint.store(true, Ordering::Release);
            self.wake_one();
        }
    }

    /// Wake an executor for a lane that just became ready, if any is waiting.
    fn wake_one(&self) {
        // Spinners are idle too but need no wake-up; they look again when
        // they stop. Read in this order, a spinner that stopped since is seen
        // as idle and not spinning, and is woken.
        let spinning = self.spinning.load(Ordering::SeqCst);
        if self.idle.load(Ordering::SeqCst) > spinning {
            #[cfg(test)]
            self.wakes.fetch_add(1, Ordering::Relaxed);
            self.work.notify_one();
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.work.notify_waiters();
        self.space.notify_waiters();
    }

    fn register_waiter(&self) -> Waiting<'_> {
        self.waiting.fetch_add(1, Ordering::SeqCst);
        Waiting(&self.waiting)
    }

    /// Pop a job and finish its lane at once, for tests that stand in for
    /// the executors.
    #[cfg(test)]
    pub(crate) fn try_take(self: &Arc<Self>) -> Option<PublishJob> {
        let (lane, job) = self.queue.lock().pop()?;
        self.popped(1);
        self.complete(&lane);
        Some(job)
    }
}

/// Holds a lane from the moment its job is taken. Dropping it lets the lane's
/// next job run.
///
/// A guard, not a call, so that a job that panics, or a task that is dropped
/// mid-way, still frees its lane: a lane left busy would hold every later
/// publish to its shard forever, without a word.
pub(crate) struct LaneGuard {
    partition: Arc<Partition>,
    lane: LaneKey,
}

impl LaneGuard {
    /// Take up to `max` more of this lane's queued jobs, in order, while
    /// `take` accepts the next one; it sees each job and its scheduling cost.
    /// They run under this guard, as part of the job it was taken for.
    pub(crate) fn take_more(
        &self,
        max: usize,
        take: impl FnMut(&PublishJob, usize) -> bool,
    ) -> Vec<PublishJob> {
        if max == 0 {
            return Vec::new();
        }
        let taken = self.partition.queue.lock().take_more(&self.lane, max, take);
        if !taken.is_empty() {
            self.partition.popped(taken.len());
        }
        taken
    }
}

impl Drop for LaneGuard {
    fn drop(&mut self) {
        self.partition.complete(&self.lane);
    }
}

/// What a job must be ordered against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LaneKey {
    /// A shard this broker writes, by its stream handle id. Also picks the
    /// core-shard partition, the same way core shards pick a stream's core.
    Local(u64),
    /// A shard written somewhere else, by a hash of its name. A collision
    /// only makes two shards share a lane, which costs them concurrency and
    /// never order.
    Remote(u64),
}

impl LaneKey {
    fn of(target: &PublishTarget) -> Self {
        match target {
            PublishTarget::Resolved { handle, .. } | PublishTarget::Idempotent { handle, .. } => {
                Self::Local(handle.id())
            }
            PublishTarget::Forward { key, .. } => Self::Remote(name_hash(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
            )),
            #[cfg(test)]
            PublishTarget::Named {
                tenant_id,
                namespace,
                stream,
            } => Self::Remote(name_hash(tenant_id, namespace, stream, 0)),
        }
    }

    fn partition(self, partitions: usize) -> usize {
        match self {
            Self::Local(id) | Self::Remote(id) => (id % partitions.max(1) as u64) as usize,
        }
    }
}

thread_local! {
    /// Partitions that got a lane ready during the current [`batched`] poll,
    /// whose wake-up waits for the end of that poll.
    static DEFERRED: RefCell<Option<Vec<Arc<Partition>>>> = const { RefCell::new(None) };
}

/// Hold `partition`'s wake-up until the running [`batched`] poll ends.
/// `false` when no poll is batching, and the caller wakes now.
fn defer_wake(partition: &Arc<Partition>) -> bool {
    DEFERRED.with(|deferred| match deferred.borrow_mut().as_mut() {
        Some(partitions) => {
            if !partitions.iter().any(|held| Arc::ptr_eq(held, partition)) {
                partitions.push(Arc::clone(partition));
            }
            true
        }
        None => false,
    })
}

/// Ends a batching poll: wakes one executor for each partition that got a
/// lane ready during it. A guard, so a panicking poll still wakes them.
struct Batch {
    outer: bool,
}

impl Batch {
    fn begin() -> Self {
        let outer = DEFERRED.with(|deferred| {
            let mut deferred = deferred.borrow_mut();
            let outer = deferred.is_none();
            if outer {
                *deferred = Some(Vec::new());
            }
            outer
        });
        Self { outer }
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        if self.outer {
            let held = DEFERRED.with(|deferred| deferred.borrow_mut().take());
            for partition in held.into_iter().flatten() {
                partition.wake_one();
            }
        }
    }
}

/// Run `task`, a connection's read loop, so that the publishes it queues in
/// one poll wake at most one executor per partition, at the end of that poll.
///
/// A read pass that finds several frames already buffered queues all their
/// jobs before the task next suspends, and an executor woken for the first
/// would only have run it while the rest were still being queued. Each job is
/// still queued, admitted and refused exactly as before, in order; only the
/// wake-up is held back. The poll's end is the latest it can be held: a task
/// that blocks anywhere, on a byte budget, a full queue, its ack window or
/// the next read, returns from its poll first, so work it queued is never
/// left waiting on a wake-up its own blocking would have to produce. A lone
/// frame wakes its executor when its poll ends, as soon as it was queued.
pub(crate) async fn batched<F: Future>(task: F) -> F::Output {
    let mut task = std::pin::pin!(task);
    std::future::poll_fn(|cx| {
        let _batch = Batch::begin();
        task.as_mut().poll(cx)
    })
    .await
}

/// Counts an executor as idle for as long as it is held.
struct Counted<'a>(&'a AtomicUsize);

impl<'a> Counted<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn report_ingress_depth() {
    GLOBAL_INGRESS_GAUGE.report(
        || GLOBAL_INGRESS_DEPTH.load(Ordering::Relaxed),
        |depth| t_gauge!("felix_broker_ingress_queue_depth").set(depth as f64),
    );
}

static GLOBAL_INGRESS_GAUGE: DepthGauge = DepthGauge::new();

/// The queue depth gauge, set when the depth has moved a long way or emptied.
///
/// Setting it on every push and pop costs a registry lookup per job, which
/// shows up when jobs are small. Between sets it can be off by up to
/// `DEPTH_STEP`. Writers serialize on a lock, taken only when a set is due,
/// and re-read the depth inside it: the last writer to run therefore leaves
/// the gauge at a depth it saw after every earlier change, and a queue that
/// has emptied reads zero once its last push or pop returns.
struct DepthGauge {
    reported: AtomicUsize,
    write: Mutex<()>,
}

impl DepthGauge {
    const fn new() -> Self {
        Self {
            reported: AtomicUsize::new(0),
            write: Mutex::new(()),
        }
    }

    /// Call after every change to the depth.
    fn report(&self, depth: impl Fn() -> usize, set: impl Fn(usize)) {
        if !depth_gauge_due(self.reported.load(Ordering::Acquire), depth()) {
            return;
        }
        let _write = self.write.lock();
        let depth = depth();
        if depth_gauge_due(self.reported.load(Ordering::Acquire), depth) {
            set(depth);
            self.reported.store(depth, Ordering::Release);
        }
    }
}

fn depth_gauge_due(last: usize, depth: usize) -> bool {
    depth.abs_diff(last) >= DEPTH_STEP || (depth == 0 && last != 0)
}

/// Counts a submitter as waiting for room for as long as it is held.
struct Waiting<'a>(&'a AtomicUsize);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A scheduler with no executors, and the two ends tests use in their place:
/// a sender to fill the queue, and a receiver that takes what was queued. Like
/// a channel's, dropping the receiver closes the queue.
#[cfg(test)]
pub(crate) fn test_channel(capacity: usize) -> (Arc<PublishScheduler>, TestSender, TestReceiver) {
    let scheduler = Arc::new(PublishScheduler::new(1, capacity, capacity));
    let receiver = TestReceiver(Arc::clone(&scheduler.partitions[0]));
    (Arc::clone(&scheduler), TestSender(scheduler), receiver)
}

#[cfg(test)]
pub(crate) struct TestSender(Arc<PublishScheduler>);

#[cfg(test)]
impl TestSender {
    /// Queue `job` without waiting, as a tenant of its own.
    pub(crate) fn try_send(
        &self,
        job: PublishJob,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<Box<PublishJob>>> {
        use futures::FutureExt;
        match self
            .0
            .submit("test-filler", job, std::future::ready(()))
            .now_or_never()
            .expect("a submit that may not wait never waits")
        {
            Ok(()) => Ok(()),
            Err(Rejected::Full(job)) => Err(tokio::sync::mpsc::error::TrySendError::Full(job)),
            Err(Rejected::Closed) => panic!("the test queue is closed"),
        }
    }
}

#[cfg(test)]
pub(crate) struct TestReceiver(Arc<Partition>);

#[cfg(test)]
impl TestReceiver {
    pub(crate) async fn recv(&mut self) -> Option<PublishJob> {
        self.0.next().await.map(|(job, _lane)| job)
    }

    pub(crate) fn try_recv(
        &mut self,
    ) -> Result<PublishJob, tokio::sync::mpsc::error::TryRecvError> {
        self.0
            .try_take()
            .ok_or(tokio::sync::mpsc::error::TryRecvError::Empty)
    }
}

#[cfg(test)]
impl Drop for TestReceiver {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn name_hash(tenant_id: &str, namespace: &str, stream: &str, shard: u32) -> u64 {
    let mut hasher = DefaultHasher::new();
    (tenant_id, namespace, stream, shard).hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests;

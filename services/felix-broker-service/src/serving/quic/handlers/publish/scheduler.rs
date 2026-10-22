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

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
    pub(crate) fn new(partitions: usize, capacity: usize, share: usize) -> Self {
        Self {
            partitions: (0..partitions.max(1))
                .map(|_| Arc::new(Partition::new(capacity, share)))
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
    /// `notify_one` calls made, for tests.
    #[cfg(test)]
    wakes: AtomicUsize,
    /// Wakes submitters waiting for room.
    space: Notify,
    /// Submitters waiting on `space`, so a pop only pays for a wake-up when
    /// someone is listening.
    waiting: AtomicUsize,
    closed: AtomicBool,
}

impl Partition {
    fn new(capacity: usize, share: usize) -> Self {
        Self {
            queue: Mutex::new(FairQueue::new(capacity, share, QUANTUM_BYTES)),
            work: Notify::new(),
            idle: AtomicUsize::new(0),
            #[cfg(test)]
            wakes: AtomicUsize::new(0),
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
                // Jobs behind a busy lane still count, so this waits for them.
                if self.closed.load(Ordering::Acquire) && queue.is_empty() {
                    return None;
                }
            }
            work.await;
        }
    }

    fn try_push(
        &self,
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
        report_depth(GLOBAL_INGRESS_DEPTH.fetch_add(1, Ordering::Relaxed) + 1);
        if ready {
            self.wake_one();
        }
        Ok(())
    }

    fn popped(&self, jobs: usize) {
        let global = GLOBAL_INGRESS_DEPTH
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(jobs))
            })
            .unwrap_or(0)
            .saturating_sub(jobs);
        report_depth(global);
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
            self.wake_one();
        }
    }

    /// Wake an executor for a lane that just became ready, if any is waiting.
    fn wake_one(&self) {
        if self.idle.load(Ordering::SeqCst) > 0 {
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

/// Publish the queue depth gauge when it has moved a long way, or emptied.
///
/// Setting it on every push and pop costs a registry lookup per job, which
/// shows up when jobs are small. The gauge reads within `DEPTH_STEP` of the
/// true depth and is exact whenever the queue is empty.
fn report_depth(depth: usize) {
    static REPORTED: AtomicUsize = AtomicUsize::new(0);
    let last = REPORTED.load(Ordering::Relaxed);
    if depth_gauge_due(last, depth) {
        REPORTED.store(depth, Ordering::Relaxed);
        t_gauge!("felix_broker_ingress_queue_depth").set(depth as f64);
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

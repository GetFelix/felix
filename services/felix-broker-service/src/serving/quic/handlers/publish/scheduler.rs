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
//! A submitter allowed to wait for room parks its job in the partition, and a
//! freed slot is handed straight to the oldest parked job its tenant may queue.
//! One slot wakes one submitter, however many are waiting.
//!
//! With core shards on there is one partition per core, each with its own
//! queue and executors on that core, so a stream's claims stay on the core
//! that owns it.

mod fair_queue;

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};

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
        let (partition, lane, cost) = self.place(&job);
        let job = match partition.try_push(tenant, lane, job, cost) {
            Err(Rejected::Full(job)) => *job,
            done => return done,
        };
        tokio::pin!(give_up);
        // A caller that may not wait is refused without touching the waiters.
        if futures::FutureExt::now_or_never(&mut give_up).is_some() {
            return Err(Rejected::Full(Box::new(job)));
        }
        match self.park(tenant, job)? {
            Submitted::Queued => Ok(()),
            Submitted::Parked(ticket) => match ticket.wait(give_up).await {
                Waited::Admitted => Ok(()),
                Waited::GaveUp(job) => Err(Rejected::Full(Box::new(job))),
                Waited::Closed(_) => Err(Rejected::Closed),
            },
        }
    }

    /// Queue `job`, or park it behind the jobs already waiting for room, without
    /// waiting. Never [`Rejected::Full`].
    ///
    /// Parking takes the job's place in line at once, so the caller can go on
    /// to its next job and still have this one admitted first.
    pub(crate) fn park(&self, tenant: &str, job: PublishJob) -> Result<Submitted, Rejected> {
        let (partition, lane, cost) = self.place(&job);
        match partition.park(tenant, lane, job, cost) {
            Park::Done(done) => done.map(|()| Submitted::Queued),
            Park::Waiting(id, tenant, admitted) => Ok(Submitted::Parked(Ticket {
                partition: Arc::clone(partition),
                id,
                tenant,
                admitted,
            })),
        }
    }

    /// The partition `job` belongs to, its lane and what it costs.
    fn place(&self, job: &PublishJob) -> (&Arc<Partition>, LaneKey, usize) {
        let lane = LaneKey::of(&job.target);
        let partition = &self.partitions[lane.partition(self.partitions.len())];
        let cost = job
            .payloads
            .iter()
            .map(Bytes::len)
            .sum::<usize>()
            .saturating_add(JOB_COST_BYTES);
        (partition, lane, cost)
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

    /// Queue `job` as `tenant`, waiting for room however long it takes.
    #[cfg(test)]
    pub(crate) async fn send_as(&self, tenant: &str, job: PublishJob) {
        if self
            .submit(tenant, job, std::future::pending())
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

/// What [`PublishScheduler::park`] did with a job.
pub(crate) enum Submitted {
    Queued,
    /// Waiting for room. Dropping the ticket takes the job back out.
    Parked(Ticket),
}

/// How a parked job's wait ended.
pub(crate) enum Waited {
    Admitted,
    /// The caller gave up first and has the job back.
    GaveUp(PublishJob),
    /// The queue closed. The job comes back so it can still be answered.
    Closed(Option<PublishJob>),
}

/// A job parked in a partition, waiting for a slot.
pub(crate) struct Ticket {
    partition: Arc<Partition>,
    id: u64,
    tenant: Arc<str>,
    admitted: oneshot::Receiver<Wake>,
}

impl Ticket {
    /// Wait until the job is admitted, the queue closes, or `give_up`
    /// resolves.
    pub(crate) async fn wait<G: Future<Output = ()>>(mut self, give_up: G) -> Waited {
        tokio::pin!(give_up);
        let waited = tokio::select! {
            biased;
            woken = &mut self.admitted => woken.into(),
            () = &mut give_up => match self.partition.unpark(self.id, &self.tenant) {
                Some(job) => Waited::GaveUp(job),
                // Taken out of line in the instant before giving up, by an
                // admission or a close whose wake is about to arrive.
                None => (&mut self.admitted).await.into(),
            },
        };
        self.id = NOT_PARKED;
        waited
    }
}

impl From<Result<Wake, oneshot::error::RecvError>> for Waited {
    fn from(woken: Result<Wake, oneshot::error::RecvError>) -> Self {
        match woken {
            Ok(Ok(())) => Waited::Admitted,
            Ok(Err(job)) => Waited::Closed(Some(job)),
            Err(_) => Waited::Closed(None),
        }
    }
}

impl Drop for Ticket {
    // Dropping the ticket unparks the job. If a slot was already handed to
    // it, the job stays queued and runs with no caller waiting: the client
    // gets no ack and may retry, the same at-least-once window as a
    // connection that drops right after its publish was enqueued.
    fn drop(&mut self) {
        if self.id != NOT_PARKED {
            self.partition.unpark(self.id, &self.tenant);
        }
    }
}

/// One queue and the executors that drain it.
pub(crate) struct Partition {
    state: Mutex<State>,
    /// Wakes executors: a lane became ready, or the queue closed.
    work: Notify,
    closed: AtomicBool,
    /// Submitters woken to be admitted, for tests that pin down the wake count.
    #[cfg(test)]
    admit_wakes: AtomicUsize,
}

/// The queue and the submitters waiting for room in it, under one lock so a
/// job is either queued or parked, never both and never neither.
struct State {
    queue: FairQueue<LaneKey, PublishJob>,
    /// Parked jobs per tenant, by arrival number. Tenant names are shared
    /// with `heads`, so parking allocates only for a tenant's first waiter.
    waiters: HashMap<Arc<str>, BTreeMap<u64, Waiter>>,
    /// Each waiting tenant's oldest parked job, oldest first: the order
    /// freed slots are offered in.
    heads: BTreeSet<(u64, Arc<str>)>,
    next_id: u64,
    /// Tenants looked at while admitting, for tests.
    #[cfg(test)]
    scanned: usize,
}

/// A job that found the queue full and waits for a slot.
struct Waiter {
    lane: LaneKey,
    cost: usize,
    job: PublishJob,
    admitted: oneshot::Sender<Wake>,
}

/// What a parked job is woken with: admitted, or handed back because the
/// queue closed.
type Wake = Result<(), PublishJob>;

enum Park {
    Done(Result<(), Rejected>),
    Waiting(u64, Arc<str>, oneshot::Receiver<Wake>),
}

const NOT_PARKED: u64 = u64::MAX;

impl Partition {
    fn new(capacity: usize, share: usize) -> Self {
        Self {
            state: Mutex::new(State {
                queue: FairQueue::new(capacity, share, QUANTUM_BYTES),
                waiters: HashMap::new(),
                heads: BTreeSet::new(),
                next_id: 0,
                #[cfg(test)]
                scanned: 0,
            }),
            work: Notify::new(),
            closed: AtomicBool::new(false),
            #[cfg(test)]
            admit_wakes: AtomicUsize::new(0),
        }
    }

    /// The next job to run and the guard that holds its lane, or `None` once
    /// the queue is closed and empty.
    pub(crate) async fn next(self: &Arc<Self>) -> Option<(PublishJob, LaneGuard)> {
        loop {
            let work = self.work.notified();
            tokio::pin!(work);
            work.as_mut().enable();
            {
                let mut state = self.state.lock();
                if let Some((lane, job)) = state.queue.pop() {
                    self.popped(state, 1);
                    let guard = LaneGuard {
                        partition: Arc::clone(self),
                        lane,
                    };
                    return Some((job, guard));
                }
                // Jobs behind a busy lane still count, so this waits for them.
                if self.closed.load(Ordering::Acquire) && state.queue.is_empty() {
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
        self.state
            .lock()
            .queue
            .push(tenant, lane, job, cost)
            .map_err(|job| Rejected::Full(Box::new(job)))?;
        self.queued(1);
        Ok(())
    }

    /// Queue the job, or park it until a freed slot is handed to it. The retry
    /// and the parking happen under one lock, so a slot freed in between is
    /// not missed.
    fn park(&self, tenant: &str, lane: LaneKey, job: PublishJob, cost: usize) -> Park {
        let mut state = self.state.lock();
        if self.closed.load(Ordering::Acquire) {
            return Park::Done(Err(Rejected::Closed));
        }
        let job = match state.queue.push(tenant, lane, job, cost) {
            Ok(()) => {
                drop(state);
                self.queued(1);
                return Park::Done(Ok(()));
            }
            Err(job) => job,
        };
        let id = state.next_id;
        state.next_id += 1;
        let (admitted, wake) = oneshot::channel();
        let state = &mut *state;
        let name = match state.waiters.get_key_value(tenant) {
            Some((name, _)) => Arc::clone(name),
            None => Arc::from(tenant),
        };
        let parked = state.waiters.entry(Arc::clone(&name)).or_default();
        if parked.is_empty() {
            state.heads.insert((id, Arc::clone(&name)));
        }
        parked.insert(
            id,
            Waiter {
                lane,
                cost,
                job,
                admitted,
            },
        );
        Park::Waiting(id, name, wake)
    }

    /// Take a parked job back. `None` when it was admitted, or the queue
    /// closed, first.
    fn unpark(&self, id: u64, tenant: &Arc<str>) -> Option<PublishJob> {
        self.state.lock().take_waiter(id, tenant).map(|w| w.job)
    }

    /// Bookkeeping for `jobs` newly queued: depth gauge and an executor for
    /// each, since a single `notify_one` would leave the rest asleep.
    fn queued(&self, jobs: usize) {
        GLOBAL_INGRESS_DEPTH.fetch_add(jobs, Ordering::Relaxed);
        report_ingress_depth();
        for _ in 0..jobs {
            self.work.notify_one();
        }
    }

    /// `jobs` left the queue: hand each freed slot to the oldest parked job
    /// whose tenant may take it, and wake just those submitters.
    fn popped(&self, mut state: parking_lot::MutexGuard<'_, State>, jobs: usize) {
        let _ = GLOBAL_INGRESS_DEPTH.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
            Some(depth.saturating_sub(jobs))
        });
        report_ingress_depth();
        if state.heads.is_empty() {
            return;
        }
        let admitted = state.admit_waiters();
        drop(state);
        if admitted.is_empty() {
            return;
        }
        self.queued(admitted.len());
        #[cfg(test)]
        self.admit_wakes
            .fetch_add(admitted.len(), Ordering::Relaxed);
        for wake in admitted {
            // A submitter that went away took its job back first.
            let _ = wake.send(Ok(()));
        }
    }

    fn complete(&self, lane: &LaneKey) {
        self.state.lock().queue.complete(lane);
        // The lane may be ready again; and a closing queue may now be empty,
        // which every idle executor needs to hear to exit.
        if self.closed.load(Ordering::Acquire) {
            self.work.notify_waiters();
        } else {
            self.work.notify_one();
        }
    }

    fn close(&self) {
        let parked = {
            let mut state = self.state.lock();
            self.closed.store(true, Ordering::Release);
            state.heads.clear();
            std::mem::take(&mut state.waiters)
        };
        // Each job goes back to its submitter, which still owes it an answer.
        for waiter in parked.into_values().flat_map(BTreeMap::into_values) {
            let _ = waiter.admitted.send(Err(waiter.job));
        }
        self.work.notify_waiters();
    }

    /// Pop a job and finish its lane at once, for tests that stand in for
    /// the executors.
    #[cfg(test)]
    pub(crate) fn try_take(self: &Arc<Self>) -> Option<PublishJob> {
        let mut state = self.state.lock();
        let (lane, job) = state.queue.pop()?;
        self.popped(state, 1);
        self.complete(&lane);
        Some(job)
    }
}

impl State {
    /// Remove one parked job, keeping `heads` and the per-tenant map in step.
    fn take_waiter(&mut self, id: u64, tenant: &Arc<str>) -> Option<Waiter> {
        let parked = self.waiters.get_mut(tenant)?;
        let was_head = parked.keys().next() == Some(&id);
        let waiter = parked.remove(&id)?;
        if was_head {
            self.heads.remove(&(id, Arc::clone(tenant)));
            match parked.keys().next() {
                Some(&next) => {
                    self.heads.insert((next, Arc::clone(tenant)));
                }
                None => {
                    self.waiters.remove(tenant);
                }
            }
        }
        Some(waiter)
    }

    /// Queue parked jobs while there is room, the tenant with the oldest
    /// waiter first. A tenant the fair queue refuses is skipped by name for
    /// the rest of the pass without looking at its jobs, since its answer
    /// cannot change until something else leaves. Returns the submitters to
    /// wake.
    fn admit_waiters(&mut self) -> Vec<oneshot::Sender<Wake>> {
        let mut woken = Vec::new();
        let mut refused: Vec<Arc<str>> = Vec::new();
        while !self.queue.is_full() {
            let Some((id, tenant)) = self
                .heads
                .iter()
                .find(|(_, tenant)| !refused.iter().any(|r| Arc::ptr_eq(r, tenant)))
                .cloned()
            else {
                break;
            };
            #[cfg(test)]
            {
                self.scanned += refused.len() + 1;
            }
            if !self.queue.admits_tenant(&tenant) {
                refused.push(tenant);
                continue;
            }
            let waiter = self
                .take_waiter(id, &tenant)
                .expect("heads name parked jobs");
            if self
                .queue
                .push(&tenant, waiter.lane, waiter.job, waiter.cost)
                .is_err()
            {
                unreachable!("admits_tenant said there was room");
            }
            woken.push(waiter.admitted);
        }
        woken
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
        let mut state = self.partition.state.lock();
        let taken = state.queue.take_more(&self.lane, max, take);
        if taken.is_empty() {
            return taken;
        }
        self.partition.popped(state, taken.len());
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
        // `reported` is stored before the set, so a change that lands during
        // it is compared with the depth being set and takes the lock if due.
        // The loop covers one that compared with the old value and left.
        loop {
            let depth = depth();
            if !depth_gauge_due(self.reported.load(Ordering::Acquire), depth) {
                break;
            }
            self.reported.store(depth, Ordering::Release);
            set(depth);
        }
    }
}

fn depth_gauge_due(last: usize, depth: usize) -> bool {
    depth.abs_diff(last) >= DEPTH_STEP || (depth == 0 && last != 0)
}

fn name_hash(tenant_id: &str, namespace: &str, stream: &str, shard: u32) -> u64 {
    let mut hasher = DefaultHasher::new();
    (tenant_id, namespace, stream, shard).hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests;

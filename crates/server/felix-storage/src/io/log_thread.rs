//! A thread that belongs to one log and runs one kind of its blocking work:
//! its flushes on one, its appends on another.
//!
//! An `fsync` blocks, and so does a `write` once the kernel throttles a
//! process that dirties pages faster than the device takes them, so neither
//! can run on a reactor thread. Tokio's blocking pool is a poor place for them
//! too. Every `spawn_blocking` goes through one queue shared with reads,
//! rollovers and every other shard's work, so a job waits behind all of them
//! and the dispatch cost climbs with the number of shards busy at once. A
//! thread of the log's own is one channel send and one wake-up away, whatever
//! else the process is doing.
//!
//! The thread starts on the first job and exits after `IDLE` without one, so
//! a broker holding many quiet shards does not hold threads for them. A
//! spinning thread polls for its next job only while jobs keep arriving
//! within the spin window, so a quiet log parks instead of burning the window
//! after every job. See `docs/storage-performance.md` for the measurements.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak, mpsc};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// How long the thread waits for another job before exiting. The same
/// keep-alive the blocking pool uses for its threads.
const IDLE: Duration = Duration::from_secs(10);

/// A job, which sends its own result back, given the count of jobs not yet
/// finished.
type Job = Box<dyn FnOnce(&AtomicUsize) + Send>;

/// A job and when it was sent, which tells the thread how soon it came after
/// the job before, whatever the thread's own wake-up cost.
type Sent = (Instant, Job);

/// Runs one log's jobs of one kind on a dedicated thread, one at a time, in
/// the order they were submitted.
pub(crate) struct LogThread {
    name: String,
    idle: Duration,
    /// How long the thread, and a caller alone in the queue, poll before
    /// parking. See [`Self::spinning`].
    spin: Duration,
    /// Jobs submitted and not yet finished.
    pending: Arc<AtomicUsize>,
    /// The running thread's queue, if there is a thread. Jobs are only sent
    /// under this lock, and the thread clears it under the same lock before
    /// exiting, so no job can be left behind in a queue nobody reads.
    queue: Arc<Mutex<Option<mpsc::Sender<Sent>>>>,
    /// The thread's polls, by how they ended.
    #[cfg(test)]
    polls: Arc<PollCounts>,
}

impl LogThread {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self::with_idle(name, IDLE)
    }

    /// A thread for jobs shorter than a wake-up: after a job it polls for the
    /// next for up to `spin` before parking, as long as the job before came
    /// within `spin` too, and a caller whose job is the only one queued polls
    /// for its result as long before yielding. Back-to-back jobs then pay
    /// neither wake-up, and jobs spaced wider than `spin` pay no polling.
    pub(crate) fn spinning(name: impl Into<String>, spin: Duration) -> Self {
        Self {
            spin,
            ..Self::new(name)
        }
    }

    fn with_idle(name: impl Into<String>, idle: Duration) -> Self {
        Self {
            name: name.into(),
            idle,
            spin: Duration::ZERO,
            pending: Arc::new(AtomicUsize::new(0)),
            queue: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            polls: Arc::default(),
        }
    }

    /// Run `work` behind every job already submitted: on the calling thread
    /// when none is pending, since there is nothing to queue behind, and on
    /// the thread otherwise. For short work that only reads what earlier
    /// jobs left. Skipping the hand-off saves a wake-up of the thread and of
    /// the caller.
    pub(crate) async fn run_behind<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        if self.pending.load(Ordering::Acquire) == 0 {
            return work();
        }
        self.run(work).await
    }

    /// Run `work` on the thread and return its result.
    ///
    /// The work owns everything it touches and runs to the end even if the
    /// caller stops waiting, so a caller that gives up does not close a file
    /// under a sync in progress. If no thread can be started the work goes to
    /// the blocking pool instead: durability must not depend on getting a
    /// thread of our own.
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        self.run_then(move |_| (work(), ()), |()| {}).await
    }

    /// [`Self::run`], with `then` run on the thread after the caller has its
    /// result: work the caller does not wait for. The next job still waits
    /// behind it. `work` is told whether the caller has stopped waiting.
    pub(crate) async fn run_then<T: Send + 'static, A: Send + 'static>(
        &self,
        work: impl FnOnce(&dyn Fn() -> bool) -> (io::Result<T>, A) + Send + 'static,
        then: impl FnOnce(A) + Send + 'static,
    ) -> io::Result<T> {
        let (reply, mut answer) = oneshot::channel();
        // Behind other jobs a caller would poll for nothing, on a worker with
        // better things to do.
        let alone = self.pending.fetch_add(1, Ordering::AcqRel) == 0;
        let job: Job = Box::new(move |pending: &AtomicUsize| {
            let (result, after) = work(&|| reply.is_closed());
            pending.fetch_sub(1, Ordering::AcqRel);
            let _ = reply.send(result);
            then(after);
        });
        // Under a simulator the job runs on the simulated runtime, as if no
        // thread could be started.
        let submitted = if felix_common::blocking::on_runtime() {
            Err(job)
        } else {
            self.submit(job)
        };
        if let Err(job) = submitted {
            // Off the thread, jobs keep their order only through whatever
            // locks they take: the append lock, for appends. Callers that
            // need submission order (a shard's claims) are serialised above.
            let pending = Arc::clone(&self.pending);
            felix_common::blocking::spawn(move || job(&pending))
                .await
                .map_err(io::Error::other)?;
        }
        // A current-thread runtime (a Raft group thread) has other groups'
        // tasks to run, and polling here would hold them up.
        if alone
            && !self.spin.is_zero()
            && !on_current_thread_runtime()
            && let Some(_spinning) = Spinning::enter()
        {
            let started = Instant::now();
            while started.elapsed() < self.spin {
                if let Ok(result) = answer.try_recv() {
                    return result;
                }
                std::hint::spin_loop();
            }
        }
        // The reply is dropped unsent only if the work panicked.
        answer
            .await
            .unwrap_or_else(|_| Err(io::Error::other("the job panicked")))
    }

    /// Whether every job submitted so far has finished.
    #[cfg(feature = "raft")]
    pub(crate) fn is_idle(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0
    }

    /// Run `work` on the thread, behind the jobs already submitted, without
    /// waiting for it. If no thread can be started it goes to the blocking
    /// pool, as [`Self::run`] does.
    #[cfg(feature = "raft")]
    pub(crate) fn spawn(&self, work: impl FnOnce() + Send + 'static) {
        self.pending.fetch_add(1, Ordering::AcqRel);
        let job: Job = Box::new(move |pending: &AtomicUsize| {
            work();
            pending.fetch_sub(1, Ordering::AcqRel);
        });
        let submitted = if felix_common::blocking::on_runtime() {
            Err(job)
        } else {
            self.submit(job)
        };
        if let Err(job) = submitted {
            let pending = Arc::clone(&self.pending);
            drop(felix_common::blocking::spawn(move || job(&pending)));
        }
    }

    /// Hand `job` to the thread, starting one if there is none. Gives the job
    /// back if no thread could be started.
    fn submit(&self, job: Job) -> Result<(), Job> {
        let mut queue = self.queue.lock();
        let job = match queue.as_ref() {
            Some(sender) => match sender.send((Instant::now(), job)) {
                Ok(()) => return Ok(()),
                Err(mpsc::SendError((_, job))) => job,
            },
            None => job,
        };
        let (sender, receiver) = mpsc::channel();
        let slot = Arc::downgrade(&self.queue);
        let pending = Arc::clone(&self.pending);
        let spin = Spin {
            window: self.spin,
            #[cfg(test)]
            polls: Arc::clone(&self.polls),
        };
        let idle = self.idle;
        let started = std::thread::Builder::new()
            .name(self.name.clone())
            .spawn(move || serve(receiver, slot, &pending, idle, &spin));
        match started {
            Ok(_) => {
                // The thread cannot exit before it takes the lock held here.
                let result = sender
                    .send((Instant::now(), job))
                    .map_err(|mpsc::SendError((_, job))| job);
                *queue = Some(sender);
                result
            }
            Err(err) => {
                tracing::warn!(error = %err, "could not start a log thread; using the blocking pool");
                *queue = None;
                Err(job)
            }
        }
    }
}

impl std::fmt::Debug for LogThread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogThread")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn on_current_thread_runtime() -> bool {
    tokio::runtime::Handle::try_current().is_ok_and(|runtime| {
        runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread
    })
}

/// Callers polling for a result right now, across every log.
static SPINNING: AtomicUsize = AtomicUsize::new(0);

/// A caller's turn to poll for its result instead of parking.
///
/// Capped at a quarter of the cores, so hundreds of logs each with one
/// append in flight cannot keep every runtime worker polling. Past the cap a
/// caller parks and pays the wake-up.
struct Spinning;

impl Spinning {
    fn enter() -> Option<Self> {
        static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let cap = *CAP.get_or_init(|| {
            std::thread::available_parallelism().map_or(1, |cores| (cores.get() / 4).max(1))
        });
        SPINNING
            .try_update(Ordering::AcqRel, Ordering::Acquire, |now| {
                (now < cap).then_some(now + 1)
            })
            .ok()
            .map(|_| Self)
    }
}

impl Drop for Spinning {
    fn drop(&mut self) {
        SPINNING.fetch_sub(1, Ordering::AcqRel);
    }
}

/// How long the thread polls for its next job before parking.
struct Spin {
    window: Duration,
    #[cfg(test)]
    polls: Arc<PollCounts>,
}

#[cfg(test)]
#[derive(Default)]
struct PollCounts {
    /// Polls that found a job.
    caught: std::sync::atomic::AtomicU64,
    /// Polls that ran out the window and parked.
    wasted: std::sync::atomic::AtomicU64,
}

/// The thread body. Returns when the log is dropped, or after `IDLE` with
/// nothing to do.
fn serve(
    queue: mpsc::Receiver<Sent>,
    slot: Weak<Mutex<Option<mpsc::Sender<Sent>>>>,
    pending: &AtomicUsize,
    idle: Duration,
    spin: &Spin,
) {
    // Whether the last job was sent within the window after the one before
    // it finished. A job that came later says the log is not that busy, and
    // the next one will likely be late too: polling for it would burn the
    // whole window for nothing. Measured from the send, so a slow wake-up
    // does not keep a busy log parking.
    let mut busy = false;
    let mut finished = Instant::now();
    loop {
        let polled = if busy { poll(&queue, spin) } else { None };
        let (sent, job) = match polled.map_or_else(|| queue.recv_timeout(idle), Ok) {
            Ok(job) => job,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let Some(slot) = slot.upgrade() else { return };
                let mut sender = slot.lock();
                // A job may have been sent since the timeout fired. With the
                // lock held nothing more can arrive, so an empty queue here
                // is safe to walk away from.
                match queue.try_recv() {
                    Ok(job) => job,
                    Err(_) => {
                        *sender = None;
                        return;
                    }
                }
            }
        };
        busy = sent.saturating_duration_since(finished) < spin.window;
        // A panic is this job's failure, not the thread's: if it took the
        // thread down, jobs already queued behind it would fail with it.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(pending)));
        finished = Instant::now();
    }
}

/// The next job, if one arrives within the spin window.
fn poll(queue: &mpsc::Receiver<Sent>, spin: &Spin) -> Option<Sent> {
    let started = Instant::now();
    loop {
        match queue.try_recv() {
            Ok(job) => {
                #[cfg(test)]
                spin.polls.caught.fetch_add(1, Ordering::Relaxed);
                return Some(job);
            }
            Err(mpsc::TryRecvError::Empty) if started.elapsed() < spin.window => {
                std::hint::spin_loop();
            }
            Err(mpsc::TryRecvError::Empty) => {
                #[cfg(test)]
                spin.polls.wasted.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            Err(mpsc::TryRecvError::Disconnected) => return None,
        }
    }
}

#[cfg(test)]
mod tests;

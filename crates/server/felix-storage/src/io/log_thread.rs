//! A thread that belongs to one log and runs one kind of its blocking work, in
//! order: its flushes on one, its appends on another.
//!
//! An `fsync` blocks, and so can a `write` into the page cache once the kernel
//! starts throttling dirty pages, so neither may run on a reactor thread. Tokio's
//! blocking pool is a poor place for them too. Every `spawn_blocking` goes
//! through one queue shared with reads, rollovers and every other shard's work,
//! so a job waits behind all of them and the dispatch cost climbs with the
//! number of shards busy at once. A thread that belongs to one log is one
//! channel send and one wake-up away, whatever else the process is doing.
//!
//! The thread starts on the first job and exits after `IDLE` without one, so a
//! broker holding many quiet shards does not hold threads for them. See
//! `docs/storage-performance.md` for the measurements.

use std::io;
use std::sync::{Arc, Weak, mpsc};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// How long the thread waits for another job before exiting. The same
/// keep-alive the blocking pool uses for its threads.
const IDLE: Duration = Duration::from_secs(10);

/// A job, which sends its own result back.
type Job = Box<dyn FnOnce() + Send>;

/// Runs one log's jobs of one kind on a dedicated thread, one at a time, in the
/// order they were submitted.
pub(crate) struct LogThread {
    name: String,
    idle: Duration,
    /// How long the thread, and a caller waiting on it, poll before parking.
    /// See [`Self::spinning`].
    spin: Duration,
    /// Jobs submitted and not yet finished. A caller polls for its result
    /// only when its job is the only one: behind others it would spin for
    /// nothing, on a thread with better things to do.
    pending: Arc<std::sync::atomic::AtomicUsize>,
    /// The running thread's queue, if there is a thread. Jobs are only sent
    /// under this lock, and the thread clears it under the same lock before
    /// exiting, so no job can be left behind in a queue nobody reads.
    queue: Arc<Mutex<Option<mpsc::Sender<Job>>>>,
    /// Held across every job, wherever it runs. Uncontended unless a job had
    /// to go to the blocking pool, and then it is what keeps that job from
    /// overlapping another: an append relies on nothing else changing its
    /// segment while it writes.
    serial: Arc<Mutex<()>>,
}

impl LogThread {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self::with_idle(name, IDLE)
    }

    /// A thread for jobs short enough that parking and waking around each one
    /// would cost more than the job: after a job the thread polls for the
    /// next for up to `spin` before it parks, and a caller polls for its
    /// result for as long before it yields.
    ///
    /// A wake-up through the kernel costs microseconds on each side, as much
    /// as an append's write into the page cache. Polling briefly is what lets
    /// back-to-back appends skip both, and a job that takes longer than `spin`
    /// costs the caller `spin` of spinning and then an ordinary wait.
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
            pending: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            queue: Arc::new(Mutex::new(None)),
            serial: Arc::new(Mutex::new(())),
        }
    }

    /// [`Self::call`] for work that reports an I/O result.
    pub(crate) async fn run(
        &self,
        work: impl FnOnce() -> io::Result<()> + Send + 'static,
    ) -> io::Result<()> {
        self.call(work).await?
    }

    /// Run `work` on this thread and return what it returns.
    ///
    /// The work owns everything it touches, so a caller that stops waiting
    /// does not close a file under a sync in progress, and the work still
    /// runs. What it returns is then dropped on this thread. If no thread can
    /// be started the work goes to the blocking pool instead: durability must
    /// not depend on getting a thread of our own.
    pub(crate) async fn call<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> io::Result<T> {
        self.call_attended(|_| work()).await
    }

    /// [`Self::call`], with a way for the work to ask whether its caller is
    /// still waiting for the result: work nobody wants any more can then be
    /// skipped, or undone if that is still possible.
    pub(crate) async fn call_attended<T: Send + 'static>(
        &self,
        work: impl FnOnce(&dyn Fn() -> bool) -> T + Send + 'static,
    ) -> io::Result<T> {
        let (reply, mut answer) = oneshot::channel();
        let serial = Arc::clone(&self.serial);
        let alone = self
            .pending
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            == 0;
        // Dropped with the job, run or not.
        let done = Done(Arc::clone(&self.pending));
        let job: Job = Box::new(move || {
            let held = serial.lock();
            let result = work(&|| !reply.is_closed());
            drop(held);
            drop(done);
            let _ = reply.send(result);
        });
        if let Err(job) = self.submit(job) {
            tokio::task::spawn_blocking(job)
                .await
                .map_err(io::Error::other)?;
        } else if alone && !self.spin.is_zero() {
            let started = std::time::Instant::now();
            loop {
                match answer.try_recv() {
                    Ok(result) => return Ok(result),
                    Err(oneshot::error::TryRecvError::Closed) => break,
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }
                if started.elapsed() >= self.spin {
                    break;
                }
                std::hint::spin_loop();
            }
        }
        // The reply goes unsent only if the work panicked.
        answer
            .await
            .map_err(|_| io::Error::other("work on the log thread panicked"))
    }

    /// Hand `job` to the thread, starting one if there is none. Gives the job
    /// back if no thread could be started.
    fn submit(&self, job: Job) -> Result<(), Job> {
        let mut queue = self.queue.lock();
        let job = match queue.as_ref() {
            Some(sender) => match sender.send(job) {
                Ok(()) => return Ok(()),
                Err(mpsc::SendError(job)) => job,
            },
            None => job,
        };
        let (sender, receiver) = mpsc::channel();
        let slot = Arc::downgrade(&self.queue);
        let (idle, spin) = (self.idle, self.spin);
        let started = std::thread::Builder::new()
            .name(self.name.clone())
            .spawn(move || serve(receiver, slot, idle, spin));
        match started {
            Ok(_) => {
                // The thread cannot exit before it takes the lock held here.
                let result = sender.send(job).map_err(|mpsc::SendError(job)| job);
                *queue = Some(sender);
                result
            }
            Err(err) => {
                tracing::warn!(thread = %self.name, error = %err, "could not start a log thread; using the blocking pool");
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

/// The thread body. Returns when the log is dropped, or after `IDLE` with
/// nothing to do.
fn serve(
    queue: mpsc::Receiver<Job>,
    slot: Weak<Mutex<Option<mpsc::Sender<Job>>>>,
    idle: Duration,
    spin: Duration,
) {
    loop {
        if let Some(job) = poll_for(&queue, spin) {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
            continue;
        }
        let job = match queue.recv_timeout(idle) {
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
        // A panic is this job's failure, not the thread's: if it took the
        // thread down, jobs already queued behind it would fail with it. The
        // dropped reply is how the caller learns of it.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }
}

/// Counts a job out of [`LogThread::pending`] when it is dropped.
struct Done(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// The next job, if one arrives within `spin`, polled for without parking.
fn poll_for(queue: &mpsc::Receiver<Job>, spin: Duration) -> Option<Job> {
    if spin.is_zero() {
        return None;
    }
    let started = std::time::Instant::now();
    loop {
        match queue.try_recv() {
            Ok(job) => return Some(job),
            Err(mpsc::TryRecvError::Disconnected) => return None,
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if started.elapsed() >= spin {
            return None;
        }
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod tests;

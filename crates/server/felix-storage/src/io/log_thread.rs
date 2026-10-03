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
//! a broker holding many quiet shards does not hold threads for them. See
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

/// Runs one log's jobs of one kind on a dedicated thread, one at a time, in
/// the order they were submitted.
pub(crate) struct LogThread {
    name: String,
    idle: Duration,
    /// The running thread's queue, if there is a thread. Jobs are only sent
    /// under this lock, and the thread clears it under the same lock before
    /// exiting, so no job can be left behind in a queue nobody reads.
    queue: Arc<Mutex<Option<mpsc::Sender<Job>>>>,
}

impl LogThread {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self::with_idle(name, IDLE)
    }

    fn with_idle(name: impl Into<String>, idle: Duration) -> Self {
        Self {
            name: name.into(),
            idle,
            queue: Arc::new(Mutex::new(None)),
        }
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
        let (reply, answer) = oneshot::channel();
        let job: Job = Box::new(move || {
            let _ = reply.send(work());
        });
        if let Err(job) = self.submit(job) {
            tokio::task::spawn_blocking(job)
                .await
                .map_err(io::Error::other)?;
        }
        // The reply is dropped unsent only if the work panicked.
        answer
            .await
            .unwrap_or_else(|_| Err(io::Error::other("the job panicked")))
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
        let idle = self.idle;
        let started = std::thread::Builder::new()
            .name(self.name.clone())
            .spawn(move || serve(receiver, slot, idle));
        match started {
            Ok(_) => {
                // The thread cannot exit before it takes the lock held here.
                let result = sender.send(job).map_err(|mpsc::SendError(job)| job);
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

/// The thread body. Returns when the log is dropped, or after `IDLE` with
/// nothing to do.
fn serve(queue: mpsc::Receiver<Job>, slot: Weak<Mutex<Option<mpsc::Sender<Job>>>>, idle: Duration) {
    loop {
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
        // thread down, jobs already queued behind it would fail with it.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }
}

#[cfg(test)]
mod tests;

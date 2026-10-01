//! Device flushes submitted to `io_uring` instead of a blocking thread.
//!
//! Every other way of issuing an `fsync` from async Rust hands the call to a
//! thread and waits for it to come back: `spawn_blocking` uses the shared pool,
//! a dedicated flusher uses one warm thread. Both keep a reactor thread free,
//! which is the point, and both cost two thread wake-ups per flush.
//!
//! `IORING_OP_FSYNC` removes the hand-off rather than making it cheaper. The
//! request goes into a submission queue, the kernel performs it, and a
//! completion arrives. No thread is parked for the duration, and — unlike
//! running the sync inline — the `await` is still a yield point, so background
//! work like rollover and retention still gets scheduled.
//!
//! One ring serves the whole process. Flushes are already serialised per log by
//! the group-commit lock, so the concurrency that matters here is *across* logs:
//! a broker with fifty shards can have fifty flushes outstanding, and on the
//! blocking pool that is fifty threads. Here it is one ring and one thread.
//!
//! That thread spends its life blocked in `submit_and_wait`, so a request that
//! arrives while other flushes are in flight has to wake it, or it sits in the
//! channel until one of them completes and one log's slow sync delays every
//! other log's. Callers ring an eventfd the thread keeps a poll armed on.
//!
//! **Measured expectations, so nobody is surprised.** On a 4-way NVMe RAID0 the
//! hand-off is about 9% of a flush (583µs with it, 530µs without), and the
//! device itself sustains ~1,346 MB/s against Felix's ~940 MB/s. This is not a
//! throughput fix; it removes a hand-off that should not be there on a
//! Linux-only server, and the dividend is small. See #547 and #548.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::sync::{Arc, OnceLock};

use io_uring::{IoUring, opcode, types};
use tokio::sync::oneshot;

/// How many flushes may be outstanding in the ring at once.
///
/// One per shard log is the shape to size for; a broker with more shards than
/// this simply queues, which is what the blocking pool did anyway.
const RING_ENTRIES: u32 = 256;

/// Operations in flight at once, leaving a slot for the wake poll. Requests
/// past this wait in the service thread's backlog until completions free a
/// slot. A full ring must never become an I/O error: a failed flush poisons
/// its log.
const MAX_IN_FLIGHT: usize = RING_ENTRIES as usize - 1;

static RING: OnceLock<Option<Ring>> = OnceLock::new();
/// Flush ids start at 1; this one marks the wake poll's completion.
const WAKE_ID: u64 = 0;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The process-wide ring's submission side; the service thread owns the rest.
struct Ring {
    tx: std::sync::mpsc::Sender<Submission>,
    wake: Arc<Wake>,
    /// An errno the service thread reports in place of its next submit.
    #[cfg(test)]
    submit_fault: Arc<std::sync::atomic::AtomicI32>,
}

/// Interrupts the service thread's wait when a request is queued.
struct Wake {
    fd: OwnedFd,
    /// Set by the first caller to signal since the thread last drained the
    /// channel, so a burst of requests costs one eventfd write, not one each.
    pending: AtomicBool,
}

impl Wake {
    fn new() -> io::Result<Self> {
        // Non-blocking, so resetting an already-zero counter returns instead
        // of parking the service thread.
        // Safety: plain syscall; the result is checked before use.
        let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // Safety: `raw` is a fresh descriptor nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        Ok(Self {
            fd,
            pending: AtomicBool::new(false),
        })
    }

    /// Call after queueing. Pairs with the fence in the service loop: either
    /// the thread's drain sees the request, or this sees `pending` cleared and
    /// writes.
    fn signal(&self) {
        fence(Ordering::SeqCst);
        if self.pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let one: u64 = 1;
        // Safety: writes 8 bytes from a live local. Can only fail if the
        // counter would overflow, and then the thread is already woken.
        unsafe { libc::write(self.fd.as_raw_fd(), (&raw const one).cast(), 8) };
    }

    /// Reset the counter, so the next poll waits for a new signal.
    fn consume(&self) {
        let mut count: u64 = 0;
        // Safety: reads 8 bytes into a live local.
        unsafe { libc::read(self.fd.as_raw_fd(), (&raw mut count).cast(), 8) };
    }

    /// Arm a one-shot poll that completes once the eventfd is signalled.
    fn arm(&self, uring: &mut IoUring) {
        let entry = opcode::PollAdd::new(types::Fd(self.fd.as_raw_fd()), libc::POLLIN as u32)
            .build()
            .user_data(WAKE_ID);
        // Safety: the eventfd lives as long as the ring's thread. The queue
        // has room: at most `MAX_IN_FLIGHT` other entries are outstanding.
        let _ = unsafe { uring.submission().push(&entry) };
    }
}

/// One flush waiting to be pushed into the ring, and where to send its result.
///
/// Owns the file until the completion arrives. A caller that stops waiting
/// would otherwise close the descriptor under an in-flight sync, and the
/// kernel could hand the number to an unrelated file.
struct Submission {
    file: Arc<File>,
    op: Op,
    reply: oneshot::Sender<io::Result<()>>,
}

#[derive(Clone, Copy)]
enum Op {
    Fsync,
    /// Completes when the descriptor becomes readable. Tests use it on a pipe
    /// to hold an operation in the ring for as long as they like.
    #[cfg(test)]
    PollReadable,
}

/// Operations the kernel may still be using, keyed by flush id. The reply is
/// taken when a submit error fails the waiter early; the file stays until the
/// completion is reaped.
type Waiting = HashMap<u64, (Arc<File>, Option<oneshot::Sender<io::Result<()>>>)>;

/// Whether flushes should go through `io_uring`.
///
/// `FELIX_STORAGE_IO_URING=1`. Off by default: the kernel floor is 5.1 for
/// `IORING_OP_FSYNC`, and a broker that cannot build a ring must keep working,
/// so this stays opt-in until it has run somewhere real.
pub(crate) fn enabled() -> bool {
    #[cfg(test)]
    if FORCED.load(Ordering::SeqCst) > 0 {
        return true;
    }
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("FELIX_STORAGE_IO_URING")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// While one is held, every log's flushes go through the ring whatever the
/// environment says. For tests that must cover this path; a test running
/// alongside simply flushes through the ring too, which is a correct path.
#[cfg(test)]
pub(crate) struct ForceForTests(());

#[cfg(test)]
static FORCED: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
impl ForceForTests {
    pub(crate) fn hold() -> Self {
        FORCED.fetch_add(1, Ordering::SeqCst);
        Self(())
    }
}

#[cfg(test)]
impl Drop for ForceForTests {
    fn drop(&mut self) {
        FORCED.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Whether a ring could be (or has been) built in this process.
#[cfg(test)]
pub(crate) fn available() -> bool {
    ring().is_some()
}

/// `fdatasync` the file through the ring.
///
/// `None` means the ring is unavailable and the caller should use its own
/// fallback.
pub(crate) async fn fsync(file: Arc<File>) -> Option<io::Result<()>> {
    // Checked first so a missing ring falls back before the fault hooks run:
    // the fallback runs them itself.
    ring()?;
    #[cfg(any(debug_assertions, test, feature = "fault-injection"))]
    {
        crate::fault::refresh();
        if let Some(delay) = crate::fault::fsync_delay() {
            tokio::time::sleep(delay).await;
        }
        if let Some(err) = crate::fault::injected_failure() {
            return Some(Err(err));
        }
    }
    #[cfg(any(test, debug_assertions, feature = "fault-injection"))]
    super::power_loss::observe_file(&file, super::SyncKind::Uring);
    submit(file, Op::Fsync).await
}

/// Wait through the ring for `file` to become readable.
#[cfg(test)]
async fn poll_readable(file: Arc<File>) -> Option<io::Result<()>> {
    submit_to(ring()?, file, Op::PollReadable).await
}

async fn submit(file: Arc<File>, op: Op) -> Option<io::Result<()>> {
    submit_to(ring()?, file, op).await
}

async fn submit_to(ring: &Ring, file: Arc<File>, op: Op) -> Option<io::Result<()>> {
    let (reply, wait) = oneshot::channel();
    // A send failure means the service thread is gone, which is the same
    // situation as no ring at all.
    if ring.tx.send(Submission { file, op, reply }).is_err() {
        return None;
    }
    ring.wake.signal();
    match wait.await {
        Ok(outcome) => Some(outcome),
        Err(_) => Some(Err(io::Error::other("io_uring service thread stopped"))),
    }
}

/// Start the ring and its service thread, once.
///
/// Returns `None` if the ring cannot be created — an old kernel, or a
/// container that forbids the syscall. The caller falls back to the blocking
/// path rather than failing the publish: a durability mechanism must not
/// depend on an optimisation being available.
fn ring() -> Option<&'static Ring> {
    RING.get_or_init(Ring::start).as_ref()
}

impl Ring {
    fn start() -> Option<Self> {
        let uring = match IoUring::new(RING_ENTRIES) {
            Ok(uring) => uring,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "io_uring unavailable; device flushes stay on the blocking pool"
                );
                return None;
            }
        };
        let wake = match Wake::new() {
            Ok(wake) => Arc::new(wake),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "eventfd unavailable; device flushes stay on the blocking pool"
                );
                return None;
            }
        };
        let (tx, rx) = std::sync::mpsc::channel::<Submission>();
        #[cfg(test)]
        let submit_fault = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let service = Service {
            uring,
            wake: Arc::clone(&wake),
            rx,
            waiting: Waiting::new(),
            backlog: VecDeque::new(),
            #[cfg(test)]
            submit_fault: Arc::clone(&submit_fault),
        };
        std::thread::Builder::new()
            .name("felix-uring-fsync".into())
            .spawn(move || service.run())
            .ok()?;
        Some(Ring {
            tx,
            wake,
            #[cfg(test)]
            submit_fault,
        })
    }
}

/// The service thread's state. Owned exclusively by that thread: the
/// submission and completion queues are not safe to touch from several.
struct Service {
    uring: IoUring,
    wake: Arc<Wake>,
    rx: std::sync::mpsc::Receiver<Submission>,
    waiting: Waiting,
    backlog: VecDeque<Submission>,
    #[cfg(test)]
    submit_fault: Arc<std::sync::atomic::AtomicI32>,
}

impl Service {
    fn run(mut self) {
        // The wake poll is always armed, so the wait below returns on either a
        // completion or a newly queued request.
        self.wake.arm(&mut self.uring);
        loop {
            // Cleared before draining: a request queued after the drain
            // signals again and ends the next wait.
            self.wake.pending.store(false, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            self.backlog.extend(self.rx.try_iter());
            while self.waiting.len() < MAX_IN_FLIGHT {
                let Some(submission) = self.backlog.pop_front() else {
                    break;
                };
                if let Err(submission) = push(&mut self.uring, &mut self.waiting, submission) {
                    // The queue drains on the submit below.
                    self.backlog.push_front(submission);
                    break;
                }
            }

            let submitted = self.submit_and_wait();
            if let Err(err) = &submitted {
                self.on_submit_error(err);
            }
            let reaped = self.reap();
            if submitted.is_err() && reaped == 0 {
                // A ring that keeps refusing must not become a busy loop.
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }

    fn submit_and_wait(&mut self) -> io::Result<usize> {
        #[cfg(test)]
        {
            let errno = self.submit_fault.swap(0, Ordering::SeqCst);
            if errno != 0 {
                return Err(io::Error::from_raw_os_error(errno));
            }
        }
        self.uring.submit_and_wait(1)
    }

    /// A failed `io_uring_enter` leaves every entry the kernel already took
    /// in flight, and every entry it did not take in the submission queue for
    /// the next submit. Either way the kernel may still use the descriptor, so
    /// each file stays in `waiting` until its completion is reaped.
    fn on_submit_error(&mut self, err: &io::Error) {
        // REVERTED for the revert-and-fail check: the pre-fix behaviour.
        if err.kind() == io::ErrorKind::Interrupted {
            return;
        }
        for (_, (_file, reply)) in self.waiting.drain() {
            if let Some(reply) = reply {
                let _ = reply.send(Err(io::Error::new(err.kind(), err.to_string())));
            }
        }
    }

    /// Deliver every available completion and release its file.
    fn reap(&mut self) -> usize {
        let completions: Vec<(u64, i32)> = self
            .uring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();
        for &(id, result) in &completions {
            if id == WAKE_ID {
                self.wake.consume();
                self.wake.arm(&mut self.uring);
                continue;
            }
            if let Some((_file, Some(reply))) = self.waiting.remove(&id) {
                let outcome = if result < 0 {
                    Err(io::Error::from_raw_os_error(-result))
                } else {
                    Ok(())
                };
                let _ = reply.send(outcome);
            }
        }
        completions.len()
    }
}

/// Queue one operation, or hand it back if the submission queue is full.
fn push(
    uring: &mut IoUring,
    waiting: &mut Waiting,
    submission: Submission,
) -> Result<(), Submission> {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    // `DATASYNC`, matching `io::sync_data` on the blocking path: an append
    // changes data and size, not the metadata a full fsync also writes.
    let fd = types::Fd(submission.file.as_raw_fd());
    let entry = match submission.op {
        Op::Fsync => opcode::Fsync::new(fd)
            .flags(types::FsyncFlags::DATASYNC)
            .build(),
        #[cfg(test)]
        Op::PollReadable => opcode::PollAdd::new(fd, libc::POLLIN as u32).build(),
    }
    .user_data(id);
    // Safety: `waiting` keeps the `File` alive until its completion is
    // collected, so the descriptor stays open for the whole operation.
    let pushed = unsafe { uring.submission().push(&entry).is_ok() };
    if !pushed {
        return Err(submission);
    }
    waiting.insert(id, (submission.file, Some(submission.reply)));
    Ok(())
}

#[cfg(test)]
mod tests;

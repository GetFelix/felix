//! One device flush shared by many small durable writes.
//!
//! A vote is a small file written whole and renamed into place, and each
//! log has its own. When many logs save one in the same moment, a flush per
//! file makes the disk the bound on how long an election takes. Here the
//! callers that arrive while a flush runs wait for the next one together.
//!
//! What a caller is promised is unchanged: its write is on stable storage
//! before the call returns.

use std::collections::HashMap;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, LazyLock};

use parking_lot::{Condvar, Mutex, MutexGuard};

/// The flushes of one device, in the order they start.
#[derive(Default)]
struct Barrier {
    state: Mutex<State>,
    finished: Condvar,
}

#[derive(Default)]
struct State {
    started: u64,
    finished: u64,
    running: bool,
    /// The first failure. It stays: after a failed flush the kernel may
    /// have dropped pages a later flush would then report as written.
    failed: Option<(io::ErrorKind, String)>,
}

static BARRIERS: LazyLock<Mutex<HashMap<u64, Arc<Barrier>>>> = LazyLock::new(Mutex::default);

impl Barrier {
    /// Returns once a flush that started after this call has finished.
    /// The caller runs `flush` itself if none is running.
    fn pass(&self, flush: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let mut state = self.state.lock();
        // A flush already running may have started before this caller's
        // write reached the kernel.
        let covering = state.started + 1;
        loop {
            if let Some((kind, reason)) = &state.failed {
                return Err(io::Error::new(*kind, reason.clone()));
            }
            if state.finished >= covering {
                return Ok(());
            }
            if state.running {
                self.finished.wait(&mut state);
                continue;
            }
            state.running = true;
            state.started += 1;
            let mine = state.started;
            let flushed = MutexGuard::unlocked(&mut state, flush);
            state.running = false;
            state.finished = mine;
            if let Err(err) = &flushed {
                state.failed = Some((err.kind(), err.to_string()));
            }
            self.finished.notify_all();
            return flushed;
        }
    }
}

/// Make `file`, or the directory it is, durable through its device's
/// shared flush.
pub(super) fn sync(file: &std::fs::File) -> io::Result<()> {
    let device = file.metadata()?.dev();
    let barrier = Arc::clone(BARRIERS.lock().entry(device).or_default());
    push(file)?;
    barrier.pass(|| flush_device(file))
}

/// Linux: `syncfs` writes everything dirty on the filesystem, this file
/// included, so there is nothing to do first.
#[cfg(target_os = "linux")]
fn push(_file: &std::fs::File) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn flush_device(file: &std::fs::File) -> io::Result<()> {
    // SAFETY: `file` is a live descriptor for the duration of the call.
    match unsafe { libc::syncfs(file.as_raw_fd()) } {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

/// macOS: `fsync` moves this file to the drive, which may hold it in a
/// volatile cache. The shared `F_FULLFSYNC` then empties that cache, and it
/// is the drive's whole cache whichever file asks.
#[cfg(not(target_os = "linux"))]
fn push(file: &std::fs::File) -> io::Result<()> {
    // SAFETY: `file` is a live descriptor for the duration of the call.
    match unsafe { libc::fsync(file.as_raw_fd()) } {
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

#[cfg(not(target_os = "linux"))]
fn flush_device(file: &std::fs::File) -> io::Result<()> {
    // `sync_all` is `F_FULLFSYNC` on macOS and a plain fsync elsewhere.
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// Callers that arrive during a flush share the next one, and none
    /// returns on a flush that started before it arrived.
    #[test]
    fn callers_during_a_flush_share_the_next_one() {
        let barrier = Arc::new(Barrier::default());
        let flushes = Arc::new(AtomicU32::new(0));
        let (entered, running) = std::sync::mpsc::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let first = {
            let (barrier, flushes) = (Arc::clone(&barrier), Arc::clone(&flushes));
            std::thread::spawn(move || {
                barrier.pass(|| {
                    entered.send(()).expect("test");
                    held.recv().expect("test");
                    flushes.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
            })
        };
        running.recv().expect("first flush running");
        let later: Vec<_> = (0..8)
            .map(|_| {
                let (barrier, flushes) = (Arc::clone(&barrier), Arc::clone(&flushes));
                std::thread::spawn(move || {
                    barrier.pass(|| {
                        flushes.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    })?;
                    // Not the first flush alone: it began before this caller.
                    assert!(flushes.load(Ordering::Relaxed) >= 2);
                    io::Result::Ok(())
                })
            })
            .collect();
        while barrier.state.lock().started < 1 || !barrier.state.lock().running {
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        release.send(()).expect("test");
        first.join().expect("thread").expect("flush");
        for caller in later {
            caller.join().expect("thread").expect("flush");
        }
        // Eight callers, far fewer flushes than one each.
        assert!(flushes.load(Ordering::Relaxed) <= 4);
    }

    #[test]
    fn a_failed_flush_fails_every_later_caller() {
        let barrier = Barrier::default();
        barrier
            .pass(|| Err(io::Error::other("the disk is gone")))
            .expect_err("the flush failed");
        let err = barrier.pass(|| Ok(())).expect_err("still failed");
        assert!(err.to_string().contains("the disk is gone"));
    }
}

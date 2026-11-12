//! The store's lock, `lock`: one holder at a time, across processes.
//!
//! An exclusive `flock` on the file, held for as long as the store is open.
//! The kernel ties it to the file itself, so a second open is refused
//! whichever path or symlink it came by, and a process that dies gives it
//! up. The file holds nothing; only the lock on it matters.
//!
//! The simulator's filesystem has no descriptors to lock, and neither has a
//! platform without `flock`. There the claim is a path in this process.
//!
//! A first start is serialized the same way, on the data directory itself
//! ([`while_building`]), so two starts do not build the store at once.
//!
//! `flock` is only as good as the filesystem. On NFS under Linux it is
//! emulated with byte-range locks, which belong to the process: two opens
//! in one process do not exclude each other there, and a lock is lost when
//! any descriptor of the file is closed. Keep the data directory on a local
//! disk.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

pub(super) const FILE_NAME: &str = "lock";

/// Stores claimed in this process where there is no file to lock.
static CLAIMED: parking_lot::Mutex<Option<HashSet<PathBuf>>> = parking_lot::Mutex::new(None);

/// Data directories being built in, where there is no descriptor to lock.
static BUILDING: parking_lot::Mutex<Option<HashSet<PathBuf>>> = parking_lot::Mutex::new(None);
static BUILT: parking_lot::Condvar = parking_lot::Condvar::new();

/// Run `build` holding the exclusive lock on the data directory `parent`,
/// which exists. Waits for another builder rather than refusing: a build is
/// short, and the caller re-checks what it found before the wait.
pub(super) fn while_building<T>(
    parent: &Path,
    build: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    #[cfg(unix)]
    if !crate::fs::in_memory(parent) {
        use std::os::fd::AsRawFd;

        let dir = std::fs::File::open(parent)?;
        loop {
            // SAFETY: `dir` is open, and `flock` only reads the descriptor.
            if unsafe { libc::flock(dir.as_raw_fd(), libc::LOCK_EX) } == 0 {
                break;
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(err);
            }
        }
        // Closing `dir` releases the lock.
        return build();
    }
    let key = std::path::absolute(parent)?;
    let mut building = BUILDING.lock();
    while !building.get_or_insert_default().insert(key.clone()) {
        BUILT.wait(&mut building);
    }
    drop(building);
    let built = build();
    if let Some(building) = BUILDING.lock().as_mut() {
        building.remove(&key);
    }
    BUILT.notify_all();
    built
}

/// The lock on one store, given up when dropped.
#[derive(Debug)]
pub(super) enum StoreLock {
    #[cfg(unix)]
    File(std::fs::File),
    Claim(PathBuf),
}

impl StoreLock {
    /// Lock the store in `dir`, which exists. Never waits: a store that is
    /// held is refused with [`io::ErrorKind::ResourceBusy`].
    pub(super) fn take(dir: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        if !crate::fs::in_memory(dir) {
            return flock(dir).map(Self::File);
        }
        let key = std::path::absolute(dir)?;
        if !CLAIMED.lock().get_or_insert_default().insert(key.clone()) {
            return Err(busy(dir));
        }
        Ok(Self::Claim(key))
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        match self {
            // Unlocked, not just closed: a child process forked while the
            // store was open holds a copy of the descriptor until it execs,
            // and a close alone would leave the lock held through it.
            #[cfg(unix)]
            Self::File(file) => {
                use std::os::fd::AsRawFd;
                // SAFETY: `file` is open, and `flock` only reads the
                // descriptor.
                unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
            }
            Self::Claim(key) => {
                if let Some(claimed) = CLAIMED.lock().as_mut() {
                    claimed.remove(key);
                }
            }
        }
    }
}

#[cfg(unix)]
fn flock(dir: &Path) -> io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;

    // Opened with `std::fs`: the seam's files in memory cannot be locked,
    // and this path is on disk.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(FILE_NAME))?;
    loop {
        // SAFETY: `file` is open, and `flock` only reads the descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(file);
        }
        let err = io::Error::last_os_error();
        match err.kind() {
            io::ErrorKind::Interrupted => {}
            io::ErrorKind::WouldBlock => return Err(busy(dir)),
            _ => return Err(err),
        }
    }
}

fn busy(dir: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::ResourceBusy,
        format!(
            "{} is already open, in this process or another",
            dir.display()
        ),
    )
}

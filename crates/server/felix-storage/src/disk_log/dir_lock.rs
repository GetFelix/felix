//! The lock on a Raft log's directory: one open log at a time, across
//! processes and within one.
//!
//! An exclusive `flock` on the directory itself, so no file is added to it
//! and no segment file is touched. The kernel ties the lock to the open
//! directory, so a second open is refused whichever path or symlink it came
//! by, including a second open in the same process, and a process that dies
//! gives it up.
//!
//! The simulator's filesystem has no descriptors to lock, and neither has a
//! platform without `flock`. There the claim is a path in this process, and
//! a simulated crash or reboot of the tree gives it up, as a process death
//! gives up a `flock`.
//!
//! As with the metadata store's lock, `flock` is only as good as the
//! filesystem: on NFS under Linux two opens in one process do not exclude
//! each other. Keep the data directory on a local disk.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Directories claimed in this process where there is no descriptor to
/// lock, each with the claim that holds it.
static CLAIMED: parking_lot::Mutex<Option<HashMap<PathBuf, u64>>> = parking_lot::Mutex::new(None);
static NEXT_CLAIM: AtomicU64 = AtomicU64::new(0);

/// The lock on one log's directory, given up when dropped.
#[derive(Debug)]
pub(super) enum DirLock {
    #[cfg(unix)]
    Dir(std::fs::File),
    Claim(PathBuf, u64),
}

impl DirLock {
    /// Lock `dir`, which exists. Never waits: a directory another open log
    /// holds is refused with [`io::ErrorKind::ResourceBusy`].
    pub(super) fn take(dir: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        if !crate::fs::in_memory(dir) {
            return flock(dir).map(Self::Dir);
        }
        let key = std::path::absolute(dir)?;
        let mut claimed = CLAIMED.lock();
        let claimed = claimed.get_or_insert_default();
        if claimed.contains_key(&key) {
            return Err(busy(dir));
        }
        let claim = NEXT_CLAIM.fetch_add(1, Ordering::Relaxed);
        claimed.insert(key.clone(), claim);
        Ok(Self::Claim(key, claim))
    }
}

/// Give up every claim below `root`, for a simulated crash: the logs open
/// there belong to a process that is gone. Their handles, dropped later,
/// leave a newer claim on the same directory alone.
#[cfg(feature = "sim")]
pub(crate) fn release_under(root: &Path) {
    let Ok(root) = std::path::absolute(root) else {
        return;
    };
    if let Some(claimed) = CLAIMED.lock().as_mut() {
        claimed.retain(|dir, _| !dir.starts_with(&root));
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        match self {
            // Unlocked, not just closed: a child forked while the log was
            // open holds a copy of the descriptor until it execs.
            #[cfg(unix)]
            Self::Dir(dir) => {
                use std::os::fd::AsRawFd;
                // SAFETY: `dir` is open, and `flock` only reads the
                // descriptor.
                unsafe { libc::flock(dir.as_raw_fd(), libc::LOCK_UN) };
            }
            Self::Claim(key, claim) => {
                if let Some(claimed) = CLAIMED.lock().as_mut()
                    && claimed.get(key) == Some(claim)
                {
                    claimed.remove(key);
                }
            }
        }
    }
}

#[cfg(unix)]
fn flock(dir: &Path) -> io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;

    let file = std::fs::File::open(dir)?;
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
            "the log in {} is already open, in this process or another",
            dir.display()
        ),
    )
}

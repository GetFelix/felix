//! The lock on a log's directory: one open log at a time, across processes
//! and within one. Every open takes it, and so does the root of a
//! [`crate::DiskLogProvider`], so two brokers on one data directory, or a
//! broker and `felix-log-tool`, never both write, cut or rotate a log.
//!
//! An exclusive `flock` on the directory itself, so no file is added to it
//! and no segment file is touched. The kernel ties the lock to the open
//! directory, so a second open is refused whichever path or symlink it came
//! by, including a second open in the same process, and a process that dies
//! gives it up. A lock is only good while its directory is still the one at
//! the path, so a lock taken on a directory a rotation moved away meanwhile
//! is refused too.
//!
//! The simulator's filesystem has no descriptors to lock, and neither has a
//! platform without `flock`. There the claim is a path in this process, and
//! a simulated crash or reboot of the tree gives it up, as a process death
//! gives up a `flock`.
//!
//! On NFS under Linux `flock` is emulated with byte-range locks, which need
//! a descriptor open for writing, and a directory cannot be opened that way:
//! the open fails there. Keep the data directory on a local disk.

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
pub(crate) enum DirLock {
    #[cfg(unix)]
    Dir(std::fs::File),
    Claim(PathBuf, u64),
}

impl DirLock {
    /// Lock `dir`, which exists. Never waits: a directory another open log
    /// holds is refused with [`io::ErrorKind::ResourceBusy`].
    pub(crate) fn take(dir: &Path) -> io::Result<Self> {
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

    /// Lock the new directory at `dir` too, after a rotation renamed the
    /// locked one away, and hand back the lock on the old one, for the
    /// caller to keep until it is done with it. A claim names the path, not
    /// the directory, so it stays and `None` comes back.
    #[cfg(feature = "raft")]
    pub(crate) fn follow(&mut self, dir: &Path) -> io::Result<Option<Self>> {
        #[cfg(unix)]
        if let Self::Dir(_) = self {
            return Ok(Some(std::mem::replace(self, Self::Dir(flock(dir)?))));
        }
        let _ = dir;
        Ok(None)
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

/// Wait until every directory directly under `root`, or `root` itself, is
/// let go: a test that drops a log to stand in for a crash reopens it only
/// once the old log's last background write has finished with it.
#[cfg(test)]
pub(crate) async fn released(root: &Path) {
    let mut dirs = vec![root.to_path_buf()];
    if let Ok(entries) = std::fs::read_dir(root) {
        dirs.extend(
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_dir()),
        );
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    for dir in dirs {
        while let Err(err) = DirLock::take(&dir) {
            if err.kind() != io::ErrorKind::ResourceBusy {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{} is still held by a dropped log",
                dir.display()
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
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

    use std::os::unix::fs::MetadataExt;

    let file = std::fs::File::open(dir)?;
    loop {
        // SAFETY: `file` is open, and `flock` only reads the descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            // A rotation can rename the directory away between the open and
            // the lock, and the path then names a directory its holder has.
            let (locked, at_path) = (file.metadata()?, std::fs::metadata(dir)?);
            if (locked.dev(), locked.ino()) != (at_path.dev(), at_path.ino()) {
                return Err(busy(dir));
            }
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
            "{} is locked by another open: a second broker on the same data \
             directory, felix-log-tool, or an open in this process that was not \
             closed yet",
            dir.display()
        ),
    )
}

//! A Raft log holds its directory: a second open, in this process or
//! another, is refused until the first is closed or dropped.

use std::path::Path;

use super::*;

fn raft_config() -> LogConfig {
    LogConfig {
        raft_log: true,
        ..config(FsyncMode::OnCommit)
    }
}

fn open_raft(dir: &Path) -> Result<DiskLog> {
    DiskLog::open(dir, "t/ns/raft/0", raft_config())
}

fn is_busy(opened: Result<DiskLog>) -> bool {
    matches!(opened, Err(StorageError::Io(err)) if err.kind() == std::io::ErrorKind::ResourceBusy)
}

#[tokio::test]
async fn a_second_open_in_this_process_is_refused_until_the_first_closes() {
    let dir = tempdir().expect("tempdir");
    let log = open_raft(dir.path()).expect("open");
    assert!(is_busy(open_raft(dir.path())), "a second open was let in");

    // A clone left over does not hold the directory once the log is closed:
    // it can no longer write.
    let clone = log.clone();
    log.close().await.expect("close");
    open_raft(dir.path()).expect("reopen after close");
    drop(clone);
}

#[tokio::test]
async fn a_dropped_log_lets_go_of_its_directory() {
    let dir = tempdir().expect("tempdir");
    let log = open_raft(dir.path()).expect("open");
    let clone = log.clone();
    drop(log);
    assert!(is_busy(open_raft(dir.path())), "a clone still holds it");
    drop(clone);
    open_raft(dir.path()).expect("reopen after drop");
}

#[cfg(unix)]
#[tokio::test]
async fn another_path_to_the_directory_is_refused_too() {
    let dir = tempdir().expect("tempdir");
    let log_dir = dir.path().join("log");
    let _log = open_raft(&log_dir).expect("open");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&log_dir, &link).expect("symlink");

    assert!(is_busy(open_raft(&link)), "a symlink got around the lock");
}

/// Another process's lock is a `flock` on the directory through its own
/// descriptor, which its death closes. Taken and dropped here the same way.
#[cfg(unix)]
#[tokio::test]
async fn a_lock_held_elsewhere_refuses_the_open_until_its_holder_goes() {
    use std::os::fd::AsRawFd;

    let dir = tempdir().expect("tempdir");
    let holder = std::fs::File::open(dir.path()).expect("open dir");
    // SAFETY: `holder` is open, and `flock` only reads the descriptor.
    let locked = unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(locked, 0, "{}", std::io::Error::last_os_error());

    assert!(is_busy(open_raft(dir.path())));
    // The refused open changed nothing in the directory.
    assert_eq!(std::fs::read_dir(dir.path()).expect("list").count(), 0);

    drop(holder);
    open_raft(dir.path()).expect("open once the holder is gone");
}

#[cfg(feature = "sim")]
#[tokio::test]
async fn on_the_simulated_filesystem_the_claim_is_per_process() {
    let root = std::env::temp_dir().join(format!("felix-simfs-dirlock-{}", std::process::id()));
    crate::sim_fs::register_root(&root);
    let dir = root.join("log");

    let log = open_raft(&dir).expect("open");
    assert!(is_busy(open_raft(&dir)), "a second open was let in");
    log.close().await.expect("close");
    let reopened = open_raft(&dir).expect("reopen after close");
    drop(reopened);
    let before_the_crash = open_raft(&dir).expect("reopen after drop");

    // A crash ends the process that held it. Its handle, dropped after the
    // next boot opened the log, does not free that boot's claim.
    crate::sim_fs::crash(&root, 0).expect("crash");
    let after_the_crash = open_raft(&dir).expect("open after the crash");
    drop(before_the_crash);
    assert!(
        is_busy(open_raft(&dir)),
        "the old handle freed the new claim"
    );
    drop(after_the_crash);

    crate::sim_fs::unregister_root(&root);
}

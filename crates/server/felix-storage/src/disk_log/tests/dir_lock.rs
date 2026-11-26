//! A log holds its directory: a second open, in this process or another, is
//! refused until the first is closed or dropped.

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

/// A stream log holds its directory as a Raft log does: a second broker or
/// `felix-log-tool` on it would repair a tail the first is still writing.
#[tokio::test]
async fn a_second_open_of_a_stream_log_is_refused_until_the_first_closes() {
    let dir = tempdir().expect("tempdir");
    let open = || DiskLog::open(dir.path(), "t/ns/orders/0", config(FsyncMode::OnCommit));
    let log = open().expect("open");
    let refused = open();
    assert!(
        matches!(&refused, Err(StorageError::Io(err)) if err.to_string().contains(&dir.path().display().to_string())),
        "a second open was let in, or its error does not name the directory: {refused:?}"
    );
    assert!(is_busy(refused), "the refusal is not ResourceBusy");
    log.close().await.expect("close");
    open().expect("reopen after close");
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

/// A close lets go of the directory before it returns, with appends in
/// flight, a periodic flusher and rollovers running, so a reopen right after
/// it never finds the directory busy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_log_reopens_at_once_after_a_close_under_load() {
    let dir = tempdir().expect("tempdir");
    let config = LogConfig {
        segment_size_bytes: crate::segment::SEGMENT_HEADER_LEN + 512,
        fsync_mode: FsyncMode::Periodic {
            interval: std::time::Duration::from_millis(1),
        },
        ..config(FsyncMode::None)
    };
    for round in 0..20 {
        let log = DiskLog::open(dir.path(), "t/ns/orders/0", config.clone()).expect("open");
        let writers: Vec<_> = (0..4)
            .map(|_| {
                let log = log.clone();
                tokio::spawn(async move { while log.append(&records(&["load"])).await.is_ok() {} })
            })
            .collect();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        log.close().await.expect("close");
        let again = DiskLog::open(dir.path(), "t/ns/orders/0", config.clone());
        assert!(again.is_ok(), "round {round}: {:?}", again.err());
        again.expect("open").close().await.expect("close");
        for writer in writers {
            writer.await.expect("writer");
        }
    }
}

/// A shutdown lets go of the directory as a close does: a broker stopping
/// calls it, and the next open in this process must not find the directory
/// held by clones of the old log, which can no longer write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_log_reopens_after_a_shutdown_with_clones_alive() {
    let dir = tempdir().expect("tempdir");
    let open = || DiskLog::open(dir.path(), "t/ns/orders/0", config(FsyncMode::OnCommit));
    let mut clones = Vec::new();
    for round in 0..20u64 {
        let log = open().unwrap_or_else(|err| panic!("round {round}: {err}"));
        log.append(&records(&["one"])).await.expect("append");
        clones.push(log.clone());
        log.shutdown().await.expect("shutdown");
        drop(log);
    }
    for clone in &clones {
        assert!(
            matches!(
                clone.append(&records(&["late"])).await,
                Err(StorageError::Closed(_))
            ),
            "a clone wrote after its log was shut down"
        );
    }
    let log = open().expect("open");
    assert_eq!(log.tail_offset().await.expect("tail"), 20);
    log.close().await.expect("close");
}

/// A task holding the only handle is aborted while a reservation extension
/// it started is still parked: the extension must not keep the directory,
/// so the next open finds it free.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_task_with_an_extension_in_flight_lets_go_of_the_directory() {
    let dir = tempdir().expect("tempdir");
    let config = LogConfig {
        preallocate_segments: true,
        ..config(FsyncMode::None)
    };
    let log = DiskLog::open(dir.path(), "t/ns/orders/0", config.clone()).expect("open");
    let (release, held) = std::sync::mpsc::channel();
    *log.inner.hold_next_extension.lock() = Some(held);
    let (parked_tx, parked) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let payload = "x".repeat(64 * 1024);
        while log.inner.hold_next_extension.lock().is_some() {
            log.append(&records(&[payload.as_str()]))
                .await
                .expect("append");
        }
        let _ = parked_tx.send(());
        std::future::pending::<()>().await;
        drop(log);
    });
    parked.await.expect("an extension parked");
    task.abort();
    assert!(task.await.expect_err("aborted").is_cancelled());
    let reopened = DiskLog::open(dir.path(), "t/ns/orders/0", config);
    drop(release);
    let reopened = reopened.expect("reopen after the task was aborted");
    reopened.close().await.expect("close");
}

const CHILD_DIR: &str = "FELIX_TEST_DIR_LOCK_CHILD";

/// Run by [`a_holder_that_dies_without_closing_does_not_block_the_next_open`]
/// in a second process: opens the log named in the environment, appends one
/// record, says so, and exits without closing once its stdin ends, unless
/// it is killed first. Does nothing as a test of its own.
#[tokio::test]
async fn dir_lock_child() {
    let Some(dir) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let log = DiskLog::open(
        Path::new(&dir),
        "t/ns/orders/0",
        config(FsyncMode::OnCommit),
    )
    .expect("child open");
    log.append(&records(&["held"])).await.expect("child append");
    println!("held");
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    std::process::exit(0);
}

/// A holder that dies without a close, killed or exiting on its own, leaves
/// nothing behind that refuses the next open: the kernel drops a `flock`
/// with the last descriptor, and a process's death closes them all.
#[cfg(unix)]
#[tokio::test]
async fn a_holder_that_dies_without_closing_does_not_block_the_next_open() {
    use std::io::BufRead;

    for killed in [true, false] {
        let dir = tempdir().expect("tempdir");
        let mut child = std::process::Command::new(std::env::current_exe().expect("this binary"))
            .args([
                "--exact",
                "disk_log::tests::dir_lock::dir_lock_child",
                "--nocapture",
            ])
            .env(CHILD_DIR, dir.path())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("child");
        // Kept open to the end, so the child never writes into a closed pipe.
        let mut stdout = std::io::BufReader::new(child.stdout.take().expect("stdout")).lines();
        let held = stdout.any(|line| line.is_ok_and(|line| line.trim() == "held"));
        assert!(held, "the child never opened the log");
        let open = || DiskLog::open(dir.path(), "t/ns/orders/0", config(FsyncMode::OnCommit));
        assert!(
            is_busy(open()),
            "the child's open does not hold the directory"
        );

        if killed {
            child.kill().expect("kill");
        } else {
            drop(child.stdin.take());
        }
        let status = child.wait().expect("wait");
        assert!(killed || status.success(), "the child failed: {status}");
        let reopened = open().unwrap_or_else(|err| {
            panic!("killed: {killed}: the dead holder still blocks the open: {err}")
        });
        assert_eq!(read_all(&reopened, 0).await, ["held"]);
        reopened.close().await.expect("close");
        drop(stdout);
    }
}

/// **A filesystem that cannot lock a directory fails the open with the
/// directory's name and what to do**, not a bare OS error.
#[test]
fn a_lock_the_filesystem_refuses_names_the_directory() {
    let root = tempfile::tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    crate::fault::fail_locks_under(root.path());
    let opened = DiskLog::open(&dir, "t/ns/s/0", config(FsyncMode::OnCommit));
    crate::fault::stop_failing_locks_under(root.path());
    let Err(err) = opened else {
        panic!("opened a log the filesystem cannot lock");
    };
    let err = err.to_string();
    assert!(
        err.contains(&dir.display().to_string()) && err.contains("local disk"),
        "{err}"
    );
}

use tempfile::tempdir;

use super::*;
use crate::disk_log::DiskLog;
use crate::log::{FsyncMode, LogConfig};

fn hybrid() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        ..LogConfig::default()
    }
}

fn raft() -> LogConfig {
    LogConfig {
        raft_log: true,
        ..hybrid()
    }
}

fn why(err: StorageError) -> String {
    match err {
        StorageError::Io(err) => err.to_string(),
        other => panic!("not an open refusal: {other:?}"),
    }
}

#[tokio::test]
async fn a_raft_directory_is_refused_by_the_hybrid() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    claim_for_raft(&dir, "t/n/s/0").expect("claim");
    let log = DiskLog::open(&dir, "t/n/s/0", raft()).expect("open as raft");
    log.shutdown().await.expect("shutdown");
    drop(log);

    let Err(err) = DiskLog::open(&dir, "t/n/s/0", hybrid()) else {
        panic!("opened");
    };
    assert!(why(err).contains("holds a Raft member's log"));
    // Claiming again is how every restart opens it.
    claim_for_raft(&dir, "t/n/s/0").expect("claim again");
    DiskLog::open(&dir, "t/n/s/0", raft()).expect("open as raft again");
}

#[tokio::test]
async fn a_hybrid_directory_is_refused_by_raft() {
    let root = tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    let log = DiskLog::open(&dir, "t/n/s/0", hybrid()).expect("open as hybrid");
    log.shutdown().await.expect("shutdown");
    drop(log);

    let err = claim_for_raft(&dir, "t/n/s/0").expect_err("refused");
    assert!(why(err).contains("the hybrid engine wrote"));
    assert!(!path_in(&dir).exists(), "a refused claim leaves no marker");
    DiskLog::open(&dir, "t/n/s/0", hybrid()).expect("still the hybrid's");
}

#[test]
fn an_unknown_engine_is_refused_by_both() {
    let root = tempdir().expect("tempdir");
    std::fs::write(path_in(root.path()), b"paxos\n").expect("write");
    let err = claim_for_raft(root.path(), "t/n/s/0").expect_err("refused");
    assert!(why(err).contains("does not know"));
    let Err(err) = DiskLog::open(root.path(), "t/n/s/0", hybrid()) else {
        panic!("opened");
    };
    assert!(why(err).contains("does not know"));
}

//! A group write whose caller is cancelled still lands, on its own task, after
//! the caller has let go of the shard lock. The next write to the same key has
//! to see it rather than read the old value and land on top.
//!
//! Its own test binary because the injected fsync delay is process-wide. The
//! delay is what holds the cancelled write in flight.

use std::time::Duration;

use felix_broker::{ConsumerGroups, DeadLetters, GroupKey};
use felix_storage::fault::set_fsync_delay;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::tempdir;

const FLUSH: Duration = Duration::from_millis(1500);
/// Long enough to stage the write, well short of its flush.
const CANCEL_AFTER: Duration = Duration::from_millis(200);

fn config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key() -> GroupKey {
    GroupKey {
        tenant_id: "t1".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        group: "workers".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_after_a_cancelled_one_does_not_move_the_group_backwards() {
    let dir = tempdir().expect("dir");
    let groups = ConsumerGroups::open(dir.path(), config()).expect("groups");
    // Opens the shard's log before the delay, so only the commits pay it.
    groups
        .commit("t1", "ns", "orders", 0, "workers", 10)
        .await
        .expect("first commit");
    set_fsync_delay(FLUSH);

    let cancelled = tokio::time::timeout(
        CANCEL_AFTER,
        groups.commit("t1", "ns", "orders", 0, "workers", 100),
    )
    .await;
    assert!(cancelled.is_err(), "the commit finished before its flush");

    let held = groups
        .commit("t1", "ns", "orders", 0, "workers", 50)
        .await
        .expect("late commit");
    assert_eq!(held, 100, "the late commit did not see the one in flight");
    assert_eq!(
        groups
            .committed("t1", "ns", "orders", 0, "workers")
            .await
            .expect("committed"),
        Some(100),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finishing_a_redrive_keeps_a_cancelled_return_to_dead() {
    let dir = tempdir().expect("dir");
    let dead = DeadLetters::open(dir.path(), config()).expect("dead letters");
    let key = key();
    dead.record(&key, 7).await.expect("record");
    assert!(dead.redrive(&key, 7).await.expect("redrive"));
    set_fsync_delay(FLUSH);

    // The redriven record failed again; its caller gave up mid-flush.
    let cancelled = tokio::time::timeout(CANCEL_AFTER, dead.record(&key, 7)).await;
    assert!(cancelled.is_err(), "the record finished before its flush");

    dead.finish_redrive(&key, 7).await.expect("finish redrive");
    assert_eq!(
        dead.list(&key).await.expect("list"),
        vec![7],
        "finishing the redrive erased a dead letter recorded after it",
    );
}

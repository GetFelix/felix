//! Consumer-group cursors: what has to be true before anything is delivered.
use super::*;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: felix_storage::log::FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

const T: &str = "t1";
const NS: &str = "ns";
const S: &str = "orders";
const G: &str = "workers";

fn groups(dir: &std::path::Path) -> ConsumerGroups {
    ConsumerGroups::open(dir, config()).expect("open")
}

#[tokio::test]
async fn a_group_with_no_history_has_no_position() {
    let dir = tempfile::tempdir().expect("tempdir");
    let groups = groups(dir.path());

    assert_eq!(
        groups.committed(T, NS, S, 0, G).await.expect("committed"),
        None,
        "a position must not be invented for a group that has never committed",
    );
}

#[tokio::test]
async fn a_commit_is_readable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let groups = groups(dir.path());

    assert_eq!(groups.commit(T, NS, S, 0, G, 42).await.expect("commit"), 42);
    assert_eq!(
        groups.committed(T, NS, S, 0, G).await.expect("committed"),
        Some(42),
    );
}

/// **The property everything else rests on.** A late acknowledgement from a
/// consumer that has already been superseded must not move the group backwards:
/// doing so redelivers every record between, to a group that has finished them.
#[tokio::test]
async fn a_commit_never_moves_a_group_backwards() {
    let dir = tempfile::tempdir().expect("tempdir");
    let groups = groups(dir.path());

    groups.commit(T, NS, S, 0, G, 100).await.expect("commit");
    let held = groups
        .commit(T, NS, S, 0, G, 40)
        .await
        .expect("late commit");

    assert_eq!(held, 100, "the late commit should report the position held");
    assert_eq!(
        groups.committed(T, NS, S, 0, G).await.expect("committed"),
        Some(100),
    );
}

/// Racing acknowledgements are serialised, so the highest wins and none of them
/// leaves the group behind where it already was.
#[tokio::test]
async fn concurrent_commits_settle_on_the_highest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let groups = std::sync::Arc::new(groups(dir.path()));

    let mut tasks = Vec::new();
    for offset in [7u64, 3, 91, 15, 64, 2] {
        let groups = std::sync::Arc::clone(&groups);
        tasks.push(tokio::spawn(async move {
            groups.commit(T, NS, S, 0, G, offset).await.expect("commit")
        }));
    }
    for task in tasks {
        task.await.expect("join");
    }

    assert_eq!(
        groups.committed(T, NS, S, 0, G).await.expect("committed"),
        Some(91),
    );
}

/// Groups are independent of each other, and of the shards they read.
#[tokio::test]
async fn groups_and_shards_do_not_share_a_position() {
    let dir = tempfile::tempdir().expect("tempdir");
    let groups = groups(dir.path());

    groups.commit(T, NS, S, 0, "a", 10).await.expect("commit");
    groups.commit(T, NS, S, 0, "b", 20).await.expect("commit");
    groups.commit(T, NS, S, 1, "a", 30).await.expect("commit");

    assert_eq!(groups.committed(T, NS, S, 0, "a").await.unwrap(), Some(10));
    assert_eq!(groups.committed(T, NS, S, 0, "b").await.unwrap(), Some(20));
    assert_eq!(groups.committed(T, NS, S, 1, "a").await.unwrap(), Some(30));
    assert_eq!(groups.committed(T, NS, S, 1, "b").await.unwrap(), None);
}

/// **Positions outlive the broker.** A group that restarted from zero would
/// redeliver everything it had already processed, which is the failure the
/// durability is for.
#[tokio::test]
async fn a_position_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let groups = groups(dir.path());
        groups.commit(T, NS, S, 0, G, 512).await.expect("commit");
        groups.shutdown().await.expect("shutdown");
    }

    let reopened = groups(dir.path());
    assert_eq!(
        reopened.committed(T, NS, S, 0, G).await.expect("committed"),
        Some(512),
        "the group lost its position across a restart",
    );
}

#[tokio::test]
async fn forgetting_a_group_reports_whether_it_had_a_position() {
    let dir = tempfile::tempdir().expect("tempdir");
    let groups = groups(dir.path());

    assert!(!groups.forget(T, NS, S, 0, G).await.expect("forget"));
    groups.commit(T, NS, S, 0, G, 5).await.expect("commit");
    assert!(groups.forget(T, NS, S, 0, G).await.expect("forget"));
    assert_eq!(groups.committed(T, NS, S, 0, G).await.unwrap(), None);
}

/// A forgotten group starts over rather than resuming, and the forgetting
/// survives a restart like any other write.
#[tokio::test]
async fn forgetting_survives_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let groups = groups(dir.path());
        groups.commit(T, NS, S, 0, G, 77).await.expect("commit");
        groups.forget(T, NS, S, 0, G).await.expect("forget");
        groups.shutdown().await.expect("shutdown");
    }

    let reopened = groups(dir.path());
    assert_eq!(
        reopened.committed(T, NS, S, 0, G).await.expect("committed"),
        None,
        "a forgotten group came back after a restart",
    );
}

/// A commit cancelled after it spawned its write but before that write ran
/// must not let the next commit read the old position and move it back.
///
/// The runtime is current-thread, so the spawned write cannot start until
/// this task yields. The commit is polled by hand, sleeping the thread rather
/// than yielding while its reads finish, until the write task exists.
#[tokio::test(flavor = "current_thread")]
async fn a_commit_cancelled_before_its_write_started_is_not_overwritten() {
    use std::future::Future;
    use std::task::Poll;

    let dir = tempfile::tempdir().expect("tempdir");
    let groups = groups(dir.path());
    groups.commit(T, NS, S, 0, G, 50).await.expect("commit");

    let metrics = tokio::runtime::Handle::current().metrics();
    let tasks_before = metrics.num_alive_tasks();
    let mut cancelled = Box::pin(groups.commit(T, NS, S, 0, G, 100));
    loop {
        let polled = std::future::poll_fn(|cx| Poll::Ready(cancelled.as_mut().poll(cx))).await;
        assert!(
            polled.is_pending(),
            "the write cannot finish without a yield"
        );
        if metrics.num_alive_tasks() > tasks_before {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    drop(cancelled);

    assert_eq!(
        groups.commit(T, NS, S, 0, G, 70).await.expect("commit"),
        100
    );
    assert_eq!(
        groups.committed(T, NS, S, 0, G).await.expect("committed"),
        Some(100)
    );
}

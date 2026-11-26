//! `save_committed` on a slow disk: the group applies without waiting on a
//! replica-state write, in every fsync mode, and what lands on disk is an id
//! it was given, behind memory and never ahead of it.

use std::time::{Duration, Instant};

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;

use super::*;
use crate::disk_log::COMMIT_PERSIST_INTERVAL;

type Raft = RaftLog<Types, Codec>;

/// Longer than the whole burst of saves takes without a write, so one save
/// that waits on a write fails the timing check as well as the count.
const SLOW_WRITE: Duration = Duration::from_secs(1);

fn relaxed_modes() -> [(bool, FsyncMode); 2] {
    [
        (
            true,
            FsyncMode::Periodic {
                interval: Duration::from_secs(3600),
            },
        ),
        (true, FsyncMode::None),
    ]
}

fn open_in(root: &std::path::Path, mode: FsyncMode) -> Raft {
    let dir = root.join("shard");
    std::fs::create_dir_all(&dir).expect("shard dir");
    let config = LogConfig {
        fsync_mode: mode,
        ..config()
    };
    let log = DiskLog::open(&dir, "t/ns/raft/0", config).expect("open");
    RaftLog::new(log, Codec).expect("a Raft log")
}

async fn append(raft: &mut Raft, entries: Vec<TestEntry>) {
    let (tx, rx) = Types::oneshot();
    raft.append(entries, IOFlushed::signal(tx))
        .await
        .expect("append");
    rx.await.expect("callback").expect("flushed");
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy");
        }
    }
}

fn on_disk(raft: &Raft) -> u64 {
    crate::disk_log::saved_commit(raft.disk_log().dir())
        .expect("replica state")
        .0
}

/// **`save_committed` waits on no replica-state write**, on a disk whose
/// writes take a second, and writes stay at about one per
/// [`COMMIT_PERSIST_INTERVAL`] however often the id moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_save_returns_at_once_and_writes_stay_bounded_on_a_slow_disk() {
    for (relaxed, mode) in relaxed_modes() {
        let root = tempdir().expect("dir");
        crate::run_state::begin(root.path(), relaxed, None, None, || false, || [7; 16])
            .expect("begin");
        let mut raft = open_in(root.path(), mode);
        let entries: Vec<_> = (0..400).map(|index| normal(1, index, "a")).collect();
        append(&mut raft, entries).await;
        let log = raft.disk_log().clone();
        log.slow_replica_writes(SLOW_WRITE);

        // The first save finds nothing written yet, so a write is due at once.
        let started = Instant::now();
        for index in 0..20 {
            raft.save_committed(Some(log_id(1, index)))
                .await
                .expect("save");
        }
        let burst = started.elapsed();
        assert_eq!(
            log.commit_path_writes(),
            0,
            "{mode:?}: a save waited on a write"
        );
        assert!(burst < SLOW_WRITE, "{mode:?}: 20 saves took {burst:?}");

        // Keep moving the id for a while; the writes it causes stay bounded.
        let mut index = 20;
        while started.elapsed() < Duration::from_millis(2500) {
            raft.save_committed(Some(log_id(1, index)))
                .await
                .expect("save");
            index += 1;
            let held = on_disk(&raft);
            assert!(
                held <= log.commit_offset(),
                "{mode:?}: disk ahead of memory"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let elapsed = started.elapsed();
        let writes = log.replica_writes();
        let bound = elapsed.as_millis() / COMMIT_PERSIST_INTERVAL.as_millis() + 1;
        assert_eq!(
            log.commit_path_writes(),
            0,
            "{mode:?}: a save waited on a write"
        );
        assert!(
            writes >= 1 && u128::from(writes) <= bound,
            "{mode:?}: {writes} writes in {elapsed:?}, bound {bound}"
        );

        // A clean close writes the last id saved.
        log.slow_replica_writes(Duration::ZERO);
        let last = index - 1;
        log.close().await.expect("close");
        drop((raft, log));
        let mut raft = open_in(root.path(), mode);
        assert_eq!(
            raft.read_committed().await.expect("committed"),
            Some(log_id(1, last)),
            "{mode:?}"
        );
    }
}

/// **After a crash the id read back is an earlier one it was given, never a
/// later one.** A save whose write was still waiting out the interval is
/// lost with the process; the open finds the one before it.
#[tokio::test]
async fn a_crash_before_the_write_behind_reads_back_an_earlier_id() {
    for (relaxed, mode) in relaxed_modes() {
        let root = tempdir().expect("dir");
        crate::run_state::begin(root.path(), relaxed, None, None, || false, || [7; 16])
            .expect("begin");
        let mut raft = open_in(root.path(), mode);
        let entries: Vec<_> = (0..=5).map(|index| normal(1, index, "a")).collect();
        append(&mut raft, entries).await;
        raft.save_committed(Some(log_id(1, 2))).await.expect("save");
        raft.disk_log().commit_offset_persisted().await;
        // Within the interval of the last write, so it waits behind.
        raft.save_committed(Some(log_id(1, 5))).await.expect("save");
        assert_eq!(on_disk(&raft), 3, "{mode:?}: the second save is behind");
        // The files as a crash now leaves them, opened by a new run.
        let after = tempdir().expect("dir");
        crate::run_state::begin(after.path(), relaxed, None, None, || false, || [7; 16])
            .expect("begin");
        copy_dir(&root.path().join("shard"), &after.path().join("shard"));
        drop(raft);

        let mut raft = open_in(after.path(), mode);
        assert_eq!(
            raft.read_committed().await.expect("committed"),
            Some(log_id(1, 2)),
            "{mode:?}"
        );
    }
}

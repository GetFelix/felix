//! A copy a re-form stopped: it never opens as itself again, and rejoins
//! only under a new copy id.

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::raft_log::RaftLog;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, normal};

type Log = RaftLog<Types, Codec>;

const LABEL: &str = "t/ns/orders/0";

/// A copy that voted and holds two records, then stopped.
async fn stopped_copy(copies: &CopyRoot, dir: &Path) -> u64 {
    let copy = voted_copy(copies, dir).await;
    stop(dir, Some(1)).expect("stop");
    copy
}

/// A copy that voted and holds two records, closed.
async fn voted_copy(copies: &CopyRoot, dir: &Path) -> u64 {
    let opened = copies
        .open_copy(SHARD, dir, LABEL, config())
        .await
        .expect("open");
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(opened.marks.clone(), LogMarkMode::Exact)
        .expect("marks");
    append(&mut log, &[normal(1, 0, "a"), normal(1, 1, "b")])
        .await
        .expect("append")
        .await
        .expect("flush");
    opened.log.save_vote(2, "1", true).await.expect("vote");
    opened.marks.vote(vote(2, "1", true)).await.expect("mark");
    drop(log);
    opened.log.close().await.expect("close");
    opened.copy
}

/// A re-form compares the plan from the post-stop reads with the one the
/// operator read, so the stop marker must not change what a read sees:
/// the copy reads the same, and three such copies plan the same.
#[tokio::test]
async fn a_stopped_copy_reads_and_plans_as_it_did_before_the_stop() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = voted_copy(&copies, &dir).await;
    let probes = BTreeSet::from([0, 1, 2]);
    let plan = |read: &CopyRead| {
        let reports: BTreeMap<_, _> = ["a", "b", "c"]
            .into_iter()
            .map(|name| {
                // The test root keeps no `raft-copy` in the directory;
                // the report names the copy as a broker would.
                let mut current = read.clone();
                current.copy = Some(copy);
                let report = BrokerReport {
                    leads: false,
                    names: BTreeSet::new(),
                    memberships: Some(Vec::new()),
                    copies: BrokerCopies {
                        current,
                        kept: vec![],
                    },
                };
                (name, report)
            })
            .collect();
        let record = GroupRecord {
            incarnation: 1,
            bases: Vec::new(),
            floor: None,
            offset_floor: None,
            placed: Some(BTreeSet::from(["a", "b", "c"])),
            dead: BTreeSet::new(),
            seed: Some(BTreeSet::from([("a", copy), ("b", copy), ("c", copy)])),
            ordered: Some(Vec::new()),
            restore: None,
        };
        plan_reform(&record, &reports)
    };
    let before = read_copy(&dir, 4096, &probes).expect("read");
    let planned = plan(&before).expect("a plan");

    stop(&dir, Some(1)).expect("stop");
    assert!(stopped(&dir).expect("marker").is_some());
    let after = read_copy(&dir, 4096, &probes).expect("read");
    assert_eq!(after, before);
    assert_eq!(plan(&after).expect("a plan"), planned);
}

#[tokio::test]
async fn a_stopped_copy_never_opens_under_its_own_id() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = Some(stopped_copy(&copies, &dir).await);
    assert_eq!(
        stopped(&dir).expect("read"),
        Some(StoppedCopy {
            epoch: 0,
            copy,
            last: Some(1)
        })
    );

    for open in [copies.open_copy(SHARD, &dir, LABEL, config()).await, {
        // Nor after a restart: the marker is on disk, not in memory.
        root.open().open_copy(SHARD, &dir, LABEL, config()).await
    }] {
        let err = open.err().expect("a stopped copy opened");
        assert_eq!(
            StoppedCopy::of(&err),
            Some(StoppedCopy {
                epoch: 0,
                copy,
                last: Some(1)
            })
        );
    }
}

#[tokio::test]
async fn a_rejoin_rotates_the_stopped_copy_and_keeps_its_records() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let old = stopped_copy(&copies, &dir).await;

    let rejoined = copies
        .rejoin(SHARD, &dir, LABEL, config())
        .await
        .expect("rejoin");
    let rotation = rejoined.rotation.clone().expect("rotated");
    assert_eq!(rotation.reason, RotateReason::Stopped);
    assert_eq!(rotation.replaced, Some(old));
    assert_ne!(rejoined.copy, old);
    // Never voted, and nothing in the log to vote with.
    assert_eq!(root.marks_on_disk(), Some(ShardMarks::fresh(rejoined.copy)));
    assert_eq!(
        crate::disk_log::saved_vote_generation(&dir).expect("vote"),
        0
    );
    // The old records stay evidence for a re-form, as any rotation keeps
    // them, and the vote they hold is not trusted.
    let kept = rotation.kept.expect("kept");
    assert_eq!(
        kept_copies(&dir).expect("kept"),
        vec![KeptCopy {
            path: kept.clone(),
            copy: Some(old)
        }]
    );
    assert_eq!(rotated_by::load(&kept).expect("by"), RotatedBy::Restore);
    assert_eq!(
        crate::disk_log::saved_vote_generation(&kept).expect("vote"),
        2
    );
    assert_eq!(stopped(&dir).expect("read"), None);
    rejoined.log.close().await.expect("close");

    // The new copy opens as itself from here on.
    let again = copies
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("reopen");
    assert_eq!((again.copy, again.rotation.clone()), (rejoined.copy, None));
    again.log.close().await.expect("close");
}

#[tokio::test]
async fn only_a_stopped_copy_rejoins() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let opened = copies
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    opened.log.close().await.expect("close");
    let err = copies
        .rejoin(SHARD, &dir, LABEL, config())
        .await
        .err()
        .expect("a running copy rotated");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

/// A marker for an epoch the copy has left is spent.
#[test]
fn a_marker_from_an_earlier_epoch_does_not_stop_the_copy() {
    let root = Root::new();
    let dir = root.shard_dir();
    std::fs::create_dir_all(&dir).expect("dir");
    stop(&dir, None).expect("stop");
    epoch_file::store(
        &dir,
        &EpochFile {
            incarnation: 0,
            bases: vec![EpochBase {
                base_last: None,
                first_index: 0,
            }],
        },
    )
    .expect("epoch");
    assert_eq!(stopped(&dir).expect("read"), None);
    stop(&dir, Some(7)).expect("stop again");
    assert_eq!(
        stopped(&dir).expect("read"),
        Some(StoppedCopy {
            epoch: 1,
            copy: None,
            last: Some(7),
        })
    );
    std::fs::write(dir.join("raft-stopped"), b"stopped 1\n").expect("garble");
    assert!(stopped(&dir).is_err());
}

/// The new epoch's bases: the base ends at `last` in term 1, the epoch
/// starts at 10.
fn bases(last: u64) -> Vec<EpochBase> {
    vec![EpochBase {
        base_last: Some(EpochLogId {
            index: last,
            epoch: 0,
            term: 1,
        }),
        first_index: 10,
    }]
}

fn learner() -> WireStart {
    WireStart {
        voter: None,
        kept: None,
        floor: None,
        needs_accept_loss: false,
        accepted: false,
    }
}

/// A fold whose snapshot is `b"folded"` at `gap_end` moved by `.0`.
struct Given(u64);

/// The fold the tests start with.
const FOLD: Given = Given(0);

impl crate::raft_copy::GapFold for Given {
    fn fold<'a>(
        &'a self,
        _dir: &'a Path,
        _log: &'a crate::DiskLog,
        _base: Option<crate::raft_log::LogPoint>,
        gap_end: crate::raft_log::LogPoint,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::io::Result<crate::raft_log::StoredSnapshot>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            Ok(crate::raft_log::StoredSnapshot {
                last: Some(crate::raft_log::SnapshotId {
                    term: gap_end.term,
                    index: gap_end.index + self.0,
                }),
                meta: bytes::Bytes::new(),
                data: bytes::Bytes::from_static(b"folded"),
            })
        })
    }
}

async fn start_stopped(
    copies: &CopyRoot,
    dir: &Path,
    bases: Vec<EpochBase>,
    wire: &WireStart,
) -> std::io::Result<()> {
    copies
        .start_stopped(SHARD, dir, 1, bases, wire, &FOLD, "b#1", &config())
        .await
}

/// An emptied learner's log is below its old mark. Without the mark
/// lowered before the start, the open rotates it to a new copy id.
#[tokio::test]
async fn a_started_learner_opens_under_its_own_copy_id_in_the_new_epoch() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = stopped_copy(&copies, &dir).await;

    start_stopped(&copies, &dir, bases(0), &learner())
        .await
        .expect("start");
    let opened = root
        .open()
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    assert_eq!(opened.rotation.map(|rotation| rotation.reason), None);
    assert_eq!(opened.copy, copy);
    assert_eq!(group_epoch(&dir).expect("epoch"), Some(1));
    assert_eq!(
        crate::raft_log::last_point(&opened.log)
            .await
            .expect("last"),
        None
    );
}

/// The stopped member's batch still marks its log's last flushed position
/// once after the close. Landing over the start's lowering, it put the
/// mark above the emptied learner and the open rotated it as a power cut.
#[tokio::test]
async fn a_closed_logs_batch_does_not_raise_a_started_learners_mark() {
    use crate::raft_log::LogPositions;

    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = stopped_copy(&copies, &dir).await;
    let (positions, receiver) = tokio::sync::watch::channel(LogPositions::default());
    positions.send_modify(|at| at.flushed = Some(point(1, 1)));
    copies
        .mark_log()
        .for_copy(Arc::from(SHARD), copy)
        .batch_from(receiver, std::time::Duration::from_secs(3600));

    start_stopped(&copies, &dir, bases(0), &learner())
        .await
        .expect("start");
    drop(positions);
    copies.mark_log().mark_batched().await.expect("batch");
    assert_eq!(root.marks_on_disk().expect("marks").log, None);
    let opened = root
        .open()
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    assert_eq!(opened.rotation.map(|rotation| rotation.reason), None);
    assert_eq!(opened.copy, copy);
}

#[tokio::test]
async fn a_started_voter_keeps_its_copy_id_and_its_base() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = stopped_copy(&copies, &dir).await;
    let voter = WireStart {
        voter: Some(copy),
        ..learner()
    };

    start_stopped(&copies, &dir, bases(0), &voter)
        .await
        .expect("start");
    let opened = root
        .open()
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    assert_eq!(opened.rotation.map(|rotation| rotation.reason), None);
    assert_eq!(opened.copy, copy);
    assert_eq!(group_epoch(&dir).expect("epoch"), Some(1));
    let stored = crate::raft_log::snapshot::read(&dir)
        .expect("read")
        .expect("a snapshot");
    assert_eq!(stored.data.as_ref(), b"folded");
    assert_eq!(stored.last.map(|last| last.index), Some(9));
}

/// A fold that answers a snapshot anywhere but the gap's end is refused,
/// and the copy stays mid-start, so it never opens.
#[tokio::test]
async fn a_voter_refuses_a_snapshot_folded_off_the_gaps_end() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = stopped_copy(&copies, &dir).await;
    let voter = WireStart {
        voter: Some(copy),
        ..learner()
    };
    copies
        .start_stopped(
            SHARD,
            &dir,
            1,
            bases(0),
            &voter,
            &Given(1),
            "b#1",
            &config(),
        )
        .await
        .expect_err("stored a misplaced snapshot");
    assert!(building(&dir).expect("marker"));
}

/// Only a copy stopped in the epoch just before the new one starts, so a
/// survey cannot start a served copy or skip an epoch.
#[tokio::test]
async fn only_a_copy_stopped_in_the_epoch_before_starts() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    voted_copy(&copies, &dir).await;
    let err = start_stopped(&copies, &dir, bases(0), &learner())
        .await
        .expect_err("a served copy started");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    stop(&dir, Some(1)).expect("stop");
    let mut two = bases(0);
    two.extend(bases(0));
    let err = start_stopped(&copies, &dir, two, &learner())
        .await
        .expect_err("an epoch skipped");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(group_epoch(&dir).expect("epoch"), None);
}

#[tokio::test]
async fn a_start_refuses_another_copy_a_stray_kept_dir_and_an_unaccepted_loss() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = stopped_copy(&copies, &dir).await;
    let refused = [
        WireStart {
            voter: Some(copy ^ 1),
            ..learner()
        },
        WireStart {
            voter: Some(copy),
            kept: Some(root.path().join("elsewhere").display().to_string()),
            ..learner()
        },
        WireStart {
            needs_accept_loss: true,
            ..learner()
        },
    ];
    for wire in &refused {
        start_stopped(&copies, &dir, bases(0), wire)
            .await
            .expect_err("started");
        assert!(stopped(&dir).expect("marker").is_some(), "{wire:?}");
        assert!(!building(&dir).expect("marker"), "{wire:?}");
    }
    start_stopped(
        &copies,
        &dir,
        bases(0),
        &WireStart {
            needs_accept_loss: true,
            accepted: true,
            ..learner()
        },
    )
    .await
    .expect("an accepted loss starts");
}

/// The `start` field is additive: a learner's part with no loss carries
/// only the two flags.
#[test]
fn a_learners_start_is_two_flags_on_the_wire() {
    assert_eq!(
        serde_json::to_string(&learner()).expect("json"),
        r#"{"needs_accept_loss":false,"accepted":false}"#
    );
}

/// Every file directly in `dir`, by name, with its bytes.
fn files_in(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .expect("list")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.is_file())
        .map(|path| {
            let name = path
                .file_name()
                .expect("name")
                .to_string_lossy()
                .into_owned();
            (name, std::fs::read(&path).expect("read"))
        })
        .collect()
}

/// **A start is refused on a directory another open holds**, a member's
/// log or another start, before it writes anything. Two brokers on one data
/// directory must never both change a copy: a learner's start empties the
/// log the other one is serving.
#[tokio::test]
async fn a_start_is_refused_before_it_writes_while_another_open_holds_the_copy() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    stopped_copy(&copies, &dir).await;

    let member = crate::DiskLog::open(&dir, LABEL, config()).expect("open");
    let before = files_in(&dir);
    let err = start_stopped(&copies, &dir, bases(0), &learner())
        .await
        .expect_err("started under an open log");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy, "{err}");
    assert_eq!(files_in(&dir), before, "a refused start wrote");
    member.close().await.expect("close");

    let starting = crate::disk_log::dir_lock::DirLock::take(&dir).expect("lock");
    let before = files_in(&dir);
    let err = start_stopped(&copies, &dir, bases(0), &learner())
        .await
        .expect_err("started under another start");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy, "{err}");
    assert_eq!(files_in(&dir), before, "a refused start wrote");
    drop(starting);

    start_stopped(&copies, &dir, bases(0), &learner())
        .await
        .expect("start once nothing holds the copy");
}

/// **A stop, and a fresh copy's epoch, are refused while a log holds the
/// copy**, so neither marks a directory another broker serves.
#[tokio::test]
async fn a_stop_and_a_fresh_epoch_are_refused_while_a_log_holds_the_copy() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    voted_copy(&copies, &dir).await;

    let member = crate::DiskLog::open(&dir, LABEL, config()).expect("open");
    let before = files_in(&dir);
    let err = stop(&dir, Some(1)).expect_err("stopped under an open log");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy, "{err}");
    let err = write_fresh_epoch(&dir, 1, &bases(0), config().index_spacing_bytes)
        .expect_err("a fresh epoch under an open log");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy, "{err}");
    assert_eq!(files_in(&dir), before, "a refused write landed");
    member.close().await.expect("close");
    stop(&dir, Some(1)).expect("stop once closed");
}

/// #3005: the start forgets the stopped log's batch and lowers its mark,
/// but a batched append after it is marked as on any copy, so losing that
/// append still reads as a log below its mark and rotates.
#[tokio::test]
async fn a_started_copy_that_loses_a_marked_append_rotates() {
    let root = Root::new();
    let copies = root.open();
    let dir = root.shard_dir();
    let copy = stopped_copy(&copies, &dir).await;
    let voter = WireStart {
        voter: Some(copy),
        ..learner()
    };
    start_stopped(&copies, &dir, bases(0), &voter)
        .await
        .expect("start");
    let backup = tempfile::tempdir().expect("backup");
    copy_tree(&dir, backup.path());

    let opened = copies
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    assert_eq!((opened.copy, opened.rotation.clone()), (copy, None));
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(
        opened.marks.clone(),
        LogMarkMode::Batched(std::time::Duration::from_secs(3600)),
    )
    .expect("marks");
    log.mark_recovered().await.expect("recovered");
    append(&mut log, &[normal(2, 10, "c")])
        .await
        .expect("append")
        .await
        .expect("flush");
    copies.mark_log().mark_batched().await.expect("batch");
    assert_eq!(root.marks_on_disk().expect("marks").log, Some(point(2, 10)));
    drop(log);
    opened.log.close().await.expect("close");

    restore(backup.path(), &dir);
    let restored = copies
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    assert_eq!(
        restored.rotation.clone().map(|rotation| rotation.reason),
        Some(RotateReason::LogBelowMark)
    );
    assert_ne!(restored.copy, copy);
    restored.log.close().await.expect("close");
}

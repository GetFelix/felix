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

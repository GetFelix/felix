//! A group of one re-forming itself after a relaxed crash rotated its copy.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::*;
use crate::disk_log::crash_point;
use crate::raft_copy::{Alone, Aloned, EpochRecord, LocalRecord, WireReport};
use crate::raft_log::RaftLog;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, normal};
use crate::run_state::{self, BootId};

type Log = RaftLog<Types, Codec>;

const LABEL: &str = "t/ns/orders/0";
const BROKER: &str = "standalone";

struct NoMemberships;

impl MembershipReader<Arc<str>> for NoMemberships {
    fn voter_sets(&self, _payload: &[u8]) -> io::Result<VoterSets<Arc<str>>> {
        Ok(Vec::new())
    }

    fn snapshot_voter_sets(&self, _meta: &[u8]) -> io::Result<Option<(u64, VoterSets<Arc<str>>)>> {
        Ok(None)
    }
}

/// A fold that stores `b"folded"` at the gap's end.
struct Fold;

impl GapFold for Fold {
    fn fold<'a>(
        &'a self,
        _dir: &'a Path,
        _config: &'a LogConfig,
        _base: Option<LogPoint>,
        gap_end: LogPoint,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = io::Result<crate::raft_log::StoredSnapshot>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            Ok(crate::raft_log::StoredSnapshot {
                last: Some(crate::raft_log::SnapshotId {
                    term: gap_end.term,
                    index: gap_end.index,
                }),
                meta: bytes::Bytes::new(),
                data: bytes::Bytes::from_static(b"folded"),
            })
        })
    }
}

fn boot(n: u8) -> Option<BootId> {
    Some(BootId::from_bytes([n; 16]))
}

/// What a relaxed crash does to the root: the run record says it was
/// running in another boot, so `begin` rotates the id and records why.
fn relaxed_crash(root: &Root, n: u8) {
    let mut next = [10 * n; 16];
    next[0] = n;
    run_state::begin(root.path(), true, None, boot(n), || false, || next).expect("begin");
}

/// A copy holding `count` records, all committed, closed.
async fn written_copy(copies: &CopyRoot, dir: &Path, count: u64) -> u64 {
    let opened = copies
        .open_copy(SHARD, dir, LABEL, config())
        .await
        .expect("open");
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(opened.marks.clone(), LogMarkMode::Exact)
        .expect("marks");
    let entries: Vec<_> = (0..count).map(|index| normal(1, index, "x")).collect();
    append(&mut log, &entries)
        .await
        .expect("append")
        .await
        .expect("flush");
    opened.log.save_vote(2, "1", true).await.expect("vote");
    opened.marks.vote(vote(2, "1", true)).await.expect("mark");
    opened
        .log
        .save_committed(
            count - 1,
            crate::raft_log::generation_of(1).expect("generation"),
        )
        .await
        .expect("commit");
    drop(log);
    opened.log.close().await.expect("close");
    opened.copy
}

async fn reform(copies: &CopyRoot, dir: &Path) -> io::Result<Aloned> {
    let spacing = config().index_spacing_bytes;
    let dir_owned = dir.to_path_buf();
    let read = move |probes: &BTreeSet<u64>, epoch: u64| {
        build_report(&dir_owned, spacing, probes, epoch, false, &NoMemberships)
            .map(|report| WireReport::from(&report))
    };
    let fold = |_copy: u64| Box::new(Fold) as Box<dyn GapFold>;
    let member = |copy: u64| format!("{BROKER}#{copy}");
    copies
        .reform_alone(Alone {
            shard: SHARD,
            dir,
            label: LABEL,
            incarnation: 1,
            broker: BROKER,
            config: &config(),
            read: &read,
            fold: &fold,
            member: &member,
            record: &LocalRecord,
        })
        .await
}

/// A root whose storage id a relaxed crash rotated after the shard wrote
/// `count` records, and a gate that passes on it.
async fn crashed(count: u64) -> (Root, PathBuf) {
    let root = Root::new();
    run_state::begin(root.path(), true, None, boot(1), || false, || [9; 16]).expect("begin");
    let dir = root.shard_dir();
    written_copy(&root.open(), &dir, count).await;
    relaxed_crash(&root, 2);
    root.witness_current_id();
    (root, dir)
}

#[tokio::test]
async fn a_relaxed_crash_re_forms_a_group_of_one_past_every_offset_it_gave_out() {
    let (root, dir) = crashed(3).await;
    let copies = root.open();
    let done = reform(&copies, &dir).await.expect("re-form");
    assert_eq!(done, Aloned::Reformed { epoch: 1 });
    let top = highest_reserved(&dir)
        .expect("reserved")
        .expect("a reservation");
    let bases = crate::raft_copy::epoch_file::load(&dir)
        .expect("epoch")
        .expect("an epoch file")
        .bases;
    assert_eq!(bases.len(), 1);
    assert!(
        bases[0].first_index > top.min(2),
        "the epoch starts past what was handed out"
    );
    assert!(bases[0].first_index >= 3);
    assert_eq!(bases[0].base_end(), 3, "every committed record survives");
    // Running again finds the work done, and the copy opens as its own voter.
    assert_eq!(reform(&copies, &dir).await.expect("again"), Aloned::NotOwed);
    let opened = copies
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    assert!(opened.rotation.is_none());
    opened.log.close().await.expect("close");
}

#[tokio::test]
async fn without_a_relaxed_crash_nothing_is_re_formed() {
    let root = Root::new();
    run_state::begin(root.path(), true, None, boot(1), || false, || [9; 16]).expect("begin");
    let dir = root.shard_dir();
    written_copy(&root.open(), &dir, 3).await;
    // A clean restart keeps the id, and nothing was rotated.
    let copies = root.open();
    assert_eq!(reform(&copies, &dir).await.expect("run"), Aloned::NotOwed);
    assert_eq!(group_epoch(&dir).expect("epoch"), None);
}

#[tokio::test]
async fn an_operator_rotation_after_the_crash_voids_the_evidence() {
    let (root, dir) = crashed(3).await;
    crate::run_state::copy_trust::rotate_storage_id(root.path(), [77; 16]).expect("rotate");
    root.witness_current_id();
    let copies = root.open();
    assert_eq!(reform(&copies, &dir).await.expect("run"), Aloned::NotOwed);
    assert_eq!(group_epoch(&dir).expect("epoch"), None);
}

/// A crash at every step of the re-form, then a run again: the group
/// serves in one new epoch and no index below it is given out again.
#[tokio::test]
async fn a_crash_at_every_step_resumes_without_reusing_an_offset() {
    let mut stop = 0;
    loop {
        let (root, dir) = crashed(3).await;
        let parent = dir.parent().expect("parent").to_path_buf();
        let copies = root.open();
        crash_point::watch_under(&parent, Some(stop));
        let first = reform(&copies, &dir).await;
        let steps = crash_point::unwatch(&parent);
        let reached = first.is_err();
        let copies = root.open();
        let done = reform(&copies, &dir).await;
        let epoch = group_epoch(&dir).expect("epoch");
        assert!(
            matches!(
                done,
                Ok(Aloned::Reformed { epoch: 1 }) | Ok(Aloned::NotOwed)
            ),
            "stop {stop}: {done:?}"
        );
        assert_eq!(epoch, Some(1), "stop {stop}");
        let file = crate::raft_copy::epoch_file::load(&dir)
            .expect("epoch")
            .expect("file");
        assert_eq!(file.bases.len(), 1, "stop {stop}: one epoch, not two");
        assert!(file.bases[0].first_index >= 3, "stop {stop}");
        assert_eq!(
            file.bases[0].base_end(),
            3,
            "stop {stop}: the committed records survive"
        );
        if !reached {
            assert!(steps <= stop, "{steps} steps, stopped at {stop}");
            break;
        }
        stop += 1;
    }
    assert!(stop > 3, "the re-form has several steps to crash at");
}

/// A second relaxed crash while the start is half built: the half-built
/// copy is set aside, and the group plans again above the first plan.
#[tokio::test]
async fn a_second_crash_mid_start_plans_again() {
    let (root, dir) = crashed(3).await;
    let parent = dir.parent().expect("parent").to_path_buf();
    // Stop after the plan is recorded and the copy stopped.
    let mut stop = 0;
    loop {
        let copies = root.open();
        crash_point::watch_under(&parent, Some(stop));
        let first = reform(&copies, &dir).await;
        crash_point::unwatch(&parent);
        if first.is_err() && crate::raft_copy::building(&dir).expect("building") {
            break;
        }
        assert!(first.is_err() || stop > 40, "never built");
        stop += 1;
        if first.is_ok() {
            panic!("the start finished before a crash could leave it half built");
        }
    }
    relaxed_crash(&root, 3);
    root.witness_current_id();
    let copies = root.open();
    let done = reform(&copies, &dir).await.expect("re-form again");
    assert!(matches!(done, Aloned::Reformed { .. }));
    let file = crate::raft_copy::epoch_file::load(&dir)
        .expect("epoch")
        .expect("file");
    assert_eq!(file.bases.len(), 1);
    assert_eq!(file.bases[0].base_end(), 3);
    assert!(!crate::raft_copy::building(&dir).expect("building"));
}

#[test]
fn a_plan_is_kept_across_a_restart() {
    let dir = tempfile::tempdir().expect("dir");
    assert_eq!(LocalRecord.load(dir.path()).expect("load"), None);
    let plan = crate::raft_copy::AlonePlan {
        copy: 7,
        minted: "id".to_owned(),
        bases: vec![WireBase {
            base_last: Some((2, 0, 1)),
            first_index: 9,
        }],
        start: WireStart {
            voter: Some(7),
            kept: None,
            floor: None,
            needs_accept_loss: true,
            accepted: true,
        },
        last: Some(2),
    };
    LocalRecord.store(dir.path(), &plan).expect("store");
    assert_eq!(LocalRecord.load(dir.path()).expect("load"), Some(plan));
}

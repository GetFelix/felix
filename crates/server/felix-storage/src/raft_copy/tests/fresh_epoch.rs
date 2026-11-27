//! A copy placed fresh past epoch 0 takes its `raft-epoch` from the record,
//! and only while it holds nothing an earlier epoch could have written.

use super::*;
use crate::disk_log::layout::KEPT_DIR;
use crate::raft_log::RaftLog;
use crate::raft_log::reserved;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, normal};

const LABEL: &str = "t/ns/orders/0";
const INCARNATION: u64 = 7;

fn bases(epochs: u64) -> Vec<EpochBase> {
    (1..=epochs)
        .map(|epoch| EpochBase {
            base_last: Some(EpochLogId {
                index: epoch * 10,
                epoch: epoch - 1,
                term: 1,
            }),
            first_index: epoch * 10 + 5,
        })
        .collect()
}

fn write(dir: &Path, epochs: u64) -> FreshEpoch {
    write_fresh_epoch(
        dir,
        INCARNATION,
        &bases(epochs),
        config().index_spacing_bytes,
    )
    .expect("write")
}

/// A copy opened once and closed with nothing written: its copy id and
/// whatever an empty log leaves.
async fn opened_once(root: &Root) -> PathBuf {
    let dir = root.shard_dir();
    let opened = root
        .open()
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    opened.log.close().await.expect("close");
    dir
}

#[tokio::test]
async fn a_copy_holding_nothing_takes_the_records_epoch() {
    let root = Root::new();
    let dir = root.shard_dir();
    assert_eq!(write(&dir, 1), FreshEpoch::Written);
    assert_eq!(group_epoch(&dir).expect("read"), Some(1));

    // A copy that opened once and wrote nothing is just as fresh.
    let root = Root::new();
    let dir = opened_once(&root).await;
    let names: Vec<_> = crate::fs::read_dir(&dir)
        .expect("dir")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(write(&dir, 1), FreshEpoch::Written, "{names:?}");
    assert_eq!(write(&dir, 1), FreshEpoch::Named);
}

/// **A directory that holds anything an earlier epoch could have written
/// keeps an unknown epoch.** A restore older than the epoch, with its log
/// or vote, would otherwise vote in an epoch its log was never part of.
#[tokio::test]
async fn a_copy_holding_a_vote_or_entries_is_refused() {
    let root = Root::new();
    let dir = root.shard_dir();
    let opened = root
        .open()
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    opened.log.save_vote(2, "1", true).await.expect("vote");
    opened.log.close().await.expect("close");
    assert!(matches!(write(&dir, 1), FreshEpoch::Refused(_)));
    assert_eq!(group_epoch(&dir).expect("read"), None);

    let root = Root::new();
    let dir = root.shard_dir();
    let opened = root
        .open()
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    let mut log = RaftLog::<Types, Codec>::new(opened.log.clone(), Codec).expect("a Raft log");
    append(&mut log, &[normal(1, 0, "a")])
        .await
        .expect("append")
        .await
        .expect("flush");
    drop(log);
    opened.log.close().await.expect("close");
    assert!(matches!(write(&dir, 1), FreshEpoch::Refused(_)));
    assert_eq!(group_epoch(&dir).expect("read"), None);
}

/// **A stop marker is never spent by a fresh epoch.** A copy without
/// `raft-epoch` is stopped at epoch 0; naming epoch 1 over it would let it
/// rejoin under its old copy id.
#[tokio::test]
async fn a_stopped_copy_is_refused() {
    let root = Root::new();
    let dir = opened_once(&root).await;
    stop(&dir, None).expect("stop");
    assert_eq!(write(&dir, 1), FreshEpoch::Refused("a re-form stopped it"));
    assert_eq!(group_epoch(&dir).expect("read"), None);
    assert!(stopped(&dir).expect("read").is_some());
}

/// A marker a start spent (the copy names a later epoch) refuses nothing:
/// the started copy's `raft-epoch` is its own and is left alone.
#[tokio::test]
async fn a_spent_stop_marker_is_not_a_stop() {
    let root = Root::new();
    let dir = opened_once(&root).await;
    stop(&dir, None).expect("stop");
    epoch_file::store(
        &dir,
        &EpochFile {
            incarnation: INCARNATION,
            bases: bases(1),
        },
    )
    .expect("epoch");
    assert_eq!(write(&dir, 1), FreshEpoch::Named);
}

#[tokio::test]
async fn a_reservation_or_a_kept_copy_is_refused() {
    let root = Root::new();
    let dir = opened_once(&root).await;
    reserved::store(&dir, reserved::STEP).expect("reserve");
    assert!(matches!(write(&dir, 1), FreshEpoch::Refused(_)));
    assert_eq!(group_epoch(&dir).expect("read"), None);

    let root = Root::new();
    let dir = opened_once(&root).await;
    let kept = dir
        .parent()
        .expect("parent")
        .join(KEPT_DIR)
        .join(format!("0.{:016x}", 9));
    crate::fs::create_dir_all(&kept).expect("kept");
    assert_eq!(
        write(&dir, 1),
        FreshEpoch::Refused("a rotation kept an earlier copy of it")
    );
    assert_eq!(group_epoch(&dir).expect("read"), None);
}

/// A file written from a view an epoch behind is brought up to the record
/// while the copy still holds nothing. A file the copy got any other way
/// names its own epoch.
#[tokio::test]
async fn only_an_earlier_fresh_epoch_is_replaced() {
    let root = Root::new();
    let dir = opened_once(&root).await;
    assert_eq!(write(&dir, 1), FreshEpoch::Written);
    assert_eq!(write(&dir, 2), FreshEpoch::Written);
    assert_eq!(group_epoch(&dir).expect("read"), Some(2));
    assert_eq!(write(&dir, 1), FreshEpoch::Named);
    assert_eq!(group_epoch(&dir).expect("read"), Some(2));

    let other = write_fresh_epoch(
        &dir,
        INCARNATION + 1,
        &bases(3),
        config().index_spacing_bytes,
    )
    .expect("write");
    assert_eq!(other, FreshEpoch::Named);
    assert_eq!(group_epoch(&dir).expect("read"), Some(2));
}

const ABANDONED_AT: u64 = 1000;

/// An abandoned epoch's bases: one empty base past epoch 0.
fn abandoned() -> Vec<EpochBase> {
    vec![EpochBase {
        base_last: None,
        first_index: ABANDONED_AT,
    }]
}

/// A fold that stores an empty snapshot at the gap's end.
struct Empty;

impl GapFold for Empty {
    fn fold<'a>(
        &'a self,
        _dir: &'a Path,
        _log: &'a crate::DiskLog,
        base: Option<crate::raft_log::LogPoint>,
        gap_end: crate::raft_log::LogPoint,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::io::Result<crate::raft_log::StoredSnapshot>>
                + Send
                + 'a,
        >,
    > {
        assert_eq!(base, None, "an abandoned epoch folds over no base");
        Box::pin(async move {
            Ok(crate::raft_log::StoredSnapshot {
                last: Some(crate::raft_log::SnapshotId {
                    term: gap_end.term,
                    index: gap_end.index,
                }),
                meta: bytes::Bytes::new(),
                data: bytes::Bytes::new(),
            })
        })
    }
}

/// A copy opened once with the abandoned epoch written fresh, and its id.
async fn blank_abandoned(root: &Root) -> (PathBuf, u64) {
    let dir = opened_once(root).await;
    let fresh = write_fresh_epoch(
        &dir,
        INCARNATION,
        &abandoned(),
        config().index_spacing_bytes,
    )
    .expect("write");
    assert_eq!(fresh, FreshEpoch::Written);
    let copy = copy_file::load(&dir)
        .expect("read")
        .expect("a copy id")
        .copy;
    (dir, copy)
}

async fn start_abandoned(root: &Root, dir: &Path, copy: u64) -> std::io::Result<bool> {
    root.open()
        .start_abandoned(
            SHARD,
            dir,
            INCARNATION,
            abandoned(),
            copy,
            &Empty,
            "b1",
            &config(),
        )
        .await
}

/// **A blank copy of an abandoned epoch starts at the epoch's first
/// index.** Reserved, purged and committed through the index before it, so
/// its first entry never reuses an offset the lost group gave out.
#[tokio::test]
async fn an_abandoned_copy_starts_at_the_first_index() {
    let root = Root::new();
    let (dir, copy) = blank_abandoned(&root).await;
    assert!(
        abandon_start_due(
            &dir,
            INCARNATION,
            &abandoned(),
            copy,
            config().index_spacing_bytes
        )
        .expect("due")
    );
    assert!(start_abandoned(&root, &dir, copy).await.expect("start"));
    assert!(reserved::load(&dir).expect("reserved") >= Some(ABANDONED_AT - 1));
    let held =
        crate::raft_log::held::read(&dir, LABEL, config().index_spacing_bytes).expect("held");
    assert_eq!(held.purged, Some(ABANDONED_AT - 1));
    assert_eq!(held.last_record, None);
    assert_eq!(
        crate::disk_log::saved_commit(&dir).expect("commit").0,
        ABANDONED_AT
    );
    assert!(started_in(&dir, 1).expect("started"));
    // Started once: a second call, as after a crash past the start, leaves it.
    assert!(!start_abandoned(&root, &dir, copy).await.expect("again"));
}

/// **Only a blank copy, under its own id and the record's exact epoch,
/// starts.** A copy that voted or took an entry, a stopped one, another
/// copy id, or a base that is not empty is left alone.
#[tokio::test]
async fn an_abandon_start_refuses_a_copy_that_is_not_blank() {
    let root = Root::new();
    let (dir, copy) = blank_abandoned(&root).await;
    assert!(
        !start_abandoned(&root, &dir, copy + 1)
            .await
            .expect("other id")
    );
    let reformed = bases(1);
    assert!(
        !abandon_start_due(
            &dir,
            INCARNATION,
            &reformed,
            copy,
            config().index_spacing_bytes
        )
        .expect("due")
    );
    assert!(
        !abandon_start_due(
            &dir,
            INCARNATION + 1,
            &abandoned(),
            copy,
            config().index_spacing_bytes
        )
        .expect("due")
    );

    let opened = root
        .open()
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    opened.log.save_vote(2, "1", true).await.expect("vote");
    opened.log.close().await.expect("close");
    assert!(!start_abandoned(&root, &dir, copy).await.expect("voted"));
    assert_eq!(reserved::load(&dir).expect("reserved"), None);

    let root = Root::new();
    let (dir, copy) = blank_abandoned(&root).await;
    stop(&dir, None).expect("stop");
    assert!(!start_abandoned(&root, &dir, copy).await.expect("stopped"));
}

/// A power cut at any step of an abandon start leaves the copy unopenable,
/// and the start run again finishes it.
#[tokio::test]
async fn an_abandon_start_cut_at_any_step_finishes_when_run_again() {
    let mut cut_at = 0;
    loop {
        let root = Root::new();
        let (dir, copy) = blank_abandoned(&root).await;
        crate::disk_log::crash_point::watch(&dir, Some(cut_at));
        let cut = start_abandoned(&root, &dir, copy).await;
        crate::disk_log::crash_point::unwatch(&dir);
        if cut.is_ok() {
            break;
        }
        if crate::raft_copy::start::building(&dir).expect("marker") {
            let refused = root.open().open_copy(SHARD, &dir, LABEL, config()).await;
            assert!(refused.is_err(), "cut at {cut_at} opened");
        }
        assert!(
            start_abandoned(&root, &dir, copy).await.expect("run again"),
            "cut at {cut_at} was not resumed"
        );
        assert_eq!(
            crate::disk_log::saved_commit(&dir).expect("commit").0,
            ABANDONED_AT
        );
        assert!(started_in(&dir, 1).expect("started"));
        cut_at += 1;
    }
    assert!(cut_at > 3, "the start has steps to cut");
}

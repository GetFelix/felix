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

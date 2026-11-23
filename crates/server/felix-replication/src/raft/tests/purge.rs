//! Installing a purged stream shard's snapshot (#2602).

use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::disk_log::CarriedFloor;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf};

use super::{log_config, open_log};
use crate::ShardKind;
use crate::raft::group::{PurgeStep, purge_step, stream_installable};
use crate::raft::machine::{Machine, SnapshotData};
use crate::raft::stream_snapshot::StreamSnapshot;
use crate::raft::types::ShardTypes;

fn meta(last: u64) -> SnapshotMetaOf<ShardTypes> {
    SnapshotMetaOf::<ShardTypes> {
        last_log_id: Some(LogIdOf::<ShardTypes>::new(1.into(), last)),
        last_membership: Default::default(),
    }
}

/// A purge snapshot carrying `floor`.
fn snapshot(floor: u64) -> Bytes {
    StreamSnapshot {
        purge: Some(CarriedFloor {
            offset: floor,
            floor,
            headers: Bytes::from_static(b"why"),
        }),
        ..StreamSnapshot::default()
    }
    .encode()
}

/// The install rule: every entry the snapshot skips must be below the floor
/// it carries. At or above it the member would drop records still served.
#[test]
fn a_snapshot_at_or_above_its_own_floor_is_refused() {
    assert!(stream_installable(&meta(3), &snapshot(4)).is_ok());
    assert!(stream_installable(&meta(2), &snapshot(4)).is_ok());
    assert!(stream_installable(&meta(4), &snapshot(4)).is_err());
    assert!(stream_installable(&meta(5), &snapshot(4)).is_err());
}

/// A snapshot no purge wrote carries no floor, and is refused as every
/// stream snapshot was before purges.
#[test]
fn a_snapshot_without_a_floor_is_refused() {
    let plain = StreamSnapshot::default().encode();
    assert!(stream_installable(&meta(0), &plain).is_err());
    assert!(stream_installable(&meta(3), b"not a snapshot").is_err());
}

/// A stream shard's state machine and log over `dir`, reopened as a restart
/// would.
async fn stream_machine(dir: &std::path::Path) -> (Machine, DiskLog) {
    let log = open_log(dir, "m", 0, log_config());
    let snapshots = felix_storage::raft_log::SnapshotStore::open(dir).expect("store");
    let opened = Machine::open(snapshots, ShardKind::Stream, Some(&log), None).await;
    (opened.expect("open").machine, log)
}

fn data(bytes: Bytes) -> SnapshotData {
    SnapshotData {
        bytes,
        restored: None,
    }
}

/// A member installs a snapshot carrying floor 4, then a later one from a
/// leader that only reached floor 3. It keeps floor 4 across a restart,
/// refuses reads below it, and passes floor 4 on in its own snapshot: a
/// member that took 3 from it could be read below 4, and a re-form reading
/// it could give offset 3 out again.
#[tokio::test]
async fn an_older_floor_never_lowers_an_installed_one() {
    let dir = tempfile::tempdir().expect("dir");
    let (mut machine, first) = stream_machine(dir.path()).await;
    machine
        .install_snapshot(&meta(1), data(snapshot(4)))
        .await
        .expect("install floor 4");
    machine
        .install_snapshot(&meta(2), data(snapshot(3)))
        .await
        .expect("install floor 3");
    drop(machine);
    drop(first);

    let (mut machine, log) = stream_machine(dir.path()).await;
    assert_eq!(log.head_trim().map(|trim| trim.floor), Some(4));
    assert_eq!(log.carried_floor().map(|carried| carried.floor), Some(4));
    // Streams never build a snapshot on their own, so what a leader sends
    // is the stored one.
    let current = machine
        .get_current_snapshot()
        .await
        .expect("current")
        .expect("a snapshot");
    let sent = StreamSnapshot::decode(&current.snapshot.bytes).expect("decode");
    assert_eq!(sent.purge.map(|purge| purge.floor), Some(4));
    let built = machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect("build");
    let built = StreamSnapshot::decode(&built.snapshot.bytes).expect("decode");
    assert_eq!(built.purge.map(|purge| purge.floor), Some(4));
}

/// Cut batches as `producers.forgotten` lays them out: producer 7 refused
/// up to sequence 3.
fn cuts() -> felix_storage::disk_log::ForgottenCuts {
    let mut body = Vec::new();
    body.extend_from_slice(&1u32.to_be_bytes());
    body.extend_from_slice(&7u64.to_be_bytes());
    body.extend_from_slice(&3u64.to_be_bytes());
    let mut bytes = 0x464C_5046u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    bytes.extend_from_slice(&body);
    felix_storage::disk_log::ForgottenCuts::decode(&bytes).expect("cut batches")
}

/// A member that installs a purge snapshot refuses a re-send of a batch the
/// snapshot cut, though it never held the batch.
#[tokio::test]
async fn an_installed_purge_snapshot_refuses_its_cut_batches() {
    use felix_storage::disk_log::ProducerSequence;

    let dir = tempfile::tempdir().expect("dir");
    let (mut machine, log) = stream_machine(dir.path()).await;
    assert_eq!(log.producer_sequence(7, 0), ProducerSequence::Unknown);
    let bytes = StreamSnapshot {
        cuts: cuts(),
        purge: Some(CarriedFloor {
            offset: 4,
            floor: 4,
            headers: Bytes::new(),
        }),
        ..StreamSnapshot::default()
    }
    .encode();
    machine
        .install_snapshot(&meta(1), data(bytes))
        .await
        .expect("install");
    assert_eq!(log.producer_sequence(7, 0), ProducerSequence::Expired);
}

/// A purge build that reads the stored snapshot, then loses the entries it
/// folds to an install that lands meanwhile, answers the installed snapshot
/// instead of failing. A failed build stops the member.
#[tokio::test]
async fn a_purge_build_racing_an_install_answers_the_installed_snapshot() {
    use crate::raft::codec::ShardCodec;
    use felix_storage::raft_log::RaftLog;

    let dir = tempfile::tempdir().expect("dir");
    let (mut machine, log) = stream_machine(dir.path()).await;
    let raft = RaftLog::<ShardTypes, ShardCodec>::new(log.clone(), ShardCodec).expect("raft log");
    let purge_at = machine.purging(raft.reader());
    machine
        .install_snapshot(&meta(0), data(snapshot(10)))
        .await
        .expect("install at 0");

    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go, go_rx) = tokio::sync::oneshot::channel();
    *machine.fold_gate.lock() = Some((reached, go_rx));
    *purge_at.lock() = Some(3);
    let mut builder = machine.get_snapshot_builder().await;
    let build = tokio::spawn(async move { builder.build_snapshot().await });
    reached_rx.await.expect("the build reached the fold");
    machine
        .install_snapshot(&meta(5), data(snapshot(10)))
        .await
        .expect("install at 5");
    go.send(()).expect("build waiting");

    let built = build.await.expect("join").expect("build");
    assert_eq!(built.meta.last_log_id.map(|id| id.index), Some(5));
}

/// A pass that stored its snapshot at 5 but stopped before purging the log
/// purges it on the next pass, though the floor has not moved.
#[test]
fn a_pass_stopped_after_its_snapshot_is_finished_by_the_next() {
    assert_eq!(purge_step(Some(5), None, 5), Some(PurgeStep::Purge));
    assert_eq!(purge_step(Some(5), Some(2), 5), Some(PurgeStep::Purge));
    assert_eq!(purge_step(Some(5), Some(5), 5), None);
    assert_eq!(
        purge_step(Some(2), Some(2), 5),
        Some(PurgeStep::SnapshotThenPurge)
    );
    assert_eq!(
        purge_step(None, None, 5),
        Some(PurgeStep::SnapshotThenPurge)
    );
}

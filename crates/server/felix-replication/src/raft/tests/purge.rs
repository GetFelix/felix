//! Installing a purged stream shard's snapshot (#2602).

use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::disk_log::CarriedFloor;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf};

use super::{log_config, open_log};
use crate::ShardKind;
use crate::raft::group::stream_installable;
use crate::raft::machine::{Machine, SnapshotData};
use crate::raft::stream_snapshot::{PurgeFloors, StreamSnapshot};
use crate::raft::types::ShardTypes;

fn meta(last: u64) -> SnapshotMetaOf<ShardTypes> {
    SnapshotMetaOf::<ShardTypes> {
        last_log_id: Some(LogIdOf::<ShardTypes>::new(1.into(), last)),
        last_membership: Default::default(),
    }
}

/// A purge snapshot whose tables are as of floor 2 and which carries `floor`.
fn snapshot(floor: u64) -> Bytes {
    StreamSnapshot {
        purge: Some(PurgeFloors {
            tables: 2,
            carried: CarriedFloor {
                offset: floor,
                floor,
                headers: Bytes::from_static(b"why"),
            },
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
    let opened = Machine::open(snapshots, ShardKind::Stream, Some(&log)).await;
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
    let built = machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect("build");
    let sent = StreamSnapshot::decode(&built.snapshot.bytes).expect("decode");
    assert_eq!(sent.purge.map(|purge| purge.carried.floor), Some(4));
}

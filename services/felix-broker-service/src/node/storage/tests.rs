//! Which retention hold logs open with, decided from configuration alone.
use felix_storage::log::RetentionHold;

use super::*;
use crate::config::MembershipConfig;

fn membership() -> MembershipConfig {
    MembershipConfig {
        node_id: "broker-a".to_string(),
        advertise_addr: "10.0.0.4:7000".to_string(),
        client_advertise_addr: None,
        kafka_advertise_addr: None,
        region: "us-west-2".to_string(),
        zone: None,
        region_bridges: Vec::new(),
        features: Default::default(),
    }
}

/// Nothing advances a standalone broker's commit offsets, so a saved hold
/// would stop retention for good.
#[test]
fn a_standalone_broker_lifts_saved_holds() {
    assert_eq!(
        retention_hold(&BrokerConfig::default()),
        RetentionHold::Lifted
    );
}

/// A clustered broker opens its logs before its replication starts; it must
/// keep their holds through that.
#[test]
fn a_clustered_broker_keeps_saved_holds() {
    let member = BrokerConfig {
        membership: Some(membership()),
        ..BrokerConfig::default()
    };
    assert_eq!(retention_hold(&member), RetentionHold::AsSaved);
    let peer = BrokerConfig {
        peer_transport: Some(felix_replication::peer::PeerTransportConfig::default()),
        ..BrokerConfig::default()
    };
    assert_eq!(retention_hold(&peer), RetentionHold::AsSaved);
}

/// The id outlives a restart only where there is a disk to keep it on.
#[test]
fn the_storage_id_is_kept_on_disk_and_random_without_one() {
    let root = tempfile::tempdir().expect("tempdir");
    let first = storage_id(Some(root.path())).expect("create");
    assert_eq!(storage_id(Some(root.path())).expect("load"), first);
    assert_eq!(first.to_hex().len(), 32);
    assert_ne!(
        storage_id(None).expect("random"),
        storage_id(None).expect("random")
    );
}

/// **A crash under `Periodic` or `None` that the boot cannot vouch for
/// starts under a new storage id (#1284)**, since its unsynced tail may be
/// gone. On Linux a process crash in the same boot keeps it (#1367): the
/// page cache kept the tail. A clean stop keeps the id under every mode, and so does a
/// crash under `OnCommit`, which acknowledged nothing it had not synced.
#[test]
fn only_a_crash_under_a_relaxed_fsync_mode_rotates_the_storage_id() {
    let modes = [
        (FsyncMode::None, true),
        (
            FsyncMode::Periodic {
                interval: Duration::from_millis(250),
            },
            true,
        ),
        (FsyncMode::OnCommit, false),
    ];
    // Only Linux can sync the page cache to the device and has a boot id to
    // compare; elsewhere, macOS included, every relaxed crash rotates.
    let known_boot = cfg!(target_os = "linux") && felix_storage::run_state::this_boot().is_some();
    for (fsync_mode, relaxed) in modes {
        let root = tempfile::tempdir().expect("tempdir");
        let log = felix_storage::log::LogConfig {
            fsync_mode,
            ..Default::default()
        };
        let first = begin_run(root.path(), &log, None).expect("first run").id;
        mark_clean_stop(root.path(), first).expect("clean stop");
        let second = begin_run(root.path(), &log, None)
            .expect("after a clean stop")
            .id;
        assert_eq!(second, first, "{fsync_mode:?}: a clean stop kept the id");
        // No clean stop: the process died, the machine did not.
        let killed = begin_run(root.path(), &log, None).expect("after a process crash");
        assert_eq!(killed.rotated, relaxed && !known_boot, "{fsync_mode:?}");
        // A failed fsync, or a boot that cannot be compared, leaves the
        // boot unknown, as a machine crash does.
        forget_boot(root.path()).expect("forget the boot");
        let third = begin_run(root.path(), &log, None).expect("after a crash");
        assert_eq!(third.rotated, relaxed, "{fsync_mode:?}");
        let third = third.id;
        assert_eq!(third != killed.id, relaxed, "{fsync_mode:?}");
        assert_eq!(
            storage_id(Some(root.path())).expect("load"),
            third,
            "the id announced is the one on disk"
        );
    }
}

/// **A volume restored from a snapshot starts under a new id even under
/// `OnCommit` (#1334)**, once the control plane has registered the node since
/// the snapshot. A clean restart at the incarnation it recorded keeps it.
#[test]
fn a_directory_older_than_the_last_registration_rotates_the_storage_id() {
    let root = tempfile::tempdir().expect("tempdir");
    let log = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        ..Default::default()
    };
    let first = begin_run(root.path(), &log, None).expect("first run").id;
    felix_storage::run_state::record_registration(root.path(), 2).expect("record");
    mark_clean_stop(root.path(), first).expect("clean stop");
    let kept = begin_run(root.path(), &log, Some(2)).expect("same incarnation");
    assert!(!kept.rotated);
    assert_eq!(kept.id, first);
    mark_clean_stop(root.path(), kept.id).expect("clean stop");
    let restored = begin_run(root.path(), &log, Some(5)).expect("control plane ahead");
    assert!(restored.rotated);
    assert_ne!(restored.id, first);
    assert_eq!(storage_id(Some(root.path())).expect("load"), restored.id);
}

fn registered_as(id: Option<StorageId>) -> Registered {
    Registered {
        incarnation: 0,
        storage_id: id.map(|id| id.to_hex()),
    }
}

/// **A broker new to the control plane is not a replaced one (#1331)**,
/// however new its directory: it held no copy under another id, so its
/// copies vote in the sets that add it. Nor is one the control plane knows
/// only from before storage ids.
#[test]
fn a_new_node_does_not_start_replaced() {
    let log = felix_storage::log::LogConfig::default();
    let root = tempfile::tempdir().expect("tempdir");
    let (_, _, replaced) = start_run(root.path(), &log, None).expect("new node");
    assert!(!replaced);
    let root = tempfile::tempdir().expect("tempdir");
    let (_, _, replaced) =
        start_run(root.path(), &log, Some(&registered_as(None))).expect("older registration");
    assert!(!replaced);
}

/// **A node the control plane last registered under another storage id
/// starts replaced**, which makes its copies learners of the shards it may
/// have held (#1331). So does every later start on that directory, after the
/// new id has registered: a copy it has not heard of since is still one it
/// may have held.
#[test]
fn a_node_registered_under_another_id_starts_replaced() {
    let log = felix_storage::log::LogConfig::default();
    let root = tempfile::tempdir().expect("tempdir");
    let other = StorageId::from_bytes([9; 16]);
    let (id, _, replaced) =
        start_run(root.path(), &log, Some(&registered_as(Some(other)))).expect("wiped");
    assert!(replaced);
    assert_ne!(id, other);
    mark_clean_stop(root.path(), id).expect("clean stop");
    let (again, _, replaced) =
        start_run(root.path(), &log, Some(&registered_as(Some(id)))).expect("restart");
    assert_eq!(again, id);
    assert!(replaced, "the registration of the new id ends nothing");
}

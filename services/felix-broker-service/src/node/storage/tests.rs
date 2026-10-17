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
        let first = begin_run(root.path(), &log).expect("first run").id;
        mark_clean_stop(root.path(), first).expect("clean stop");
        let second = begin_run(root.path(), &log).expect("after a clean stop").id;
        assert_eq!(second, first, "{fsync_mode:?}: a clean stop kept the id");
        // No clean stop: the process died, the machine did not.
        let killed = begin_run(root.path(), &log).expect("after a process crash");
        assert_eq!(killed.rotated, relaxed && !known_boot, "{fsync_mode:?}");
        // A failed fsync, or a boot that cannot be compared, leaves the
        // boot unknown, as a machine crash does.
        forget_boot(root.path()).expect("forget the boot");
        let third = begin_run(root.path(), &log).expect("after a crash");
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

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
/// crash under `OnCommit`, which acknowledged nothing it had not synced,
/// unless a failed fsync forgot the boot first.
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
        // boot unknown, as a machine crash does. That includes `OnCommit`:
        // a poisoned log may hold bytes its failed flush never wrote.
        forget_boot(root.path()).expect("forget the boot");
        let third = begin_run(root.path(), &log, None).expect("after a crash");
        assert!(third.rotated, "{fsync_mode:?}");
        let third = third.id;
        assert_ne!(third, killed.id, "{fsync_mode:?}");
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
    let (_, _, replaced, _) = start_run(root.path(), &log, None, None).expect("new node");
    assert!(!replaced);
    let root = tempfile::tempdir().expect("tempdir");
    let (_, _, replaced, _) =
        start_run(root.path(), &log, Some(&registered_as(None)), None).expect("older registration");
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
    let (id, _, replaced, _) =
        start_run(root.path(), &log, Some(&registered_as(Some(other))), None).expect("wiped");
    assert!(replaced);
    assert_ne!(id, other);
    mark_clean_stop(root.path(), id).expect("clean stop");
    let (again, _, replaced, _) =
        start_run(root.path(), &log, Some(&registered_as(Some(id))), None).expect("restart");
    assert_eq!(again, id);
    assert!(replaced, "the registration of the new id ends nothing");
}

/// **Only a crash on a directory that never lost a copy keeps every
/// promise.** Ballots are always synced, so a relaxed crash rotates the id
/// with every ballot kept, however many times in a row. A wiped directory may
/// hold ballots taken after the node lost others, a restored one rolled its
/// ballots back, and without the control plane's record of the node nothing
/// shows which it is: none lets a doubted copy answer a continuing leader,
/// at that start or any later one (#2047).
#[test]
fn only_a_crash_on_a_directory_that_lost_nothing_keeps_its_promises() {
    let relaxed = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        ..Default::default()
    };
    let crash = |root: &Path, registered: Option<&Registered>| {
        forget_boot(root).expect("forget the boot");
        let (id, rotated, _, kept) =
            start_run(root, &relaxed, registered, None).expect("after a crash");
        assert!(rotated, "a relaxed crash rotates the id");
        (id, kept.promises)
    };

    let root = tempfile::tempdir().expect("tempdir");
    let (id, rotated, _, kept) = start_run(root.path(), &relaxed, None, None).expect("first run");
    assert!(!rotated && !kept.promises, "a first run kept nothing");
    let (id, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(kept, "a crash keeps every ballot");
    let (_, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(kept, "so does the next one");
    let (_, kept) = crash(root.path(), None);
    assert!(!kept, "without the control plane's record");

    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first run");
    let (_, kept) = crash(root.path(), Some(&registered_as(None)));
    assert!(!kept, "with a record that names no storage id");
    let (_, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(!kept, "with a record of an id this start did not rotate");

    let other = StorageId::from_bytes([9; 16]);
    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, kept) = start_run(
        root.path(),
        &relaxed,
        Some(&registered_as(Some(other))),
        None,
    )
    .expect("wiped");
    assert!(!kept.promises);
    let (_, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(!kept, "a wiped directory's later crash");

    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first run");
    felix_storage::run_state::record_registration(root.path(), 2).expect("record");
    mark_clean_stop(root.path(), id).expect("clean stop");
    let restored = |id: StorageId| Registered {
        incarnation: 5,
        storage_id: Some(id.to_hex()),
    };
    let (id, rotated, _, kept) =
        start_run(root.path(), &relaxed, Some(&restored(id)), None).expect("restored");
    assert!(rotated);
    assert!(!kept.promises, "a restored directory");
    felix_storage::run_state::record_registration(root.path(), 5).expect("record");
    let (_, kept) = crash(root.path(), Some(&restored(id)));
    assert!(!kept, "a restored directory's later crash");

    // An older build wrote the replaced-storage marker after a crash too.
    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first run");
    std::fs::remove_file(
        root.path()
            .join(felix_storage::run_state::LOST_COPY_FILE_NAME),
    )
    .expect("an older build's directory");
    felix_storage::run_state::record_replaced(root.path(), id, 0).expect("marker");
    let (_, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(!kept, "an older build's replaced directory");
}

/// **A crash that keeps every ballot records the id it rotated away**, so a
/// set still naming that id names this directory's own copy. A restore or a
/// wiped directory records nothing: the copy such a set names is lost.
#[test]
fn only_a_crash_that_keeps_every_ballot_records_the_id_it_rotated() {
    let relaxed = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        ..Default::default()
    };
    let rotated_ids = |root: &Path| felix_storage::run_state::rotated_ids(root).expect("read");
    let crash = |root: &Path, registered: Option<&Registered>| {
        forget_boot(root).expect("forget the boot");
        let (id, rotated, _, _) = start_run(root, &relaxed, registered, None).expect("crash");
        assert!(rotated);
        id
    };

    let root = tempfile::tempdir().expect("tempdir");
    let (first, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first run");
    assert!(rotated_ids(root.path()).is_empty());
    let second = crash(root.path(), Some(&registered_as(Some(first))));
    assert_eq!(rotated_ids(root.path()), vec![first]);
    crash(root.path(), Some(&registered_as(Some(second))));
    assert_eq!(rotated_ids(root.path()), vec![second, first]);

    let other = StorageId::from_bytes([9; 16]);
    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, _) = start_run(
        root.path(),
        &relaxed,
        Some(&registered_as(Some(other))),
        None,
    )
    .expect("wiped");
    crash(root.path(), Some(&registered_as(Some(id))));
    assert!(rotated_ids(root.path()).is_empty(), "a wiped directory");

    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first run");
    felix_storage::run_state::record_registration(root.path(), 2).expect("record");
    mark_clean_stop(root.path(), id).expect("clean stop");
    let restored = Registered {
        incarnation: 5,
        storage_id: Some(id.to_hex()),
    };
    let (_, rotated, _, _) =
        start_run(root.path(), &relaxed, Some(&restored), None).expect("restored");
    assert!(rotated);
    assert!(rotated_ids(root.path()).is_empty(), "a restored directory");
}

/// **A fence answer attests kept ballots only from a directory that never
/// lost one.** A clean start and a crash of the directory the control plane
/// names keep every ballot; a wiped directory, a restored one, and a crash
/// with no record that names the old id do not, now or at a later start.
/// A restore that went unnoticed would let a forgotten promise count
/// (`CountsForgotten` in `docs/formal/FelixShardUnified.tla`); a wiped
/// directory counted as kept is `CountNonKept`.
#[test]
fn only_a_directory_that_never_lost_a_ballot_attests_kept_ballots() {
    let relaxed = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        ..Default::default()
    };
    let clean = |root: &Path, id: StorageId, registered: Option<&Registered>| {
        mark_clean_stop(root, id).expect("clean stop");
        let (id, rotated, _, kept) = start_run(root, &relaxed, registered, None).expect("clean");
        assert!(!rotated);
        (id, kept.ballots)
    };
    let crash = |root: &Path, registered: Option<&Registered>| {
        forget_boot(root).expect("forget the boot");
        let (id, rotated, _, kept) = start_run(root, &relaxed, registered, None).expect("crash");
        assert!(rotated);
        (id, kept.ballots)
    };

    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, kept) = start_run(root.path(), &relaxed, None, None).expect("first run");
    assert!(kept.ballots, "a new directory has lost nothing");
    let (id, kept) = clean(root.path(), id, Some(&registered_as(Some(id))));
    assert!(kept, "a clean restart");
    let (_, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(kept, "a crash of the directory the control plane names");
    let (_, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(
        !kept,
        "a crash with a record of an id this start did not rotate"
    );

    let root = tempfile::tempdir().expect("tempdir");
    let (_, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first run");
    let (_, kept) = crash(root.path(), None);
    assert!(!kept, "a crash without the control plane's record");

    // Registered before storage ids, then wiped: nothing tells the new
    // directory from the old one.
    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, kept) = start_run(root.path(), &relaxed, Some(&registered_as(None)), None)
        .expect("a record without a storage id");
    assert!(!kept.ballots, "a record without a storage id");
    let (_, kept) = clean(root.path(), id, Some(&registered_as(None)));
    assert!(!kept, "nor at a later clean start");

    let other = StorageId::from_bytes([9; 16]);
    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, kept) = start_run(
        root.path(),
        &relaxed,
        Some(&registered_as(Some(other))),
        None,
    )
    .expect("wiped");
    assert!(!kept.ballots, "a directory that replaced a lost copy");
    let (id, kept) = clean(root.path(), id, Some(&registered_as(Some(id))));
    assert!(!kept, "nor at its later clean start");
    let (_, kept) = crash(root.path(), Some(&registered_as(Some(id))));
    assert!(!kept, "nor at its later crash");

    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first run");
    felix_storage::run_state::record_registration(root.path(), 2).expect("record");
    let restored = Registered {
        incarnation: 5,
        storage_id: Some(id.to_hex()),
    };
    let (_, kept) = {
        mark_clean_stop(root.path(), id).expect("clean stop");
        let (id, _, _, kept) =
            start_run(root.path(), &relaxed, Some(&restored), None).expect("restored");
        (id, kept.ballots)
    };
    assert!(!kept, "a restored directory");
}

/// **A start that rotated the id and stopped before the new id registered
/// leaves the next start its ballots.** The control plane still names the
/// id the crash rotated away, which is this directory's own copy, not a lost
/// one. Counting it lost made every copy here a non-voter until a set newer
/// than the loss readmitted it, which a shard whose other copy is the new
/// leader cannot open to write.
#[test]
fn a_start_that_rotated_before_registering_keeps_its_ballots_next_time() {
    let log = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        ..Default::default()
    };
    let root = tempfile::tempdir().expect("tempdir");
    let (first, _, _, _) = start_run(root.path(), &log, None, None).expect("first run");
    let registered = registered_as(Some(first));
    // A failed fsync, then a kill: the next start rotates.
    forget_boot(root.path()).expect("forget the boot");
    let (second, rotated, _, kept) =
        start_run(root.path(), &log, Some(&registered), None).expect("after the poison");
    assert!(rotated && kept.ballots);
    // That start exits before it registers, and the next one starts under
    // the rotated id with the old one still registered.
    let (again, rotated, _, kept) =
        start_run(root.path(), &log, Some(&registered), None).expect("started again");
    assert_eq!(again, second);
    assert!(!rotated);
    assert!(kept.ballots, "the registered id is this directory's own");
    let lost = felix_storage::run_state::lost_copy(root.path()).expect("read");
    assert_eq!(lost, Some(false));
}

/// **A start from the view attests no kept ballot and no readmission**,
/// whatever the directory says. Its restore check waits for the control
/// plane, and until then a restored directory reads as one that kept
/// everything, its copy-trust record and the readmissions in it included.
#[test]
fn a_start_from_the_view_attests_no_kept_ballot() {
    let kept = Kept {
        promises: true,
        ballots: true,
        readmits: true,
        minted: true,
        deferred: false,
    };
    assert!(kept.attested(false).ballots && kept.attested(false).readmits);
    assert!(!kept.attested(true).ballots);
    assert!(!kept.attested(true).readmits);
    assert!(!Kept::default().attested(false).ballots);
}

/// **Only a start that minted the storage id says so.** A wiped directory
/// and a crash get a new id, which no set from before the start names. A
/// directory swapped back after the node ran under another id lost its copies
/// too, but keeps an id older sets may name.
#[test]
fn only_a_new_or_rotated_id_is_minted() {
    let relaxed = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        ..Default::default()
    };
    let other = StorageId::from_bytes([9; 16]);
    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, kept) = start_run(
        root.path(),
        &relaxed,
        Some(&registered_as(Some(other))),
        None,
    )
    .expect("wiped");
    assert!(kept.minted && !kept.ballots, "a wiped directory");
    forget_boot(root.path()).expect("forget the boot");
    let (id, rotated, _, kept) =
        start_run(root.path(), &relaxed, Some(&registered_as(Some(id))), None).expect("crash");
    assert!(rotated && kept.minted, "a crash");

    mark_clean_stop(root.path(), id).expect("clean stop");
    let (again, rotated, _, kept) = start_run(
        root.path(),
        &relaxed,
        Some(&registered_as(Some(other))),
        None,
    )
    .expect("swapped back");
    assert_eq!(again, id);
    assert!(!rotated && !kept.ballots, "lost without a rotation");
    assert!(!kept.minted, "a swapped-back directory keeps an older id");
}

/// **A start from the view attests what an asked start would once its
/// restore check passes**: kept per its own run, except under a record from
/// before storage ids, which cannot tell the directory from a wiped one.
#[test]
fn a_checked_start_from_the_view_attests_its_own_kept_ballots() {
    let kept = Kept {
        promises: false,
        ballots: true,
        readmits: false,
        minted: false,
        deferred: true,
    };
    let record = |storage_id: Option<&str>| Registered {
        incarnation: 5,
        storage_id: storage_id.map(str::to_owned),
    };
    assert!(
        kept.checked(None).ballots,
        "a node the control plane never registered"
    );
    assert!(kept.checked(Some(&record(Some("00ff")))).ballots);
    assert!(
        !kept.checked(Some(&record(None))).ballots,
        "a record without a storage id"
    );
    assert!(
        !Kept::default().checked(None).ballots,
        "a run that lost a copy"
    );
    assert!(kept.checked(Some(&record(Some("00ff")))).readmits);
    assert!(!kept.checked(Some(&record(None))).readmits);
    assert!(!kept.checked(None).readmits);
    assert!(!kept.checked(None).deferred);
}

/// **A start from the view readmits nothing until its restore check passes,
/// then exactly as a start that asked first would.** Once the check found the
/// directory current the run is a normal start, so it gets the asked start's
/// kept ballots, readmission and minted id, no more and no less.
#[test]
fn a_passed_check_readmits_as_an_asked_start_would() {
    let relaxed = felix_storage::log::LogConfig {
        fsync_mode: FsyncMode::None,
        ..Default::default()
    };
    let root = tempfile::tempdir().expect("tempdir");
    let (id, _, _, _) = start_run(root.path(), &relaxed, None, None).expect("first start");
    mark_clean_stop(root.path(), id).expect("clean stop");
    let (again, rotated, _, own) =
        start_run(root.path(), &relaxed, None, None).expect("start from the view");
    assert!(again == id && !rotated);
    let before = own.attested(true);
    assert!(!before.ballots && !before.readmits, "before the check");

    let record = registered_as(Some(id));
    let checked = own.checked(Some(&record));
    mark_clean_stop(root.path(), id).expect("clean stop");
    let (_, _, _, asked) =
        start_run(root.path(), &relaxed, Some(&record), None).expect("asked start");
    assert_eq!(
        (
            checked.ballots,
            checked.readmits,
            checked.minted,
            checked.promises
        ),
        (asked.ballots, asked.readmits, asked.minted, asked.promises)
    );
    assert!(checked.ballots && checked.readmits && !checked.minted);
}

/// **Every run witnesses its directory anew**, with the control plane's
/// answer or without it, so a copy from before the run is never the
/// directory a later start from the view finds witnessed (#2071).
#[test]
fn every_run_witnesses_the_directory_anew() {
    use felix_storage::run_state::witness::{TOKEN_FILE_NAME, witnessed};

    let log = felix_storage::log::LogConfig::default();
    let root = tempfile::tempdir().expect("tempdir");
    let shm = tempfile::tempdir().expect("witnesses");
    let token = root.path().join(TOKEN_FILE_NAME);

    let (id, _, _, _) = start_run(root.path(), &log, None, Some(shm.path())).expect("first run");
    let first = std::fs::read(&token).expect("the first run's token");
    mark_clean_stop(root.path(), id).expect("clean stop");
    start_run(
        root.path(),
        &log,
        Some(&registered_as(Some(id))),
        Some(shm.path()),
    )
    .expect("a run the control plane answered");
    assert_ne!(std::fs::read(&token).expect("token"), first);
    // Where the platform has a boot id, the directory as the run left it is
    // witnessed.
    let boot = this_boot();
    assert_eq!(
        witnessed(root.path(), Some(shm.path()), boot),
        boot.is_some()
    );
    // A restore of what the first run left is not.
    std::fs::write(&token, &first).expect("restore");
    assert!(!witnessed(root.path(), Some(shm.path()), boot));
}

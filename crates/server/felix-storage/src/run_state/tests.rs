//! A crash under a relaxed fsync mode rotates the storage id; a clean stop,
//! a crash under `OnCommit` and an upgrade without a sign of a crash keep it.
//! A directory older than the node's last registration rotates too.
use super::*;

fn counter(start: u8) -> impl FnMut() -> [u8; 16] {
    let mut next = start;
    move || {
        next += 1;
        [next; 16]
    }
}

const PERIODIC: bool = true;
const ON_COMMIT: bool = false;

fn no_repair() -> bool {
    false
}

fn begin_on(root: &Path, relaxed: bool, fresh: &mut impl FnMut() -> [u8; 16]) -> Begun {
    begin(root, relaxed, None, no_repair, fresh).expect("begin")
}

#[test]
fn a_new_directory_starts_unrecorded_and_running() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let begun = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert_eq!(begun.origin, Origin::Created);
    assert_eq!(begun.last_run, LastRun::Unrecorded);
    assert!(!begun.rotated);
    assert_eq!(
        read(dir.path(), begun.id).expect("read"),
        LastRun::Crashed { relaxed: true },
        "a run under way reads as a crash until it stops cleanly"
    );
}

/// **A crash under `Periodic` or `None` gives the directory a new id (#1284).**
/// The unsynced tail may be gone, so peers must not keep counting it.
#[test]
fn a_crash_under_a_relaxed_fsync_mode_rotates_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_on(dir.path(), PERIODIC, &mut fresh);
    // No `mark_clean`: the process died.
    let second = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert_eq!(second.last_run, LastRun::Crashed { relaxed: true });
    assert!(second.rotated);
    assert_ne!(second.id, first.id);
    assert_eq!(
        storage_identity::load_or_create(dir.path(), &mut fresh).expect("load"),
        (second.id, Origin::Loaded),
        "the rotation is on disk"
    );
}

#[test]
fn a_clean_stop_keeps_the_id_under_every_fsync_mode() {
    for relaxed in [PERIODIC, ON_COMMIT] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut fresh = counter(0);
        let first = begin_on(dir.path(), relaxed, &mut fresh);
        mark_clean(dir.path(), first.id).expect("mark clean");
        let second = begin_on(dir.path(), relaxed, &mut fresh);
        assert_eq!(second.last_run, LastRun::Clean);
        assert!(!second.rotated);
        assert_eq!(second.id, first.id, "relaxed: {relaxed}");
    }
}

/// Under `OnCommit` nothing acknowledged was unsynced, so a crash loses
/// nothing peers counted.
#[test]
fn a_crash_under_on_commit_keeps_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_on(dir.path(), ON_COMMIT, &mut fresh);
    let second = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert_eq!(second.last_run, LastRun::Crashed { relaxed: false });
    assert_eq!(second.id, first.id);
}

/// The record names the mode of the run that crashed, not the next one's.
#[test]
fn a_periodic_crash_rotates_even_when_the_next_run_is_on_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_on(dir.path(), PERIODIC, &mut fresh);
    let second = begin_on(dir.path(), ON_COMMIT, &mut fresh);
    assert!(second.rotated);
    assert_ne!(second.id, first.id);
}

/// Starting a run takes the clean record away, so a crash after it is seen.
#[test]
fn a_crash_after_a_clean_restart_is_still_seen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_on(dir.path(), PERIODIC, &mut fresh);
    mark_clean(dir.path(), first.id).expect("mark clean");
    let second = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert_eq!(second.id, first.id);
    let third = begin_on(dir.path(), PERIODIC, &mut fresh);
    assert!(third.rotated);
    assert_ne!(third.id, first.id);
}

/// The first start after an upgrade has an id and no record. It rotates only
/// when startup has something to repair, so a rolling upgrade does not make
/// every leader copy every follower again.
#[test]
fn an_upgrade_rotates_only_on_a_pending_repair() {
    for (pending, rotates) in [(false, false), (true, true)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut fresh = counter(0);
        let (old, _) = storage_identity::load_or_create(dir.path(), &mut fresh).expect("create");
        let begun = begin(dir.path(), PERIODIC, None, || pending, &mut fresh).expect("begin");
        assert_eq!(begun.last_run, LastRun::Unrecorded);
        assert_eq!(begun.rotated, rotates, "pending: {pending}");
        assert_eq!(begun.id != old, rotates);
    }
}

/// A new id needs no second one, and the repair check is not worth its reads.
#[test]
fn a_created_id_is_never_rotated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let begun = begin(
        dir.path(),
        PERIODIC,
        Some(9),
        || panic!("asked about repairs for a new directory"),
        &mut fresh,
    )
    .expect("begin");
    assert!(!begun.rotated);
}

#[test]
fn a_damaged_or_foreign_record_reads_as_a_relaxed_crash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let begun = begin_on(dir.path(), ON_COMMIT, &mut fresh);
    mark_clean(dir.path(), begun.id).expect("mark clean");
    let path = dir.path().join(FILE_NAME);
    let good = std::fs::read(&path).expect("read");
    let mut flipped = good.clone();
    flipped[10] ^= 0xff;
    let mut unknown_state = good.clone();
    unknown_state[6] = 9;
    for damaged in [flipped, unknown_state, good[..20].to_vec(), Vec::new()] {
        std::fs::write(&path, &damaged).expect("damage");
        assert_eq!(
            read(dir.path(), begun.id).expect("read"),
            LastRun::Crashed { relaxed: true }
        );
    }
    std::fs::write(&path, &good).expect("restore");
    assert_eq!(read(dir.path(), begun.id).expect("read"), LastRun::Clean);
    assert_eq!(
        read(dir.path(), StorageId::from_bytes([0xee; 16])).expect("read"),
        LastRun::Crashed { relaxed: true },
        "a clean stop under another id says nothing about this one"
    );
}

#[test]
fn records_round_trip() {
    let id = StorageId::from_bytes([7; 16]);
    for (state, flags) in [(RUNNING, 0), (RUNNING, RELAXED), (STOPPED, 0)] {
        assert_eq!(decode(&encode(state, flags, id)), Some((state, flags, id)));
    }
}

fn begin_registered(
    root: &Path,
    registered: Option<u64>,
    fresh: &mut impl FnMut() -> [u8; 16],
) -> Begun {
    begin(root, ON_COMMIT, registered, no_repair, fresh).expect("begin")
}

/// **A directory older than the node's last registration gets a new id
/// (#1334).** A volume snapshot recorded the incarnation of the run it was
/// taken in; the node registered again after it, so the control plane is
/// ahead. Under `OnCommit` and after a clean stop alike.
#[test]
fn a_restored_directory_rotates_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_registered(dir.path(), None, &mut fresh);
    record_registration(dir.path(), 4).expect("record");
    mark_clean(dir.path(), first.id).expect("mark clean");
    // The live directory went on to record 5 and 6; this one is the snapshot.
    let restored = begin_registered(dir.path(), Some(6), &mut fresh);
    assert_eq!(restored.recorded, Recorded::At(4));
    assert_eq!(restored.last_run, LastRun::Clean);
    assert!(restored.rotated);
    assert_ne!(restored.id, first.id);
    assert_eq!(
        storage_identity::load_or_create(dir.path(), &mut fresh).expect("load"),
        (restored.id, Origin::Loaded),
        "the rotation is on disk"
    );
    assert_eq!(
        read(dir.path(), restored.id).expect("read"),
        LastRun::Crashed { relaxed: false },
        "the run record names the new id"
    );
}

/// An ordinary restart finds the control plane where it left it.
#[test]
fn the_same_incarnation_keeps_the_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let first = begin_registered(dir.path(), None, &mut fresh);
    record_registration(dir.path(), 3).expect("record");
    record_registration(dir.path(), 4).expect("a second registration in one run");
    // An `OnCommit` crash: nothing it acknowledged was unsynced.
    let second = begin_registered(dir.path(), Some(4), &mut fresh);
    assert_eq!(second.recorded, Recorded::At(4));
    assert!(!second.rotated);
    assert_eq!(second.id, first.id);
}

/// Without a record, or without an answer, there is no evidence. A control
/// plane behind the record was itself restored or forgot the node, which says
/// nothing about this directory.
#[test]
fn no_record_or_no_answer_or_a_control_plane_behind_keeps_the_id() {
    let cases = [(None, Some(9)), (Some(4), None), (Some(4), Some(2))];
    for (recorded, registered) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut fresh = counter(0);
        let first = begin_registered(dir.path(), None, &mut fresh);
        if let Some(incarnation) = recorded {
            record_registration(dir.path(), incarnation).expect("record");
        }
        mark_clean(dir.path(), first.id).expect("mark clean");
        let second = begin_registered(dir.path(), registered, &mut fresh);
        assert!(!second.rotated, "{recorded:?} vs {registered:?}");
        assert_eq!(second.id, first.id, "{recorded:?} vs {registered:?}");
    }
}

/// A wiped directory has a new id already and needs no second.
#[test]
fn a_created_id_is_not_rotated_for_a_registration() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter(0);
    let begun = begin_registered(dir.path(), Some(9), &mut fresh);
    assert_eq!(begun.origin, Origin::Created);
    assert!(!begun.rotated);
}

#[test]
fn a_damaged_registration_record_is_behind_any_registration() {
    let dir = tempfile::tempdir().expect("tempdir");
    record_registration(dir.path(), 4).expect("record");
    let path = dir.path().join(REGISTRATION_FILE_NAME);
    let good = std::fs::read(&path).expect("read");
    let mut flipped = good.clone();
    flipped[8] ^= 0xff;
    for damaged in [flipped, good[..10].to_vec(), Vec::new()] {
        std::fs::write(&path, &damaged).expect("damage");
        let recorded = read_registration(dir.path()).expect("read");
        assert_eq!(recorded, Recorded::Unreadable);
        assert!(recorded.behind(Some(0)));
        assert!(!recorded.behind(None));
    }
}

#[test]
fn registration_records_round_trip() {
    for incarnation in [0, 1, u64::MAX] {
        assert_eq!(
            decode_registration(&encode_registration(incarnation)),
            Some(incarnation)
        );
    }
}

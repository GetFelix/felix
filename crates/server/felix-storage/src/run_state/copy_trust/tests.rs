//! A rotation distrusts every copy until each is vouched for again, every
//! mark survives clean restarts, and a damaged or missing record fails safe.
use super::*;
use crate::run_state::{BootId, begin, mark_clean};

const A: &[u8] = b"shard-a";
const B: &[u8] = b"shard-b";

fn start(root: &Path, boot: u8, fresh: &mut impl FnMut() -> [u8; 16]) -> crate::run_state::Begun {
    begin(
        root,
        true,
        None,
        Some(BootId::from_bytes([boot; 16])),
        || false,
        fresh,
    )
    .expect("begin")
}

fn counter() -> impl FnMut() -> [u8; 16] {
    let mut next = 0u8;
    move || {
        next += 1;
        [next; 16]
    }
}

fn marks(trust: &CopyTrust, key: &[u8]) -> (bool, bool, bool) {
    (
        trust.trusted(key),
        trust.doubted(key),
        trust.unkept(key).is_some(),
    )
}

#[test]
fn a_directory_that_never_rotated_trusts_every_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter();
    let begun = start(dir.path(), 1, &mut fresh);
    assert!(!begun.rotated);
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert_eq!(marks(&trust, A), (true, false, false));
    trust.vouch(A, true).expect("vouch");
    assert_eq!(trust.writes.load(std::sync::atomic::Ordering::Relaxed), 0);
}

/// **An unclean start distrusts every copy until a leader vouches for it
/// through its mark, and only that clears it, durably, across clean
/// restarts.**
#[test]
fn an_unclean_start_distrusts_every_copy_until_it_is_vouched_for() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let mut fresh = counter();
    start(root, 1, &mut fresh);
    // A machine crash: the next start is in another boot.
    let begun = start(root, 2, &mut fresh);
    assert!(begun.rotated);
    let clean_restart = |id, fresh: &mut _| {
        mark_clean(root, id).expect("clean stop");
        let begun = start(root, 2, fresh);
        assert!(!begun.rotated);
        (begun.id, CopyTrust::open(root).expect("open"))
    };

    let mut id = begun.id;
    for _ in 0..3 {
        let trust;
        (id, trust) = clean_restart(id, &mut fresh);
        assert!(!trust.trusted(A), "a clean restart keeps the distrust");
    }
    let trust = CopyTrust::open(root).expect("open");
    trust.vouch(A, false).expect("vouch");
    assert!(!trust.trusted(A), "level through the start only");
    trust.vouch(A, true).expect("vouch");
    for _ in 0..3 {
        let trust;
        (id, trust) = clean_restart(id, &mut fresh);
        assert!(trust.trusted(A), "the vouch survives clean restarts");
        assert!(!trust.trusted(B), "and covers only its copy");
    }

    // The next crash forgets the vouch.
    let begun = start(root, 3, &mut fresh);
    assert!(begun.rotated);
    assert!(!CopyTrust::open(root).expect("open").trusted(A));
}

/// **A doubt and an unkept ballot outlive restarts and rotations until
/// cleared.**
#[test]
fn doubts_and_unkept_ballots_outlive_restarts_until_cleared() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let mut fresh = counter();
    let begun = start(root, 1, &mut fresh);
    let trust = CopyTrust::open(root).expect("open");
    trust.doubt(A).expect("doubt");
    trust.unkeep(B, 4).expect("unkeep");
    trust.unkeep(B, 6).expect("unkeep");
    trust.unkeep(B, 5).expect("unkeep");
    mark_clean(root, begun.id).expect("clean stop");
    start(root, 1, &mut fresh);
    let trust = CopyTrust::open(root).expect("open");
    assert_eq!(marks(&trust, A), (true, true, false));
    assert_eq!(marks(&trust, B), (true, false, true));
    assert_eq!(trust.unkept(B), Some(Since::At(6)), "the newest ballot");
    start(root, 2, &mut fresh);
    let trust = CopyTrust::open(root).expect("open");
    assert_eq!(
        marks(&trust, A),
        (false, true, false),
        "a rotation keeps it"
    );
    trust.vouch(A, false).expect("vouch");
    trust.clear_unkept(B).expect("clear");
    let trust = CopyTrust::open(root).expect("open");
    assert_eq!(marks(&trust, A), (false, false, false));
    assert_eq!(marks(&trust, B), (false, false, false));
}

/// **A record that does not decode marks every copy every way, and keeps
/// them so when it is next written.** Each copy clears by the usual rules.
#[test]
fn a_damaged_record_marks_every_copy_until_each_clears() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join(FILE_NAME), b"copy_trust\njunk\n").expect("write");
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert_eq!(marks(&trust, A), (false, true, true));
    trust.vouch(A, true).expect("vouch");
    trust.clear_unkept(A).expect("clear");
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert_eq!(marks(&trust, A), (true, false, false));
    assert_eq!(
        marks(&trust, B),
        (false, true, true),
        "the rewrite kept the rest"
    );
}

/// **A directory that ran before without the record, as an older release's,
/// starts with every copy untrusted.** Not unkept: an older release gave no
/// learner a ballot. A new directory starts clean.
#[test]
fn a_directory_that_ran_without_the_record_starts_untrusted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter();
    let begun = start(dir.path(), 1, &mut fresh);
    assert_eq!(
        marks(&CopyTrust::open(dir.path()).expect("open"), A),
        (true, false, false)
    );
    mark_clean(dir.path(), begun.id).expect("clean stop");
    // As an older release left it: neither file.
    std::fs::remove_file(dir.path().join(FILE_NAME)).expect("remove");
    std::fs::remove_file(dir.path().join(KEPT_FILE_NAME)).expect("remove");
    let begun = start(dir.path(), 1, &mut fresh);
    assert!(!begun.rotated);
    assert_eq!(
        marks(&CopyTrust::open(dir.path()).expect("open"), A),
        (false, false, false)
    );
}

/// **A record lost on a directory this release ran on marks every copy every
/// way.** Only a directory with no sign of this release gets the older
/// release's treatment.
#[test]
fn a_lost_record_marks_every_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter();
    let begun = start(dir.path(), 1, &mut fresh);
    let trust = CopyTrust::open(dir.path()).expect("open");
    trust.doubt(A).expect("doubt");
    mark_clean(dir.path(), begun.id).expect("clean stop");
    std::fs::remove_file(dir.path().join(FILE_NAME)).expect("remove");
    start(dir.path(), 1, &mut fresh);
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert_eq!(marks(&trust, A), (false, true, true));
    assert_eq!(marks(&trust, B), (false, true, true));
}

/// **A rotation matches the running broker's record however the root is
/// spelled.**
#[cfg(unix)]
#[test]
fn a_rotation_finds_the_live_record_by_its_resolved_root() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter();
    start(dir.path(), 1, &mut fresh);
    let trust = CopyTrust::open_live(dir.path()).expect("open");
    let links = tempfile::tempdir().expect("tempdir");
    let spelled = links.path().join("data");
    std::os::unix::fs::symlink(dir.path(), &spelled).expect("link");
    rotate_storage_id(&spelled, [43; 16]).expect("rotate");
    assert!(!trust.trusted(A));
}

/// **A rotation while the broker runs distrusts every copy in the record it
/// holds, at once, and on disk under the new id.** Every rotation takes this
/// path: one that skipped it would leave a copy trusted on a directory a
/// restore just cut back.
#[test]
fn a_rotation_while_running_distrusts_every_copy_at_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let mut fresh = counter();
    let begun = start(root, 1, &mut fresh);
    let trust = CopyTrust::open_live(root).expect("open");
    assert!(trust.trusted(A));

    let id = rotate_storage_id(root, [42; 16]).expect("rotate");
    assert_ne!(id, begun.id);
    assert!(!trust.trusted(A), "the running broker's readers");
    trust.vouch(B, true).expect("vouch");
    let reopened = CopyTrust::open(root).expect("open");
    assert!(!reopened.trusted(A), "on disk");
    assert!(reopened.trusted(B), "written under the new id");
}

/// **An unkept copy whose generation the record lost is pinned at the first
/// ballot it holds.** Only a set newer than that readmits it.
#[test]
fn an_unknown_unkept_generation_is_pinned_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join(FILE_NAME), b"junk").expect("write");
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert_eq!(trust.unkept(A), Some(Since::Unknown));
    trust.unkeep(A, 0).expect("pin");
    assert_eq!(trust.unkept(A), Some(Since::At(0)));
    trust.unkeep(A, 7).expect("pin");
    assert_eq!(
        CopyTrust::open(dir.path()).expect("open").unkept(A),
        Some(Since::At(7))
    );
}

/// **Changes made while a write is in flight share the next one.**
#[test]
fn concurrent_vouches_share_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join(FILE_NAME), b"copy_trust\nuntrusted all\n").expect("write");
    let trust = std::sync::Arc::new(CopyTrust::open(dir.path()).expect("open"));
    let held = trust.writer.lock();
    let threads: Vec<_> = (0..32u8)
        .map(|n| {
            let trust = std::sync::Arc::clone(&trust);
            std::thread::spawn(move || trust.vouch(&[n], true).expect("vouch"))
        })
        .collect();
    while trust.wanted.lock().1 < 32 {
        std::thread::yield_now();
    }
    drop(held);
    for thread in threads {
        thread.join().expect("join");
    }
    assert_eq!(trust.writes.load(std::sync::atomic::Ordering::Relaxed), 1);
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert!((0..32u8).all(|n| trust.trusted(&[n])));
}

#[test]
fn records_round_trip() {
    let state = State {
        untrusted: Marks {
            all: true,
            except: [A.to_vec(), vec![0, 255]].into_iter().collect(),
        },
        doubted: Marks {
            all: false,
            except: [B.to_vec()].into_iter().collect(),
        },
        unkept: Marks::everyone(),
        unkept_since: [(B.to_vec(), 7)].into_iter().collect(),
    };
    let id = Some(crate::storage_identity::StorageId::from_bytes([5; 16]));
    assert_eq!(decode(&encode(&state, id)), Some((state.clone(), id)));
    assert_eq!(decode(&encode(&state, None)), Some((state, None)));
    assert_eq!(decode(HEADER.as_bytes()), Some((State::default(), None)));
    for damaged in [
        &b"copy_trust\nuntrusted 0"[..],
        b"copy_trust\nother all\n",
        b"copy_trust\ndoubted except 00\n",
        b"copy_trust\nuntrusted all\nuntrusted 00\n",
        b"copy_trust\nstorage_id 00\n",
        b"junk",
    ] {
        assert_eq!(decode(damaged), None, "{damaged:?}");
    }
}

/// **A record written under another storage id reads as unknown, and stays
/// so until each copy clears.** An older release may rotate the id after a
/// crash without touching the record, and stop cleanly after: a vouch from
/// before then says nothing of the copy now.
#[test]
fn a_record_from_before_another_releases_rotation_marks_every_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let mut fresh = counter();
    start(root, 1, &mut fresh);
    start(root, 2, &mut fresh);
    let trust = CopyTrust::open(root).expect("open");
    trust.vouch(A, true).expect("vouch");
    trust.vouch(B, true).expect("vouch");
    // The older release's crash, rotation and clean stop.
    let id = crate::storage_identity::replace(root, [99; 16]).expect("replace");
    mark_clean(root, id).expect("clean stop");

    let begun = start(root, 2, &mut fresh);
    assert!(!begun.rotated);
    let trust = CopyTrust::open(root).expect("open");
    assert_eq!(marks(&trust, A), (false, true, true));
    trust.vouch(A, true).expect("vouch");
    mark_clean(root, begun.id).expect("clean stop");
    start(root, 2, &mut fresh);
    let trust = CopyTrust::open(root).expect("open");
    assert_eq!(marks(&trust, A), (true, false, true));
    assert_eq!(
        marks(&trust, B),
        (false, true, true),
        "sticky until cleared"
    );
}

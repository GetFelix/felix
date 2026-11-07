//! A rotation distrusts every copy until each is named again, and the record
//! survives any number of clean restarts.
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

#[test]
fn a_directory_that_never_rotated_trusts_every_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fresh = counter();
    let begun = start(dir.path(), 1, &mut fresh);
    assert!(!begun.rotated);
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert!(trust.trusted(A));
    trust.trust(A).expect("trust");
    assert!(
        !crate::fs::try_exists(dir.path().join(FILE_NAME)).expect("exists"),
        "nothing to record"
    );
}

/// **An unclean start distrusts every copy until a leader vouches for it,
/// and only that clears it, durably, across clean restarts.**
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
    CopyTrust::open(root)
        .expect("open")
        .trust(A)
        .expect("trust");
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

/// **A doubted copy stays doubted until cleared, through clean restarts and
/// later rotations.**
#[test]
fn a_doubt_outlives_restarts_until_cleared() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let mut fresh = counter();
    start(root, 1, &mut fresh);
    let begun = start(root, 2, &mut fresh);
    CopyTrust::open(root)
        .expect("open")
        .doubt(A)
        .expect("doubt");
    mark_clean(root, begun.id).expect("clean stop");
    start(root, 2, &mut fresh);
    let trust = CopyTrust::open(root).expect("open");
    assert!(trust.doubted(A) && !trust.doubted(B));
    start(root, 3, &mut fresh);
    let trust = CopyTrust::open(root).expect("open");
    assert!(trust.doubted(A), "a rotation keeps the doubt");
    trust.clear_doubt(A).expect("clear");
    assert!(!CopyTrust::open(root).expect("open").doubted(A));
}

#[test]
fn records_round_trip_and_a_damaged_one_trusts_nothing() {
    let state = State {
        distrusted: true,
        trusted: [A.to_vec(), vec![0, 255]].into_iter().collect(),
        doubted: [B.to_vec()].into_iter().collect(),
    };
    assert_eq!(decode(&encode(&state)), Some(state));
    assert_eq!(decode(HEADER.as_bytes()), Some(State::default()));
    for damaged in [
        &b"copy_trust\ntrusted 0"[..],
        b"copy_trust\nother\n",
        b"junk",
    ] {
        assert_eq!(decode(damaged), None, "{damaged:?}");
    }

    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join(FILE_NAME), b"junk").expect("write");
    let trust = CopyTrust::open(dir.path()).expect("open");
    assert!(!trust.trusted(A));
}

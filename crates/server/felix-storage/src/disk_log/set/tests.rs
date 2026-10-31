//! The file's own promises: it round-trips with and without a set being left,
//! it is absent as no set, a copy that does not decode is an error, a crash
//! mid-write keeps the old set, and a stored set is on disk when `store`
//! returns.
use super::*;

fn member(node: &str, storage: Option<u8>) -> Member {
    Member {
        node: node.to_string(),
        storage: storage.map(|byte| StorageId::from_bytes([byte; 16])),
        learner: false,
    }
}

/// The file's bytes for a set `encode` would refuse, checksummed.
fn raw(set: &ReplicaSet) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.extend_from_slice(&VERSION.to_be_bytes());
    let flags = if set.leaving.is_some() {
        FLAG_LEAVING
    } else {
        0
    };
    out.extend_from_slice(&flags.to_be_bytes());
    set.current.encode_into(&mut out);
    if let Some(leaving) = &set.leaving {
        leaving.encode_into(&mut out);
    }
    sealed(out)
}

fn settled(generation: u64) -> ReplicaSet {
    ReplicaSet {
        current: Membership {
            generation,
            members: vec![
                member("a", Some(1)),
                member("b", Some(2)),
                member("c", None),
            ],
        },
        leaving: None,
    }
}

fn changing(generation: u64) -> ReplicaSet {
    ReplicaSet {
        leaving: Some(Membership {
            generation: generation - 1,
            members: vec![
                member("a", Some(1)),
                member("b", Some(2)),
                member("x", Some(9)),
            ],
        }),
        ..settled(generation)
    }
}

#[test]
fn a_set_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &settled(7)).expect("store");
    assert_eq!(load(dir.path()).expect("load"), Some(settled(7)));
}

#[test]
fn a_set_with_a_pending_old_set_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &changing(8)).expect("store");
    assert_eq!(load(dir.path()).expect("load"), Some(changing(8)));
}

#[test]
fn writing_a_set_without_the_old_set_clears_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &changing(8)).expect("store");
    store(dir.path(), &settled(8)).expect("clear");
    let found = load(dir.path()).expect("load").expect("set");
    assert_eq!(found.leaving, None);
    assert_eq!(found, settled(8));
}

#[test]
fn an_empty_set_round_trips() {
    let set = ReplicaSet {
        current: Membership {
            generation: 0,
            members: Vec::new(),
        },
        leaving: None,
    };
    assert_eq!(ReplicaSet::decode(&set.encode().unwrap()), Some(set));
}

#[test]
fn an_absent_file_reads_as_no_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    assert_eq!(load(dir.path()).expect("load"), None);
}

#[test]
fn a_crash_before_the_rename_still_reads_the_old_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &settled(7)).expect("store");
    // What a crash between the temporary's write and the rename leaves: the
    // new set whole, or half of it.
    let temporary = path_in(dir.path()).with_extension("tmp");
    let next = changing(8).encode().unwrap();
    std::fs::write(&temporary, &next).expect("whole");
    assert_eq!(load(dir.path()).expect("load"), Some(settled(7)));
    std::fs::write(&temporary, &next[..next.len() / 2]).expect("half");
    assert_eq!(load(dir.path()).expect("load"), Some(settled(7)));
    // Reading neither used nor removed it.
    assert_eq!(
        std::fs::read(&temporary).expect("kept"),
        &next[..next.len() / 2]
    );

    // The next write replaces the leftover with a different set, and the
    // leftover's contents do not leak into it.
    store(dir.path(), &settled(9)).expect("store");
    assert_eq!(load(dir.path()).expect("load"), Some(settled(9)));
    assert!(!temporary.exists(), "the rename consumed the temporary");
}

#[test]
fn a_crash_before_the_first_rename_reads_as_no_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let temporary = path_in(dir.path()).with_extension("tmp");
    std::fs::write(&temporary, settled(1).encode().unwrap()).expect("write");
    assert_eq!(load(dir.path()).expect("load"), None);
    assert!(temporary.exists());
    assert!(!path_in(dir.path()).exists(), "load created no file");
}

#[test]
fn a_damaged_set_fails_rather_than_reading_as_none() {
    // None would let a replica count a majority of a set it never held.
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &changing(8)).expect("store");
    let path = dir.path().join(FILE_NAME);
    let bytes = std::fs::read(&path).expect("read");
    for at in [0, 5, HEADER_LEN, bytes.len() / 2, bytes.len() - 1] {
        let mut damaged = bytes.clone();
        damaged[at] ^= 0xFF;
        std::fs::write(&path, damaged).expect("write");
        assert!(load(dir.path()).is_err(), "byte {at}");
    }
    std::fs::write(&path, &bytes[..bytes.len() - 1]).expect("truncate");
    assert!(load(dir.path()).is_err());
    std::fs::write(&path, b"").expect("empty");
    assert!(load(dir.path()).is_err());
}

/// Rewrites the CRC so a test reaches the parsing behind it.
fn sealed(mut body: Vec<u8>) -> Vec<u8> {
    let crc = crate::segment::format::crc32(&[&body]);
    body.extend_from_slice(&crc.to_be_bytes());
    body
}

#[test]
fn a_checksummed_but_malformed_body_does_not_decode() {
    let good = settled(3).encode().unwrap();
    let body = good[..good.len() - 4].to_vec();

    let mut slack = body.clone();
    slack.push(0);
    assert_eq!(ReplicaSet::decode(&sealed(slack)), None);

    let mut flags = body.clone();
    flags[6..8].copy_from_slice(&2u16.to_be_bytes());
    assert_eq!(ReplicaSet::decode(&sealed(flags)), None);

    // A flag for a set that is not there.
    let mut missing = body.clone();
    missing[6..8].copy_from_slice(&FLAG_LEAVING.to_be_bytes());
    assert_eq!(ReplicaSet::decode(&sealed(missing)), None);

    // A storage flag that is neither 0 nor 1.
    let mut flag = ReplicaSet {
        current: Membership {
            generation: 1,
            members: vec![member("a", None)],
        },
        leaving: None,
    }
    .encode()
    .unwrap();
    let at = flag.len() - 5;
    flag[at] = 4;
    flag.truncate(flag.len() - 4);
    assert_eq!(ReplicaSet::decode(&sealed(flag)), None);
}

#[test]
fn a_set_the_file_cannot_hold_is_refused_before_it_is_written() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut set = settled(1);
    set.current.members = (0..=MAX_MEMBERS)
        .map(|i| member(&i.to_string(), None))
        .collect();
    assert!(store(dir.path(), &set).is_err());
    let mut set = settled(1);
    set.current.members[0].node = "n".repeat(MAX_NODE_LEN + 1);
    assert!(store(dir.path(), &set).is_err());
    assert_eq!(load(dir.path()).expect("load"), None);
}

/// The set is on the device when `store` returns: the file's data before the
/// rename, the rename after it. Under a simulated power loss that keeps any
/// subset of unflushed pages, a shard whose stores have returned reads back
/// its last set in every image. Without the file's flush the image can hold a
/// torn or empty file; without the directory's, the old name.
#[cfg(target_os = "linux")]
#[test]
fn a_stored_set_survives_a_power_loss_once_store_returns() {
    use crate::io::power_loss::{PowerLoss, Writeback};

    let dir = tempfile::tempdir().expect("tempdir");
    let observer = PowerLoss::install(dir.path()).expect("install");
    store(dir.path(), &settled(7)).expect("first");
    store(dir.path(), &changing(8)).expect("second");

    let counts = observer.counts();
    assert!(counts.all >= 2 && counts.dir >= 2, "{counts:?}");
    for writeback in [Writeback::AnySubset, Writeback::InOrder] {
        for seed in 0..64 {
            let image = tempfile::tempdir().expect("image");
            observer
                .crash(seed, writeback, image.path())
                .expect("crash");
            assert_eq!(
                load(image.path()).expect("load"),
                Some(changing(8)),
                "seed {seed} {writeback:?}"
            );
        }
    }
}

fn one(members: Vec<Member>) -> ReplicaSet {
    ReplicaSet {
        current: Membership {
            generation: 5,
            members,
        },
        leaving: None,
    }
}

#[test]
fn the_member_limit_is_enforced_on_a_full_body() {
    let full = |n: usize| one((0..n).map(|i| member(&format!("n{i}"), None)).collect());
    assert_eq!(
        ReplicaSet::decode(&raw(&full(MAX_MEMBERS))),
        Some(full(MAX_MEMBERS))
    );
    assert_eq!(ReplicaSet::decode(&raw(&full(MAX_MEMBERS + 1))), None);
}

#[test]
fn the_node_id_limit_is_enforced_on_a_full_body() {
    let long = |n: usize| one(vec![member(&"n".repeat(n), None)]);
    assert_eq!(
        ReplicaSet::decode(&raw(&long(MAX_NODE_LEN))),
        Some(long(MAX_NODE_LEN))
    );
    assert_eq!(ReplicaSet::decode(&raw(&long(MAX_NODE_LEN + 1))), None);
}

#[test]
fn a_member_listed_twice_does_not_decode_or_encode() {
    for set in [
        one(vec![member("a", Some(1)), member("a", Some(2))]),
        one(vec![member("a", Some(1)), member("b", Some(1))]),
    ] {
        assert_eq!(ReplicaSet::decode(&raw(&set)), None);
        assert!(set.encode().is_err());
    }
    // Two members with no storage id are not the same member.
    let set = one(vec![member("a", None), member("b", None)]);
    assert_eq!(ReplicaSet::decode(&set.encode().unwrap()), Some(set));
    // A set being left is checked too.
    let mut set = changing(8);
    set.leaving
        .as_mut()
        .unwrap()
        .members
        .push(member("a", None));
    assert_eq!(ReplicaSet::decode(&raw(&set)), None);
}

#[test]
fn a_set_being_left_must_be_older() {
    for left in [8, 9] {
        let mut set = changing(8);
        set.leaving.as_mut().unwrap().generation = left;
        assert_eq!(ReplicaSet::decode(&raw(&set)), None, "{left}");
        assert!(set.encode().is_err());
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(store(dir.path(), &set).is_err());
        assert_eq!(load(dir.path()).expect("load"), None);
    }
}

#[test]
fn a_learner_round_trips_and_is_distinct_from_a_voter() {
    let mut set = settled(4);
    set.current.members[2].learner = true;
    set.current.members[0].storage = None;
    set.current.members[0].learner = true;
    let found = ReplicaSet::decode(&set.encode().unwrap()).expect("decodes");
    assert_eq!(found, set);
    assert_ne!(found, settled(4));
}

#[test]
fn concurrent_stores_leave_one_whole_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sets = [settled(7), changing(8)];
    std::thread::scope(|scope| {
        for round in 0..50 {
            for set in &sets {
                scope.spawn(|| store(dir.path(), set).expect("store"));
            }
            if round % 10 == 0 {
                let seen = load(dir.path()).expect("load");
                assert!(seen.is_none() || sets.contains(seen.as_ref().unwrap()));
            }
        }
    });
    let found = load(dir.path()).expect("load").expect("set");
    assert!(sets.contains(&found));
}

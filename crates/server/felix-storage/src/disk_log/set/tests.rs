//! The file's own promises: it round-trips with and without a set being left,
//! it is absent as no set, a copy that does not decode is an error, a crash
//! mid-write keeps the old set, and a stored set is on disk when `store`
//! returns.
use super::*;

fn member(node: &str, storage: Option<u8>) -> Member {
    Member {
        node: node.to_string(),
        storage: storage.map(|byte| StorageId::from_bytes([byte; 16])),
    }
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
    assert_eq!(ReplicaSet::decode(&set.encode()), Some(set));
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
    let next = changing(8).encode();
    std::fs::write(&temporary, &next).expect("whole");
    assert_eq!(load(dir.path()).expect("load"), Some(settled(7)));
    std::fs::write(&temporary, &next[..next.len() / 2]).expect("half");
    assert_eq!(load(dir.path()).expect("load"), Some(settled(7)));

    // The next write replaces the leftover.
    store(dir.path(), &changing(8)).expect("store");
    assert_eq!(load(dir.path()).expect("load"), Some(changing(8)));
}

#[test]
fn a_crash_before_the_first_rename_reads_as_no_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let temporary = path_in(dir.path()).with_extension("tmp");
    std::fs::write(&temporary, settled(1).encode()).expect("write");
    assert_eq!(load(dir.path()).expect("load"), None);
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
    let good = settled(3).encode();
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

    let mut count = body.clone();
    count[HEADER_LEN + 8..HEADER_LEN + 10].copy_from_slice(&(MAX_MEMBERS as u16 + 1).to_be_bytes());
    assert_eq!(ReplicaSet::decode(&sealed(count)), None);

    // A storage flag that is neither 0 nor 1.
    let mut flag = ReplicaSet {
        current: Membership {
            generation: 1,
            members: vec![member("a", None)],
        },
        leaving: None,
    }
    .encode();
    let at = flag.len() - 5;
    flag[at] = 2;
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

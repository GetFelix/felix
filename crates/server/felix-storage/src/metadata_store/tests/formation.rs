//! The formation file: this member's own mark and the members it recorded.

use std::collections::BTreeSet;

use super::super::formation::{decode, encode};
use super::*;
use crate::metadata_store::Formation;

/// This member's own mark, and members 2 and 3 recorded.
const FIXTURE: [u8; 30] = [
    0x46, 0x4C, 0x46, 0x4D, // "FLFM"
    0x00, 0x01, // version
    0x00, 0x01, // flags: forming
    0x00, 0x02, // id count
    0x1B, 0x35, 0x15, 0x9C, // crc
    0, 0, 0, 0, 0, 0, 0, 2, // member 2
    0, 0, 0, 0, 0, 0, 0, 3, // member 3
];

fn fixture() -> Formation {
    Formation {
        forming: true,
        formed: BTreeSet::from([2, 3]),
    }
}

fn resealed(mut bytes: Vec<u8>) -> Vec<u8> {
    let crc = crate::segment::format::crc32(&[&bytes[..10], &bytes[14..]]);
    bytes[10..14].copy_from_slice(&crc.to_be_bytes());
    bytes
}

/// The bytes on disk are the documented ones, in both directions.
#[test]
fn the_format_is_pinned_by_bytes() {
    assert_eq!(encode(&fixture()).expect("encode"), FIXTURE);
    assert_eq!(decode(&FIXTURE), Ok(fixture()));
}

/// A new store has no formation file. The mark and each record are in the
/// file when the call returns, and a record of the same member twice does
/// not write.
#[tokio::test]
async fn a_mark_and_records_read_back_after_a_reopen() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");
    assert_eq!(file.formation(), Formation::default());
    assert!(!store.join("formation").exists(), "written before a change");

    file.record_formed(3).await.expect("record");
    file.mark_forming().await.expect("mark");
    file.record_formed(2).await.expect("record");
    assert_eq!(file.formation(), fixture());
    assert_eq!(
        std::fs::read(store.join("formation")).expect("read"),
        FIXTURE
    );

    crash_point::watch(&store, None);
    file.record_formed(2).await.expect("again");
    file.mark_forming().await.expect("again");
    assert_eq!(crash_point::unwatch(&store), 0);
    // The member file is its own file: a change there leaves this one.
    file.set_vote_withheld(true).await.expect("withhold");
    drop(file);

    let file = MemberFile::open(&store, CLUSTER).expect("reopen");
    assert_eq!(file.formation(), fixture());
    assert!(file.member().vote_withheld);
}

/// Any damaged byte, a short file, a long one, another version, an unknown
/// flag and ids out of order are refused, and the open fails instead of
/// reading the store as one that recorded nothing.
#[tokio::test]
async fn a_damaged_file_is_not_read_as_no_record() {
    for at in 0..FIXTURE.len() {
        for bit in [0x01, 0x80] {
            let mut bytes = FIXTURE;
            bytes[at] ^= bit;
            assert!(decode(&bytes).is_err(), "byte {at}, bit {bit:#x}");
        }
    }
    for len in 0..FIXTURE.len() {
        assert!(decode(&FIXTURE[..len]).is_err(), "cut to {len}");
    }
    let mut long = FIXTURE.to_vec();
    long.push(0);
    assert!(decode(&long).is_err(), "a byte too many");

    let mut version = FIXTURE.to_vec();
    version[5] = 2;
    let why = decode(&resealed(version)).expect_err("another version");
    assert!(why.contains("format version 2"), "{why}");
    let mut flag = FIXTURE.to_vec();
    flag[7] |= 0x02;
    let why = decode(&resealed(flag)).expect_err("unknown flag");
    assert!(why.contains("flags this build does not know"), "{why}");
    for second in [1, 2] {
        let mut unordered = FIXTURE.to_vec();
        unordered[29] = second;
        assert!(decode(&resealed(unordered)).is_err(), "2 then {second}");
    }

    let dir = tempdir().expect("dir");
    let store = whole_store(dir.path(), &super::encode(&claimed()).expect("encode"));
    let path = store.join("formation");
    std::fs::write(&path, &FIXTURE[..29]).expect("write");
    let err = MemberFile::open(&store, CLUSTER).expect_err("damaged");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    assert_eq!(std::fs::read(&path).expect("read"), &FIXTURE[..29]);
}

/// A failed write of either file stops every later change of both: what is
/// on disk is unknown, and the copy in memory is what the next write would
/// be made from.
#[tokio::test]
async fn a_failed_record_stops_every_later_change() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");

    // Refused at its last step, the directory sync.
    crash_point::watch(&store, Some(3));
    file.record_formed(2).await.expect_err("stopped");
    crash_point::unwatch(&store);

    assert!(file.formation().formed.is_empty(), "an unconfirmed record");
    let err = file.record_formed(2).await.expect_err("after a failure");
    assert!(err.to_string().contains("earlier write"), "{err}");
    file.mark_forming().await.expect_err("after a failure");
    file.mark_started().await.expect_err("after a failure");
    drop(file);
    // The rename happened: a reopen reads what is on disk.
    let file = MemberFile::open(&store, CLUSTER).expect("reopen");
    assert_eq!(file.formation().formed, BTreeSet::from([2]));
}

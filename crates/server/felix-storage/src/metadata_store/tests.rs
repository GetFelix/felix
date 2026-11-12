//! The member file and the start decision.

#[cfg(feature = "sim")]
mod crash_points;

use std::io;
use std::path::Path;

use tempfile::tempdir;

use super::member::{decode, encode};
use super::{LOG_DIR, Member, MemberFile, REDB_FILE, STORE_DIR, Start, decide_start};
use crate::disk_log::crash_point;

/// `prod-eu`, vote withheld, started, not converted.
const FIXTURE: [u8; 21] = [
    0x46, 0x4C, 0x4D, 0x42, // "FLMB"
    0x00, 0x01, // version
    0x00, 0x05, // flags: vote withheld, started
    0x00, 0x07, // cluster id length
    0x56, 0x7E, 0x81, 0xA7, // crc
    b'p', b'r', b'o', b'd', b'-', b'e', b'u',
];
/// A converted store that has no cluster id yet.
const FIXTURE_NO_ID: [u8; 14] = [
    0x46, 0x4C, 0x4D, 0x42, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x17, 0x60, 0xA1, 0x30,
];

fn fixture() -> Member {
    Member {
        cluster_id: Some("prod-eu".to_string()),
        vote_withheld: true,
        converted: false,
        started: true,
    }
}

/// `bytes` with a checksum that matches them, as another build would write
/// a file this one must still refuse.
fn resealed(mut bytes: Vec<u8>) -> Vec<u8> {
    let crc = crate::segment::format::crc32(&[&bytes[..10], &bytes[14..]]);
    bytes[10..14].copy_from_slice(&crc.to_be_bytes());
    bytes
}

fn listing(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).expect("list") {
        let path = entry.expect("entry").path();
        let name = path
            .strip_prefix(dir)
            .expect("inside")
            .display()
            .to_string();
        if path.is_dir() {
            found.push((name.clone(), Vec::new()));
            for (inner, bytes) in listing(&path) {
                found.push((format!("{name}/{inner}"), bytes));
            }
        } else {
            found.push((name, std::fs::read(&path).expect("read")));
        }
    }
    found.sort();
    found
}

/// The bytes on disk are the documented ones, in both directions.
#[test]
fn the_format_is_pinned_by_bytes() {
    assert_eq!(encode(&fixture()).expect("encode"), FIXTURE);
    assert_eq!(decode(&FIXTURE), Ok(fixture()));

    let no_id = Member {
        converted: true,
        ..Member::default()
    };
    assert_eq!(encode(&no_id).expect("encode"), FIXTURE_NO_ID);
    assert_eq!(decode(&FIXTURE_NO_ID), Ok(no_id));
}

/// A store written by the fixture's bytes opens as the fixture.
#[tokio::test]
async fn a_file_of_the_pinned_bytes_opens() {
    let dir = tempdir().expect("dir");
    std::fs::write(dir.path().join("member"), FIXTURE).expect("write");
    let file = MemberFile::open(dir.path()).expect("open");
    assert_eq!(file.member(), fixture());
}

/// Each change is in the file when it returns, and a reopen reads it.
#[tokio::test]
async fn changes_read_back_after_a_reopen() {
    let dir = tempdir().expect("dir");
    let file = MemberFile::open(dir.path()).expect("open");
    assert_eq!(file.member(), Member::default());
    assert!(
        !dir.path().join("member").exists(),
        "an open writes nothing"
    );

    file.claim_cluster_id("prod-eu").await.expect("claim");
    file.set_vote_withheld(true).await.expect("withhold");
    file.mark_started().await.expect("start");
    assert_eq!(file.member(), fixture());
    assert_eq!(
        std::fs::read(dir.path().join("member")).expect("read"),
        FIXTURE
    );
    assert_eq!(
        MemberFile::open(dir.path()).expect("open").member(),
        fixture()
    );

    file.set_vote_withheld(false).await.expect("give back");
    file.set_converted(true).await.expect("convert");
    let reopened = MemberFile::open(dir.path()).expect("open").member();
    assert!(!reopened.vote_withheld && reopened.converted && reopened.started);
    file.set_converted(false).await.expect("clear");
    let reopened = MemberFile::open(dir.path()).expect("open").member();
    assert!(!reopened.converted && reopened.started, "{reopened:?}");
}

/// A store belongs to one cluster for good. A second claim of the same id
/// is fine; another id is refused and nothing is written.
#[tokio::test]
async fn a_store_keeps_its_first_cluster_id() {
    let dir = tempdir().expect("dir");
    let file = MemberFile::open(dir.path()).expect("open");
    let err = file.claim_cluster_id("").await.expect_err("empty");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    file.claim_cluster_id("prod-eu").await.expect("claim");
    let written = listing(dir.path());

    file.claim_cluster_id("prod-eu").await.expect("again");
    let err = file.claim_cluster_id("prod-us").await.expect_err("other");
    assert!(
        err.to_string().contains("'prod-eu', not 'prod-us'"),
        "{err}"
    );
    assert_eq!(listing(dir.path()), written);
    assert_eq!(file.member().cluster_id.as_deref(), Some("prod-eu"));

    let reopened = MemberFile::open(dir.path()).expect("open");
    reopened
        .claim_cluster_id("prod-us")
        .await
        .expect_err("other, after a reopen");
}

/// A change that changes nothing does not write.
#[tokio::test]
async fn an_unchanged_member_is_not_rewritten() {
    let dir = tempdir().expect("dir");
    let file = MemberFile::open(dir.path()).expect("open");
    file.mark_started().await.expect("start");

    crash_point::watch(dir.path(), None);
    file.mark_started().await.expect("again");
    file.set_vote_withheld(false).await.expect("unchanged");
    assert_eq!(crash_point::unwatch(dir.path()), 0);
}

/// Any damaged byte, a short file and a long one are all refused, and the
/// open fails instead of reading the store as a new one.
#[tokio::test]
async fn a_damaged_file_is_not_read_as_a_new_store() {
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
    // The id is under the checksum, so a valid one still has to be text.
    let mut not_text = FIXTURE.to_vec();
    not_text[20] = 0xFF;
    assert!(decode(&resealed(not_text)).is_err(), "not UTF-8");

    let dir = tempdir().expect("dir");
    let path = dir.path().join("member");
    let mut bytes = FIXTURE;
    bytes[7] ^= 0x04;
    std::fs::write(&path, bytes).expect("write");
    let err = MemberFile::open(dir.path()).expect_err("damaged");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    std::fs::write(&path, b"").expect("write");
    MemberFile::open(dir.path()).expect_err("empty");
    assert_eq!(std::fs::read(&path).expect("read"), b"", "left as found");
}

/// A file of another format version is refused by its version, whatever
/// else it holds, and so is one that sets a flag this build does not know.
#[tokio::test]
async fn another_version_and_an_unknown_flag_are_refused() {
    for version in [0u16, 2, 0x0100] {
        let mut bytes = FIXTURE.to_vec();
        bytes[4..6].copy_from_slice(&version.to_be_bytes());
        let why = decode(&resealed(bytes.clone())).expect_err("another version");
        assert!(why.contains(&format!("format version {version}")), "{why}");

        let dir = tempdir().expect("dir");
        std::fs::write(dir.path().join("member"), resealed(bytes)).expect("write");
        let err = MemberFile::open(dir.path()).expect_err("another version");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    }

    let mut bytes = FIXTURE.to_vec();
    bytes[7] |= 0x08;
    let why = decode(&resealed(bytes)).expect_err("unknown flag");
    assert!(why.contains("flags this build does not know"), "{why}");
}

/// The member file is the first thing a store holds. A log or a snapshot
/// without one means the file was lost, with the withheld vote in it.
#[tokio::test]
async fn a_store_with_state_and_no_member_file_is_refused() {
    let with_log = tempdir().expect("dir");
    std::fs::create_dir(with_log.path().join(LOG_DIR)).expect("log");
    let err = MemberFile::open(with_log.path()).expect_err("a log");
    assert!(err.to_string().contains("no member file"), "{err}");

    let with_snapshot = tempdir().expect("dir");
    std::fs::write(with_snapshot.path().join("raft-snapshot"), b"x").expect("snapshot");
    MemberFile::open(with_snapshot.path()).expect_err("a snapshot");

    std::fs::write(with_log.path().join("member"), FIXTURE).expect("write");
    MemberFile::open(with_log.path()).expect("with its member file");
}

/// A temporary a crash left is not the member file, and the open deletes
/// it.
#[tokio::test]
async fn a_leftover_temporary_is_deleted_and_never_read() {
    let dir = tempdir().expect("dir");
    let temporary = dir.path().join("member.tmp");
    std::fs::write(&temporary, FIXTURE).expect("write");
    let file = MemberFile::open(dir.path()).expect("open");
    assert_eq!(file.member(), Member::default());
    assert!(!temporary.exists());
}

/// After a write fails the file may be the old one or the new one, so every
/// later change fails too.
#[tokio::test]
async fn a_failed_write_stops_every_later_change() {
    let dir = tempdir().expect("dir");
    let file = MemberFile::open(dir.path()).expect("open");
    file.claim_cluster_id("prod-eu").await.expect("claim");

    // The write is refused at its last step, the directory sync.
    crash_point::watch(dir.path(), Some(3));
    file.mark_started().await.expect_err("stopped");
    crash_point::unwatch(dir.path());

    assert!(
        !file.member().started,
        "an unconfirmed mark is not reported"
    );
    let err = file.mark_started().await.expect_err("after a failure");
    assert!(err.to_string().contains("earlier write"), "{err}");
    file.set_vote_withheld(true)
        .await
        .expect_err("after a failure");
    // The rename happened: a reopen reads what is on disk.
    assert!(MemberFile::open(dir.path()).expect("open").member().started);
}

async fn store_with(data_dir: &Path, converted: bool) {
    let store = data_dir.join(STORE_DIR);
    std::fs::create_dir(&store).expect("store");
    let file = MemberFile::open(&store).expect("open");
    file.claim_cluster_id("prod-eu").await.expect("claim");
    file.set_converted(converted).await.expect("mark");
}

/// The four rows of the table in `docs/metadata-raft-design.md`.
#[tokio::test]
async fn a_start_is_decided_from_names_and_the_converted_mark() {
    let new = tempdir().expect("dir");
    assert_eq!(decide_start(new.path()).expect("new"), Start::New);
    assert!(listing(new.path()).is_empty(), "deciding creates nothing");

    let store_only = tempdir().expect("dir");
    store_with(store_only.path(), false).await;
    assert_eq!(
        decide_start(store_only.path()).expect("store"),
        Start::Existing
    );

    let converted = tempdir().expect("dir");
    store_with(converted.path(), true).await;
    std::fs::write(converted.path().join(REDB_FILE), b"the stub").expect("stub");
    assert_eq!(
        decide_start(converted.path()).expect("converted"),
        Start::Existing
    );

    let unconverted = tempdir().expect("dir");
    std::fs::write(unconverted.path().join(REDB_FILE), b"a group's state").expect("redb");
    let before = listing(unconverted.path());
    let err = decide_start(unconverted.path()).expect_err("redb alone");
    assert!(err.to_string().contains("conversion tool"), "{err}");
    assert!(err.to_string().contains(REDB_FILE), "{err}");
    assert_eq!(
        listing(unconverted.path()),
        before,
        "a refusal writes nothing"
    );

    // A store without the mark beside a redb file: not the conversion's.
    store_with(unconverted.path(), false).await;
    let before = listing(unconverted.path());
    decide_start(unconverted.path()).expect_err("no mark");
    assert_eq!(
        listing(unconverted.path()),
        before,
        "a refusal writes nothing"
    );
    // A store directory with no member file at all.
    std::fs::remove_file(unconverted.path().join(STORE_DIR).join("member")).expect("remove");
    let before = listing(unconverted.path());
    decide_start(unconverted.path()).expect_err("no member file");
    assert_eq!(
        listing(unconverted.path()),
        before,
        "a refusal writes nothing"
    );
}

/// A restore clears the mark before it puts the redb file back, so the
/// service stops starting on that store from the moment the mark is gone.
#[tokio::test]
async fn a_cleared_mark_stops_the_start() {
    let dir = tempdir().expect("dir");
    store_with(dir.path(), true).await;
    std::fs::write(dir.path().join(REDB_FILE), b"the stub").expect("stub");
    let file = MemberFile::open(dir.path().join(STORE_DIR)).expect("open");
    file.mark_started().await.expect("start");
    assert_eq!(
        decide_start(dir.path()).expect("converted"),
        Start::Existing
    );

    file.set_converted(false).await.expect("clear");
    decide_start(dir.path()).expect_err("cleared");
    assert!(file.member().started, "the started mark stays");
}

/// With a redb file present, a member file that does not decode is a
/// refusal, never "no mark" read as something else, and the file is left.
#[tokio::test]
async fn a_damaged_member_file_refuses_the_start() {
    let dir = tempdir().expect("dir");
    store_with(dir.path(), true).await;
    std::fs::write(dir.path().join(REDB_FILE), b"the stub").expect("stub");
    let member = dir.path().join(STORE_DIR).join("member");
    let mut bytes = std::fs::read(&member).expect("read");
    bytes[11] ^= 0x10;
    std::fs::write(&member, &bytes).expect("write");

    let before = listing(dir.path());
    let err = decide_start(dir.path()).expect_err("damaged");
    assert!(err.to_string().contains("damaged"), "{err}");
    assert_eq!(listing(dir.path()), before);
}

/// The decision never opens the redb file: one that cannot be read at all
/// decides the same way.
#[cfg(unix)]
#[tokio::test]
async fn the_redb_file_is_never_read() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().expect("dir");
    store_with(dir.path(), true).await;
    let redb = dir.path().join(REDB_FILE);
    std::fs::write(&redb, b"unreadable").expect("redb");
    std::fs::set_permissions(&redb, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    assert_eq!(decide_start(dir.path()).expect("decide"), Start::Existing);

    // A name that cannot be examined is not read as absent.
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).expect("dir");
    std::fs::write(locked.join(REDB_FILE), b"a group's state").expect("redb");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    let decided = decide_start(&locked);
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    // Root reads through the mode, and then sees the unconverted file.
    decided.expect_err("not a new member");
}

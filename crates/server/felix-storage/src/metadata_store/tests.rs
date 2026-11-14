//! The member file and the start decision.

#[cfg(feature = "sim")]
mod crash_points;
mod formation;
#[cfg(feature = "sim")]
mod model;
mod store;
#[cfg(feature = "sim")]
mod store_crash_points;

use std::io;
use std::path::{Path, PathBuf};

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
/// Converted, with no cluster id: a file no build writes.
const FIXTURE_NO_ID: [u8; 14] = [
    0x46, 0x4C, 0x4D, 0x42, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x17, 0x60, 0xA1, 0x30,
];
const CLUSTER: &str = "prod-eu";

fn fixture() -> Member {
    Member {
        cluster_id: CLUSTER.to_string(),
        vote_withheld: true,
        converted: false,
        started: true,
    }
}

fn claimed() -> Member {
    Member {
        cluster_id: CLUSTER.to_string(),
        ..Member::default()
    }
}

/// `bytes` with a checksum that matches them, as another build would write
/// a file this one must still refuse.
fn resealed(mut bytes: Vec<u8>) -> Vec<u8> {
    let crc = crate::segment::format::crc32(&[&bytes[..10], &bytes[14..]]);
    bytes[10..14].copy_from_slice(&crc.to_be_bytes());
    bytes
}

/// A store in `dir` as a build would leave it, with `member` as its member
/// file.
fn whole_store(dir: &Path, member: &[u8]) -> PathBuf {
    let store = dir.join(STORE_DIR);
    std::fs::create_dir_all(store.join(LOG_DIR)).expect("log");
    std::fs::write(store.join("member"), member).expect("write");
    store
}

fn reopened(store: &Path) -> Member {
    MemberFile::open(store, CLUSTER).expect("reopen").member()
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
    // Every store is created for a cluster, so a file without an id is
    // damaged.
    let why = decode(&FIXTURE_NO_ID).expect_err("no id");
    assert!(why.contains("no cluster id"), "{why}");
}

/// A store written by the fixture's bytes opens as the fixture.
#[tokio::test]
async fn a_file_of_the_pinned_bytes_opens() {
    let dir = tempdir().expect("dir");
    let store = whole_store(dir.path(), &FIXTURE);
    assert_eq!(reopened(&store), fixture());
}

/// The open makes the store, member file and log directory, and each
/// change is in the file when it returns.
#[tokio::test]
async fn changes_read_back_after_a_reopen() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");
    assert_eq!(file.member(), claimed());
    assert_eq!(file.log_dir(), store.join(LOG_DIR));
    assert_eq!(
        listing(dir.path()),
        [
            (STORE_DIR.to_string(), Vec::new()),
            (format!("{STORE_DIR}/lock"), Vec::new()),
            (format!("{STORE_DIR}/{LOG_DIR}"), Vec::new()),
            (
                format!("{STORE_DIR}/member"),
                encode(&claimed()).expect("encode")
            ),
        ]
    );

    file.set_vote_withheld(true).await.expect("withhold");
    file.mark_started().await.expect("start");
    assert_eq!(file.member(), fixture());
    assert_eq!(std::fs::read(store.join("member")).expect("read"), FIXTURE);
    drop(file);
    assert_eq!(reopened(&store), fixture());

    let file = MemberFile::open(&store, CLUSTER).expect("open");
    file.set_vote_withheld(false).await.expect("give back");
    file.set_converted(true).await.expect("convert");
    drop(file);
    let member = reopened(&store);
    assert!(!member.vote_withheld && member.converted && member.started);

    let file = MemberFile::open(&store, CLUSTER).expect("open");
    file.set_converted(false).await.expect("clear");
    drop(file);
    let member = reopened(&store);
    assert!(!member.converted && member.started, "{member:?}");
}

/// The member file is on disk before the open returns, so a log written
/// next and a crash before anything else leave a store that opens.
#[tokio::test]
async fn a_log_written_right_after_the_open_does_not_strand_the_store() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");
    std::fs::write(file.log_dir().join("00000000000000000000.log"), b"x").expect("log");
    drop(file);
    assert_eq!(reopened(&store), claimed());
}

/// A store belongs to one cluster for good: another id is refused and
/// nothing is written. An empty id is refused before anything is made.
#[tokio::test]
async fn a_store_keeps_its_first_cluster_id() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let err = MemberFile::open(&store, "").expect_err("empty");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(listing(dir.path()).is_empty(), "{err}");

    drop(MemberFile::open(&store, CLUSTER).expect("open"));
    let written = listing(dir.path());
    let err = MemberFile::open(&store, "prod-us").expect_err("other");
    assert!(
        err.to_string().contains("'prod-eu', not 'prod-us'"),
        "{err}"
    );
    assert_eq!(listing(dir.path()), written);
    assert_eq!(reopened(&store), claimed());
}

/// Each open writes the file from its own copy, so a second open of a
/// store would undo the first's changes. It is refused while the first is
/// open, and allowed once every clone is gone.
#[tokio::test]
async fn a_store_opens_once_at_a_time() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");
    let clone = file.clone();
    let err = MemberFile::open(&store, CLUSTER).expect_err("second open");
    assert_eq!(err.kind(), io::ErrorKind::ResourceBusy, "{err}");
    // The same store by another spelling.
    let other = dir.path().join(".").join(STORE_DIR);
    MemberFile::open(&other, CLUSTER).expect_err("another spelling");

    file.set_vote_withheld(true).await.expect("withhold");
    drop(file);
    MemberFile::open(&store, CLUSTER).expect_err("a clone is still open");
    clone.mark_started().await.expect("start");
    drop(clone);
    let member = reopened(&store);
    assert!(member.vote_withheld && member.started, "{member:?}");
}

/// The lock is on the file, not the path: a store reached through a
/// symlink is the same store.
#[cfg(unix)]
#[tokio::test]
async fn a_store_reached_through_a_symlink_is_refused() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&store, &link).expect("symlink");
    let data_link = dir.path().join("data-link");
    std::os::unix::fs::symlink(dir.path(), &data_link).expect("symlink");
    for path in [link.clone(), data_link.join(STORE_DIR)] {
        let err = MemberFile::open(&path, CLUSTER).expect_err("through a symlink");
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy, "{err}");
    }
    drop(file);
    MemberFile::open(&link, CLUSTER).expect("released on drop");
}

const CHILD_STORE: &str = "FELIX_TEST_LOCK_CHILD_STORE";

/// Run by [`another_process_is_refused_while_the_store_is_open`] in a
/// second process: exits 0 when the store named in the environment is
/// busy, 3 when it opened. Does nothing as a test of its own.
#[test]
fn lock_child() {
    let Some(store) = std::env::var_os(CHILD_STORE) else {
        return;
    };
    let code = match MemberFile::open(PathBuf::from(store), CLUSTER) {
        Err(err) if err.kind() == io::ErrorKind::ResourceBusy => 0,
        Ok(_) => 3,
        Err(_) => 4,
    };
    std::process::exit(code);
}

/// A second process is refused while the store is open here, and gets in
/// once it is dropped. This is what an in-process registry could not do.
#[cfg(unix)]
#[tokio::test]
async fn another_process_is_refused_while_the_store_is_open() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let child = || {
        std::process::Command::new(std::env::current_exe().expect("this binary"))
            .args(["--exact", "metadata_store::tests::lock_child"])
            .env(CHILD_STORE, &store)
            .output()
            .expect("child")
            .status
            .code()
    };
    let file = MemberFile::open(&store, CLUSTER).expect("open");
    assert_eq!(child(), Some(0), "a second process opened a held store");
    drop(file);
    assert_eq!(child(), Some(3), "the lock outlived its holder");
}

/// A started mark proves the mark of its own store only.
#[tokio::test]
async fn a_started_mark_names_its_store() {
    let dir = tempdir().expect("dir");
    let (one, two) = (dir.path().join("one"), dir.path().join("two"));
    let started = MemberFile::open(&one, CLUSTER)
        .expect("open")
        .mark_started()
        .await
        .expect("start");
    assert_eq!(started.store_dir(), one);
    started.check(&one).expect("its own store");
    let err = started.check(&two).expect_err("another store");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
}

/// A change that changes nothing does not write.
#[tokio::test]
async fn an_unchanged_member_is_not_rewritten() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");
    file.mark_started().await.expect("start");

    crash_point::watch(&store, None);
    file.mark_started().await.expect("again");
    file.set_vote_withheld(false).await.expect("unchanged");
    assert_eq!(crash_point::unwatch(&store), 0);
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
    let mut bytes = FIXTURE;
    bytes[7] ^= 0x04;
    let store = whole_store(dir.path(), &bytes);
    let path = store.join("member");
    let err = MemberFile::open(&store, CLUSTER).expect_err("damaged");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    std::fs::write(&path, b"").expect("write");
    MemberFile::open(&store, CLUSTER).expect_err("empty");
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
        let store = whole_store(dir.path(), &resealed(bytes));
        let err = MemberFile::open(&store, CLUSTER).expect_err("another version");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    }

    let mut bytes = FIXTURE.to_vec();
    bytes[7] |= 0x08;
    let why = decode(&resealed(bytes)).expect_err("unknown flag");
    assert!(why.contains("flags this build does not know"), "{why}");
}

/// A store is created with its member file and its log directory. One
/// without either has lost state, maybe the vote, and both the open and
/// the start decision refuse it.
#[tokio::test]
async fn a_store_missing_its_member_file_or_its_log_is_refused() {
    type Make = fn(&Path);
    let cases: [(&str, Make); 4] = [
        ("a log and no member file", |store| {
            std::fs::create_dir(store.join(LOG_DIR)).expect("log");
        }),
        ("a snapshot and no member file", |store| {
            std::fs::write(store.join("raft-snapshot"), b"x").expect("snapshot");
        }),
        ("an emptied store", |_| {}),
        ("a started member file and no log", |store| {
            std::fs::write(store.join("member"), FIXTURE).expect("member");
        }),
    ];
    for (case, make) in cases {
        let dir = tempdir().expect("dir");
        let store = dir.path().join(STORE_DIR);
        std::fs::create_dir(&store).expect("store");
        make(&store);
        let before = listing(dir.path());
        let err = decide_start(dir.path()).expect_err(case);
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
        let err = MemberFile::open(&store, CLUSTER).expect_err(case);
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{case}: {err}");
        assert_eq!(listing(dir.path()), before, "{case}: left as found");
    }
}

/// A temporary a crash left is not the member file, and the open deletes
/// it.
#[tokio::test]
async fn a_leftover_temporary_is_deleted_and_never_read() {
    let dir = tempdir().expect("dir");
    let store = whole_store(dir.path(), &FIXTURE);
    let temporary = store.join("member.tmp");
    std::fs::write(&temporary, encode(&claimed()).expect("encode")).expect("write");
    assert_eq!(reopened(&store), fixture());
    assert!(!temporary.exists());
}

/// A store a crash left half built under its temporary name is not the
/// store: the start is new, and the open builds it again.
#[tokio::test]
async fn a_half_built_store_is_replaced() {
    let dir = tempdir().expect("dir");
    let building = dir.path().join(format!("{STORE_DIR}.tmp"));
    std::fs::create_dir_all(building.join(LOG_DIR)).expect("log");
    std::fs::write(building.join("member"), FIXTURE).expect("member");
    assert_eq!(decide_start(dir.path()).expect("decide"), Start::New);
    assert_eq!(reopened(&dir.path().join(STORE_DIR)), claimed());
    assert!(!building.exists());
}

/// After a write fails the file may be the old one or the new one, so every
/// later change fails too.
#[tokio::test]
async fn a_failed_write_stops_every_later_change() {
    let dir = tempdir().expect("dir");
    let store = dir.path().join(STORE_DIR);
    let file = MemberFile::open(&store, CLUSTER).expect("open");

    // The write is refused at its last step, the directory sync.
    crash_point::watch(&store, Some(3));
    file.mark_started().await.expect_err("stopped");
    crash_point::unwatch(&store);

    assert!(
        !file.member().started,
        "an unconfirmed mark is not reported"
    );
    let err = file.mark_started().await.expect_err("after a failure");
    assert!(err.to_string().contains("earlier write"), "{err}");
    file.set_vote_withheld(true)
        .await
        .expect_err("after a failure");
    drop(file);
    // The rename happened: a reopen reads what is on disk.
    assert!(reopened(&store).started);
}

async fn store_with(data_dir: &Path, converted: bool) -> Member {
    let file = MemberFile::open(data_dir.join(STORE_DIR), CLUSTER).expect("open");
    file.set_converted(converted).await.expect("mark");
    file.member()
}

/// The rows of the table in `docs/storage-format.md`.
#[tokio::test]
async fn a_start_is_decided_from_names_and_the_converted_mark() {
    let new = tempdir().expect("dir");
    assert_eq!(decide_start(new.path()).expect("new"), Start::New);
    assert!(listing(new.path()).is_empty(), "deciding creates nothing");

    let store_only = tempdir().expect("dir");
    let member = store_with(store_only.path(), false).await;
    assert_eq!(
        decide_start(store_only.path()).expect("store"),
        Start::Existing(member)
    );

    let converted = tempdir().expect("dir");
    let member = store_with(converted.path(), true).await;
    std::fs::write(converted.path().join(REDB_FILE), b"the stub").expect("stub");
    assert_eq!(
        decide_start(converted.path()).expect("converted"),
        Start::Existing(member)
    );

    let unconverted = tempdir().expect("dir");
    std::fs::write(unconverted.path().join(REDB_FILE), b"a group's state").expect("redb");
    let before = listing(unconverted.path());
    let err = decide_start(unconverted.path()).expect_err("redb alone");
    assert!(err.to_string().contains("cannot read"), "{err}");
    assert!(err.to_string().contains(REDB_FILE), "{err}");
    assert_eq!(
        listing(unconverted.path()),
        before,
        "a refusal writes nothing"
    );

    // A store without the mark beside a redb file: not the conversion's.
    store_with(unconverted.path(), false).await;
    let before = listing(unconverted.path());
    let err = decide_start(unconverted.path()).expect_err("no mark");
    assert!(err.to_string().contains("cannot read"), "{err}");
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
    let file = MemberFile::open(dir.path().join(STORE_DIR), CLUSTER).expect("open");
    file.mark_started().await.expect("start");
    assert!(matches!(
        decide_start(dir.path()).expect("converted"),
        Start::Existing(Member { started: true, .. })
    ));

    file.set_converted(false).await.expect("clear");
    decide_start(dir.path()).expect_err("cleared");
    assert!(file.member().started, "the started mark stays");
}

/// A member file that does not decode is a refusal, with or without a redb
/// file, never "no mark" or "a new member", and the file is left.
#[tokio::test]
async fn a_damaged_member_file_refuses_the_start() {
    for redb in [false, true] {
        let dir = tempdir().expect("dir");
        store_with(dir.path(), true).await;
        if redb {
            std::fs::write(dir.path().join(REDB_FILE), b"the stub").expect("stub");
        }
        let member = dir.path().join(STORE_DIR).join("member");
        let mut bytes = std::fs::read(&member).expect("read");
        bytes[11] ^= 0x10;
        std::fs::write(&member, &bytes).expect("write");

        let before = listing(dir.path());
        let err = decide_start(dir.path()).expect_err("damaged");
        assert!(err.to_string().contains("damaged"), "redb {redb}: {err}");
        assert_eq!(listing(dir.path()), before);
    }
}

/// The decision never opens the redb file: one that cannot be read at all
/// decides the same way.
#[cfg(unix)]
#[tokio::test]
async fn the_redb_file_is_never_read() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().expect("dir");
    let member = store_with(dir.path(), true).await;
    let redb = dir.path().join(REDB_FILE);
    std::fs::write(&redb, b"unreadable").expect("redb");
    std::fs::set_permissions(&redb, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    assert_eq!(
        decide_start(dir.path()).expect("decide"),
        Start::Existing(member)
    );

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

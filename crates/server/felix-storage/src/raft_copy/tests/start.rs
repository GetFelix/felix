//! Starting a copy in a re-formed epoch, and a power cut at every step of it.

use std::collections::BTreeSet;
use std::path::Path;

use bytes::Bytes;
use openraft::storage::RaftLogStorage;

use super::*;
use crate::DiskLog;
use crate::disk_log::crash_point;
use crate::raft_log::tests::writes::append_flushed;
use crate::raft_log::tests::{Codec, Types, log_id, normal};
use crate::raft_log::{LogPoint, RaftLog, reserved};

const SPACING: u64 = 4096;

/// A log of entries 0..`len` in term 1, closed.
async fn log_of(dir: &Path, len: u64) {
    let log = DiskLog::open(dir, "t/ns/raft/0", config()).expect("open");
    let mut raft = RaftLog::<Types, Codec>::new(log, Codec).expect("a Raft log");
    let entries: Vec<_> = (0..len).map(|index| normal(1, index, "base")).collect();
    append_flushed(&mut raft, &entries).await;
    raft.disk_log().close().await.expect("close");
}

/// Epoch 1 from a base ending at (1, 4), first index 8.
fn start(role: Role) -> Start {
    Start {
        epoch: EpochFile {
            incarnation: 1,
            bases: vec![EpochBase {
                base_last: Some(EpochLogId {
                    index: 4,
                    epoch: 0,
                    term: 1,
                }),
                first_index: 8,
            }],
        },
        role,
        meta: Bytes::from_static(b"meta"),
        data: Bytes::from_static(b"data"),
    }
}

/// A started voter reads as its base, is reserved through the gap, has its
/// commit saved at the gap's end, and its log's next entry is the first index.
#[tokio::test]
async fn a_started_voter_keeps_its_base_and_appends_at_the_first_index() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 5).await;
    start_copy(dir.path(), &start(Role::Voter), SPACING).expect("start");

    assert!(!building(dir.path()).expect("marker"));
    assert!(reserved::load(dir.path()).expect("reserved") >= Some(7));
    let (commit, _) = crate::disk_log::saved_commit(dir.path()).expect("commit");
    assert_eq!(commit, 8);
    let read = read_copy(dir.path(), SPACING, &BTreeSet::from([4])).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 1, index: 4 }));
    assert_eq!(read.committed, Some(7));

    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    let mut raft = RaftLog::<Types, Codec>::new(log, Codec).expect("a Raft log");
    let state = raft.get_log_state().await.expect("state");
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 7)));
    assert_eq!(state.last_log_id, Some(log_id(1, 7)));
    raft.disk_log().close().await.expect("close");
}

/// A voter holding records past the base is refused: the build cuts them
/// first.
#[tokio::test]
async fn a_voter_past_its_base_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 6).await;
    start_copy(dir.path(), &start(Role::Voter), SPACING).expect_err("refused");
    assert!(building(dir.path()).expect("marker"));
}

/// A learner with records past the base empties its log; it catches up by
/// snapshot.
#[tokio::test]
async fn a_learner_past_the_base_empties_its_log() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    start_copy(dir.path(), &start(Role::Learner), SPACING).expect("start");

    let read = read_copy(dir.path(), SPACING, &BTreeSet::new()).expect("read");
    assert_eq!((read.last, read.purged), (None, None));
    assert_eq!(read.epoch, Some(start(Role::Learner).epoch));
    assert!(reserved::load(dir.path()).expect("reserved") >= Some(7));
    DiskLog::open(dir.path(), "t/ns/raft/0", config())
        .expect("an emptied learner opens")
        .close()
        .await
        .expect("close");
}

/// A learner within the base keeps what it holds.
#[tokio::test]
async fn a_learner_within_the_base_keeps_its_prefix() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 3).await;
    start_copy(dir.path(), &start(Role::Learner), SPACING).expect("start");
    let read = read_copy(dir.path(), SPACING, &BTreeSet::new()).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 1, index: 2 }));
}

/// #2640: the check before the marker goes refuses a copy without a
/// reservation through the gap, or a voter whose saved commit is below the
/// base's end.
#[tokio::test]
async fn the_start_check_refuses_a_missing_reservation_or_a_low_commit() {
    let dir = tempfile::tempdir().expect("dir");
    let voter = start(Role::Voter);
    crate::raft_copy::start::check_started(dir.path(), &voter).expect_err("no reservation");
    reserved::store(dir.path(), 7).expect("reserve");
    crate::raft_copy::start::check_started(dir.path(), &voter).expect_err("no commit");
    crate::disk_log::store_saved_commit(dir.path(), 5, 2).expect("commit");
    crate::raft_copy::start::check_started(dir.path(), &voter).expect("both kept");
}

/// A power cut at any step leaves the marker, so the copy does not open, and
/// running the start again finishes it as an uncut run would.
#[tokio::test]
async fn a_start_cut_at_any_step_does_not_open_and_finishes_when_run_again() {
    for role in [Role::Voter, Role::Learner] {
        let whole = tempfile::tempdir().expect("dir");
        log_of(whole.path(), if role == Role::Voter { 5 } else { 7 }).await;
        start_copy(whole.path(), &start(role), SPACING).expect("start");
        let want = read_copy(whole.path(), SPACING, &BTreeSet::from([4])).expect("read");

        let mut cut_at = 0;
        loop {
            let root = Root::new();
            let dir = root.shard_dir();
            std::fs::create_dir_all(&dir).expect("dir");
            log_of(&dir, if role == Role::Voter { 5 } else { 7 }).await;
            crash_point::watch(&dir, Some(cut_at));
            let cut = start_copy(&dir, &start(role), SPACING);
            crash_point::unwatch(&dir);
            if cut.is_ok() {
                break;
            }
            // Cut before the marker is written: nothing else is either.
            if !building(&dir).expect("marker") {
                assert_eq!(
                    crate::raft_copy::epoch_file::load(&dir).expect("epoch"),
                    None
                );
                start_copy(&dir, &start(role), SPACING).expect("run again");
                cut_at += 1;
                continue;
            }
            let refused = root
                .open()
                .open_copy("orders/0", &dir, "t/ns/orders/0", config())
                .await;
            assert!(refused.is_err(), "{role:?} cut at {cut_at} opened");

            start_copy(&dir, &start(role), SPACING).expect("run again");
            let mut got = read_copy(&dir, SPACING, &BTreeSet::from([4])).expect("read");
            got.path = want.path.clone();
            assert_eq!(got, want, "{role:?} cut at {cut_at}");
            cut_at += 1;
        }
        assert!(cut_at > 3, "{role:?}: only {cut_at} steps");
    }
}

/// #2640: a copy that never took a reservation is reserved through the gap
/// before it starts.
#[tokio::test]
async fn a_start_reserves_through_the_gap() {
    let dir = tempfile::tempdir().expect("dir");
    start_copy(dir.path(), &start(Role::Learner), SPACING).expect("start");
    assert_eq!(reserved::load(dir.path()).expect("reserved"), Some(7));
}

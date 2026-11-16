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
use crate::raft_log::{LogPoint, RaftLog, held, reserved};

const SPACING: u64 = 4096;

/// A log of entries 0..`len` in term 1, closed.
async fn log_of(dir: &Path, len: u64) {
    log_in_terms(dir, &vec![1; len as usize]).await;
}

/// A log whose entry at index `i` is in term `terms[i]`, closed.
async fn log_in_terms(dir: &Path, terms: &[u64]) {
    let log = DiskLog::open(dir, "t/ns/raft/0", config()).expect("open");
    let mut raft = RaftLog::<Types, Codec>::new(log, Codec).expect("a Raft log");
    let entries: Vec<_> = (0..)
        .zip(terms)
        .map(|(index, term)| normal(*term, index, "base"))
        .collect();
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
        base_terms: vec![(0, 1)],
        accept_loss: AcceptLoss::assumed(false),
        kept: None,
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
    start_copy(dir.path(), &start(Role::Voter), &config())
        .await
        .expect("start");

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

/// A voter holding uncommitted records past the base cuts them and starts.
#[tokio::test]
async fn a_voter_past_its_base_is_cut_back_to_it() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    start_copy(dir.path(), &start(Role::Voter), &config())
        .await
        .expect("start");
    let read = read_copy(dir.path(), SPACING, &BTreeSet::from([4])).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 1, index: 4 }));
    assert_eq!(
        held::read(dir.path(), "t", SPACING)
            .expect("held")
            .last_record
            .map(|(index, _)| index),
        Some(4)
    );
}

/// A voter whose saved commit covers records past the base cuts them only
/// in a re-form that accepts the loss.
#[tokio::test]
async fn a_voter_cuts_committed_records_past_its_base_only_on_accepted_loss() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    crate::disk_log::store_saved_commit(dir.path(), 7, 2).expect("commit");
    start_copy(dir.path(), &start(Role::Voter), &config())
        .await
        .expect_err("would cut acked records");
    assert_eq!(
        held::read(dir.path(), "t", SPACING)
            .expect("held")
            .last_record
            .map(|(index, _)| index),
        Some(6)
    );

    let mut lossy = start(Role::Voter);
    lossy.accept_loss = AcceptLoss::assumed(true);
    start_copy(dir.path(), &lossy, &config())
        .await
        .expect("start");
    let read = read_copy(dir.path(), SPACING, &BTreeSet::from([4])).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 1, index: 4 }));
}

/// A voter starting from a kept directory gets its records in place of its
/// own, cut to the base, and the kept directory is left as it was.
#[tokio::test]
async fn a_voter_starts_from_a_kept_copy() {
    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 7).await;
    let before = read_copy(kept.path(), SPACING, &BTreeSet::from([6])).expect("read");
    let dir = tempfile::tempdir().expect("dir");
    log_in_terms(dir.path(), &[3, 3]).await;
    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect("start");

    let read = read_copy(dir.path(), SPACING, &BTreeSet::from([0, 4])).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 1, index: 4 }));
    assert_eq!(read.terms[&0], Some(1));
    assert_eq!(
        read_copy(kept.path(), SPACING, &BTreeSet::from([6])).expect("read"),
        before
    );
    assert!(std::fs::read_dir(dir.path()).expect("ls").all(|entry| {
        !entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .ends_with(".copying")
    }));
}

/// Whether `dir` holds no file copied in and left unrenamed.
fn no_temporaries(dir: &Path) -> bool {
    std::fs::read_dir(dir).expect("ls").all(|entry| {
        !entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .ends_with(".copying")
    })
}

/// A kept directory that does not end in the base's term at the base's
/// last index is not the base, and the start refuses.
#[tokio::test]
async fn a_voter_whose_log_ends_in_another_term_is_refused() {
    let kept = tempfile::tempdir().expect("kept");
    log_in_terms(kept.path(), &[1, 1, 1, 1, 2]).await;
    let dir = tempfile::tempdir().expect("dir");
    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect_err("not the base");
    assert!(building(dir.path()).expect("marker"));
}

/// The kept directory's saved commit is the claim checked: one covering
/// records past the base refuses without an accepted loss.
#[tokio::test]
async fn a_kept_copy_committed_past_the_base_is_refused_without_accepted_loss() {
    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 7).await;
    crate::disk_log::store_saved_commit(kept.path(), 7, 2).expect("commit");
    let dir = tempfile::tempdir().expect("dir");
    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect_err("would cut acked records");
    from_kept.accept_loss = AcceptLoss::assumed(true);
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect("start");
}

/// The refusal names the commit that refused: the kept copy's, not the
/// voter's own commit at the first index, which covers no record of its own.
#[tokio::test]
async fn a_kept_copy_refusal_names_the_kept_commit() {
    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 7).await;
    crate::disk_log::store_saved_commit(kept.path(), 7, 2).expect("commit");
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 5).await;
    crate::disk_log::store_saved_commit(dir.path(), 8, 2).expect("commit");
    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    let err = start_copy(dir.path(), &from_kept, &config())
        .await
        .expect_err("would cut acked records");
    assert!(err.to_string().contains("committed through 6 "), "{err}");
}

/// The start's accept-loss is the plan's: a marked or vote-cut plan
/// accepts, an unmarked one does not.
#[test]
fn accept_loss_comes_from_the_plan() {
    let plan = |marked: bool, cut_by_vote: bool| crate::raft_copy::ReformPlan::<&str> {
        epoch: 1,
        base_last: None,
        source: ("a", crate::raft_copy::Source::Current),
        voters: Vec::new(),
        learners: Vec::new(),
        first_index: 0,
        top: (0, 0),
        marked: marked.then_some(crate::raft_copy::Marked::Unknown(
            crate::raft_copy::VoterSource::Seed,
        )),
        cut_by_vote,
    };
    assert_eq!(
        AcceptLoss::of(&plan(true, false)),
        AcceptLoss::assumed(true)
    );
    assert_eq!(
        AcceptLoss::of(&plan(false, true)),
        AcceptLoss::assumed(true)
    );
    assert_eq!(
        AcceptLoss::of(&plan(false, false)),
        AcceptLoss::assumed(false)
    );
}

/// A plan does not read a copy whose start did not finish.
#[tokio::test]
async fn a_half_built_copy_is_not_read_by_a_plan() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    crate::raft_log::state_file::store(dir.path(), crate::raft_copy::start::BUILDING, &[])
        .expect("marker");
    read_copy(dir.path(), SPACING, &BTreeSet::new()).expect_err("half built");
}

/// A learner with records past the base empties its log; it catches up by
/// snapshot.
#[tokio::test]
async fn a_learner_past_the_base_empties_its_log() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    let mark = dir
        .path()
        .join(crate::disk_log::durable_mark::mark_file_name());
    assert!(mark.exists());
    // Left by a copy from a kept directory that a cut stopped.
    std::fs::write(dir.path().join("9.copying"), b"partial").expect("stray");
    start_copy(dir.path(), &start(Role::Learner), &config())
        .await
        .expect("start");

    let read = read_copy(dir.path(), SPACING, &BTreeSet::new()).expect("read");
    assert_eq!((read.last, read.purged), (None, None));
    // It named a segment that is gone.
    assert!(!mark.exists());
    assert!(no_temporaries(dir.path()));
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
    start_copy(dir.path(), &start(Role::Learner), &config())
        .await
        .expect("start");
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

/// The copies the power-cut test starts: a voter at its base, a learner
/// past it, a voter cut back to it, and a voter rebuilt from a kept copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Case {
    Voter,
    Learner,
    VoterPast,
    VoterFromKept,
}

/// Lay out `case` in `dir`, and return its start.
async fn set_up(case: Case, dir: &Path, kept: &Path) -> Start {
    match case {
        Case::Voter => log_of(dir, 5).await,
        Case::Learner | Case::VoterPast => log_of(dir, 7).await,
        Case::VoterFromKept => {
            log_in_terms(dir, &[3, 3]).await;
            if std::fs::read_dir(kept).expect("ls").next().is_none() {
                log_of(kept, 7).await;
            }
        }
    }
    let mut start = start(if case == Case::Learner {
        Role::Learner
    } else {
        Role::Voter
    });
    if case == Case::VoterFromKept {
        start.kept = Some(kept.to_path_buf());
    }
    start
}

/// A power cut at any step leaves the marker, so the copy does not open, and
/// running the start again finishes it as an uncut run would.
#[tokio::test]
async fn a_start_cut_at_any_step_does_not_open_and_finishes_when_run_again() {
    for case in [
        Case::Voter,
        Case::Learner,
        Case::VoterPast,
        Case::VoterFromKept,
    ] {
        let kept = tempfile::tempdir().expect("kept");
        let whole = tempfile::tempdir().expect("dir");
        let started = set_up(case, whole.path(), kept.path()).await;
        start_copy(whole.path(), &started, &config())
            .await
            .expect("start");
        let want = read_copy(whole.path(), SPACING, &BTreeSet::from([0, 4])).expect("read");

        let mut cut_at = 0;
        loop {
            let root = Root::new();
            let dir = root.shard_dir();
            std::fs::create_dir_all(&dir).expect("dir");
            let started = set_up(case, &dir, kept.path()).await;
            crash_point::watch(&dir, Some(cut_at));
            let cut = start_copy(&dir, &started, &config()).await;
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
                start_copy(&dir, &started, &config())
                    .await
                    .expect("run again");
                cut_at += 1;
                continue;
            }
            let refused = root
                .open()
                .open_copy("orders/0", &dir, "t/ns/orders/0", config())
                .await;
            assert!(refused.is_err(), "{case:?} cut at {cut_at} opened");
            read_copy(&dir, SPACING, &BTreeSet::new()).expect_err("a plan reads a half-built copy");

            start_copy(&dir, &started, &config())
                .await
                .expect("run again");
            let mut got = read_copy(&dir, SPACING, &BTreeSet::from([0, 4])).expect("read");
            got.path = want.path.clone();
            assert_eq!(got, want, "{case:?} cut at {cut_at}");
            assert!(no_temporaries(&dir), "{case:?} cut at {cut_at}");
            cut_at += 1;
        }
        assert!(cut_at > 3, "{case:?}: only {cut_at} steps");
    }
}

/// #2640: a copy that never took a reservation is reserved through the gap
/// before it starts.
#[tokio::test]
async fn a_start_reserves_through_the_gap() {
    let dir = tempfile::tempdir().expect("dir");
    start_copy(dir.path(), &start(Role::Learner), &config())
        .await
        .expect("start");
    assert_eq!(reserved::load(dir.path()).expect("reserved"), Some(7));
}

/// A base whose term changes from 1 to 2 at index 2, ending at (2, 4).
fn two_term_learner() -> Start {
    let mut start = start(Role::Learner);
    start.base_terms = vec![(0, 1), (2, 2)];
    start.epoch.bases[0].base_last = Some(EpochLogId {
        index: 4,
        epoch: 0,
        term: 2,
    });
    start
}

/// A learner within the base keeps its prefix only where its terms are the
/// base's: one that holds a different entry at an index the base has is
/// emptied, so no two members hold different entries at one index.
#[tokio::test]
async fn a_learner_whose_terms_differ_from_the_base_is_emptied() {
    let same = tempfile::tempdir().expect("dir");
    log_in_terms(same.path(), &[1, 1, 2, 2]).await;
    start_copy(same.path(), &two_term_learner(), &config())
        .await
        .expect("start");
    let read = read_copy(same.path(), SPACING, &BTreeSet::new()).expect("read");
    assert_eq!(read.last, Some(LogPoint { term: 2, index: 3 }));

    for terms in [&[1, 1, 1, 1][..], &[1, 1, 1, 2], &[1, 2], &[2]] {
        let dir = tempfile::tempdir().expect("dir");
        log_in_terms(dir.path(), terms).await;
        start_copy(dir.path(), &two_term_learner(), &config())
            .await
            .expect("start");
        let read = read_copy(dir.path(), SPACING, &BTreeSet::new()).expect("read");
        assert_eq!((read.last, read.purged), (None, None), "{terms:?}");
    }
}

/// An emptied learner keeps no saved commit, including when the start was
/// cut right after the emptying and runs again.
#[tokio::test]
async fn an_emptied_learner_claims_no_commit() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    crate::disk_log::store_saved_commit(dir.path(), 6, 2).expect("commit");
    start_copy(dir.path(), &start(Role::Learner), &config())
        .await
        .expect("start");
    assert_eq!(
        crate::disk_log::saved_commit(dir.path()).expect("commit").0,
        0
    );

    let dir = tempfile::tempdir().expect("dir");
    crate::disk_log::store_saved_commit(dir.path(), 6, 2).expect("commit");
    start_copy(dir.path(), &start(Role::Learner), &config())
        .await
        .expect("start");
    assert_eq!(
        crate::disk_log::saved_commit(dir.path()).expect("commit").0,
        0
    );
}

/// A voter's saved commit past the first index is lowered only by a re-form
/// that accepts the loss; otherwise the start refuses and changes nothing
/// the vouch rule reads.
#[tokio::test]
async fn a_voter_commit_past_the_first_index_is_lowered_only_on_accepted_loss() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 5).await;
    crate::disk_log::store_saved_commit(dir.path(), 9, 2).expect("commit");
    start_copy(dir.path(), &start(Role::Voter), &config())
        .await
        .expect_err("would lower the commit");
    assert_eq!(
        crate::disk_log::saved_commit(dir.path()).expect("commit").0,
        9
    );
    assert!(building(dir.path()).expect("marker"));

    let mut lossy = start(Role::Voter);
    lossy.accept_loss = AcceptLoss::assumed(true);
    start_copy(dir.path(), &lossy, &config())
        .await
        .expect("start");
    assert_eq!(
        crate::disk_log::saved_commit(dir.path()).expect("commit").0,
        8
    );
}

/// A learner holding records in a gap an earlier re-form left is emptied,
/// though each record's term matches the stretch around it.
#[tokio::test]
async fn a_learner_with_records_in_an_earlier_gap_is_emptied() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    let mut later = start(Role::Learner);
    // Epoch 1 kept 0..=2 and left a gap through 7; epoch 2's base ends at 8.
    later.epoch.bases = vec![
        EpochBase {
            base_last: Some(EpochLogId {
                index: 2,
                epoch: 0,
                term: 1,
            }),
            first_index: 8,
        },
        EpochBase {
            base_last: Some(EpochLogId {
                index: 8,
                epoch: 1,
                term: 1,
            }),
            first_index: 9,
        },
    ];
    start_copy(dir.path(), &later, &config())
        .await
        .expect("start");
    let read = read_copy(dir.path(), SPACING, &BTreeSet::new()).expect("read");
    assert_eq!((read.last, read.purged), (None, None));
}

/// A kept directory that is gone fails the start before the voter's own
/// log is touched.
#[tokio::test]
async fn a_missing_kept_copy_leaves_the_voter_log_alone() {
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(dir.path().join("gone"));
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect_err("no kept copy");
    let last = held::read(dir.path(), "t", SPACING)
        .expect("held")
        .last_record;
    assert_eq!(last.map(|(index, _)| index), Some(6));
}

/// A voter rebuilt from a kept copy whose own log is committed past the
/// base needs an accepted loss too: its own log is replaced.
#[tokio::test]
async fn a_voters_own_commit_past_the_base_counts_when_rebuilt_from_a_kept_copy() {
    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 5).await;
    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 7).await;
    crate::disk_log::store_saved_commit(dir.path(), 7, 2).expect("commit");
    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect_err("would cut acked records");
    from_kept.accept_loss = AcceptLoss::assumed(true);
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect("start");
}

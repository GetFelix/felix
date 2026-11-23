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
        floor: None,
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

/// Save cuts naming `producer`'s batch `sequence` in the log in `dir`, as
/// an install or a purge leaves them: in no record.
async fn forget(dir: &Path, producer: u64, sequence: u64) {
    let log = DiskLog::open(dir, "t/ns/raft/0", config()).expect("open");
    let base = log.base_offset();
    log.remember_forgotten_cuts(
        base,
        crate::disk_log::ForgottenCuts::of([(producer, sequence)]),
    )
    .await
    .expect("remember");
    log.close().await.expect("close");
}

/// A voter rebuilt from a kept copy refuses what the kept log forgot and
/// nothing its own old log did: the fold's cuts and the new epoch's
/// re-send answers come from these. The kept log's floor comes too.
#[tokio::test]
async fn a_voter_from_a_kept_copy_carries_its_forgotten_batches_and_floor() {
    use crate::disk_log::carried_floor::{self, CarriedFloor};
    use crate::disk_log::{ProducerSequence, UnfinishedBatch};

    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 7).await;
    forget(kept.path(), 9, 0).await;
    let floor = CarriedFloor {
        offset: 2,
        floor: 2,
        headers: Bytes::from_static(b"kept"),
    };
    carried_floor::raise(kept.path(), floor.clone()).expect("floor");
    let dir = tempfile::tempdir().expect("dir");
    log_in_terms(dir.path(), &[3, 3]).await;
    forget(dir.path(), 7, 5).await;
    carried_floor::raise(
        dir.path(),
        CarriedFloor {
            offset: 1,
            floor: 1,
            headers: Bytes::from_static(b"own"),
        },
    )
    .expect("own floor");

    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect("start");

    assert_eq!(carried_floor::load(dir.path()).expect("load"), Some(floor));
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    let cuts = log.forgotten_cuts_from(8).await.expect("cuts");
    assert_eq!(
        cuts.sequence(9),
        Some(0),
        "the kept log's cut is not carried"
    );
    assert_eq!(cuts.sequence(7), None, "the voter's own cut is carried");
    assert_eq!(
        log.producer_unfinished(9, 0),
        Some(UnfinishedBatch {
            first: None,
            held: 0,
            len: 0,
            digest: None,
        }),
        "a re-send of the kept log's cut batch is not refused"
    );
    assert_eq!(log.producer_sequence(7, 5), ProducerSequence::Unknown);
    log.close().await.expect("close");
}

/// A kept copy that forgot nothing and kept no floor leaves the voter
/// none: its own old log's are not the new log's.
#[tokio::test]
async fn a_voter_from_a_kept_copy_drops_its_own_forgotten_batches_and_floor() {
    use crate::disk_log::carried_floor::{self, CarriedFloor};

    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 7).await;
    let dir = tempfile::tempdir().expect("dir");
    log_in_terms(dir.path(), &[3, 3]).await;
    forget(dir.path(), 7, 5).await;
    carried_floor::raise(
        dir.path(),
        CarriedFloor {
            offset: 1,
            floor: 1,
            headers: Bytes::from_static(b"own"),
        },
    )
    .expect("own floor");

    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    start_copy(dir.path(), &from_kept, &config())
        .await
        .expect("start");

    assert_eq!(carried_floor::load(dir.path()).expect("load"), None);
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    let cuts = log.forgotten_cuts_from(8).await.expect("cuts");
    assert!(
        cuts.is_empty(),
        "the voter's own cuts are carried: {cuts:?}"
    );
    log.close().await.expect("close");
}

/// An emptied learner drops its own forgotten batches and floor, as a voter
/// rebuilt from a kept copy does: a re-send of a batch only its old log cut
/// is taken alike on both, not refused on the learner alone. Its cuts
/// arrive with the snapshot it installs.
#[tokio::test]
async fn an_emptied_learner_drops_its_forgotten_batches_and_floor() {
    use crate::disk_log::ProducerSequence;
    use crate::disk_log::carried_floor::{self, CarriedFloor};

    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 7).await;
    let voter = tempfile::tempdir().expect("voter");
    log_of(voter.path(), 7).await;
    forget(voter.path(), 7, 5).await;
    let learner = tempfile::tempdir().expect("learner");
    log_of(learner.path(), 7).await;
    forget(learner.path(), 7, 5).await;
    for dir in [voter.path(), learner.path()] {
        carried_floor::raise(
            dir,
            CarriedFloor {
                offset: 1,
                floor: 1,
                headers: Bytes::from_static(b"own"),
            },
        )
        .expect("own floor");
    }

    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    start_copy(voter.path(), &from_kept, &config())
        .await
        .expect("voter start");
    start_copy(learner.path(), &start(Role::Learner), &config())
        .await
        .expect("learner start");

    for (name, dir) in [("voter", voter.path()), ("learner", learner.path())] {
        assert_eq!(
            carried_floor::load(dir).expect("load"),
            None,
            "the {name} kept its own floor"
        );
        let log = DiskLog::open(dir, "t/ns/raft/0", config()).expect("open");
        assert_eq!(
            log.producer_sequence(7, 5),
            ProducerSequence::Unknown,
            "the {name} refuses a re-send of its own old cut"
        );
        let cuts = log
            .forgotten_cuts_from(log.base_offset())
            .await
            .expect("cuts");
        assert!(cuts.is_empty(), "the {name} carries its own cuts: {cuts:?}");
        log.close().await.expect("close");
    }
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

/// A plan that needs the loss accepted starts only when the operator
/// accepted it; one that does not never lowers a commit.
#[test]
fn accept_loss_needs_the_operator_for_a_lossy_plan() {
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
        floor: None,
    };
    for (marked, cut) in [(true, false), (false, true)] {
        assert_eq!(
            AcceptLoss::of(&plan(marked, cut), false),
            Err(crate::raft_copy::LossNotAccepted)
        );
        assert_eq!(
            AcceptLoss::of(&plan(marked, cut), true),
            Ok(AcceptLoss::assumed(true))
        );
    }
    for accepted in [false, true] {
        assert_eq!(
            AcceptLoss::of(&plan(false, false), accepted),
            Ok(AcceptLoss::assumed(false))
        );
    }
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
            // A survey answers a start from this, and is not asked again.
            assert!(
                !crate::raft_copy::started_in(&dir, 1).expect("started"),
                "{case:?} cut at {cut_at} reads as started"
            );
            read_copy(&dir, SPACING, &BTreeSet::new()).expect_err("a plan reads a half-built copy");

            start_copy(&dir, &started, &config())
                .await
                .expect("run again");
            let mut got = read_copy(&dir, SPACING, &BTreeSet::from([0, 4])).expect("read");
            got.path = want.path.clone();
            assert_eq!(got, want, "{case:?} cut at {cut_at}");
            assert!(crate::raft_copy::started_in(&dir, 1).expect("started"));
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

/// #2602 (ReformKeepsOwnFloor): this copy kept floor 3, another copy read
/// by the plan floor 5. The new epoch refuses every read below 5, with the
/// trim headers of the copy that held it.
#[tokio::test]
async fn a_started_copy_keeps_the_highest_floor_read() {
    use crate::disk_log::carried_floor::{self, CarriedFloor};
    use crate::log::{AppendOnlyLog, ReadRange};

    let dir = tempfile::tempdir().expect("dir");
    log_of(dir.path(), 5).await;
    let own = CarriedFloor {
        offset: 3,
        floor: 3,
        headers: Bytes::from_static(b"three"),
    };
    carried_floor::raise(dir.path(), own).expect("own floor");
    let highest = CarriedFloor {
        offset: 6,
        floor: 5,
        headers: Bytes::from_static(b"five"),
    };
    let started = Start {
        floor: Some(highest.clone()),
        ..start(Role::Voter)
    };
    start_copy(dir.path(), &started, &config())
        .await
        .expect("start");

    assert_eq!(
        carried_floor::load(dir.path()).expect("load"),
        Some(highest)
    );
    let log = DiskLog::open(dir.path(), "t/ns/raft/0", config()).expect("open");
    for below in [0, 3, 4] {
        match log
            .read_range(ReadRange {
                start: below,
                max_bytes: usize::MAX,
            })
            .await
        {
            Err(crate::StorageError::Trimmed {
                oldest,
                trim_headers,
                ..
            }) => {
                assert_eq!(oldest, 5, "read at {below}");
                assert_eq!(trim_headers.as_deref(), Some(&b"five"[..]));
            }
            other => panic!("read at {below} below floor 5: {other:?}"),
        }
    }
    log.close().await.expect("close");
}

/// #2602: a start made from a plan carries the plan's floor, so the re-form
/// driver cannot start a copy that serves reads below it.
#[test]
fn a_start_from_a_plan_carries_its_floor() {
    let floor = crate::disk_log::CarriedFloor {
        offset: 6,
        floor: 5,
        headers: Bytes::from_static(b"five"),
    };
    let plan = crate::raft_copy::ReformPlan::<&str> {
        epoch: 1,
        base_last: None,
        source: ("a", crate::raft_copy::Source::Current),
        voters: Vec::new(),
        learners: Vec::new(),
        first_index: 7,
        top: (0, 0),
        marked: None,
        cut_by_vote: false,
        floor: Some(floor.clone()),
    };
    let base = start(Role::Voter);
    let copy = crate::raft_copy::CopyStart {
        epoch: base.epoch.clone(),
        role: base.role,
        base_terms: base.base_terms.clone(),
        kept: None,
        meta: base.meta.clone(),
        data: base.data.clone(),
    };
    let started = Start::of(&plan, false, copy).expect("start");
    assert_eq!(started.floor, Some(floor));
    assert_eq!(started.accept_loss, AcceptLoss::assumed(false));
}

/// What a fold saw in the voter's directory when the start ran it.
#[derive(Default)]
struct Seen(parking_lot::Mutex<Option<Option<crate::raft_log::StoredSnapshot>>>);

impl crate::raft_copy::GapFold for Seen {
    fn fold<'a>(
        &'a self,
        dir: &'a Path,
        _config: &'a crate::log::LogConfig,
        _base: Option<LogPoint>,
        gap_end: LogPoint,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::io::Result<crate::raft_log::StoredSnapshot>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            *self.0.lock() = Some(crate::raft_log::snapshot::read(dir)?);
            Ok(crate::raft_log::StoredSnapshot {
                last: Some(crate::raft_log::SnapshotId {
                    term: gap_end.term,
                    index: gap_end.index,
                }),
                meta: bytes::Bytes::new(),
                data: bytes::Bytes::from_static(b"folded"),
            })
        })
    }
}

/// A voter rebuilt from a kept copy folds from the kept copy's snapshot,
/// which its log may be purged below, and stores the fold at the gap's end.
#[tokio::test]
async fn a_voter_from_a_kept_copy_folds_from_its_snapshot() {
    let kept = tempfile::tempdir().expect("kept");
    log_of(kept.path(), 7).await;
    let snapshot = crate::raft_log::StoredSnapshot {
        last: Some(crate::raft_log::SnapshotId { term: 1, index: 2 }),
        meta: bytes::Bytes::from_static(b"meta"),
        data: bytes::Bytes::from_static(b"kept state"),
    };
    crate::raft_log::snapshot::store_in(kept.path(), &snapshot).expect("snapshot");
    let dir = tempfile::tempdir().expect("dir");
    log_in_terms(dir.path(), &[3, 3]).await;
    let mut from_kept = start(Role::Voter);
    from_kept.kept = Some(kept.path().to_path_buf());
    let seen = Seen::default();
    crate::raft_copy::start::start_with(dir.path(), &from_kept, &config(), Some(&seen))
        .await
        .expect("start");

    assert_eq!(seen.0.lock().clone(), Some(Some(snapshot)));
    let stored = crate::raft_log::snapshot::read(dir.path())
        .expect("read")
        .expect("a snapshot");
    assert_eq!(stored.data.as_ref(), b"folded");
}

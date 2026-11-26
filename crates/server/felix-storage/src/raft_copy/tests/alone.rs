//! A group of one re-forming itself after a relaxed crash rotated its copy.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::*;
use crate::disk_log::crash_point;
use crate::raft_copy::{Alone, Aloned, EpochRecord, LocalRecord, WireReport};
use crate::raft_log::RaftLog;
use crate::raft_log::tests::writes::append;
use crate::raft_log::tests::{Codec, Types, normal};
use crate::run_state::{self, BootId};

type Log = RaftLog<Types, Codec>;

const LABEL: &str = "t/ns/orders/0";
const BROKER: &str = "standalone";

struct NoMemberships;

impl MembershipReader<Arc<str>> for NoMemberships {
    fn voter_sets(&self, _payload: &[u8]) -> io::Result<VoterSets<Arc<str>>> {
        Ok(Vec::new())
    }

    fn snapshot_voter_sets(&self, _meta: &[u8]) -> io::Result<Option<(u64, VoterSets<Arc<str>>)>> {
        Ok(None)
    }
}

/// A fold that stores `b"folded"` at the gap's end.
struct Fold;

impl GapFold for Fold {
    fn fold<'a>(
        &'a self,
        _dir: &'a Path,
        _log: &'a crate::DiskLog,
        _base: Option<LogPoint>,
        gap_end: LogPoint,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = io::Result<crate::raft_log::StoredSnapshot>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
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

fn boot(n: u8) -> Option<BootId> {
    Some(BootId::from_bytes([n; 16]))
}

/// What a relaxed crash does to the root: the run record says it was
/// running in another boot, so `begin` rotates the id and records why.
fn relaxed_crash(root: &Root, n: u8) {
    let mut next = [10 * n; 16];
    next[0] = n;
    run_state::begin(root.path(), true, None, boot(n), || false, || next).expect("begin");
}

/// A copy holding `count` records, all committed, closed.
async fn written_copy(copies: &CopyRoot, dir: &Path, count: u64) -> u64 {
    written_copy_saved(copies, dir, count, count).await
}

/// A copy holding `count` synced records whose saved commit covers only the
/// first `saved`, as a commit saved at most once a second leaves it.
async fn written_copy_saved(copies: &CopyRoot, dir: &Path, count: u64, saved: u64) -> u64 {
    let opened = copies
        .open_copy(SHARD, dir, LABEL, config())
        .await
        .expect("open");
    let mut log = Log::new(opened.log.clone(), Codec).expect("a Raft log");
    log.mark_with(opened.marks.clone(), LogMarkMode::Exact)
        .expect("marks");
    let entries: Vec<_> = (0..count).map(|index| normal(1, index, "x")).collect();
    append(&mut log, &entries)
        .await
        .expect("append")
        .await
        .expect("flush");
    opened.log.save_vote(2, "1", true).await.expect("vote");
    opened.marks.vote(vote(2, "1", true)).await.expect("mark");
    opened
        .log
        .save_committed(
            saved - 1,
            crate::raft_log::generation_of(1).expect("generation"),
        )
        .await
        .expect("commit");
    drop(log);
    opened.log.close().await.expect("close");
    opened.copy
}

async fn reform(copies: &CopyRoot, dir: &Path) -> io::Result<Aloned> {
    let spacing = config().index_spacing_bytes;
    let dir_owned = dir.to_path_buf();
    let read = move |probes: &BTreeSet<u64>, epoch: u64| {
        build_report(&dir_owned, spacing, probes, epoch, false, &NoMemberships)
            .map(|report| WireReport::from(&report))
    };
    let fold = |_copy: u64| Box::new(Fold) as Box<dyn GapFold>;
    let member = |copy: u64| format!("{BROKER}#{copy}");
    copies
        .reform_alone(Alone {
            shard: SHARD,
            dir,
            label: LABEL,
            incarnation: 1,
            broker: BROKER,
            config: &config(),
            read: &read,
            fold: &fold,
            member: &member,
            record: &LocalRecord,
            floor: None,
        })
        .await
}

async fn stop(copies: &CopyRoot, dir: &Path) -> io::Result<Option<crate::raft_copy::AlonePlan>> {
    stop_if(copies, dir, &|_| true).await
}

/// [`stop`], stopping only for a plan `may_stop` accepts.
async fn stop_if(
    copies: &CopyRoot,
    dir: &Path,
    may_stop: &(dyn Fn(&crate::raft_copy::AlonePlan) -> bool + Sync),
) -> io::Result<Option<crate::raft_copy::AlonePlan>> {
    let spacing = config().index_spacing_bytes;
    let dir_owned = dir.to_path_buf();
    let read = move |probes: &BTreeSet<u64>, epoch: u64| {
        build_report(&dir_owned, spacing, probes, epoch, false, &NoMemberships)
            .map(|report| WireReport::from(&report))
    };
    let fold = |_copy: u64| Box::new(Fold) as Box<dyn GapFold>;
    let member = |copy: u64| format!("{BROKER}#{copy}");
    copies
        .stop_alone(
            Alone {
                shard: SHARD,
                dir,
                label: LABEL,
                incarnation: 1,
                broker: BROKER,
                config: &config(),
                read: &read,
                fold: &fold,
                member: &member,
                record: &LocalRecord,
                floor: None,
            },
            may_stop,
        )
        .await
}

/// A root whose storage id a relaxed crash rotated after the shard wrote
/// `count` records, and a gate that passes on it.
async fn crashed(count: u64) -> (Root, PathBuf) {
    crashed_saved(count, count).await
}

async fn crashed_saved(count: u64, saved: u64) -> (Root, PathBuf) {
    let root = Root::new();
    run_state::begin(root.path(), true, None, boot(1), || false, || [9; 16]).expect("begin");
    let dir = root.shard_dir();
    written_copy_saved(&root.open(), &dir, count, saved).await;
    relaxed_crash(&root, 2);
    root.witness_current_id();
    (root, dir)
}

#[tokio::test]
async fn a_relaxed_crash_re_forms_a_group_of_one_past_every_offset_it_gave_out() {
    let (root, dir) = crashed(3).await;
    let copies = root.open();
    let done = reform(&copies, &dir).await.expect("re-form");
    assert_eq!(done, Aloned::Reformed { epoch: 1 });
    let top = highest_reserved(&dir)
        .expect("reserved")
        .expect("a reservation");
    let bases = crate::raft_copy::epoch_file::load(&dir)
        .expect("epoch")
        .expect("an epoch file")
        .bases;
    assert_eq!(bases.len(), 1);
    assert!(top >= 3, "the crashed run reserved past its records: {top}");
    assert!(
        bases[0].first_index > top,
        "the epoch starts past every index the crashed run reserved"
    );
    assert_eq!(bases[0].base_end(), 3, "every committed record survives");
    // Running again finds the work done, and the copy opens as its own voter.
    assert_eq!(reform(&copies, &dir).await.expect("again"), Aloned::NotOwed);
    let opened = copies
        .open_copy(SHARD, &dir, LABEL, config())
        .await
        .expect("open");
    assert!(opened.rotation.is_none());
    opened.log.close().await.expect("close");
}

/// A shard no start opened through a long run of crashes is still re-formed:
/// the record keeps the id its copy was minted under however many crashes
/// followed (#3119).
#[tokio::test]
async fn a_copy_not_opened_through_many_crashes_is_still_re_formed() {
    let (root, dir) = crashed(3).await;
    for n in 3..=12 {
        relaxed_crash(&root, n);
    }
    root.witness_current_id();
    let copies = root.open();
    let done = reform(&copies, &dir).await.expect("re-form");
    assert_eq!(done, Aloned::Reformed { epoch: 1 });
    let bases = crate::raft_copy::epoch_file::load(&dir)
        .expect("epoch")
        .expect("an epoch file")
        .bases;
    assert_eq!(bases[0].base_end(), 3, "every committed record survives");
}

#[tokio::test]
async fn a_re_form_keeps_synced_records_past_the_saved_commit() {
    let (root, dir) = crashed_saved(5, 2).await;
    let copies = root.open();
    let done = reform(&copies, &dir).await.expect("re-form");
    assert_eq!(done, Aloned::Reformed { epoch: 1 });
    let bases = crate::raft_copy::epoch_file::load(&dir)
        .expect("epoch")
        .expect("an epoch file")
        .bases;
    assert_eq!(bases[0].base_end(), 5, "every synced record survives");
    assert!(bases[0].first_index >= 5);
}

#[tokio::test]
async fn without_a_relaxed_crash_nothing_is_re_formed() {
    let root = Root::new();
    run_state::begin(root.path(), true, None, boot(1), || false, || [9; 16]).expect("begin");
    let dir = root.shard_dir();
    written_copy(&root.open(), &dir, 3).await;
    // A clean restart keeps the id, and nothing was rotated.
    let copies = root.open();
    assert_eq!(reform(&copies, &dir).await.expect("run"), Aloned::NotOwed);
    assert_eq!(group_epoch(&dir).expect("epoch"), None);
}

#[tokio::test]
async fn an_operator_rotation_after_the_crash_voids_the_evidence() {
    let (root, dir) = crashed(3).await;
    crate::run_state::copy_trust::rotate_storage_id(root.path(), [77; 16]).expect("rotate");
    root.witness_current_id();
    let copies = root.open();
    assert_eq!(reform(&copies, &dir).await.expect("run"), Aloned::NotOwed);
    assert_eq!(group_epoch(&dir).expect("epoch"), None);
}

/// A crash at every step of the re-form, then a run again: the group
/// serves in one new epoch and no index below it is given out again.
#[tokio::test]
async fn a_crash_at_every_step_resumes_without_reusing_an_offset() {
    let mut stop = 0;
    loop {
        let (root, dir) = crashed(3).await;
        let parent = dir.parent().expect("parent").to_path_buf();
        let copies = root.open();
        crash_point::watch_under(&parent, Some(stop));
        let first = reform(&copies, &dir).await;
        let steps = crash_point::unwatch(&parent);
        let reached = first.is_err();
        let copies = root.open();
        let done = reform(&copies, &dir).await;
        let epoch = group_epoch(&dir).expect("epoch");
        assert!(
            matches!(
                done,
                Ok(Aloned::Reformed { epoch: 1 }) | Ok(Aloned::NotOwed)
            ),
            "stop {stop}: {done:?}"
        );
        assert_eq!(epoch, Some(1), "stop {stop}");
        let file = crate::raft_copy::epoch_file::load(&dir)
            .expect("epoch")
            .expect("file");
        assert_eq!(file.bases.len(), 1, "stop {stop}: one epoch, not two");
        assert!(file.bases[0].first_index >= 3, "stop {stop}");
        assert_eq!(
            file.bases[0].base_end(),
            3,
            "stop {stop}: the committed records survive"
        );
        if !reached {
            assert!(steps <= stop, "{steps} steps, stopped at {stop}");
            break;
        }
        stop += 1;
    }
    assert!(stop > 3, "the re-form has several steps to crash at");
}

/// A second relaxed crash while the start is half built: the half-built
/// copy is set aside, and the group plans again above the first plan.
#[tokio::test]
async fn a_second_crash_mid_start_plans_again() {
    let (root, dir) = crashed(3).await;
    let parent = dir.parent().expect("parent").to_path_buf();
    // Stop after the plan is recorded and the copy stopped.
    let mut stop = 0;
    loop {
        let copies = root.open();
        crash_point::watch_under(&parent, Some(stop));
        let first = reform(&copies, &dir).await;
        crash_point::unwatch(&parent);
        if first.is_err() && crate::raft_copy::building(&dir).expect("building") {
            break;
        }
        assert!(first.is_err() || stop > 40, "never built");
        stop += 1;
        if first.is_ok() {
            panic!("the start finished before a crash could leave it half built");
        }
    }
    let first_plan = LocalRecord
        .load(&dir)
        .expect("load")
        .expect("the first plan")
        .bases[0]
        .first_index;
    let top = highest_reserved(&dir)
        .expect("reserved")
        .expect("a reservation");
    relaxed_crash(&root, 3);
    root.witness_current_id();
    let copies = root.open();
    let done = reform(&copies, &dir).await.expect("re-form again");
    assert!(matches!(done, Aloned::Reformed { .. }));
    let file = crate::raft_copy::epoch_file::load(&dir)
        .expect("epoch")
        .expect("file");
    assert_eq!(file.bases.len(), 1);
    assert_eq!(file.bases[0].base_end(), 3);
    assert!(
        file.bases[0].first_index > top,
        "the second plan starts past every reservation"
    );
    assert!(
        file.bases[0].first_index >= first_plan,
        "the second plan is not below the first"
    );
    assert!(!crate::raft_copy::building(&dir).expect("building"));
}

#[test]
fn a_plan_is_kept_across_a_restart() {
    let dir = tempfile::tempdir().expect("dir");
    assert_eq!(LocalRecord.load(dir.path()).expect("load"), None);
    let plan = crate::raft_copy::AlonePlan {
        copy: 7,
        minted: "id".to_owned(),
        bases: vec![WireBase {
            base_last: Some((2, 0, 1)),
            first_index: 9,
        }],
        start: WireStart {
            voter: Some(7),
            kept: None,
            floor: None,
            needs_accept_loss: true,
            accepted: true,
        },
        last: Some(2),
    };
    LocalRecord.store(dir.path(), &plan).expect("store");
    assert_eq!(LocalRecord.load(dir.path()).expect("load"), Some(plan));
}

/// **The cluster's half plans and stops the copy and writes no epoch**: the
/// control plane's record decides the epoch, so the copy stays at epoch 0
/// until a start. A resend gets the same plan, and a crash after the stop
/// leaves the copy stopped for it.
#[tokio::test]
async fn stopping_alone_plans_and_stops_without_writing_an_epoch() {
    let (root, dir) = crashed(3).await;
    let copies = root.open();
    let plan = stop(&copies, &dir).await.expect("stop").expect("owed");
    assert_eq!(plan.bases.len(), 1);
    assert_eq!(group_epoch(&dir).expect("epoch"), None, "no epoch written");
    assert!(
        crate::raft_copy::stop_file::stopped(&dir)
            .expect("stopped")
            .is_some()
    );
    assert!(crate::raft_copy::start_due(&dir, 1).expect("due"));
    let again = stop(&root.open(), &dir).await.expect("again");
    assert_eq!(again, Some(plan.clone()), "the same plan is sent again");
    assert_eq!(group_epoch(&dir).expect("epoch"), None);
    // The start the control plane's record drives finishes the re-form, and
    // nothing is owed after it.
    let done = reform(&root.open(), &dir).await.expect("start");
    assert_eq!(done, Aloned::Reformed { epoch: 1 });
    assert_eq!(group_epoch(&dir).expect("epoch"), Some(1));
    assert_eq!(stop(&root.open(), &dir).await.expect("after"), None);
}

/// **A second crash after the control plane recorded the first plan plans
/// again, and a plan the record does not hold leaves the copy unstopped.**
/// The first plan's epoch is recorded, so the second plan for the same
/// epoch, under a new copy id, matches nothing and must not stop the copy:
/// a stopped copy is one a start from the record's seed would run on.
#[tokio::test]
async fn stopping_alone_after_another_crash_plans_again() {
    let (root, dir) = crashed(3).await;
    let recorded = stop(&root.open(), &dir).await.expect("stop").expect("owed");
    relaxed_crash(&root, 3);
    root.witness_current_id();
    let held = |plan: &crate::raft_copy::AlonePlan| {
        plan.copy == recorded.copy && plan.bases == recorded.bases
    };
    let second = stop_if(&root.open(), &dir, &held)
        .await
        .expect("stop again");
    assert_eq!(second, None, "a plan the record does not hold");
    let planned = LocalRecord.load(&dir).expect("load").expect("a plan");
    assert_ne!(planned.minted, recorded.minted, "planned under the new id");
    assert_ne!(planned.copy, recorded.copy, "for the rotated copy");
    assert!(planned.bases[0].first_index >= recorded.bases[0].first_index);
    assert!(
        crate::raft_copy::stop_file::stopped(&dir)
            .expect("stopped")
            .is_none(),
        "the copy was stopped for a plan the record does not hold"
    );
    assert!(!crate::raft_copy::start_due(&dir, 1).expect("due"));
    assert_eq!(group_epoch(&dir).expect("epoch"), None);
}

#[tokio::test]
async fn stopping_alone_without_a_crash_owes_nothing() {
    let root = Root::new();
    run_state::begin(root.path(), true, None, boot(1), || false, || [9; 16]).expect("begin");
    let dir = root.shard_dir();
    written_copy(&root.open(), &dir, 3).await;
    assert_eq!(stop(&root.open(), &dir).await.expect("run"), None);
    assert!(
        crate::raft_copy::stop_file::stopped(&dir)
            .expect("stopped")
            .is_none()
    );
}

/// What a restore through the tools does to the root after the shard wrote
/// `count` records: the id is rotated with the restore marked, and the
/// start after it rotates once more.
async fn restored(count: u64) -> (Root, PathBuf) {
    let root = Root::new();
    run_state::begin(root.path(), true, None, boot(1), || false, || [9; 16]).expect("begin");
    let dir = root.shard_dir();
    written_copy(&root.open(), &dir, count).await;
    restore_again(&root, 5);
    (root, dir)
}

fn restore_again(root: &Root, n: u8) {
    run_state::rotate_for_restore(root.path(), [n; 16]).expect("restore");
    run_state::begin(root.path(), true, None, boot(n), || false, || [n + 100; 16]).expect("begin");
    root.witness_current_id();
}

fn first_indexes(dir: &Path) -> Vec<u64> {
    crate::raft_copy::epoch_file::load(dir)
        .expect("epoch")
        .expect("an epoch file")
        .bases
        .iter()
        .map(|base| base.first_index)
        .collect()
}

const R: u64 = crate::raft_copy::RESTORE_OFFSETS_PER_MILLI;
const GAP: u64 = crate::raft_copy::DEFAULT_OFFSET_GAP;
/// How far the line must clear an earlier epoch with the default gap:
/// 2^32 offsets, 2^18 ms (about 4.4 minutes) of clock.
const MARGIN_MILLIS: u64 = crate::raft_copy::MIN_OFFSET_GAP / R;
const T0: u64 = 1_800_000_000_000;

/// The highest offset an epoch starting at `first` at true time `began`
/// can have given out by `now`, at half the line's slope.
fn last_given(first: u64, began: u64, now: u64) -> u64 {
    first + R / 2 * (now - began)
}

/// The line epoch planned at `T0` by a clock that read `T0 + ahead`.
fn line_epoch(ahead: u64) -> u64 {
    R * (T0 + ahead) + GAP
}

use crate::raft_copy::alone::restored_first;

/// The clock is behind the one that planned an earlier epoch on the line:
/// the line now is below that epoch's first index, and without a refusal
/// the max with the copy's own first index is all that kept it apart.
#[test]
fn a_restore_on_a_clock_behind_an_earlier_epoch_is_refused() {
    let earlier = line_epoch(0);
    let now = T0 - 60_000;
    let err = restored_first(3, Some(earlier), now).expect_err("refused");
    assert!(err.contains("clears in about 5.4 min"), "{err}");
    assert!(err.contains("check that this clock is not behind"), "{err}");
}

/// A clock stepped back by less than the margin: one minute back, the plan
/// comes 90 s of true time after the earlier epoch began, so the line has
/// moved only 30 s while that epoch can have given out 45 s worth. Refused
/// until the clock reads the margin past that epoch's plan, and the first
/// plan let through starts past everything it gave out.
#[test]
fn a_clock_stepped_back_less_than_the_margin_waits_until_the_line_clears() {
    let earlier = line_epoch(0);
    let back = 60_000;
    let clock = |elapsed: u64| T0 + elapsed - back;
    let line = R * clock(90_000) + GAP;
    assert!(
        line < last_given(earlier, T0, T0 + 90_000),
        "the case reuses offsets without the refusal"
    );
    assert!(restored_first(3, Some(earlier), clock(90_000)).is_err());
    let through = back + MARGIN_MILLIS + 1;
    assert!(restored_first(3, Some(earlier), clock(through - 1)).is_err());
    let first = restored_first(3, Some(earlier), clock(through)).expect("a plan");
    assert!(first > last_given(earlier, T0, T0 + through));
}

/// A clock stepped back by more than the margin, ten minutes: every plan
/// while it reads below the earlier epoch's plan time plus the margin is
/// refused, so the shard waits out the step back. Past that, the bound
/// assumes clocks within the margin (`docs/per-shard-raft.md`).
#[test]
fn a_clock_stepped_back_more_than_the_margin_is_refused_while_it_reads_behind() {
    let earlier = line_epoch(0);
    let back = 10 * 60_000;
    for elapsed in [1, 60_000, back, back + MARGIN_MILLIS] {
        let clock = T0 + elapsed - back;
        assert!(
            restored_first(3, Some(earlier), clock).is_err(),
            "{elapsed} ms after the earlier epoch"
        );
    }
    assert!(restored_first(3, Some(earlier), T0 + MARGIN_MILLIS + 1).is_ok());
}

/// The line exactly the margin above what was given is refused; one past is
/// taken, and starts on the line.
#[test]
fn a_line_exactly_the_margin_above_is_refused() {
    let line = R * T0 + GAP;
    let margin = crate::raft_copy::MIN_OFFSET_GAP;
    assert!(restored_first(3, Some(line - margin), T0).is_err());
    assert_eq!(restored_first(3, Some(line - margin - 1), T0), Ok(line));
    // A copy that is past the line keeps its own first index.
    assert_eq!(restored_first(line + 7, Some(0), T0), Ok(line + 7));
    assert_eq!(restored_first(3, None, T0), Ok(line));
}

/// A restore re-forms the shard on the line, two relaxed crashes in quick
/// succession re-form it again with no wait, since a crash's epoch starts
/// past what the copy reserved and not on the clock, and only a second
/// restore within the margin of the first is refused, leaving the copy
/// unstopped.
#[tokio::test]
async fn crashes_right_after_a_restore_re_form_and_a_second_restore_waits() {
    let (root, dir) = restored(3).await;
    let copies = root.open();
    assert_eq!(
        reform(&copies, &dir).await.expect("restore"),
        Aloned::Reformed { epoch: 1 }
    );
    let line = first_indexes(&dir)[0];
    assert!(
        line > GAP,
        "the restore's epoch is on the time line: {line}"
    );
    for (n, epoch) in [(3, 2), (4, 3)] {
        relaxed_crash(&root, n);
        root.witness_current_id();
        let copies = root.open();
        assert_eq!(
            reform(&copies, &dir).await.expect("crash"),
            Aloned::Reformed { epoch },
            "crash {n}"
        );
    }
    let firsts = first_indexes(&dir);
    // Nothing was given out in between, so each crash's epoch may start
    // where the last did; none goes back below the line.
    assert!(firsts.windows(2).all(|w| w[1] >= w[0]), "{firsts:?}");

    restore_again(&root, 6);
    let copies = root.open();
    let err = reform(&copies, &dir).await.expect_err("too soon");
    assert!(err.to_string().contains("clears in about"), "{err}");
    assert_eq!(group_epoch(&dir).expect("epoch"), Some(3));
    assert!(
        crate::raft_copy::stopped(&dir).expect("stop").is_none(),
        "a refused plan leaves the copy as it is"
    );
    // The next pass is refused the same way: the copy is still owed a
    // restore's re-form, not taken for a crash's, which would start low.
    let copies = root.open();
    let again = reform(&copies, &dir).await.expect_err("still too soon");
    assert!(again.to_string().contains("clears in about"), "{again}");
    assert_eq!(group_epoch(&dir).expect("epoch"), Some(3));

    // Once the clock passes the bound, the pass re-forms it on the line.
    let ahead = MARGIN_MILLIS + 1000;
    crate::raft_copy::alone::CLOCK_AHEAD_MILLIS.with(|clock| clock.set(ahead));
    let copies = root.open();
    let done = reform(&copies, &dir).await;
    crate::raft_copy::alone::CLOCK_AHEAD_MILLIS.with(|clock| clock.set(0));
    assert_eq!(done.expect("re-form"), Aloned::Reformed { epoch: 4 });
    let firsts = first_indexes(&dir);
    assert!(
        firsts[3] > firsts[2] + crate::raft_copy::MIN_OFFSET_GAP,
        "the retry starts on the line past the earlier epochs: {firsts:?}"
    );
}

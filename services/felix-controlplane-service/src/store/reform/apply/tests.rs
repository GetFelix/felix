//! Applying plans against emulated brokers.
use std::collections::BTreeSet;
use std::time::Instant;

use super::super::tests::{LOG, answer, copy, shard};
use super::super::{ReformPlans, ReformRestore, ReformScope, RestoreShard, SurveyKind};
use super::*;
use felix_storage::raft_copy::{DEFAULT_OFFSET_GAP, MIN_OFFSET_GAP};

const NODES: [&str; 3] = ["a", "b", "c"];

/// Answer every survey of `kind` each node is asked, with `read`'s copy.
fn answer_all(
    plans: &ReformPlans,
    now: Instant,
    kind: SurveyKind,
    read: impl Fn(&str, &BTreeSet<u64>) -> felix_storage::raft_copy::WireCopy,
) -> usize {
    let mut asked = 0;
    for _ in 0..8 {
        let mut any = false;
        for node in NODES {
            for survey in plans.surveys_for(node, now) {
                assert_eq!(survey.kind, kind, "{survey:?}");
                any = true;
                asked += 1;
                let answer = answer(&survey, read(node, &survey.probes));
                assert_eq!(plans.record(node, vec![answer], now), (1, 0));
            }
        }
        if !any {
            break;
        }
    }
    asked
}

/// A plan surveyed to `done` with every broker answering `read`.
fn planned(
    now: Instant,
    read: impl Fn(&str, &BTreeSet<u64>) -> felix_storage::raft_copy::WireCopy,
) -> (ReformPlans, String) {
    let plans = ReformPlans::default();
    let id = plans.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    answer_all(&plans, now, SurveyKind::Survey, read);
    assert_eq!(plans.view(&id, now).unwrap().state, "done");
    (plans, id)
}

fn apply_state(plans: &ReformPlans, id: &str, now: Instant) -> ReformApplyView {
    let view = plans.view(id, now).unwrap();
    view.shards[0].apply.clone().expect("applied")
}

fn whole(_: &str, probes: &BTreeSet<u64>) -> felix_storage::raft_copy::WireCopy {
    copy(&LOG, Some(4), Some(2), probes)
}

#[test]
fn a_plan_is_written_only_after_every_broker_stopped() {
    let now = Instant::now();
    let (plans, id) = planned(now, whole);
    assert_eq!(plans.apply(&id, &[], now), Ok(1));
    assert_eq!(apply_state(&plans, &id, now).state, "stopping");
    assert!(
        plans.take_due().is_empty(),
        "nothing is due before the stops"
    );

    assert_eq!(answer_all(&plans, now, SurveyKind::Stop, whole), 3);
    let mut due = plans.take_due();
    assert_eq!(due.len(), 1, "the compare passed");
    let due = due.remove(0);
    let (key, start) = (&due.key, &due.start);
    assert_eq!(key, &shard().key);
    assert_eq!((start.incarnation, start.expected_epoch), (3, 0));
    assert_eq!(start.base.base_last.map(|l| l.index), Some(4));
    assert_eq!(start.voters.len(), 3);
    assert_eq!(apply_state(&plans, &id, now).state, "writing");
    assert!(plans.take_due().is_empty(), "a write is taken once");

    due.answer(Ok(1));
    let view = apply_state(&plans, &id, now);
    assert_eq!((view.state.as_str(), view.epoch), ("starting", Some(1)));
    assert_eq!(answer_all(&plans, now, SurveyKind::Start, whole), 3);
    assert_eq!(apply_state(&plans, &id, now).state, "started");
    assert_eq!(plans.view(&id, now).unwrap().state, "applied");
}

/// A broker that predates stop rounds answers a stop as a read-only survey.
/// Its copy did not stop, so nothing is written.
#[test]
fn a_stop_answered_as_a_survey_is_ignored_and_the_round_fails() {
    let now = Instant::now();
    let (plans, id) = planned(now, whole);
    plans.apply(&id, &[], now).unwrap();
    for node in NODES {
        for survey in plans.surveys_for(node, now) {
            let mut old = answer(&survey, whole(node, &survey.probes));
            if node == "c" {
                old.kind = SurveyKind::Survey;
                assert_eq!(plans.record(node, vec![old], now), (0, 1));
            } else {
                assert_eq!(plans.record(node, vec![old], now), (1, 0));
            }
        }
    }
    assert!(plans.take_due().is_empty());
    let view = apply_state(&plans, &id, now);
    assert_eq!(
        (view.state.as_str(), view.waiting_on),
        ("stopping", vec!["c".to_string()])
    );

    let later = now + ROUND_TIMEOUT;
    let view = apply_state(&plans, &id, later);
    assert_eq!(view.state, "failed", "{view:?}");
    assert!(plans.take_due().is_empty());
    // Applying again asks for the stops again.
    assert_eq!(plans.apply(&id, &[], later), Ok(1));
}

/// A copy that took more entries between the plan and its stop makes a
/// different plan, which is never written.
#[test]
fn a_plan_that_changed_by_the_stop_is_skipped() {
    let now = Instant::now();
    let (plans, id) = planned(now, whole);
    plans.apply(&id, &[], now).unwrap();
    answer_all(&plans, now, SurveyKind::Stop, |node, probes| match node {
        "a" => copy(&[1, 1, 2, 2, 2, 2], Some(5), Some(2), probes),
        _ => whole(node, probes),
    });
    assert!(plans.take_due().is_empty());
    let view = apply_state(&plans, &id, now);
    assert_eq!(view.state, "changed", "{view:?}");
}

#[test]
fn a_plan_that_may_lose_records_needs_the_shard_named() {
    let now = Instant::now();
    let lossy = |node: &str, probes: &BTreeSet<u64>| match node {
        "a" => copy(&LOG, Some(1), Some(2), probes),
        _ => copy(&[], None, None, probes),
    };
    let (plans, id) = planned(now, lossy);
    assert_eq!(plans.apply(&id, &[], now), Ok(0));
    assert_eq!(apply_state(&plans, &id, now).state, "skipped");
    assert!(plans.surveys_for("a", now).is_empty(), "nothing is stopped");

    let named = [shard().key];
    assert_eq!(plans.apply(&id, &named, now), Ok(1));
    answer_all(&plans, now, SurveyKind::Stop, lossy);
    assert_eq!(plans.take_due().len(), 1);
}

#[test]
fn a_plan_still_surveying_or_unknown_is_not_applied() {
    let now = Instant::now();
    let plans = ReformPlans::default();
    let id = plans.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    assert!(plans.apply(&id, &[], now).is_err());
    assert!(plans.apply("nope", &[], now).is_err());
}

#[test]
fn a_refused_write_ends_the_shard() {
    let now = Instant::now();
    let (plans, id) = planned(now, whole);
    plans.apply(&id, &[], now).unwrap();
    answer_all(&plans, now, SurveyKind::Stop, whole);
    let due = plans.take_due().remove(0);
    due.answer(Err("the group is at epoch 1, not 0".to_string()));
    let view = apply_state(&plans, &id, now);
    assert_eq!(view.state, "failed");
    assert!(plans.surveys_for("a", now).is_empty(), "no start is asked");
}

/// A request dropped while its epoch write is in flight hands the write
/// back, so the next request writes it (or finds it written) instead of the
/// shard staying at `writing`.
#[test]
fn a_write_whose_request_was_dropped_is_taken_again() {
    let now = Instant::now();
    let (plans, id) = planned(now, whole);
    plans.apply(&id, &[], now).unwrap();
    answer_all(&plans, now, SurveyKind::Stop, whole);
    let first = plans.take_due();
    assert_eq!(first.len(), 1);
    assert!(plans.take_due().is_empty(), "taken once while in flight");
    drop(first);
    assert_eq!(apply_state(&plans, &id, now).state, "writing");

    let again = plans.take_due();
    assert_eq!(again.len(), 1, "the dropped write is due again");
    again.into_iter().for_each(|due| due.answer(Ok(1)));
    assert_eq!(apply_state(&plans, &id, now).state, "starting");
    assert!(
        plans.take_due().is_empty(),
        "an answered write is not redone"
    );
}

/// A broker cannot plan alone, so its `start` survey carries its part of
/// the plan; the other kinds leave the field out, as an older broker reads them.
#[test]
fn a_start_survey_carries_each_brokers_part_of_the_plan() {
    let now = Instant::now();
    let (plans, id) = planned(now, whole);
    plans.apply(&id, &[], now).unwrap();
    for survey in NODES.iter().flat_map(|node| plans.surveys_for(node, now)) {
        assert_eq!(survey.start, None);
        let json = serde_json::to_value(&survey).unwrap();
        assert!(json.get("start").is_none(), "{json}");
    }
    answer_all(&plans, now, SurveyKind::Stop, whole);
    plans.take_due().remove(0).answer(Ok(1));
    let mut asked = 0;
    for survey in NODES.iter().flat_map(|node| plans.surveys_for(node, now)) {
        assert_eq!(survey.kind, SurveyKind::Start);
        let start = survey.start.expect("a start carries the plan");
        assert!(start.voter.is_some(), "every copy holds the base");
        assert!(!start.needs_accept_loss);
        asked += 1;
    }
    assert_eq!(asked, 3);
}

fn restore(end: u64, incarnation: u64, offset_gap: Option<u64>) -> ReformRestore {
    ReformRestore {
        point: "nightly".to_string(),
        shards: vec![RestoreShard {
            key: shard().key,
            incarnation,
            end,
        }],
        offset_gap,
        accept_short_gap: false,
    }
}

fn restore_plan(now: Instant, restore: ReformRestore) -> (ReformPlans, String) {
    let plans = ReformPlans::default();
    let id = plans.create(
        ReformScope::Cluster,
        Some(restore),
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    answer_all(&plans, now, SurveyKind::Survey, whole);
    (plans, id)
}

/// The epoch a restore writes starts the point plus the gap past it, so
/// the gap is in the store before any copy opens the epoch.
#[test]
fn a_restore_writes_its_epoch_past_the_point_plus_the_gap() {
    let now = Instant::now();
    for (gap, written) in [(None, DEFAULT_OFFSET_GAP), (Some(1 << 33), 1 << 33)] {
        let (plans, id) = restore_plan(now, restore(5, 3, gap));
        let view = plans.view(&id, now).unwrap();
        assert_eq!(view.restore.as_ref().map(|r| r.offset_gap), Some(written));
        let plan = view.shards[0].plan.clone().expect("planned");
        assert_eq!(plan.first_index, 5 + written);
        assert_eq!(plan.offset_gap, Some(written));

        assert_eq!(plans.apply(&id, &[], now), Ok(1));
        answer_all(&plans, now, SurveyKind::Stop, whole);
        let due = plans.take_due();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].start.base.first_index, 5 + written);
        assert_eq!(due[0].start.base.base_last.map(|l| l.index), Some(4));
    }
}

/// A point that does not name the shard, names another incarnation of it,
/// or ends where the copies' base does not, blocks the shard with a reason.
#[test]
fn a_restore_that_does_not_match_the_shard_is_blocked() {
    let now = Instant::now();
    let mut other = restore(5, 3, None);
    other.shards[0].key.shard = 1;
    for (restore, says) in [
        (other, "does not name this shard"),
        (restore(5, 2, None), "at incarnation 2; the shard is at 3"),
        (restore(4, 3, None), "not at the backup point's 4"),
    ] {
        let (plans, id) = restore_plan(now, restore);
        let view = plans.view(&id, now).unwrap();
        let shard = &view.shards[0];
        assert_eq!(shard.state, "blocked", "{shard:?}");
        let reason = shard.reason.clone().unwrap_or_default();
        assert!(reason.contains(says), "{reason}");
        assert_eq!(plans.apply(&id, &[], now), Ok(0));
    }
}

#[test]
fn a_short_gap_is_refused_unless_accepted() {
    let mut short = restore(5, 3, Some(MIN_OFFSET_GAP - 1));
    assert!(
        short
            .refused()
            .is_some_and(|why| why.contains("accept_short_gap"))
    );
    short.accept_short_gap = true;
    assert_eq!(short.refused(), None);
    assert_eq!(restore(5, 3, Some(MIN_OFFSET_GAP)).refused(), None);
    assert_eq!(restore(5, 3, None).refused(), None);
}

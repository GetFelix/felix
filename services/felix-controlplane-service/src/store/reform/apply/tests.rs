//! Applying plans against emulated brokers.
use std::collections::BTreeSet;
use std::time::Instant;

use super::super::tests::{LOG, answer, copy, shard};
use super::super::{ReformPlans, ReformScope, SurveyKind};
use super::*;

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
    let id = plans.create(ReformScope::Cluster, &[shard()], &BTreeSet::new(), now);
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
    let due = plans.take_due();
    assert_eq!(due.len(), 1, "the compare passed");
    let (_, key, start) = &due[0];
    assert_eq!(key, &shard().key);
    assert_eq!((start.incarnation, start.expected_epoch), (3, 0));
    assert_eq!(start.base.base_last.map(|l| l.index), Some(4));
    assert_eq!(start.voters.len(), 3);
    assert_eq!(apply_state(&plans, &id, now).state, "writing");
    assert!(plans.take_due().is_empty(), "a write is taken once");

    plans.written(&id, key, Ok(1));
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
    let id = plans.create(ReformScope::Cluster, &[shard()], &BTreeSet::new(), now);
    assert!(plans.apply(&id, &[], now).is_err());
    assert!(plans.apply("nope", &[], now).is_err());
}

#[test]
fn a_refused_write_ends_the_shard() {
    let now = Instant::now();
    let (plans, id) = planned(now, whole);
    plans.apply(&id, &[], now).unwrap();
    answer_all(&plans, now, SurveyKind::Stop, whole);
    let (_, key, _) = plans.take_due().remove(0);
    plans.written(&id, &key, Err("the group is at epoch 1, not 0".to_string()));
    let view = apply_state(&plans, &id, now);
    assert_eq!(view.state, "failed");
    assert!(plans.surveys_for("a", now).is_empty(), "no start is asked");
}

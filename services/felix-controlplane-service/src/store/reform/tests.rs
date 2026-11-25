//! Re-form plans surveyed from emulated brokers.
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use felix_storage::raft_copy::{REPORT_VERSION, WireCopy, WireReport};

use super::*;
use crate::model::{PlacedInEpoch, RaftOrdered, RaftSeed, RaftTarget, ShardKind, ShardState};

fn set(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|n| n.to_string()).collect()
}

/// A Raft shard at incarnation 3, epoch 0, formed on {a, b, c} under copy
/// id 1 each, with no leader.
pub(crate) fn shard() -> ShardAssignment {
    let members: BTreeMap<String, u64> =
        ["a", "b", "c"].iter().map(|n| (n.to_string(), 1)).collect();
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            kind: ShardKind::Stream,
        },
        leader: "a".to_string(),
        replicas: vec!["b".to_string(), "c".to_string()],
        generation: 7,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 3,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: Some(PlacedInEpoch {
            epoch: 0,
            brokers: set(&["a", "b", "c"]),
        }),
        switch: None,
        raft_copy_ids: members.clone(),
        raft_target_counter: 0,
        raft_target: Some(RaftTarget {
            counter: 1,
            members: members.clone(),
        }),
        raft_floor: None,
        raft_formed: None,
        raft_voters: None,
        raft_seed: Some(RaftSeed {
            incarnation: 3,
            epoch: 0,
            members: members.clone(),
            starts: Default::default(),
        }),
        raft_bases: None,
        raft_leader: None,
        raft_ordered: Some(RaftOrdered {
            incarnation: 3,
            epoch: 0,
            targets: vec![RaftTarget {
                counter: 1,
                members,
            }],
            lost: false,
        }),
    }
}

/// A copy with entries of these terms at `0..`, read at `probes`.
pub(crate) fn copy(
    terms: &[u64],
    committed: Option<u64>,
    vote: Option<u64>,
    probes: &BTreeSet<u64>,
) -> WireCopy {
    WireCopy {
        path: "shard".to_string(),
        copy: Some(1),
        epoch: Some((3, Vec::new())),
        last: terms.last().map(|t| (*t, terms.len() as u64 - 1)),
        purged: None,
        committed,
        vote,
        reserved: None,
        floor: None,
        rotated_by: None,
        terms: probes
            .iter()
            .map(|i| (*i, terms.get(*i as usize).copied()))
            .collect(),
    }
}

fn report(current: WireCopy) -> WireReport {
    WireReport {
        version: REPORT_VERSION,
        leads: false,
        names: set(&["a", "b", "c"]),
        memberships: Some(Vec::new()),
        current,
        kept: Vec::new(),
    }
}

pub(crate) fn answer(survey: &ReformSurvey, current: WireCopy) -> ReformAnswer {
    ReformAnswer {
        plan_id: survey.plan_id.clone(),
        kind: survey.kind,
        round: survey.round,
        key: survey.key.clone(),
        report: report(current),
    }
}

/// Each broker answers every survey it is asked, with the copy `read`
/// gives for the probes, until none is left. Returns every survey asked.
fn survey(
    plans: &ReformPlans,
    now: Instant,
    read: impl Fn(&str, &BTreeSet<u64>) -> WireCopy,
) -> Vec<ReformSurvey> {
    let mut asked = Vec::new();
    for _ in 0..MAX_ROUNDS {
        let mut any = false;
        for node in ["a", "b", "c"] {
            for survey in plans.surveys_for(node, now) {
                any = true;
                let answer = answer(&survey, read(node, &survey.probes));
                assert_eq!(plans.record(node, vec![answer], now), (1, 0));
                asked.push(survey);
            }
        }
        if !any {
            break;
        }
    }
    asked
}

pub(crate) const LOG: [u64; 5] = [1, 1, 2, 2, 2];

#[test]
fn every_broker_readable_plans_an_unmarked_reform() {
    let plans = ReformPlans::default();
    let now = Instant::now();
    let id = plans.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    assert_eq!(plans.view(&id, now).unwrap().state, "surveying");

    let asked = survey(&plans, now, |_, probes| {
        copy(&LOG, Some(4), Some(2), probes)
    });
    let rounds: BTreeSet<u32> = asked.iter().map(|s| s.round).collect();
    assert!(rounds.len() <= 3, "converges in three rounds: {rounds:?}");

    let view = plans.view(&id, now).unwrap();
    assert_eq!(view.state, "done");
    let shard = &view.shards[0];
    assert_eq!(shard.state, "reform", "{shard:?}");
    let plan = shard.plan.as_ref().unwrap();
    assert_eq!(plan.marked, None, "the seed vouches for every vote");
    assert!(!plan.needs_accept_loss);
    assert_eq!(plan.base_last.map(|b| b.0), Some(4));
    assert!(plan.dropped.is_empty());
    assert_eq!(plan.voters.len(), 3);
}

/// Relaxed fsync, power cut on b and c: their logs and votes are gone, so
/// the only copy left cannot vouch past its saved commit.
#[test]
fn a_lost_majority_is_marked_and_names_what_may_be_lost() {
    let plans = ReformPlans::default();
    let now = Instant::now();
    let id = plans.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    survey(&plans, now, |node, probes| match node {
        "a" => copy(&LOG, Some(1), Some(2), probes),
        _ => copy(&[], None, None, probes),
    });

    let view = plans.view(&id, now).unwrap();
    let shard = &view.shards[0];
    assert_eq!(shard.state, "reform", "{shard:?}");
    let plan = shard.plan.as_ref().unwrap();
    assert!(plan.marked.is_some(), "{plan:?}");
    assert!(plan.needs_accept_loss);
    assert_eq!(
        plan.base_last.map(|b| b.0),
        Some(1),
        "cut to the saved commit"
    );
    assert_eq!(plan.dropped, vec![("a".to_string(), 2, 4)]);
}

#[test]
fn an_answer_for_another_round_or_broker_is_ignored() {
    let plans = ReformPlans::default();
    let now = Instant::now();
    let id = plans.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    let first = plans.surveys_for("a", now).remove(0);
    let full = |probes: &BTreeSet<u64>| copy(&LOG, Some(4), Some(2), probes);
    // The saved commit is below a's last entry, so the first round, asked
    // at no index, cannot read the base's term and a second asks it.
    let read = |node: &str, probes: &BTreeSet<u64>| match node {
        "a" => copy(&LOG, Some(1), Some(2), probes),
        _ => copy(&[], None, None, probes),
    };
    let mut next_round = first.clone();
    next_round.round += 1;
    // Not a placed broker, and not this round.
    assert_eq!(
        plans.record("d", vec![answer(&first, full(&first.probes))], now),
        (0, 1)
    );
    assert_eq!(
        plans.record("a", vec![answer(&next_round, full(&first.probes))], now),
        (0, 1)
    );
    // Once the round moves on, an answer to the old one is never planned with.
    let asked = survey(&plans, now, read);
    assert!(
        asked.iter().any(|s| s.round > first.round),
        "needs a second round"
    );
    assert_eq!(
        plans.record("a", vec![answer(&first, full(&first.probes))], now),
        (0, 1)
    );
    assert_eq!(plans.view(&id, now).unwrap().shards[0].state, "reform");
}

#[test]
fn a_broker_that_never_answers_blocks_its_shard() {
    let plans = ReformPlans::default();
    let now = Instant::now();
    let id = plans.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    for node in ["a", "b"] {
        for survey in plans.surveys_for(node, now) {
            let answer = answer(&survey, copy(&LOG, Some(4), Some(2), &survey.probes));
            plans.record(node, vec![answer], now);
        }
    }
    let view = plans.view(&id, now).unwrap();
    assert_eq!(view.state, "surveying");
    assert_eq!(view.shards[0].waiting_on, vec!["c".to_string()]);
    let later = now + ROUND_TIMEOUT + Duration::from_secs(1);
    let shard = &plans.view(&id, later).unwrap().shards[0];
    assert_eq!(shard.state, "blocked");
    assert!(
        shard.reason.as_ref().unwrap().contains("c did not answer"),
        "{shard:?}"
    );
}

#[test]
fn a_report_of_another_version_is_ignored() {
    let plans = ReformPlans::default();
    let now = Instant::now();
    plans.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    let survey = plans.surveys_for("a", now).remove(0);
    let mut answer = answer(&survey, copy(&LOG, Some(4), Some(2), &survey.probes));
    answer.report.version += 1;
    assert_eq!(plans.record("a", vec![answer], now), (0, 1));
}

/// A plan is this member's soft state: another member, or this one after
/// a restart, knows nothing of it, and brokers are asked nothing.
#[test]
fn another_member_has_no_plan() {
    let leader = ReformPlans::default();
    let now = Instant::now();
    let id = leader.create(
        ReformScope::Cluster,
        None,
        &[shard()],
        &BTreeSet::new(),
        now,
    );
    let next = ReformPlans::default();
    assert!(next.view(&id, now).is_none());
    assert!(next.surveys_for("a", now).is_empty());
}

#[test]
fn a_node_scope_covers_only_shards_placed_on_it() {
    let mut elsewhere = shard();
    elsewhere.key.shard = 1;
    elsewhere.placed.as_mut().unwrap().brokers = set(&["d", "e", "f"]);
    let picked = in_scope(vec![shard(), elsewhere], &ReformScope::Node("a".into()));
    assert_eq!(picked.len(), 1);
    assert_eq!(picked[0].key.shard, 0);
}

/// A cache shard on a Raft cluster runs a group too, so a plan covers it
/// and surveys its brokers like a stream shard's (#2978).
#[test]
fn a_raft_cache_shard_is_planned_like_a_stream_shard() {
    let mut cache = shard();
    cache.key.kind = ShardKind::Cache;
    let picked = in_scope(vec![cache.clone()], &ReformScope::Cluster);
    assert_eq!(picked.len(), 1, "the cache shard is in scope");

    let plans = ReformPlans::default();
    let now = Instant::now();
    let id = plans.create(ReformScope::Cluster, None, &picked, &BTreeSet::new(), now);
    let asked = survey(&plans, now, |_, probes| {
        copy(&LOG, Some(4), Some(2), probes)
    });
    assert!(asked.iter().all(|s| s.key.kind == ShardKind::Cache));
    let view = plans.view(&id, now).unwrap();
    let shard = &view.shards[0];
    assert_eq!(shard.state, "reform", "{shard:?}");
    assert_eq!(shard.plan.as_ref().unwrap().voters.len(), 3);
}

//! A leader on a live broker that shows no progress at its generation is
//! named again one generation up, a new fenced term, after the stuck bound.
//! Idle, slow but moving, or failing-over shards are left alone, and the
//! renames back off.
use super::super::progress::CopyProgress;
use super::*;
use crate::config::NodeLivenessConfig;
use std::collections::HashMap;

use crate::model::{ConsistencyLevel, HaltedCopy, ReplicaReport};

const NOW: u64 = 10_000_000_000;
const SECOND: u64 = 1_000;
const BOUND: u64 = 70 * SECOND;

/// Reports as of `NOW`, with the leader of every shard stuck since `since`
/// after `renames` recent renames, and `halted` reported halted.
struct Stuck {
    since: Option<u64>,
    renames: u32,
    halted: Option<&'static str>,
    reason: StuckReason,
}

impl CaughtUp for Stuck {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }

    fn halted(&self, _key: &ShardKey, node_id: &str) -> Option<&HaltedCopy> {
        static HALT: HaltedCopy = HaltedCopy {
            reason: String::new(),
            generation: 3,
            since_millis: 0,
        };
        (self.halted == Some(node_id)).then_some(&HALT)
    }

    fn stuck_leader(&self, _key: &ShardKey, generation: u64) -> Option<StuckLeader> {
        let since_millis = self.since.filter(|_| generation == 3)?;
        Some(StuckLeader {
            since_millis,
            reason: self.reason,
            recent_renames: self.renames,
        })
    }
}

fn stuck(since: u64, renames: u32) -> Stuck {
    Stuck {
        since: Some(since),
        renames,
        halted: None,
        reason: StuckReason::NotCommitting,
    }
}

fn quorum(shards: u32) -> Vec<Stream> {
    vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", shards, 3)
    }]
}

fn shard(n: u32) -> ShardAssignment {
    let mut assignment = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    assignment.key.shard = n;
    assignment
}

fn policy() -> MovePolicy {
    MovePolicy {
        stuck_leader_millis: Some(BOUND),
        ..MovePolicy::default()
    }
}

fn decide_on(
    streams: &[Stream],
    nodes: &[Node],
    existing: &[ShardAssignment],
    reports: &dyn CaughtUp,
) -> Vec<Decision> {
    plan_with(streams, &[], nodes, existing, reports, policy())
        .shards
        .into_iter()
        .map(|plan| plan.decision)
        .collect()
}

fn decide(reports: &dyn CaughtUp) -> Decision {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    decide_on(&quorum(1), &nodes, &[shard(0)], reports).remove(0)
}

fn is_stuck(decision: &Decision) -> bool {
    matches!(decision, Decision::Move(MoveStep::Stuck { .. }, _))
}

/// Past the bound the same leader is named again on the same set, at the
/// store's next generation: a new fenced term, not a new leader.
#[test]
fn a_stuck_leader_is_named_again_on_the_same_set() {
    let decision = decide(&stuck(NOW - BOUND - 1, 0));
    let Decision::Move(MoveStep::Stuck { reason }, renamed) = decision else {
        panic!("{decision:?}");
    };
    assert_eq!(reason, StuckReason::NotCommitting);
    assert_eq!(renamed.leader, "broker-a");
    assert_eq!(renamed.replicas, shard(0).replicas);
    assert_eq!(renamed.generation, 0, "the store takes the next generation");
    assert_eq!(renamed.successor, None);
}

/// Inside the bound, or with no watch, nothing changes.
#[test]
fn inside_the_bound_or_unwatched_nothing_moves() {
    assert_eq!(decide(&stuck(NOW - BOUND, 0)), Decision::Kept);
    assert_eq!(
        decide(&Stuck {
            since: None,
            ..stuck(0, 0)
        }),
        Decision::Kept
    );
}

/// Each rename in the window doubles the wait, and the window's cap stops
/// them however long the shard stays stuck.
#[test]
fn renames_back_off_and_stop_at_the_cap() {
    assert_eq!(decide(&stuck(NOW - 2 * BOUND, 1)), Decision::Kept);
    assert!(is_stuck(&decide(&stuck(NOW - 2 * BOUND - 1, 1))));
    assert_eq!(decide(&stuck(NOW - 8 * BOUND, 3)), Decision::Kept);
    assert!(is_stuck(&decide(&stuck(NOW - 8 * BOUND - 1, 3))));
    assert_eq!(decide(&stuck(NOW - 1_000 * BOUND, 4)), Decision::Kept);
}

/// A leader that never opened may still be queued behind its broker's other
/// fences, so it waits twice the bound.
#[test]
fn a_never_opened_leader_waits_twice_the_bound() {
    let never = |since| Stuck {
        reason: StuckReason::NeverOpened,
        ..stuck(since, 0)
    };
    assert_eq!(decide(&never(NOW - 2 * BOUND)), Decision::Kept);
    assert!(is_stuck(&decide(&never(NOW - 2 * BOUND - 1))));
}

/// Without a live, unhalted majority the new term would stall the same way:
/// failover and restore own that shard.
#[test]
fn no_rename_without_a_live_unhalted_majority() {
    let mut nodes = live(&["broker-a", "broker-b", "broker-c"]);
    nodes[1].status.lifecycle = NodeLifecycle::Down;
    let halted = Stuck {
        halted: Some("broker-c"),
        ..stuck(0, 0)
    };
    let decisions = decide_on(&quorum(1), &nodes, &[shard(0)], &halted);
    assert!(!is_stuck(&decisions[0]), "{decisions:?}");
    // One follower down alone still leaves a majority.
    let decisions = decide_on(&quorum(1), &nodes, &[shard(0)], &stuck(0, 0));
    assert!(is_stuck(&decisions[0]), "{decisions:?}");
}

/// A move, a fence or a set change ends through its own steps.
#[test]
fn no_rename_during_a_move_or_set_change() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let fenced = ShardAssignment {
        state: ShardState::Draining,
        successor: Some("broker-b".to_string()),
        move_started_at_millis: Some(NOW),
        move_reason: Some(crate::model::MoveReason::Balance),
        ..shard(0)
    };
    let staged = ShardAssignment {
        state: ShardState::Active,
        ..fenced.clone()
    };
    for existing in [fenced, staged] {
        let decisions = decide_on(&quorum(1), &nodes, &[existing], &stuck(0, 0));
        assert!(!is_stuck(&decisions[0]), "{decisions:?}");
    }
}

/// A shard without the promotion fence gains nothing from a new generation.
#[test]
fn a_leader_shard_is_never_renamed() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let streams = [replicated_stream("orders", 1, 3)];
    let decisions = decide_on(&streams, &nodes, &[shard(0)], &stuck(0, 0));
    assert!(!is_stuck(&decisions[0]), "{decisions:?}");
}

/// One pass renames at most a fixed number of shards: many at once looks
/// like a fleet-wide stall that renames would only add to.
#[test]
fn one_pass_renames_a_bounded_number() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing: Vec<ShardAssignment> = (0..40).map(shard).collect();
    let decisions = decide_on(&quorum(40), &nodes, &existing, &stuck(0, 0));
    let renamed = decisions.iter().filter(|d| is_stuck(d)).count();
    assert_eq!(renamed, 16);
}

// What counts as progress, watched across passes.

fn liveness() -> NodeLivenessConfig {
    NodeLivenessConfig::default()
}

/// The leader's report on shard `n` at generation `generation`: its tail,
/// and where b and c are.
fn report(n: u32, generation: u64, at: u64, tail: u64, b: u64, c: u64) -> ReplicaReport {
    ReplicaReport {
        key: shard(n).key,
        generation,
        caught_up: Default::default(),
        offsets: [("broker-b".to_string(), b), ("broker-c".to_string(), c)].into(),
        reported_at_millis: at,
        drained: false,
        leader_offset: Some(tail),
        halted: Default::default(),
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        follower_storage: Default::default(),
        leader: Some("broker-a".to_string()),
        node_incarnations: Default::default(),
        given_up: false,
        inherited_storage: Default::default(),
        joint_start_acked: None,
        refused: None,
        holds_start: Default::default(),
    }
}

/// Watches `existing` at `now` with `reports`, as one pass does.
fn observe(
    progress: &mut CopyProgress,
    existing: &[ShardAssignment],
    nodes: &[Node],
    reports: Vec<ReplicaReport>,
    now: u64,
) -> HashMap<(ShardKey, u64), StuckLeader> {
    let positions = ReplicaPositions::new(reports, &liveness(), now).with_nodes(nodes, existing);
    progress.observe_stuck(existing, &positions, Some(BOUND))
}

/// An idle leader that has opened reports nothing pending, and is never
/// stuck, however long it stays idle or however late within the TTL its
/// reports come.
#[test]
fn an_idle_opened_leader_is_never_stuck() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = [shard(0)];
    let mut progress = CopyProgress::default();
    let mut last_report = NOW;
    for pass in 0..200u64 {
        let now = NOW + pass * 5 * SECOND;
        if now - last_report >= 30 * SECOND {
            last_report = now;
        }
        let reports = vec![report(0, 3, last_report, 42, 42, 42)];
        let stuck = observe(&mut progress, &existing, &nodes, reports, now);
        assert!(
            stuck.values().all(|s| now - s.since_millis <= BOUND),
            "{stuck:?}"
        );
        let decisions = decide_on(&quorum(1), &nodes, &existing, &{
            let mut p = ReplicaPositions::new(vec![], &liveness(), now);
            p.set_stuck(stuck);
            p
        });
        assert!(!is_stuck(&decisions[0]), "pass {pass}: {decisions:?}");
    }
}

/// Writes pending behind a majority that keeps moving, however slowly, are
/// progress. A majority that stops with writes pending is not.
#[test]
fn a_slow_but_moving_majority_is_progress_and_a_stopped_one_is_not() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = [shard(0)];
    let mut progress = CopyProgress::default();
    for pass in 0..100u64 {
        let now = NOW + pass * 5 * SECOND;
        // b moves one record a minute; c is dead in the water.
        let b = pass / 12;
        let reports = vec![report(0, 3, now, 1_000, b, 0)];
        let stuck = observe(&mut progress, &existing, &nodes, reports, now);
        assert!(
            stuck.values().all(|s| now - s.since_millis <= BOUND),
            "{stuck:?}"
        );
    }
    let start = NOW + 1_000 * SECOND;
    let mut stuck = HashMap::new();
    for pass in 0..20u64 {
        let now = start + pass * 5 * SECOND;
        let reports = vec![report(0, 3, now, 1_000, 7, 0)];
        stuck = observe(&mut progress, &existing, &nodes, reports, now);
    }
    let held = stuck.get(&(shard(0).key, 3)).expect("stuck");
    assert_eq!(held.reason, StuckReason::NotCommitting);
    assert!(start + 19 * 5 * SECOND - held.since_millis > BOUND);
}

/// A leader that never reports at its generation is stuck from when this
/// instance first watched it, while its broker reports other shards. A
/// broker that reports nothing at all is not judged by its silence.
#[test]
fn a_leader_that_never_opens_is_stuck_only_while_its_broker_reports() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = [shard(0), shard(1)];
    let mut progress = CopyProgress::default();
    for now in [NOW, NOW + BOUND / 3, NOW + 2 * BOUND / 3, NOW + BOUND + 1] {
        let reports = vec![report(1, 3, now, 5, 5, 5)];
        let stuck = observe(&mut progress, &existing, &nodes, reports, now);
        assert!(!stuck.contains_key(&(shard(1).key, 3)));
        if now > NOW + BOUND {
            let held = stuck.get(&(shard(0).key, 3)).expect("stuck");
            assert_eq!(held.reason, StuckReason::NeverOpened);
            assert_eq!(held.since_millis, NOW);
        }
    }
    let mut progress = CopyProgress::default();
    for now in [NOW, NOW + 10 * BOUND] {
        let stuck = observe(&mut progress, &existing, &nodes, vec![], now);
        assert!(stuck.values().all(|s| s.since_millis == now), "{stuck:?}");
    }
}

/// The clock starts again at a new generation, a restarted leader, or a newer
/// generation reported by an election.
#[test]
fn the_clock_restarts_where_another_path_owns_the_shard() {
    let mut nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = vec![shard(0), shard(1)];
    let other = |now| report(1, 3, now, 5, 5, 5);
    let later = NOW + BOUND + 1;
    let key = (shard(0).key, 3);
    // Unchanged, the shard is stuck by `later`.
    let mut progress = CopyProgress::default();
    observe(&mut progress, &existing, &nodes, vec![other(NOW)], NOW);
    for mid in [NOW + BOUND / 3, NOW + 2 * BOUND / 3] {
        observe(&mut progress, &existing, &nodes, vec![other(mid)], mid);
    }
    let stuck = observe(&mut progress, &existing, &nodes, vec![other(later)], later);
    assert!(stuck.contains_key(&key), "{stuck:?}");

    let mut progress = CopyProgress::default();
    observe(&mut progress, &existing, &nodes, vec![other(NOW)], NOW);
    for mid in [NOW + BOUND / 3, NOW + 2 * BOUND / 3] {
        observe(&mut progress, &existing, &nodes, vec![other(mid)], mid);
    }
    let renamed = vec![
        ShardAssignment {
            generation: 4,
            ..shard(0)
        },
        shard(1),
    ];
    let stuck = observe(&mut progress, &renamed, &nodes, vec![other(later)], later);
    assert!(stuck.is_empty(), "{stuck:?}");

    let mut progress = CopyProgress::default();
    observe(&mut progress, &existing, &nodes, vec![other(NOW)], NOW);
    for mid in [NOW + BOUND / 3, NOW + 2 * BOUND / 3] {
        observe(&mut progress, &existing, &nodes, vec![other(mid)], mid);
    }
    nodes[0].status.incarnation += 1;
    let stuck = observe(&mut progress, &existing, &nodes, vec![other(later)], later);
    assert!(!stuck.contains_key(&key), "{stuck:?}");

    let mut progress = CopyProgress::default();
    observe(&mut progress, &existing, &nodes, vec![other(NOW)], NOW);
    for mid in [NOW + BOUND / 3, NOW + 2 * BOUND / 3] {
        observe(&mut progress, &existing, &nodes, vec![other(mid)], mid);
    }
    let elected = ReplicaReport {
        leader: Some("broker-b".to_string()),
        ..report(0, 9, later, 5, 5, 5)
    };
    let stuck = observe(
        &mut progress,
        &existing,
        &nodes,
        vec![other(later), elected],
        later,
    );
    assert!(!stuck.contains_key(&key), "{stuck:?}");
}

/// A watch this instance stopped looking at starts again: the time it was
/// not looking is not time it saw the shard stuck.
#[test]
fn a_watch_left_unobserved_starts_again() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = [shard(0)];
    let mut progress = CopyProgress::default();
    observe(
        &mut progress,
        &existing,
        &nodes,
        vec![report(0, 3, NOW, 9, 9, 9)],
        NOW,
    );
    let back = NOW + 10 * BOUND;
    // Caught mid-write: pending, and the majority where it was.
    let stuck = observe(
        &mut progress,
        &existing,
        &nodes,
        vec![report(0, 3, back, 10, 9, 9)],
        back,
    );
    assert!(stuck.is_empty(), "{stuck:?}");
}

/// A refusal, or a report from another leader or an older incarnation of
/// this one, shows nothing of this leader's progress.
#[test]
fn only_this_leaders_own_reports_are_progress() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = [shard(0), shard(1)];
    let later = NOW + BOUND + 1;
    let refused = ReplicaReport {
        refused: Some(felix_common::membership::RefusedGeneration {
            generation: 3,
            promised_to: String::new(),
        }),
        ..report(0, 3, later, 5, 5, 5)
    };
    let other = ReplicaReport {
        leader: Some("broker-b".to_string()),
        ..report(0, 3, later, 5, 5, 5)
    };
    let restarted = ReplicaReport {
        node_incarnations: [("broker-a".to_string(), 7)].into(),
        ..report(0, 3, later, 5, 5, 5)
    };
    for evidence in [refused, other, restarted] {
        let mut progress = CopyProgress::default();
        observe(
            &mut progress,
            &existing,
            &nodes,
            vec![report(1, 3, NOW, 5, 5, 5)],
            NOW,
        );
        for mid in [NOW + BOUND / 3, NOW + 2 * BOUND / 3] {
            observe(
                &mut progress,
                &existing,
                &nodes,
                vec![report(1, 3, mid, 5, 5, 5)],
                mid,
            );
        }
        let reports = vec![report(1, 3, later, 5, 5, 5), evidence];
        let stuck = observe(&mut progress, &existing, &nodes, reports, later);
        let held = stuck.get(&(shard(0).key, 3)).expect("stuck");
        assert_eq!(held.reason, StuckReason::NeverOpened);
    }
}

/// Every leader stuck at once, as a network that carries pings but not
/// shipping would leave them, is a fleet-wide fault: none is renamed.
#[test]
fn many_stuck_at_once_renames_none() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing: Vec<ShardAssignment> = (0..200).map(shard).collect();
    let reports = |at| (0..200).map(|n| report(n, 3, at, 10, 4, 4)).collect();
    let mut progress = CopyProgress::default();
    let mut stuck = HashMap::new();
    for pass in 0..20 {
        let now = NOW + pass * 5 * SECOND;
        stuck = observe(&mut progress, &existing, &nodes, reports(now), now);
    }
    assert!(stuck.is_empty(), "{} stuck", stuck.len());
}

/// A leader that fails at every generation it is given is renamed a handful
/// of times an hour, each wait twice the last, and never more than the cap
/// in any hour.
#[test]
fn churn_is_bounded_under_a_leader_that_never_recovers() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let streams = quorum(2);
    let mut existing = vec![shard(0), shard(1)];
    let mut progress = CopyProgress::default();
    let mut renames = Vec::new();
    for pass in 0..(3 * 3_600 / 5) {
        let now = NOW + pass * 5 * SECOND;
        let reports = vec![report(1, 3, now, 5, 5, 5)];
        let mut positions =
            ReplicaPositions::new(reports, &liveness(), now).with_nodes(&nodes, &existing);
        let stuck = progress.observe_stuck(&existing, &positions, Some(BOUND));
        positions.set_stuck(stuck);
        let plan = plan_with(&streams, &[], &nodes, &existing, &positions, policy());
        progress.note_stuck_renames(&plan, Some(now));
        for (key, step, _) in plan.moves() {
            if matches!(step, MoveStep::Stuck { .. }) && key.shard == 0 {
                renames.push(now);
                existing[0].generation += 1;
            }
        }
    }
    assert!(renames.len() >= 4, "{renames:?}");
    for window in renames.windows(5) {
        assert!(window[4] - window[0] >= 3_600 * SECOND, "{renames:?}");
    }
    for gaps in renames.windows(3).take(3) {
        assert!(gaps[2] - gaps[1] > gaps[1] - gaps[0], "{renames:?}");
    }
}

/// Through the store: a leader that never opens its generation is named
/// again past the bound on the same set, and once it reports at the new
/// generation it is left alone. A write that lands first, as an adopted
/// election would, wins over a rename planned before it.
#[tokio::test]
async fn a_never_opened_leader_is_renamed_through_the_store_and_serves() {
    use super::pending_set::election_store;
    use crate::store::ControlPlaneStore;
    let store = election_store(&["broker-a", "broker-b", "broker-c"], true).await;
    store
        .create_stream(Stream {
            consistency: ConsistencyLevel::Quorum,
            ..replicated_stream("audit", 1, 3)
        })
        .await
        .expect("stream");
    let orders = store
        .put_shard_assignment(ShardAssignment {
            generation: 0,
            ..shard(0)
        })
        .await
        .expect("orders");
    let audit = store
        .put_shard_assignment(ShardAssignment {
            key: ShardKey {
                stream: "audit".to_string(),
                ..shard(0).key
            },
            generation: 0,
            ..shard(0)
        })
        .await
        .expect("audit");
    let liveness = NodeLivenessConfig {
        heartbeat_interval_ms: 40,
        expiry_timeout_ms: 80,
        ..NodeLivenessConfig::default()
    };
    // Floored at two report TTLs: 400 ms here.
    let policy = MovePolicy {
        stuck_leader_millis: Some(1),
        ..MovePolicy::default()
    };
    // Both shards on one leader start no balance move.
    store.set_moves_paused(true).await.expect("pause");
    let reports = |assignment: &ShardAssignment, at: u64| ReplicaReport {
        key: assignment.key.clone(),
        node_incarnations: Default::default(),
        ..report(0, assignment.generation, at, 5, 5, 5)
    };
    let record = async |assignment: &ShardAssignment| {
        let at = store.now_millis().await.expect("clock");
        store
            .record_replica_report(reports(assignment, at), "broker-a")
            .await
            .expect("report");
    };
    let pass = async |progress: &mut CopyProgress| {
        let planned =
            super::super::reconciler::plan_pass(&store, &liveness, policy.clone(), progress)
                .await
                .expect("plan");
        super::super::reconciler::apply_pass(
            &store,
            &planned,
            &mut Default::default(),
            &Default::default(),
        )
        .await
    };
    let mut progress = CopyProgress::default();
    record(&audit).await;
    let first = pass(&mut progress).await;
    assert_eq!(first.moved, 0, "starts watching: {first:?}");
    // A leader that never opened waits twice the bound, watched by passes
    // closer together than half of it.
    let started = std::time::Instant::now();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        record(&audit).await;
        if pass(&mut progress).await.moved == 1 {
            break;
        }
        assert!(started.elapsed().as_secs() < 30, "never renamed");
    }
    assert!(
        started.elapsed().as_millis() >= 700,
        "waited about twice the bound"
    );
    let renamed = store.get_shard_assignment(&orders.key).await.expect("get");
    assert_eq!(renamed.leader, "broker-a");
    assert_eq!(renamed.replicas, orders.replicas);
    assert_eq!(renamed.generation, orders.generation + 1);

    // It opens at the new generation and goes idle: left alone.
    for _ in 0..60 {
        record(&renamed).await;
        record(&audit).await;
        assert_eq!(pass(&mut progress).await.moved, 0);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // It stops reporting. The second rename waits twice the first; another
    // write lands between its plan and its apply.
    let started = std::time::Instant::now();
    let planned = loop {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        record(&audit).await;
        let planned =
            super::super::reconciler::plan_pass(&store, &liveness, policy.clone(), &mut progress)
                .await
                .expect("plan");
        if planned.plan().moves().count() == 1 {
            break planned;
        }
        assert!(started.elapsed().as_secs() < 30, "never planned");
    };
    assert!(
        started.elapsed().as_millis() >= 700,
        "waited about twice the bound"
    );
    let elected = store
        .put_shard_assignment(ShardAssignment {
            leader: "broker-b".to_string(),
            replicas: vec!["broker-a".to_string(), "broker-c".to_string()],
            ..renamed.clone()
        })
        .await
        .expect("elected");
    let outcome = super::super::reconciler::apply_pass(
        &store,
        &planned,
        &mut Default::default(),
        &Default::default(),
    )
    .await;
    assert_eq!(outcome.moved, 0, "{outcome:?}");
    let after = store.get_shard_assignment(&orders.key).await.expect("get");
    assert_eq!(after, elected, "the rename planned before it is refused");
}

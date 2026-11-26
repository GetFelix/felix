//! An in-memory shard has one owner and no log: placed by `choose`, placed
//! again in the same pass once its owner cannot keep it, at a new generation,
//! and never from a replica report.
use std::sync::Arc;

use felix_router::RegionRouter;

use super::reconciler::cluster;
use super::*;
use crate::store::ControlPlaneStore;

/// Both engines: the step is the same on each.
fn policies() -> [MovePolicy; 2] {
    [
        MovePolicy::default(),
        MovePolicy {
            raft_streams: true,
            ..MovePolicy::default()
        },
    ]
}

/// Reports that would steer every hybrid step: each copy caught up, the
/// leader suspected, a member ready to re-admit, and a clock. The in-memory
/// step must decide exactly as it does with none.
struct Misleading;

impl CaughtUp for Misleading {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }
    fn reported_by(&self, _key: &ShardKey) -> Option<&str> {
        Some("broker-c")
    }
    fn suspects(&self, _key: &ShardKey, _generation: u64, _follower: &str, _leader: &str) -> bool {
        true
    }
    fn readmittable(&self, _key: &ShardKey, _generation: u64, _leader: &str) -> Option<String> {
        Some("broker-c".to_string())
    }
    fn reclaimable(&self, _key: &ShardKey, _leader: &str, _followers: &[&str]) -> bool {
        true
    }
}

fn in_memory(name: &str, shards: u32, replication_factor: u32) -> Stream {
    Stream {
        durable: false,
        ..replicated_stream(name, shards, replication_factor)
    }
}

fn decision_of<'a>(plan: &'a Plan, name: &str) -> &'a Decision {
    &plan
        .shards
        .iter()
        .find(|shard| shard.key.stream == name)
        .expect("planned")
        .decision
}

/// Applies a plan's placements as the store would, each at the next
/// generation.
fn apply(existing: &[ShardAssignment], plan: &Plan) -> Vec<ShardAssignment> {
    let mut next: Vec<ShardAssignment> = existing.to_vec();
    for (key, leader, replicas) in plan.to_place() {
        let generation = existing
            .iter()
            .find(|a| &a.key == key)
            .map_or(1, |a| a.generation + 1);
        next.retain(|a| &a.key != key);
        next.push(ShardAssignment {
            generation,
            ..assignment_for(key, leader, replicas.to_vec())
        });
    }
    next
}

/// **A new shard gets one owner and no followers**, whatever factor the
/// stream was created with: nothing replicates an in-memory shard.
#[test]
fn a_new_in_memory_shard_gets_one_owner_and_no_followers() {
    let streams = vec![in_memory("ticks", 1, 3)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    for policy in policies() {
        let plan = plan_with(&streams, &[], &nodes, &[], &NothingCaughtUp, policy.clone());
        match &plan.shards[0].decision {
            Decision::Place(owner, followers) => {
                assert!(nodes.iter().any(|n| &n.node_id == owner));
                assert!(followers.is_empty(), "{followers:?}");
            }
            other => panic!("expected a placement, got {other:?}"),
        }
        // Not reported short of copies it never gets.
        let existing = apply(&[], &plan);
        let again = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, policy);
        assert!(again.under_replicated.is_empty(), "{again:?}");
        assert_eq!(again.missing_copies, 0);
    }
}

/// **A live owner keeps its shard**, and no report moves it: not a
/// caught-up follower, not suspicion from one, not a re-admission.
#[test]
fn a_live_owner_keeps_its_shard_whatever_the_reports_say() {
    let streams = vec![in_memory("ticks", 1, 3)];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let existing = vec![assigned("ticks", "broker-a", &["broker-b", "broker-c"])];
    for policy in policies() {
        let joint = MovePolicy {
            joint_fence: true,
            ..policy
        };
        let plan = plan_with(&streams, &[], &nodes, &existing, &Misleading, joint);
        assert_eq!(plan.shards[0].decision, Decision::Kept, "{plan:?}");
    }
}

/// **An owner that is down, gone or deregistered past its lease is replaced
/// in the same pass**, with no report present, on either engine.
#[test]
fn a_lost_owner_is_replaced_in_the_same_pass_with_no_reports() {
    let streams = vec![in_memory("ticks", 1, 1)];
    let existing = vec![assigned("ticks", "broker-a", &[])];
    for lifecycle in [
        Some(NodeLifecycle::Down),
        Some(NodeLifecycle::Left),
        Some(NodeLifecycle::Draining),
        None,
    ] {
        let mut nodes = live(&["broker-b"]);
        if let Some(lifecycle) = lifecycle {
            nodes.push(node("broker-a", lifecycle, None));
        }
        for policy in policies() {
            for caught_up in [&NothingCaughtUp as &dyn CaughtUp, &Misleading] {
                let plan = plan_with(&streams, &[], &nodes, &existing, caught_up, policy.clone());
                assert_eq!(
                    plan.shards[0].decision,
                    Decision::Place("broker-b".to_string(), Vec::new()),
                    "{lifecycle:?}, raft_streams {}",
                    policy.raft_streams,
                );
            }
        }
    }
}

/// **An owner on an internal node, or outside the stream's region, is
/// replaced the way a draining one is**, and only by a node the stream may
/// use. With none, the shard says why and is not placed elsewhere.
#[test]
fn an_owner_that_may_not_hold_the_shard_is_replaced_within_its_region() {
    let homed = Stream {
        region: Some("eu".to_string()),
        ..in_memory("ticks", 1, 1)
    };
    let region = |id: &str, region: &str| {
        let mut n = node(id, NodeLifecycle::Live, None);
        n.spec.region = region.to_string();
        n
    };
    let regions = MovePolicy {
        regions: Arc::new(RegionRouter::new(String::new())),
        ..MovePolicy::default()
    };
    let existing = vec![assigned("ticks", "us-1", &[])];

    let nodes = vec![
        region("us-1", "us"),
        region("us-2", "us"),
        region("eu-1", "eu"),
    ];
    let plan = plan_with(
        std::slice::from_ref(&homed),
        &[],
        &nodes,
        &existing,
        &NothingCaughtUp,
        regions.clone(),
    );
    assert_eq!(
        plan.shards[0].decision,
        Decision::Place("eu-1".to_string(), Vec::new())
    );

    let nodes = vec![region("us-1", "us"), region("us-2", "us")];
    let plan = plan_with(&[homed], &[], &nodes, &existing, &NothingCaughtUp, regions);
    assert_eq!(
        plan.shards[0].decision,
        Decision::Unplaceable(Unplaceable::NoNodeInRegion {
            region: "eu".to_string()
        })
    );

    let mut nodes = live(&["broker-a", "broker-b"]);
    nodes[0].spec.labels.insert(
        felix_common::membership::NODE_ROLE_LABEL.to_string(),
        felix_common::membership::NODE_ROLE_INTERNAL.to_string(),
    );
    let existing = vec![assigned("ticks", "broker-a", &[])];
    for policy in policies() {
        let plan = plan_with(
            &[in_memory("ticks", 1, 1)],
            &[],
            &nodes,
            &existing,
            &NothingCaughtUp,
            policy,
        );
        assert_eq!(
            plan.shards[0].decision,
            Decision::Place("broker-b".to_string(), Vec::new())
        );
    }
}

/// **A pause holds no replacement.** The owner is gone, so waiting would
/// leave the shard with nobody serving it. A live owner is not moved
/// either way.
#[test]
fn a_pause_does_not_hold_a_lost_owners_replacement() {
    let streams = vec![in_memory("ticks", 1, 1), in_memory("quotes", 1, 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Down, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let mut quotes = assigned("quotes", "broker-b", &[]);
    quotes.key.stream = "quotes".to_string();
    let existing = vec![assigned("ticks", "broker-a", &[]), quotes];
    for policy in policies() {
        let paused = MovePolicy {
            paused: true,
            ..policy
        };
        let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, paused);
        assert_eq!(
            decision_of(&plan, "ticks"),
            &Decision::Place("broker-b".to_string(), Vec::new())
        );
        assert_eq!(decision_of(&plan, "quotes"), &Decision::Kept);
    }
}

/// **One copy is never short of a zone**, so zones neither move an owner
/// nor report it unspread, where a replicated durable shard is spread.
#[test]
fn zones_do_not_move_an_in_memory_owner() {
    let mut nodes = live(&["broker-a", "broker-b", "broker-c"]);
    for (node, zone) in nodes.iter_mut().zip(["z1", "z2", "z3"]) {
        node.spec.zone = Some(zone.to_string());
    }
    let streams = vec![in_memory("ticks", 3, 3)];
    for policy in policies() {
        let first = plan_with(&streams, &[], &nodes, &[], &NothingCaughtUp, policy.clone());
        assert!(first.unspread.is_empty(), "{first:?}");
        let existing = apply(&[], &first);
        let second = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, policy);
        assert_eq!(second.kept(), 3, "{second:?}");
        assert!(second.unspread.is_empty(), "{second:?}");
    }
}

/// **Passes fed their own decisions settle**: after the first placement and
/// after each owner loss, the next passes keep every shard where it is.
#[test]
fn repeated_passes_do_not_flap() {
    let streams = vec![in_memory("ticks", 8, 1), stream("orders", 4)];
    let mut nodes = live(&["broker-a", "broker-b", "broker-c"]);
    for policy in policies() {
        let mut existing = Vec::new();
        for lost in [None, Some(0), Some(1)] {
            if let Some(lost) = lost {
                nodes[lost].status.lifecycle = NodeLifecycle::Down;
            }
            let first = plan_with(
                &streams,
                &[],
                &nodes,
                &existing,
                &NothingCaughtUp,
                policy.clone(),
            );
            existing = apply(&existing, &first);
            for _ in 0..3 {
                let again = plan_with(
                    &streams,
                    &[],
                    &nodes,
                    &existing,
                    &NothingCaughtUp,
                    policy.clone(),
                );
                let ticks: Vec<&ShardPlan> = again
                    .shards
                    .iter()
                    .filter(|shard| shard.key.stream == "ticks")
                    .collect();
                assert!(
                    ticks.iter().all(|shard| shard.decision == Decision::Kept),
                    "{lost:?}: {ticks:?}"
                );
                existing = apply(&existing, &again);
            }
            let owners: BTreeSet<&str> = existing
                .iter()
                .filter(|a| a.key.stream == "ticks")
                .map(|a| a.leader.as_str())
                .collect();
            assert!(owners.iter().all(|owner| {
                nodes
                    .iter()
                    .any(|n| n.node_id == *owner && n.status.lifecycle == NodeLifecycle::Live)
            }));
        }
        for node in &mut nodes {
            node.status.lifecycle = NodeLifecycle::Live;
        }
    }
}

/// **A hybrid move left on an in-memory shard is ended in one write** that
/// names its live owner alone, rather than waiting for reports that never
/// come while a fenced owner refuses writes.
#[test]
fn a_hybrid_move_left_on_an_in_memory_shard_is_dropped() {
    let streams = vec![in_memory("ticks", 1, 1)];
    let nodes = live(&["broker-a", "broker-b"]);
    let mut staged = assigned("ticks", "broker-a", &["broker-b"]);
    staged.successor = Some("broker-b".to_string());
    staged.state = ShardState::Draining;
    for policy in policies() {
        let plan = plan_with(
            &streams,
            &[],
            &nodes,
            std::slice::from_ref(&staged),
            &NothingCaughtUp,
            policy,
        );
        assert_eq!(
            plan.shards[0].decision,
            Decision::Place("broker-a".to_string(), Vec::new())
        );
    }
}

/// **A durable shard never takes the in-memory step.** Its lost only copy
/// is waited for where an in-memory shard is replaced, and a Raft shard
/// keeps its own step.
#[test]
fn a_durable_shard_never_takes_the_in_memory_step() {
    let nodes = vec![
        node("broker-a", NodeLifecycle::Down, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("ticks", "broker-a", &[])];
    for policy in policies() {
        let durable = plan_with(
            &[stream("ticks", 1)],
            &[],
            &nodes,
            &existing,
            &NothingCaughtUp,
            policy.clone(),
        );
        // The hybrid waits visibly; a Raft group with no live member keeps
        // its leader named until one is back.
        let waits = if policy.raft_streams {
            Decision::Kept
        } else {
            Decision::Unplaceable(Unplaceable::OwnerUnavailable {
                leader: "broker-a".to_string(),
            })
        };
        assert_eq!(durable.shards[0].decision, waits);
    }

    // A Raft shard whose leader drains is renamed to a member, by its step.
    let streams = vec![replicated_stream("ticks", 1, 2)];
    let mut nodes = live(&["broker-a", "broker-b", "broker-c"]);
    nodes[0].status.lifecycle = NodeLifecycle::Draining;
    let existing = vec![assigned("ticks", "broker-a", &["broker-b"])];
    let raft = MovePolicy {
        raft_streams: true,
        ..MovePolicy::default()
    };
    let plan = plan_with(&streams, &[], &nodes, &existing, &NothingCaughtUp, raft);
    assert!(
        matches!(
            plan.shards[0].decision,
            Decision::Move(MoveStep::RaftLeader { .. }, _)
        ),
        "{plan:?}"
    );
}

/// **Every owner change is written at a higher generation**: a new owner
/// after a loss, after a drain, and after an operator's move.
#[tokio::test]
async fn every_owner_change_takes_a_new_generation() {
    let store = cluster(&["broker-a", "broker-b", "broker-c"]).await;
    store
        .create_stream(in_memory("ticks", 1, 1))
        .await
        .expect("stream");
    let policy = MovePolicy::default();
    reconcile_once(&store, &Default::default(), policy.clone()).await;
    let first = current(&store).await;

    store
        .set_node_lifecycle(&first.leader, NodeLifecycle::Down)
        .await
        .expect("down");
    reconcile_once(&store, &Default::default(), policy.clone()).await;
    let second = current(&store).await;
    assert_ne!(second.leader, first.leader);
    assert!(second.generation > first.generation, "{first:?} {second:?}");

    store
        .set_node_lifecycle(&second.leader, NodeLifecycle::Draining)
        .await
        .expect("drain");
    store.set_moves_paused(true).await.expect("pause");
    reconcile_once(&store, &Default::default(), policy.clone()).await;
    let third = current(&store).await;
    assert_ne!(third.leader, second.leader);
    assert_ne!(third.leader, first.leader);
    assert!(third.generation > second.generation, "{second:?} {third:?}");

    // Nothing moves a live owner on the next pass.
    reconcile_once(&store, &Default::default(), policy).await;
    assert_eq!(current(&store).await.generation, third.generation);
}

async fn current(store: &crate::store::memory::InMemoryStore) -> ShardAssignment {
    store
        .list_shard_assignments()
        .await
        .expect("list")
        .into_iter()
        .find(|a| a.key.stream == "ticks")
        .expect("placed")
}

/// **An operator's move is one write naming the new owner**, which reads no
/// report and keeps no copy on the old owner.
#[test]
fn an_operator_moves_an_in_memory_shard_in_one_write() {
    let streams = vec![in_memory("ticks", 1, 1)];
    let nodes = live(&["broker-a", "broker-b"]);
    let existing = vec![assigned("ticks", "broker-a", &[])];
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &Misleading,
        policy: MovePolicy::default(),
    };
    let step = start_move(&catalog, &existing[0].key, "broker-b").expect("moved");
    assert_eq!(
        step.step,
        MoveStep::OwnerReplaced {
            from: "broker-a".to_string(),
            to: "broker-b".to_string(),
        }
    );
    assert_eq!(step.assignment.leader, "broker-b");
    assert!(step.assignment.replicas.is_empty());
    assert!(step.assignment.successor.is_none());
    assert_eq!(step.expected_generation, existing[0].generation);
}

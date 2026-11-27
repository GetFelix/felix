//! A Raft shard placed on fewer brokers than its replication factor never
//! forms: the brokers form nothing on a short set and the store stores no
//! target for one, so the one-member top-up would wait for good (#3331).
//! Placement writes the full set in one step once enough brokers are live.
use super::super::raft_leaders::RAFT_REPLACEMENTS_PER_PASS;
use super::raft_leaders::{ABC, member_of, raft, set_of};
use super::*;
use crate::model::{
    PlacedInEpoch, RaftBases, RaftFormed, RaftOrdered, RaftSeed, RaftTarget, RaftVoters,
};

const ABCD: [&str; 4] = ["broker-a", "broker-b", "broker-c", "broker-d"];

/// `orders` shard 0 as the first placement left it on `leader` and
/// `followers` while too few brokers were live: the epoch's record names
/// those brokers alone, and the group left no trace.
fn thin(leader: &str, followers: &[&str]) -> ShardAssignment {
    let mut assignment = pinned("orders", 0, leader);
    assignment.replicas = followers.iter().map(|node| node.to_string()).collect();
    assignment.placed = Some(PlacedInEpoch {
        epoch: 0,
        brokers: set_of(&assignment),
    });
    assignment
}

/// [`thin`] for shard 0 of the cache `sessions`.
fn thin_cache(leader: &str, followers: &[&str]) -> ShardAssignment {
    let mut assignment = thin(leader, followers);
    assignment.key.stream = "sessions".to_string();
    assignment.key.kind = ShardKind::Cache;
    assignment
}

/// The one fill the plan makes, as (brokers added, the write).
fn the_fill(plan: &Plan) -> (Vec<String>, ShardAssignment) {
    let moves: Vec<_> = plan.moves().collect();
    assert_eq!(moves.len(), 1, "{plan:?}");
    match moves[0] {
        (_, MoveStep::RaftFill { to }, written) => (to.clone(), written.clone()),
        other => panic!("expected a fill, got {other:?}"),
    }
}

fn plan_of(
    streams: &[Stream],
    caches: &[Cache],
    nodes: &[Node],
    existing: ShardAssignment,
) -> Plan {
    plan_with(
        streams,
        caches,
        nodes,
        &[existing],
        &NothingCaughtUp,
        raft(),
    )
}

fn unfilled(held: Blocked) -> Blocked {
    Blocked::Unfilled {
        held: Box::new(held),
    }
}

/// **The issue's two cases, at RF 3: placed on one broker or on two, the
/// set is written in full in one step**, for a stream and for a cache, and
/// the leader keeps its seat. A top-up would wait for a target forever.
#[test]
fn a_raft_shard_placed_on_too_few_brokers_is_placed_in_full_in_one_write() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let caches = vec![super::caches::replicated_cache("sessions", 1, 3)];
    for followers in [&[][..], &["broker-b"][..]] {
        for existing in [
            thin("broker-a", followers),
            thin_cache("broker-a", followers),
        ] {
            let plan = plan_of(&streams, &caches, &live(&ABCD), existing.clone());

            let (added, written) = the_fill(&plan);
            assert_eq!(written.nodes().count(), 3, "{plan:?}");
            assert_eq!(added.len(), 3 - existing.nodes().count(), "{plan:?}");
            assert!(set_of(&existing).is_subset(&set_of(&written)), "{plan:?}");
            assert_eq!(written.leader, "broker-a", "the leader keeps its seat");
            assert_eq!(written.raft_target, None);
        }
    }
}

/// At RF 2 a set of one takes one more broker, in the same kind of write.
#[test]
fn a_raft_shard_on_one_broker_at_rf_2_is_placed_in_full() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let caches = vec![super::caches::replicated_cache("sessions", 1, 2)];
    for existing in [thin("broker-a", &[]), thin_cache("broker-a", &[])] {
        let plan = plan_of(&streams, &caches, &live(&ABCD), existing);

        let (added, written) = the_fill(&plan);
        assert_eq!(added.len(), 1, "{plan:?}");
        assert_eq!(written.nodes().count(), 2, "{plan:?}");
    }
}

/// **Not enough brokers: the shard waits, says why, and counts as
/// under-replicated; it is filled once one more joins.**
#[test]
fn a_thin_raft_shard_waits_for_enough_brokers_and_fills_when_they_join() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = thin("broker-a", &[]);

    let held = plan_of(
        &streams,
        &[],
        &live(&["broker-a", "broker-b"]),
        existing.clone(),
    );
    assert_eq!(held.moves().count(), 0, "{held:?}");
    let waits: Vec<_> = held.waiting().map(|(_, why)| why.clone()).collect();
    assert_eq!(
        waits,
        vec![unfilled(Blocked::TooFewBrokers {
            eligible: 2,
            needed: 3
        })],
        "{held:?}"
    );
    assert_eq!(
        held.under_replicated,
        vec![existing.key.clone()],
        "{held:?}"
    );

    let joined = plan_of(&streams, &[], &live(&ABC), existing);
    let (added, written) = the_fill(&joined);
    assert_eq!(added.len(), 2, "{joined:?}");
    assert_eq!(
        set_of(&written),
        ABC.iter().map(|n| n.to_string()).collect()
    );
}

/// A pause holds the fill, as it holds a top-up.
#[test]
fn a_paused_placement_does_not_fill_a_thin_raft_shard() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let policy = MovePolicy {
        paused: true,
        ..raft()
    };

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABCD),
        &[thin("broker-a", &[])],
        &NothingCaughtUp,
        policy,
    );

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    let waits: Vec<_> = plan.waiting().map(|(_, why)| why.clone()).collect();
    assert_eq!(waits, vec![unfilled(Blocked::Paused)], "{plan:?}");
}

/// Fills share the per-pass budget of set changes.
#[test]
fn filling_thin_raft_shards_takes_the_replacement_budget() {
    let shards = RAFT_REPLACEMENTS_PER_PASS as u32 + 1;
    let streams = vec![replicated_stream("orders", shards, 3)];
    let existing: Vec<ShardAssignment> = (0..shards)
        .map(|shard| {
            let mut assignment = thin("broker-a", &[]);
            assignment.key.shard = shard;
            assignment
        })
        .collect();

    let plan = plan_with(
        &streams,
        &[],
        &live(&ABCD),
        &existing,
        &NothingCaughtUp,
        raft(),
    );

    let filled = plan
        .moves()
        .filter(|(_, step, _)| matches!(step, MoveStep::RaftFill { .. }))
        .count();
    assert_eq!(filled, RAFT_REPLACEMENTS_PER_PASS, "{plan:?}");
    assert!(
        plan.waiting()
            .any(|(_, why)| why == &unfilled(Blocked::MoveLimit)),
        "{plan:?}"
    );
}

/// A follower that must leave holds no group's records, so the fill
/// leaves it out instead of waiting on a swap that needs a target.
#[test]
fn a_thin_raft_shard_fills_without_its_draining_follower() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut nodes = live(&ABCD);
    nodes[1].status.lifecycle = NodeLifecycle::Draining;

    let plan = plan_of(&streams, &[], &nodes, thin("broker-a", &["broker-b"]));

    let (_, written) = the_fill(&plan);
    assert_eq!(
        set_of(&written),
        ["broker-a", "broker-c", "broker-d"]
            .iter()
            .map(|n| n.to_string())
            .collect(),
        "{plan:?}"
    );
}

/// Adds one trace a group leaves to a record.
type Trace = fn(&mut ShardAssignment);

/// **A short set whose group may have formed is never filled in one
/// step.** A set change clears the target and the copy ids, so each trace
/// a group leaves is enough on its own to rule the fill out: brokers new to
/// the set could otherwise form a second group on empty logs (#2905).
#[test]
fn a_short_raft_set_with_any_trace_of_a_group_is_not_filled() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let incarnation = thin("broker-a", &[]).incarnation;
    let traces: Vec<(&str, Trace)> = vec![
        ("placed on a full set once", |a| {
            a.placed
                .as_mut()
                .unwrap()
                .brokers
                .extend(["broker-b", "broker-c"].iter().map(|n| n.to_string()));
        }),
        ("re-formed", |a| a.placed.as_mut().unwrap().epoch = 1),
        ("no placed record", |a| a.placed = None),
        ("a stored target", |a| {
            a.raft_target = Some(RaftTarget {
                counter: 1,
                members: [("broker-a".to_string(), 7)].into(),
            });
        }),
        ("a counter kept from a dropped target", |a| {
            a.raft_target_counter = 2
        }),
        ("a formed mark", |a| {
            a.raft_formed = Some(RaftFormed {
                incarnation: a.incarnation,
                epoch: 0,
            });
        }),
        ("reported voters", |a| {
            a.raft_voters = Some(RaftVoters {
                incarnation: a.incarnation,
                epoch: 0,
                index: 4,
                members: [("broker-a".to_string(), 7)].into(),
            });
        }),
        ("a reported floor", |a| {
            a.raft_floor = Some(crate::model::RaftFloor {
                incarnation: a.incarnation,
                offset_floor: 4,
                epoch: 0,
                index: 4,
                term: 1,
            });
        }),
        ("an epoch seed", |a| {
            a.raft_seed = Some(RaftSeed {
                incarnation: a.incarnation,
                epoch: 0,
                members: [("broker-a".to_string(), 7)].into(),
                starts: Default::default(),
            });
        }),
        ("re-form bases", |a| {
            a.raft_bases = Some(RaftBases {
                incarnation: a.incarnation,
                bases: Vec::new(),
            });
        }),
        ("an ordered target", |a| {
            a.raft_ordered = Some(RaftOrdered {
                incarnation: a.incarnation,
                epoch: 0,
                targets: vec![RaftTarget {
                    counter: 1,
                    members: [("broker-a".to_string(), 7)].into(),
                }],
                lost: false,
            });
        }),
        ("a lost ordered list", |a| {
            a.raft_ordered = Some(RaftOrdered {
                incarnation: a.incarnation,
                epoch: 0,
                targets: Vec::new(),
                lost: true,
            });
        }),
    ];
    for (trace, add) in traces {
        let mut existing = thin("broker-a", &[]);
        add(&mut existing);
        assert_eq!(existing.incarnation, incarnation);

        let plan = plan_of(&streams, &[], &live(&ABCD), existing);

        assert!(
            plan.moves()
                .all(|(_, step, _)| !matches!(step, MoveStep::RaftFill { .. })),
            "{trace}: {plan:?}"
        );
        assert!(
            plan.moves()
                .all(|(_, _, written)| written.nodes().count() <= 2),
            "{trace}: no more than one member a write: {plan:?}"
        );
    }
}

/// **A formed group whose set change cleared its target waits for the
/// target, as before.** It keeps its formed mark and voters, so the fill
/// does not apply, and the top-up waits for every member's copy id.
#[test]
fn a_formed_raft_group_with_a_cleared_target_is_not_filled() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut existing = member_of(0, "broker-a");
    existing.replicas = vec!["broker-b".to_string()];
    existing.raft_target = None;
    existing.raft_copy_ids.clear();
    existing.placed = Some(PlacedInEpoch {
        epoch: 0,
        brokers: ABC.iter().map(|n| n.to_string()).collect(),
    });

    let plan = plan_of(&streams, &[], &live(&ABCD), existing);

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    let waits: Vec<_> = plan.waiting().map(|(_, why)| why.clone()).collect();
    assert_eq!(waits, vec![Blocked::TargetUnsettled], "{plan:?}");
}

/// **A full set whose target a set change cleared before any voter report
/// is not filled.** Only `placed` remembers the full set here, and it is
/// enough: the target named copies on three brokers, so a group may have
/// formed on them.
#[test]
fn a_short_raft_set_once_full_with_its_target_cleared_is_not_filled() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut existing = thin("broker-a", &["broker-b"]);
    existing.placed = Some(PlacedInEpoch {
        epoch: 0,
        brokers: ABC.iter().map(|n| n.to_string()).collect(),
    });

    let plan = plan_of(&streams, &[], &live(&ABCD), existing);

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    let waits: Vec<_> = plan.waiting().map(|(_, why)| why.clone()).collect();
    assert_eq!(waits, vec![Blocked::TargetUnsettled], "{plan:?}");
}

/// `thin` on `leader` and `followers`, with `nodes` where `leader` is not
/// live: the fill drops it and names a live broker leader.
fn filled_without(leader: &str, followers: &[&str], nodes: &[Node]) -> ShardAssignment {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let plan = plan_with(
        &streams,
        &[],
        nodes,
        &[thin(leader, followers)],
        &NothingCaughtUp,
        raft(),
    );
    let (_, written) = the_fill(&plan);
    assert_eq!(written.nodes().count(), 3, "{plan:?}");
    assert!(!set_of(&written).contains(leader), "{plan:?}");
    assert_ne!(written.leader, leader, "{plan:?}");
    written
}

/// **A lone placed leader that is down, lost or draining is dropped by the
/// fill.** Its group never formed, so it holds no record; waiting on it
/// would hold the shard for good.
#[test]
fn a_thin_raft_shard_whose_lone_leader_is_down_lost_or_draining_is_filled_without_it() {
    use super::restore::{MINUTE, gone};
    let mut down = live(&ABCD);
    down[0].status.lifecycle = NodeLifecycle::Down;
    let mut lost = live(&ABCD);
    lost[0] = gone("broker-a", 10 * MINUTE);
    let mut draining = live(&ABCD);
    draining[0].status.lifecycle = NodeLifecycle::Draining;
    for (case, nodes) in [("down", down), ("lost", lost), ("draining", draining)] {
        let written = filled_without("broker-a", &[], &nodes);
        assert_eq!(
            set_of(&written),
            ["broker-b", "broker-c", "broker-d"]
                .iter()
                .map(|n| n.to_string())
                .collect(),
            "{case}"
        );
    }
}

/// **Placed on two at RF 3, leader dead and follower alive: the follower
/// keeps its seat and leads**, and one live broker fills the set.
#[test]
fn a_thin_raft_shard_whose_leader_died_is_led_by_its_live_follower() {
    let mut nodes = live(&ABCD);
    nodes[0].status.lifecycle = NodeLifecycle::Down;

    let written = filled_without("broker-a", &["broker-b"], &nodes);

    assert_eq!(written.leader, "broker-b");
    assert_eq!(written.replicas.len(), 2);
}

/// A dead lone leader does not count toward the brokers the fill needs.
#[test]
fn a_thin_raft_shard_with_a_dead_leader_waits_for_enough_live_brokers() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let mut nodes = live(&ABC);
    nodes[0].status.lifecycle = NodeLifecycle::Down;

    let plan = plan_of(&streams, &[], &nodes, thin("broker-a", &[]));

    assert_eq!(plan.moves().count(), 0, "{plan:?}");
    let waits: Vec<_> = plan.waiting().map(|(_, why)| why.clone()).collect();
    assert_eq!(
        waits,
        vec![unfilled(Blocked::TooFewBrokers {
            eligible: 2,
            needed: 3
        })],
        "{plan:?}"
    );
}

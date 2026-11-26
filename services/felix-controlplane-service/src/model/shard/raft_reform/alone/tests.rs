//! A group of one's self re-form: what it writes and every record or
//! request that refuses it.
use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::model::{
    PendingSet, PlacedInEpoch, RaftEpochBase, RaftFloor, RaftLogId, RaftOrdered, RaftSeed,
    RaftTarget, ShardKey, ShardKind,
};

fn copies(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

fn set(nodes: &[&str]) -> BTreeSet<String> {
    nodes.iter().map(|n| n.to_string()).collect()
}

fn target(counter: u64, pairs: &[(&str, u64)]) -> RaftTarget {
    RaftTarget {
        counter,
        members: copies(pairs),
    }
}

/// A replication factor 1 Raft shard at incarnation 3, epoch 0, alone on
/// broker `a`: placed, seeded, ordered and targeted on it only.
fn alone() -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            kind: ShardKind::Stream,
        },
        leader: "a".to_string(),
        replicas: Vec::new(),
        generation: 7,
        state: crate::model::ShardState::Active,
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
            brokers: set(&["a"]),
        }),
        switch: None,
        raft_copy_ids: copies(&[("a", 11)]),
        raft_target_counter: 0,
        raft_target: Some(target(1, &[("a", 11)])),
        raft_floor: Some(RaftFloor {
            incarnation: 3,
            offset_floor: 40,
            epoch: 0,
            index: 40,
            term: 5,
        }),
        raft_formed: None,
        raft_voters: None,
        raft_seed: Some(RaftSeed {
            incarnation: 3,
            epoch: 0,
            members: copies(&[("a", 11)]),
            starts: BTreeMap::new(),
        }),
        raft_bases: None,
        raft_leader: None,
        raft_ordered: Some(RaftOrdered {
            incarnation: 3,
            epoch: 0,
            targets: vec![target(1, &[("a", 11)])],
            lost: false,
        }),
    }
}

fn facts() -> AloneFacts {
    AloneFacts {
        replication_factor: Some(1),
        declared_dead: false,
    }
}

fn base(first_index: u64) -> RaftEpochBase {
    RaftEpochBase {
        base_last: Some(RaftLogId {
            index: 40,
            epoch: 0,
            term: 5,
        }),
        first_index,
    }
}

fn wire_start() -> felix_storage::raft_copy::WireStart {
    felix_storage::raft_copy::WireStart {
        voter: Some(11),
        kept: Some("kept".to_string()),
        floor: None,
        needs_accept_loss: false,
        accepted: false,
    }
}

/// The request broker `a` makes from epoch 0.
fn request() -> ReformEpochStart {
    ReformEpochStart {
        incarnation: 3,
        expected_epoch: 0,
        planned_floor: Some(40),
        base: base(45),
        voters: copies(&[("a", 11)]),
        dead: BTreeSet::new(),
        starts: BTreeMap::from([("a".to_string(), wire_start())]),
    }
}

/// The record is left as it was and the reason names what was refused.
fn refused(
    mut shard: ShardAssignment,
    node: &str,
    facts: AloneFacts,
    start: &ReformEpochStart,
) -> AloneReformError {
    let before = shard.clone();
    let err = shard.start_alone(node, facts, start).expect_err("refused");
    assert_eq!(shard, before, "a refusal changes nothing");
    err
}

fn not_alone(err: AloneReformError) -> &'static str {
    match err {
        AloneReformError::NotAlone { reason, .. } => reason,
        other => panic!("not a NotAlone refusal: {other:?}"),
    }
}

/// An alone group's request writes epoch 1 with the base, the seed and the
/// broker's start, and an empty ordered list.
#[test]
fn a_group_of_one_starts_its_next_epoch() {
    let mut shard = alone();
    assert_eq!(shard.start_alone("a", facts(), &request()), Ok(true));
    assert_eq!(shard.group_epoch(), 1);
    assert_eq!(
        shard.raft_bases.as_ref().expect("bases").bases,
        vec![base(45)]
    );
    let seed = shard.raft_seed.as_ref().expect("seed");
    assert_eq!(
        (seed.epoch, &seed.members, &seed.starts),
        (1, &request().voters, &request().starts)
    );
    assert_eq!(
        shard
            .placed
            .as_ref()
            .map(|placed| (placed.epoch, placed.brokers.clone())),
        Some((1, set(&["a"])))
    );
    let ordered = shard.raft_ordered.as_ref().expect("ordered");
    assert_eq!((ordered.epoch, ordered.targets.len()), (1, 0));
}

/// The same request again, after it applied, answers the record and does
/// not write a second epoch. Any other request from the old epoch is
/// refused as moved.
#[test]
fn a_replay_is_not_a_second_bump_and_another_request_is_refused() {
    let mut shard = alone();
    shard.start_alone("a", facts(), &request()).expect("first");
    let written = shard.clone();
    assert_eq!(shard.start_alone("a", facts(), &request()), Ok(false));
    assert_eq!(shard, written, "no second bump");
    assert_eq!(shard.group_epoch(), 1);

    let other = ReformEpochStart {
        base: base(50),
        ..request()
    };
    assert_eq!(
        refused(written.clone(), "a", facts(), &other),
        AloneReformError::Write(ReformWriteError::EpochMoved {
            expected: 0,
            current: 1
        })
    );
    // A replay does not lift a refusal either: a request that is not alone
    // is refused even though it equals what was written.
    let wide = ReformEpochStart {
        voters: copies(&[("a", 11), ("b", 12)]),
        ..request()
    };
    assert!(matches!(
        refused(written, "a", facts(), &wide),
        AloneReformError::NotAlone { .. }
    ));
}

/// A group with two members lost: the control plane still places three,
/// its replication factor is 3, and its seed and targets name all of them.
/// The plan made by the one broker that answered is refused, whatever it
/// says.
#[test]
fn a_group_of_three_with_two_lost_is_refused() {
    let mut shard = alone();
    shard.replicas = vec!["b".to_string(), "c".to_string()];
    shard.placed = Some(PlacedInEpoch {
        epoch: 0,
        brokers: set(&["a", "b", "c"]),
    });
    shard.raft_seed.as_mut().expect("seed").members = copies(&[("a", 11), ("b", 12), ("c", 13)]);
    shard.raft_target = Some(target(1, &[("a", 11), ("b", 12), ("c", 13)]));
    shard.raft_ordered.as_mut().expect("ordered").targets =
        vec![target(1, &[("a", 11), ("b", 12), ("c", 13)])];
    let three = AloneFacts {
        replication_factor: Some(3),
        ..facts()
    };
    assert_eq!(
        not_alone(refused(shard.clone(), "a", three, &request())),
        "the replication factor is not 1"
    );
    // With the replication factor read as 1, the placement refuses.
    assert_eq!(
        not_alone(refused(shard, "a", facts(), &request())),
        "the shard's set is not this broker alone"
    );
}

#[test]
fn an_unknown_replication_factor_is_refused() {
    let unknown = AloneFacts {
        replication_factor: None,
        ..facts()
    };
    assert_eq!(
        not_alone(refused(alone(), "a", unknown, &request())),
        "the replication factor is not 1"
    );
}

#[test]
fn a_declared_dead_broker_is_refused() {
    let dead = AloneFacts {
        declared_dead: true,
        ..facts()
    };
    assert_eq!(
        not_alone(refused(alone(), "a", dead, &request())),
        "the broker is declared dead"
    );
}

/// Growth from 1 to 3 in progress and a move in flight: each signal of a
/// set change refuses on its own, with every other field still alone.
#[test]
fn a_set_change_or_move_in_flight_is_refused() {
    type Change = fn(&mut ShardAssignment);
    let changing: [(&str, Change); 7] = [
        ("pending set", |s| {
            s.pending_set = Some(PendingSet {
                replicas: vec!["b".to_string(), "c".to_string()],
                generation: 8,
            })
        }),
        ("successor", |s| s.successor = Some("b".to_string())),
        ("joining", |s| s.joining = Some("b".to_string())),
        ("move reason", |s| {
            s.move_reason = Some(crate::model::MoveReason::Operator)
        }),
        ("learner", |s| {
            s.learners.insert("b".to_string());
        }),
        ("draining", |s| s.state = crate::model::ShardState::Draining),
        ("a follower", |s| s.replicas = vec!["b".to_string()]),
    ];
    for (what, change) in changing {
        let mut shard = alone();
        change(&mut shard);
        let reason = not_alone(refused(shard, "a", facts(), &request()));
        assert!(
            reason == "a set change or move is in flight"
                || reason == "the shard's set is not this broker alone",
            "{what}: {reason}"
        );
        // The set-change check is the one that stops all but the follower.
        if what != "a follower" {
            assert_eq!(reason, "a set change or move is in flight", "{what}");
        }
    }
}

/// A broker the epoch placed besides this one, left on the record, may hold
/// records this plan never read.
#[test]
fn another_placed_broker_is_refused() {
    let mut shard = alone();
    shard.placed = Some(PlacedInEpoch {
        epoch: 0,
        brokers: set(&["a", "b"]),
    });
    assert_eq!(
        not_alone(refused(shard, "a", facts(), &request())),
        "a broker besides this one is placed in the epoch"
    );
    let mut unknown = alone();
    unknown.placed = None;
    assert_eq!(
        not_alone(refused(unknown, "a", facts(), &request())),
        "a broker besides this one is placed in the epoch"
    );
}

/// Placement grew while the broker was planning: the target, the ordered
/// list or the seed names another broker, or is not known.
#[test]
fn a_seed_target_or_list_naming_another_broker_is_refused() {
    let wider = |pairs: &[(&str, u64)]| target(2, pairs);
    let mut seed = alone();
    seed.raft_seed.as_mut().expect("seed").members = copies(&[("a", 11), ("b", 12)]);
    assert_eq!(
        not_alone(refused(seed, "a", facts(), &request())),
        "the epoch's seed is not this broker alone"
    );
    let mut no_seed = alone();
    no_seed.raft_seed = None;
    assert_eq!(
        not_alone(refused(no_seed, "a", facts(), &request())),
        "the epoch's seed is not this broker alone"
    );
    let mut old_seed = alone();
    old_seed.raft_seed.as_mut().expect("seed").incarnation = 2;
    assert_eq!(
        not_alone(refused(old_seed, "a", facts(), &request())),
        "the epoch's seed is not this broker alone"
    );

    let mut ordered = alone();
    ordered
        .raft_ordered
        .as_mut()
        .expect("ordered")
        .targets
        .push(wider(&[("a", 11), ("b", 12)]));
    assert_eq!(
        not_alone(refused(ordered, "a", facts(), &request())),
        "a target ordered in the epoch names another broker"
    );
    let mut lost = alone();
    lost.raft_ordered.as_mut().expect("ordered").lost = true;
    assert_eq!(
        not_alone(refused(lost, "a", facts(), &request())),
        "a target ordered in the epoch names another broker"
    );
    let mut no_list = alone();
    no_list.raft_ordered = None;
    assert_eq!(
        not_alone(refused(no_list, "a", facts(), &request())),
        "a target ordered in the epoch names another broker"
    );

    let mut stored = alone();
    stored.raft_target = Some(wider(&[("a", 11), ("b", 12)]));
    assert_eq!(
        not_alone(refused(stored, "a", facts(), &request())),
        "the stored target names another broker"
    );
}

/// A plan from a restored directory, made from an older epoch, never
/// writes over the epoch the group is at, and a floor a leader reported
/// above what the plan read refuses it.
#[test]
fn a_stale_epoch_or_a_raised_floor_is_refused() {
    // The group is at epoch 1 and the plan was made at epoch 0.
    let mut moved = alone();
    moved
        .start_alone("a", facts(), &request())
        .expect("epoch 1");
    let stale = ReformEpochStart {
        base: base(60),
        ..request()
    };
    assert_eq!(
        refused(moved, "a", facts(), &stale),
        AloneReformError::Write(ReformWriteError::EpochMoved {
            expected: 0,
            current: 1
        })
    );

    let mut raised = alone();
    raised.raft_floor.as_mut().expect("floor").offset_floor = 50;
    assert_eq!(
        refused(raised, "a", facts(), &request()),
        AloneReformError::Write(ReformWriteError::FloorRaised {
            planned: 40,
            stored: 50
        })
    );

    let other = ReformEpochStart {
        incarnation: 2,
        ..request()
    };
    assert_eq!(
        refused(alone(), "a", facts(), &other),
        AloneReformError::Write(ReformWriteError::OtherIncarnation {
            planned: 2,
            current: 3
        })
    );
}

/// The request itself names this broker alone: voters, starts and the dead
/// set, and the caller is the shard's one broker.
#[test]
fn a_request_naming_another_broker_is_refused() {
    let wider = ReformEpochStart {
        voters: copies(&[("a", 11), ("b", 12)]),
        ..request()
    };
    assert_eq!(
        not_alone(refused(alone(), "a", facts(), &wider)),
        "the plan's voters are not this broker alone"
    );
    let empty = ReformEpochStart {
        voters: BTreeMap::new(),
        ..request()
    };
    assert_eq!(
        not_alone(refused(alone(), "a", facts(), &empty)),
        "the plan's voters are not this broker alone"
    );
    let other_voter = ReformEpochStart {
        voters: copies(&[("b", 12)]),
        ..request()
    };
    assert_eq!(
        not_alone(refused(alone(), "a", facts(), &other_voter)),
        "the plan's voters are not this broker alone"
    );
    let mut starts = request();
    starts.starts.insert("b".to_string(), wire_start());
    assert_eq!(
        not_alone(refused(alone(), "a", facts(), &starts)),
        "the plan starts a broker besides this one"
    );
    let dead = ReformEpochStart {
        dead: set(&["b"]),
        ..request()
    };
    assert_eq!(
        not_alone(refused(alone(), "a", facts(), &dead)),
        "the plan declares a broker dead"
    );
    // Another broker calling for this shard, with a plan that names it.
    let mut elsewhere = alone();
    let for_b = ReformEpochStart {
        voters: copies(&[("b", 12)]),
        starts: BTreeMap::from([("b".to_string(), wire_start())]),
        ..request()
    };
    assert_eq!(
        not_alone(refused(elsewhere.clone(), "b", facts(), &for_b)),
        "the shard's set is not this broker alone"
    );
    elsewhere.leader = "b".to_string();
    assert_eq!(
        not_alone(refused(elsewhere, "b", facts(), &for_b)),
        "a broker besides this one is placed in the epoch"
    );
}

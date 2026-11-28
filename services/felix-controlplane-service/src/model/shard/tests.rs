//! Shard assignment validation, transitions, and serialization.
use super::*;

fn assignment() -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            stream: "orders".to_string(),
            shard: 2,
            kind: ShardKind::Stream,
        },
        leader: "broker-a".to_string(),
        replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
        generation: 7,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: None,
        raft_copy_ids: Default::default(),
        raft_target_counter: 0,
        raft_target: None,
        raft_floor: None,
        raft_formed: None,
        raft_voters: None,
        raft_seed: None,
        raft_bases: None,
        raft_leader: None,
        raft_ordered: None,
    }
}

#[test]
fn a_well_formed_assignment_validates() {
    assert_eq!(assignment().validate(), Ok(()));
    assert_eq!(assignment().validate_within(4), Ok(()));
}

/// The bound is the stream's shard count, so shard N is valid only for a stream
/// with more than N shards.
#[test]
fn a_shard_outside_the_stream_is_rejected() {
    let a = assignment();
    assert_eq!(
        a.validate_within(2),
        Err(ShardValidationError::ShardOutOfBounds {
            shard: 2,
            shards: 2
        }),
    );
    assert_eq!(a.validate_within(3), Ok(()), "shard 2 fits in 3 shards");
}

#[test]
fn a_leader_that_is_also_a_replica_is_rejected() {
    let mut a = assignment();
    a.replicas.push("broker-a".to_string());
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::LeaderIsAlsoReplica(
            "broker-a".to_string()
        )),
    );
}

/// A repeated replica would make a quorum look larger than it is.
#[test]
fn a_repeated_replica_is_rejected() {
    let mut a = assignment();
    a.replicas = vec!["broker-b".to_string(), "broker-b".to_string()];
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::DuplicateReplica(
            "broker-b".to_string()
        )),
    );
}

#[test]
fn malformed_node_identities_are_rejected() {
    let mut a = assignment();
    a.leader = "Broker A".to_string();
    assert!(matches!(
        a.validate(),
        Err(ShardValidationError::InvalidLeader(_))
    ));

    let mut a = assignment();
    a.replicas = vec![String::new()];
    assert!(matches!(
        a.validate(),
        Err(ShardValidationError::InvalidReplica(_))
    ));
}

#[test]
fn an_assignment_with_no_replicas_is_valid() {
    let mut a = assignment();
    a.replicas.clear();
    assert_eq!(
        a.validate(),
        Ok(()),
        "an assignment without replicas is valid"
    );
}

/// A drain is entered from either serving state and left only through a
/// fresh `Assigning`: the shard never goes back to serving where it stands.
#[test]
fn a_drained_shard_leaves_only_through_a_new_assignment() {
    assert!(ShardState::Active.can_transition_to(ShardState::Draining));
    assert!(ShardState::Assigning.can_transition_to(ShardState::Draining));
    assert!(ShardState::Draining.can_transition_to(ShardState::Assigning));
    assert!(!ShardState::Draining.can_transition_to(ShardState::Active));
    assert!(!ShardState::Active.can_transition_to(ShardState::Assigning));
}

#[test]
fn a_successor_must_be_a_replica_and_not_the_leader() {
    let mut a = assignment();
    a.successor = Some(a.leader.clone());
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::SuccessorIsLeader(a.leader.clone()))
    );

    let mut a = assignment();
    a.successor = Some("broker-elsewhere".to_string());
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::SuccessorNotAReplica(
            "broker-elsewhere".to_string()
        ))
    );

    let mut a = assignment();
    a.successor = a.replicas.first().cloned();
    assert!(a.successor.is_some(), "the fixture needs a replica");
    assert_eq!(a.validate(), Ok(()));
}

/// An assignment with no move in progress encodes exactly as it did before
/// the field existed.
#[test]
fn an_idle_assignment_omits_the_successor() {
    let a = assignment();
    let json = serde_json::to_string(&a).expect("serialize");
    assert!(!json.contains("successor"), "{json}");
    let back: ShardAssignment = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.successor, None);
}

#[test]
fn every_state_can_stay_where_it_is() {
    for state in [
        ShardState::Assigning,
        ShardState::Active,
        ShardState::Draining,
    ] {
        assert!(state.can_transition_to(state), "{state:?} should be stable");
    }
}

#[test]
fn nodes_lists_the_leader_first_then_replicas() {
    let a = assignment();
    let nodes: Vec<&str> = a.nodes().map(String::as_str).collect();
    assert_eq!(nodes, vec!["broker-a", "broker-b", "broker-c"]);
}

#[test]
fn an_assignment_round_trips_through_json() {
    let before = assignment();
    let encoded = serde_json::to_string(&before).expect("serialize");
    let after: ShardAssignment = serde_json::from_str(&encoded).expect("deserialize");
    assert_eq!(after, before);
}

/// The key is flattened, so an assignment reads as one object rather than
/// nesting the identity a caller already has from the path.
#[test]
fn the_key_is_flattened_on_the_wire() {
    let encoded = serde_json::to_value(assignment()).expect("serialize");
    assert_eq!(encoded["tenant_id"], "t1");
    assert_eq!(encoded["shard"], 2);
    assert!(encoded.get("key").is_none());
}

#[test]
fn replicas_default_to_empty_when_absent() {
    let json = serde_json::json!({
        "tenant_id": "t1",
        "namespace": "payments",
        "stream": "orders",
        "shard": 0,
        "leader": "broker-a",
        "generation": 1,
        "state": "active",
    });
    let decoded: ShardAssignment = serde_json::from_value(json).expect("deserialize");
    assert!(decoded.replicas.is_empty());
    assert_eq!(decoded.state, ShardState::Active);
}

#[test]
fn states_and_ops_serialize_as_camel_case() {
    for (state, expected) in [
        (ShardState::Assigning, "\"assigning\""),
        (ShardState::Active, "\"active\""),
        (ShardState::Draining, "\"draining\""),
    ] {
        assert_eq!(serde_json::to_string(&state).expect("serialize"), expected);
    }
    for (op, expected) in [
        (ShardAssignmentChangeOp::Assigned, "\"assigned\""),
        (ShardAssignmentChangeOp::Updated, "\"updated\""),
        (ShardAssignmentChangeOp::Unassigned, "\"unassigned\""),
    ] {
        assert_eq!(serde_json::to_string(&op).expect("serialize"), expected);
    }
}

#[test]
fn a_change_round_trips_with_and_without_a_body() {
    for change in [
        ShardAssignmentChange {
            seq: 3,
            op: ShardAssignmentChangeOp::Assigned,
            key: assignment().key,
            assignment: Some(assignment()),
        },
        ShardAssignmentChange {
            seq: 4,
            op: ShardAssignmentChangeOp::Unassigned,
            key: assignment().key,
            assignment: None,
        },
    ] {
        let encoded = serde_json::to_string(&change).expect("serialize");
        let decoded: ShardAssignmentChange = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, change);
    }
}

fn report_at(generation: u64, at: u64) -> ReplicaReport {
    ReplicaReport {
        inherited_storage: Default::default(),
        given_up: false,
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        key: assignment().key,
        generation,
        caught_up: BTreeSet::new(),
        offsets: BTreeMap::new(),
        reported_at_millis: at,
        drained: false,
        leader_offset: None,
        halted: BTreeMap::new(),
        leader: None,
        node_incarnations: Default::default(),
        follower_storage: Default::default(),
        joint_start_acked: None,
        refused: None,
        holds_start: Default::default(),
    }
}

fn halted(generation: u64, since_millis: u64) -> HaltedCopy {
    HaltedCopy {
        reason: "diverged".to_string(),
        generation,
        since_millis,
    }
}

/// A halt keeps the time it was first reported, so placement can tell how
/// long a copy has been stuck across reports that each restate it.
#[test]
fn a_halt_keeps_when_it_was_first_reported() {
    let mut first = report_at(3, 100);
    first.halted.insert("b".to_string(), halted(3, 0));
    first.carry_halts(&BTreeMap::new());
    assert_eq!(first.halted["b"].since_millis, 100);

    let mut later = report_at(3, 900);
    later.halted.insert("b".to_string(), halted(3, 0));
    later.carry_halts(&first.halted);
    assert_eq!(later.halted["b"].since_millis, 100);
}

/// A node the next report does not mention stays listed for two more
/// generations, so the passes that replace it do not pick it again, and then
/// goes.
#[test]
fn a_dropped_halt_is_carried_for_two_generations() {
    let mut held = report_at(3, 100);
    held.halted.insert("b".to_string(), halted(3, 100));

    let mut next = report_at(4, 200);
    next.carry_halts(&held.halted);
    assert_eq!(next.halted["b"], halted(3, 100));

    let mut same = report_at(4, 300);
    same.carry_halts(&next.halted);
    assert!(same.halted.contains_key("b"));

    let mut after = report_at(5, 400);
    after.carry_halts(&same.halted);
    assert!(after.halted.contains_key("b"));

    let mut gone = report_at(6, 500);
    gone.carry_halts(&after.halted);
    assert!(gone.halted.is_empty());
}

/// A new generation's leader may hear from a halted follower before it finds
/// it halted again: that is not a recovery, and the halt keeps its start.
#[test]
fn a_halt_survives_the_first_answer_at_a_new_generation() {
    let mut held = report_at(3, 100);
    held.halted.insert("b".to_string(), halted(3, 100));

    let mut answered = report_at(4, 200);
    answered.offsets.insert("b".to_string(), 1);
    answered.carry_halts(&held.halted);
    assert_eq!(answered.halted["b"], halted(3, 100));

    let mut again = report_at(4, 300);
    again.halted.insert("b".to_string(), halted(4, 300));
    again.carry_halts(&answered.halted);
    assert_eq!(again.halted["b"], halted(4, 100));

    let mut level = report_at(5, 400);
    level.caught_up.insert("b".to_string());
    level.carry_halts(&again.halted);
    assert!(level.halted.is_empty(), "caught up is following");
}

/// A node that follows again is no longer halted.
#[test]
fn a_halt_that_clears_is_not_carried() {
    let mut held = report_at(3, 100);
    held.halted.insert("b".to_string(), halted(3, 100));

    let mut next = report_at(3, 200);
    next.offsets.insert("b".to_string(), 7);
    next.carry_halts(&held.halted);
    assert!(next.halted.is_empty());
}

/// A report with nothing halted is written the way it always was.
#[test]
fn a_report_with_nothing_halted_omits_the_field() {
    let json = serde_json::to_string(&report_at(3, 100)).expect("write");
    assert!(!json.contains("halted"), "{json}");
}

/// An assignment and a report with no learners and no start holders are
/// written the way they always were.
#[test]
fn learners_and_start_holders_serialize_only_when_present() {
    let json = serde_json::to_string(&assignment()).expect("write");
    assert!(!json.contains("learners"), "{json}");
    let json = serde_json::to_string(&report_at(3, 100)).expect("write");
    assert!(!json.contains("holds_start"), "{json}");

    let held = ShardAssignment {
        learners: ["broker-c".to_string()].into(),
        ..assignment()
    };
    let json = serde_json::to_string(&held).expect("write");
    assert_eq!(
        serde_json::from_str::<ShardAssignment>(&json).expect("read"),
        held
    );
}

fn ids(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(node, id)| (node.to_string(), id.to_string()))
        .collect()
}

/// A write holds as learners the members named under another storage id than
/// the one registered now, and the ones it held that no report has admitted.
/// The leader never is one, and without joint fences none is.
#[test]
fn a_write_holds_a_replaced_member_as_a_learner_until_admitted() {
    let previous = ShardAssignment {
        member_storage_ids: ids(&[("broker-a", "1"), ("broker-b", "2"), ("broker-c", "3")]),
        ..assignment()
    };
    let now = ids(&[("broker-a", "1"), ("broker-b", "22"), ("broker-c", "3")]);
    let written = |previous: &ShardAssignment, admitted: &[&str], learning: bool| {
        let mut next = assignment();
        let admitted = admitted.iter().map(|node| node.to_string()).collect();
        next.name_members(
            &now,
            learning.then_some(Learning {
                previous,
                admitted: &admitted,
            }),
        );
        next
    };

    let next = written(&previous, &[], true);
    assert_eq!(next.member_storage_ids, now);
    assert_eq!(next.learners, ["broker-b".to_string()].into());

    // Still held by the next write, and let go once admitted.
    let held = ShardAssignment {
        learners: next.learners.clone(),
        ..next.clone()
    };
    assert_eq!(written(&held, &[], true).learners, next.learners);
    assert!(written(&held, &["broker-b"], true).learners.is_empty());

    // The leader's own replaced copy is `leader_replaced`'s, not a learner's.
    let mut leader_wiped = now.clone();
    leader_wiped.insert("broker-a".to_string(), "11".to_string());
    let mut next = assignment();
    next.name_members(
        &leader_wiped,
        Some(Learning {
            previous: &previous,
            admitted: &Default::default(),
        }),
    );
    assert_eq!(next.learners, ["broker-b".to_string()].into());

    assert!(written(&previous, &[], false).learners.is_empty());
}

/// A pending set is a different set under the same leader, written at a
/// generation, with no move in progress beside it. It is not part of the
/// control plane's set, and serializes only when present.
#[test]
fn a_pending_set_must_be_another_set_with_nothing_else_moving() {
    let pending = |replicas: &[&str], generation: u64| {
        Some(PendingSet {
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation,
        })
    };
    let mut a = assignment();
    a.pending_set = pending(&["broker-c", "broker-d"], 8);
    assert_eq!(a.validate(), Ok(()));
    assert_eq!(
        a.nodes().collect::<Vec<_>>(),
        ["broker-a", "broker-b", "broker-c"]
    );
    assert_eq!(
        a.named_nodes().collect::<Vec<_>>(),
        ["broker-a", "broker-b", "broker-c", "broker-d"]
    );
    let json = serde_json::to_value(&a).expect("json");
    assert_eq!(json["pending_set"]["generation"], 8);
    assert!(
        serde_json::to_value(assignment())
            .expect("json")
            .get("pending_set")
            .is_none()
    );

    for (replicas, generation, why) in [
        (&["broker-c", "broker-b"][..], 8, "the same set"),
        (&["broker-c"][..], 0, "no generation"),
    ] {
        a.pending_set = pending(replicas, generation);
        assert_eq!(
            a.validate(),
            Err(ShardValidationError::InvalidPendingSet),
            "{why}"
        );
    }
    a.pending_set = pending(&["broker-a"], 8);
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::LeaderIsAlsoReplica(
            "broker-a".to_string()
        ))
    );
    a.pending_set = pending(&["broker-d", "broker-d"], 8);
    assert_eq!(
        a.validate(),
        Err(ShardValidationError::DuplicateReplica(
            "broker-d".to_string()
        ))
    );
    a.pending_set = pending(&["broker-d"], 8);
    a.successor = Some("broker-b".to_string());
    assert_eq!(a.validate(), Err(ShardValidationError::InvalidPendingSet));
}

/// The window and the top of the range.
#[test]
fn elected_and_issued_generations_are_bounded() {
    assert_eq!(next_generation(7), Ok(8));
    assert!(next_generation(i64::MAX as u64).is_err());
    assert!(next_generation(u64::MAX).is_err());
    assert!(check_elected_generation(7, 7 + ELECTION_GENERATION_WINDOW).is_ok());
    assert!(check_elected_generation(7, 8 + ELECTION_GENERATION_WINDOW).is_err());
    assert!(check_elected_generation(i64::MAX as u64 - 1, i64::MAX as u64 + 1).is_err());
}

/// A floor at or below the next generation changes nothing; one above it is
/// taken within the window and refused past it.
#[test]
fn a_floor_above_the_next_generation_is_taken_within_the_window() {
    assert_eq!(generation_at_least(7, 0), Ok(8));
    assert_eq!(generation_at_least(7, 8), Ok(8));
    assert_eq!(generation_at_least(7, 10), Ok(10));
    // One past a ballot at the edge of the window.
    assert_eq!(
        generation_at_least(7, 8 + ELECTION_GENERATION_WINDOW),
        Ok(8 + ELECTION_GENERATION_WINDOW)
    );
    assert!(generation_at_least(7, 9 + ELECTION_GENERATION_WINDOW).is_err());
    assert!(generation_at_least(i64::MAX as u64 - 1, i64::MAX as u64 + 1).is_err());
    assert!(generation_at_least(i64::MAX as u64, 0).is_err());
}

const LOST: &str = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a";
const NEW: &str = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b";

/// broker-c's storage was replaced and the assignment names its new copy,
/// holding it as a learner.
fn readmitting() -> ShardAssignment {
    ShardAssignment {
        member_storage_ids: [("broker-c".to_string(), NEW.to_string())].into(),
        learners: ["broker-c".to_string()].into(),
        ..assignment()
    }
}

/// broker-a, the leader, shows broker-c's new copy caught up at `generation`.
fn caught_up_at(generation: u64) -> ReplicaReport {
    ReplicaReport {
        leader: Some("broker-a".to_string()),
        caught_up: ["broker-c".to_string()].into(),
        offsets: [("broker-c".to_string(), 40)].into(),
        follower_storage: [("broker-c".to_string(), NEW.to_string())].into(),
        node_incarnations: [("broker-c".to_string(), 3)].into(),
        ..report_at(generation, 0)
    }
}

fn registered(node: &str) -> Option<(String, u64)> {
    (node == "broker-c").then(|| (NEW.to_string(), 3))
}

#[test]
fn the_leader_of_a_set_naming_a_new_copy_readmits_it() {
    assert_eq!(
        readmitting().readmitted_by(&caught_up_at(7), &registered),
        vec![Readmitted {
            node: "broker-c".to_string(),
            storage_id: NEW.to_string(),
            generation: 7,
            offset: 40,
        }]
    );
}

/// A report from before the set naming the new copy, here generation 6,
/// readmits nothing: its set was introduced before the loss.
#[test]
fn a_report_of_an_earlier_generation_readmits_nothing() {
    assert!(
        readmitting()
            .readmitted_by(&caught_up_at(6), &registered)
            .is_empty()
    );
}

#[test]
fn only_the_copy_the_set_names_and_registered_is_readmitted() {
    let named_lost = ShardAssignment {
        member_storage_ids: [("broker-c".to_string(), LOST.to_string())].into(),
        ..readmitting()
    };
    assert!(
        named_lost
            .readmitted_by(&caught_up_at(7), &registered)
            .is_empty(),
        "the set names the lost copy"
    );
    let not_learner = ShardAssignment {
        learners: Default::default(),
        ..readmitting()
    };
    assert!(
        not_learner
            .readmitted_by(&caught_up_at(7), &registered)
            .is_empty(),
        "a copy that was never replaced"
    );
    let other_copy = ReplicaReport {
        follower_storage: [("broker-c".to_string(), LOST.to_string())].into(),
        ..caught_up_at(7)
    };
    assert!(
        readmitting()
            .readmitted_by(&other_copy, &registered)
            .is_empty(),
        "the leader saw another copy"
    );
    let restarted = ReplicaReport {
        node_incarnations: [("broker-c".to_string(), 2)].into(),
        ..caught_up_at(7)
    };
    assert!(
        readmitting()
            .readmitted_by(&restarted, &registered)
            .is_empty(),
        "taken at an earlier incarnation"
    );
    let unstamped = ReplicaReport {
        leader: None,
        ..caught_up_at(7)
    };
    assert!(
        readmitting()
            .readmitted_by(&unstamped, &registered)
            .is_empty(),
        "not known to be the leader's"
    );
    assert!(
        readmitting()
            .readmitted_by(&caught_up_at(7), &|_| None)
            .is_empty(),
        "no longer registered with it"
    );
}

#[test]
fn a_readmitted_copy_keeps_its_first_entry_and_leaves_with_its_identity() {
    let mut held = readmitting();
    let first = held.readmitted_by(&caught_up_at(7), &registered);
    held.readmit(first.clone());
    assert!(
        held.readmitted_by(&caught_up_at(7), &registered).is_empty(),
        "already held"
    );

    let mut next = readmitting();
    next.carry_readmitted(&held.readmitted);
    assert_eq!(next.readmitted, first, "carried by a write naming it alike");

    let mut renamed = ShardAssignment {
        member_storage_ids: [("broker-c".to_string(), LOST.to_string())].into(),
        ..readmitting()
    };
    renamed.carry_readmitted(&held.readmitted);
    assert!(renamed.readmitted.is_empty(), "named by another identity");

    let mut dropped = ShardAssignment {
        replicas: vec!["broker-b".to_string()],
        ..readmitting()
    };
    dropped.carry_readmitted(&held.readmitted);
    assert!(dropped.readmitted.is_empty(), "no longer in the set");
}

/// An assignment without entries serializes as it did before them, so an
/// old peer reads and writes the same bytes.
#[test]
fn no_readmitted_entries_leave_the_assignment_unchanged_on_the_wire() {
    let json = serde_json::to_value(assignment()).expect("serialize");
    assert!(json.get("readmitted").is_none());
}

/// A leader that refused its generation, gave the shard up, or halted the
/// copy vouches for nothing: none of those reports is one of a leader
/// serving the set it ships to the copy on.
#[test]
fn a_refusing_giving_up_or_halting_leader_readmits_nothing() {
    let refused = ReplicaReport {
        refused: Some(felix_common::membership::RefusedGeneration::set_unknown(7)),
        ..caught_up_at(7)
    };
    assert!(
        readmitting()
            .readmitted_by(&refused, &registered)
            .is_empty(),
        "refused"
    );
    let given_up = ReplicaReport {
        given_up: true,
        ..caught_up_at(7)
    };
    assert!(
        readmitting()
            .readmitted_by(&given_up, &registered)
            .is_empty(),
        "given up"
    );
    let halted = ReplicaReport {
        halted: [("broker-c".to_string(), halted(7, 0))].into(),
        ..caught_up_at(7)
    };
    assert!(
        readmitting().readmitted_by(&halted, &registered).is_empty(),
        "halted"
    );
}

fn brokers(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| format!("broker-{name}")).collect()
}

/// `assignment()` with `leader` and `replicas` as the set.
fn set(leader: &str, replicas: &[&str]) -> ShardAssignment {
    ShardAssignment {
        leader: format!("broker-{leader}"),
        replicas: replicas.iter().map(|r| format!("broker-{r}")).collect(),
        ..assignment()
    }
}

/// Writes `sets` in order through `carry_placed`, the first as the shard's
/// first assignment.
fn written(sets: Vec<ShardAssignment>) -> ShardAssignment {
    let mut previous: Option<ShardAssignment> = None;
    for mut next in sets {
        next.carry_placed(previous.as_ref(), true);
        previous = Some(next);
    }
    previous.expect("at least one write")
}

/// The model's `TwoMovesForgotten` trace: {A, B, C} moved to {A, B, D} and
/// back. D is off every set placed now, but its disk can hold what A and B
/// acknowledged, so it stays on the record.
#[test]
fn a_broker_moved_away_stays_on_the_record() {
    let mut adding_d = set("a", &["b", "c", "d"]);
    adding_d.joining = Some("broker-d".to_string());
    let mut adding_c = set("a", &["b", "d", "c"]);
    adding_c.joining = Some("broker-c".to_string());
    let back = written(vec![
        set("a", &["b", "c"]),
        adding_d,
        set("a", &["b", "d"]),
        adding_c,
        set("a", &["b", "c"]),
    ]);
    assert_eq!(
        back.placed,
        Some(PlacedInEpoch {
            epoch: 0,
            brokers: brokers(&["a", "b", "c", "d"]),
        })
    );
}

/// A broker is on the record from the write that first names it in any role,
/// so before a leader acts on the set: a move's successor or joining copy,
/// a pending set's member, a learner.
#[test]
fn every_role_a_write_names_is_recorded() {
    let mut pending = set("a", &["b"]);
    pending.pending_set = Some(PendingSet {
        replicas: vec!["broker-p".to_string()],
        generation: 3,
    });
    pending.learners = ["broker-l".to_string()].into();
    let mut moving = set("a", &["b", "s"]);
    moving.successor = Some("broker-s".to_string());
    let record = written(vec![set("a", &["b"]), pending, moving]).placed;
    assert_eq!(
        record.expect("started").brokers,
        brokers(&["a", "b", "l", "p", "s"])
    );
}

/// The store owns the record: what a caller passes is replaced.
#[test]
fn a_callers_record_is_replaced() {
    let mut first = set("a", &["b"]);
    first.placed = Some(PlacedInEpoch {
        epoch: 9,
        brokers: brokers(&["z"]),
    });
    let mut previous = written(vec![set("a", &["b", "c"])]);
    previous.placed.as_mut().expect("started").brokers = brokers(&["a", "b", "c"]);
    first.carry_placed(Some(&previous), true);
    assert_eq!(
        first.placed,
        Some(PlacedInEpoch {
            epoch: 0,
            brokers: brokers(&["a", "b", "c"]),
        })
    );
}

/// Only a first assignment starts a record. A shard assigned before the
/// level that stores it never gets one, since the brokers it placed and
/// removed since are unknown, and neither does a first assignment proposed
/// below that level.
#[test]
fn a_record_starts_only_with_the_first_assignment() {
    let mut first = set("a", &["b"]);
    first.carry_placed(None, false);
    assert_eq!(first.placed, None, "proposed below the level");

    let mut next = set("a", &["c"]);
    next.carry_placed(Some(&first), true);
    assert_eq!(next.placed, None, "no record to grow");
}

#[test]
fn a_broker_that_does_not_answer_blocks_the_group() {
    let record = PlacedInEpoch {
        epoch: 0,
        brokers: brokers(&["a", "b", "c", "d"]),
    };
    assert_eq!(
        record.unaccounted(&brokers(&["a", "b", "c"]), &BTreeSet::new()),
        brokers(&["d"])
    );
    assert!(
        record
            .unaccounted(&brokers(&["a", "b", "c"]), &brokers(&["d"]))
            .is_empty(),
        "declared dead"
    );
}

#[test]
fn a_voter_off_the_record_is_found() {
    let record = PlacedInEpoch {
        epoch: 0,
        brokers: brokers(&["a", "b", "c"]),
    };
    let named = brokers(&["a", "b", "d", "e"]);
    assert_eq!(record.off_record(&named, &brokers(&["e"])), brokers(&["d"]));
}

/// A restored RF 1 copy starts by itself only while it is the one broker
/// the shard was ever placed on in the epoch.
#[test]
fn a_shard_moved_in_the_epoch_is_not_alone() {
    let alone = written(vec![set("a", &[])]).placed.expect("started");
    assert!(alone.only("broker-a"));
    assert!(!alone.only("broker-b"));

    let mut moving = set("a", &["b"]);
    moving.successor = Some("broker-b".to_string());
    let moved = written(vec![set("a", &[]), moving, set("b", &[])])
        .placed
        .expect("started");
    assert!(!moved.only("broker-b"));
}

/// The next epoch's record is the brokers on this one that are not dead,
/// plus the brokers placed now.
#[test]
fn a_re_form_starts_the_next_epochs_record() {
    let record = PlacedInEpoch {
        epoch: 2,
        brokers: brokers(&["a", "b", "c", "d"]),
    };
    let next = record.next_epoch(&set("a", &["b", "c"]), &BTreeSet::new());
    assert_eq!(
        next,
        PlacedInEpoch {
            epoch: 3,
            brokers: brokers(&["a", "b", "c", "d"]),
        }
    );
    let next = record.next_epoch(&set("a", &[]), &brokers(&["b", "c"]));
    assert_eq!(
        next.brokers,
        brokers(&["a", "d"]),
        "a dead broker no longer placed leaves"
    );
    let next = record.next_epoch(&set("a", &["b"]), &brokers(&["b"]));
    assert!(
        next.brokers.contains("broker-b"),
        "a broker placed now stays"
    );
}

/// The record serializes, and an assignment without one reads back without
/// one.
#[test]
fn the_record_round_trips_and_is_omitted_when_absent() {
    let json = serde_json::to_value(assignment()).expect("serialize");
    assert!(json.get("placed").is_none(), "{json}");
    let started = written(vec![set("a", &["b"])]);
    let json = serde_json::to_string(&started).expect("serialize");
    let back: ShardAssignment = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.placed, started.placed);
}

/// The report is checked against the sealer's copy id, so neither an order
/// for a leader with none nor a report against such an order passes.
#[test]
fn a_seal_needs_the_sealers_copy_id() {
    let mut shard = assignment();
    assert_eq!(shard.can_seal(), Err(SealOrderError::NoSealerCopy));
    shard.member_storage_ids = ids(&[("broker-a", "000102030405060708090a0b0c0d0e0f")]);
    assert_eq!(shard.can_seal(), Ok(()));

    let side = SealedSideLog {
        start: 0,
        end: 0,
        digest: "cd".repeat(32),
    };
    let report = SealReport {
        generation: 7,
        offset: 1,
        start: 0,
        digest: "ab".repeat(32),
        dead_letters: side.clone(),
        cursors: side,
        voters: ids(&[
            ("broker-a", "000102030405060708090a0b0c0d0e0f"),
            ("broker-b", "101112131415161718191a1b1c1d1e1f"),
        ]),
    };
    let order = shard.seal_order(7);
    assert_eq!(report.names_voters_of(&shard, &order), Ok(()));
    let silent = ShardSwitch {
        sealer_copy: None,
        ..order
    };
    assert_eq!(
        report.names_voters_of(&shard, &silent),
        Err(SealReportError::AnotherSealer("broker-a".to_string()))
    );
}

/// **A move reason's stored name reads back as the same reason**, every
/// variant, and a name no reason has is not taken for one.
#[test]
fn move_reasons_round_trip_through_their_stored_names() {
    let all = [
        MoveReason::Drain,
        MoveReason::Balance,
        MoveReason::Operator,
        MoveReason::Replace,
        MoveReason::Restore,
    ];
    for reason in all {
        assert_eq!(MoveReason::parse(reason.as_str()), Some(reason));
        assert_eq!(reason.to_string(), reason.as_str());
        // The serde name is the stored one too.
        assert_eq!(
            serde_json::to_value(reason).expect("json"),
            serde_json::json!(reason.as_str())
        );
    }
    assert_eq!(MoveReason::parse("Drain"), None);
    assert_eq!(MoveReason::parse(""), None);
}

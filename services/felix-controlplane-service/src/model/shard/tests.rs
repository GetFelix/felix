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
        key: assignment().key,
        generation,
        caught_up: BTreeSet::new(),
        offsets: BTreeMap::new(),
        reported_at_millis: at,
        drained: false,
        leader_offset: None,
        halted: BTreeMap::new(),
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

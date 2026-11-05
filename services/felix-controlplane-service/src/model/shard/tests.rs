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

fn elected(leader: &str, replicas: &[&str], generation: u64, set_generation: u64) -> ElectedLeader {
    ElectedLeader {
        leader: leader.to_string(),
        generation,
        replicas: replicas.iter().map(|node| node.to_string()).collect(),
        set_generation,
    }
}

/// A leader elected on the set the control plane records becomes its leader
/// at the elected generation.
#[test]
fn adopting_a_leader_elected_on_the_set_names_it() {
    let adopted = assignment()
        .adopting(&elected("broker-b", &["broker-a", "broker-c"], 9, 7))
        .expect("adopted");
    assert_eq!(adopted.leader, "broker-b");
    assert_eq!(adopted.generation, 9);
    assert_eq!(adopted.replicas, vec!["broker-a", "broker-c"]);
}

/// A joining follower elected leader is recorded only: the copy it replaces
/// is not named, so the set cannot be settled, and the replacement goes on.
#[test]
fn a_joining_copy_elected_leader_is_recorded_not_adopted() {
    let replacing = ShardAssignment {
        replicas: vec![
            "broker-b".to_string(),
            "broker-c".to_string(),
            "broker-d".to_string(),
        ],
        joining: Some("broker-d".to_string()),
        move_started_at_millis: Some(5),
        move_reason: Some(MoveReason::Drain),
        ..assignment()
    };
    let recorded = replacing
        .adopting(&elected(
            "broker-d",
            &["broker-a", "broker-b", "broker-c"],
            9,
            7,
        ))
        .expect("recorded");
    assert_eq!(recorded.generation, 9);
    assert_eq!(
        ShardAssignment {
            generation: replacing.generation,
            ..recorded
        },
        replacing
    );
}

/// A follower elected while another is joining leaves the replacement going,
/// its move fields kept.
#[test]
fn a_follower_elected_while_another_joins_keeps_the_replacement() {
    let replacing = ShardAssignment {
        replicas: vec![
            "broker-b".to_string(),
            "broker-c".to_string(),
            "broker-d".to_string(),
        ],
        joining: Some("broker-d".to_string()),
        move_started_at_millis: Some(5),
        move_reason: Some(MoveReason::Drain),
        ..assignment()
    };
    let adopted = replacing
        .adopting(&elected(
            "broker-b",
            &["broker-a", "broker-c", "broker-d"],
            9,
            7,
        ))
        .expect("adopted");
    assert_eq!(adopted.leader, "broker-b");
    assert_eq!(adopted.joining.as_deref(), Some("broker-d"));
    assert_eq!(adopted.move_reason, Some(MoveReason::Drain));
    assert_eq!(adopted.move_started_at_millis, Some(5));
    assert_eq!(adopted.validate(), Ok(()));
}

/// A drain of the leader ends when another leader is adopted: it is
/// `Assigning` again with the move fields cleared.
#[test]
fn adopting_over_a_draining_leader_ends_the_move() {
    let draining = ShardAssignment {
        state: ShardState::Draining,
        successor: Some("broker-b".to_string()),
        move_started_at_millis: Some(5),
        move_reason: Some(MoveReason::Drain),
        ..assignment()
    };
    let adopted = draining
        .adopting(&elected("broker-c", &["broker-a", "broker-b"], 9, 7))
        .expect("adopted");
    assert_eq!(adopted.state, ShardState::Assigning);
    assert_eq!(adopted.successor, None);
    assert_eq!(adopted.move_started_at_millis, None);
    assert_eq!(adopted.move_reason, None);
    assert_eq!(adopted.validate(), Ok(()));
}

/// A pending set's leader that is in the new set but not the old one cannot
/// be named while the old set is the control plane's: only the generation is
/// taken.
#[test]
fn a_pending_sets_leader_outside_the_old_set_is_recorded() {
    let pending = ShardAssignment {
        pending_set: Some(PendingSet {
            replicas: vec!["broker-b".to_string(), "broker-d".to_string()],
            generation: 8,
        }),
        ..assignment()
    };
    let recorded = pending
        .adopting(&elected("broker-d", &["broker-a", "broker-b"], 12, 8))
        .expect("recorded");
    assert_eq!(recorded.generation, 12);
    assert_eq!(recorded.leader, "broker-a");
    assert_eq!(recorded.replicas, pending.replicas);
    assert_eq!(recorded.pending_set, pending.pending_set);
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

/// **A collided generation moves on under the elected leader, on the set it
/// was elected on.** broker-b was elected at 7 on {b, c, d} while the control
/// plane named broker-a there on {a, b, c}. A majority of {b, c, d} may hold
/// records no majority of {a, b, c} does, so the next generation leaves the
/// elected set for the control plane's as a pending change: a failover
/// meanwhile promotes from the elected set, and the control plane's comes
/// back only once the leader reports its start record on both. Elected on
/// {b, d, e}, the leader shipping to {a, b, c} could never reach a majority
/// of the set being left, so it is named on {b, d, e} alone. On a set older
/// than the control plane's it is named on the control plane's.
#[test]
fn reissuing_over_another_elected_set_leaves_it_as_a_pending_change() {
    let named_a = assignment();
    let next = named_a
        .reissuing(
            &elected("broker-b", &["broker-c", "broker-d"], 7, 7),
            &|_| true,
        )
        .expect("reissued");
    assert_eq!(next.leader, "broker-b");
    assert_eq!(next.generation, 8);
    assert_eq!(next.replicas, vec!["broker-c", "broker-d"]);
    let pending = next
        .pending_set
        .as_ref()
        .expect("the recorded set is pending");
    assert_eq!(pending.generation, 8);
    assert_eq!(
        pending.replicas.iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([&"broker-a".to_string(), &"broker-c".to_string()])
    );
    assert_eq!(next.validate(), Ok(()));
    assert_eq!(
        next.check_pending_generation(Some(&named_a), 8),
        Ok(()),
        "the store's next generation is the pending one"
    );

    let apart = named_a
        .reissuing(
            &elected("broker-b", &["broker-d", "broker-e"], 7, 7),
            &|_| true,
        )
        .expect("reissued");
    assert_eq!(apart.leader, "broker-b");
    assert_eq!(apart.replicas, vec!["broker-d", "broker-e"]);
    assert_eq!(apart.pending_set, None);
    assert_eq!(apart.generation, 8);

    // On a set older than the recorded one, it is named on the recorded one.
    let predated = named_a
        .reissuing(
            &elected("broker-b", &["broker-c", "broker-d"], 7, 6),
            &|_| true,
        )
        .expect("reissued");
    assert_eq!(
        (predated.leader.as_str(), predated.generation),
        ("broker-b", 8)
    );
    assert_eq!(
        predated.nodes().collect::<BTreeSet<_>>(),
        named_a.nodes().collect::<BTreeSet<_>>()
    );
    assert_eq!(predated.pending_set, None);

    // Elected on the recorded set, it is named as is.
    let same = named_a
        .reissuing(
            &elected("broker-b", &["broker-c", "broker-a"], 7, 7),
            &|_| true,
        )
        .expect("reissued");
    assert_eq!(same.leader, "broker-b");
    assert_eq!(same.pending_set, None);
    assert_eq!(
        same.nodes().collect::<BTreeSet<_>>(),
        named_a.nodes().collect::<BTreeSet<_>>()
    );

    // Outside the recorded set, or while a change is pending, it is left alone.
    assert_eq!(
        named_a.reissuing(
            &elected("broker-d", &["broker-b", "broker-e"], 7, 7),
            &|_| true
        ),
        None
    );
    let pending_change = ShardAssignment {
        pending_set: Some(PendingSet {
            replicas: vec!["broker-b".to_string(), "broker-d".to_string()],
            generation: 7,
        }),
        ..assignment()
    };
    assert_eq!(
        pending_change.reissuing(
            &elected("broker-b", &["broker-d", "broker-e"], 7, 7),
            &|_| true
        ),
        None
    );
}

/// **A collided generation still moves on when an elected member left the
/// catalog.** broker-b was elected at 7 on {b, c, d}, and broker-d has since
/// been deleted. A write naming it would be refused on every report, leaving
/// the shard on a leader that never opens, so broker-b is named on the
/// control plane's set instead.
#[test]
fn reissuing_names_the_recorded_set_when_an_elected_member_is_unknown() {
    let named_a = assignment();
    let next = named_a
        .reissuing(
            &elected("broker-b", &["broker-c", "broker-d"], 7, 7),
            &|node| node != "broker-d",
        )
        .expect("reissued");
    assert_eq!(next.leader, "broker-b");
    assert_eq!(next.generation, 8);
    assert_eq!(next.pending_set, None);
    assert_eq!(
        next.nodes().collect::<BTreeSet<_>>(),
        named_a.nodes().collect::<BTreeSet<_>>()
    );
}

/// **A leader elected on another set at the window's edge is named one past
/// it, in one write.** Taking only the generation would name broker-a at 23,
/// where broker-b opened, and refusing the adoption would leave broker-a to be
/// renamed past 23 on this set alone, without the elected set's records. The
/// window is measured on the elected generation, as for any adoption.
#[test]
fn an_elected_set_at_the_window_edge_is_reissued_one_past_it() {
    let named_a = assignment();
    let edge = named_a.generation + ELECTION_GENERATION_WINDOW;
    let on_other = elected("broker-b", &["broker-c", "broker-d"], edge, 7);
    let next = named_a
        .adopting_elected_set(&on_other, &|_| true)
        .expect("reissued past the edge");
    assert_eq!(
        (next.leader.as_str(), next.generation),
        ("broker-b", edge + 1)
    );
    assert!(next.pending_set.is_some(), "the recorded set is pending");
}

/// **A leader elected on a set older than the recorded one is named on the
/// recorded set.** The control plane recorded {a, b, c} at 3 after leaving
/// {b, c, d}, written at 2, unsettled. c, elected at 4 on {b, c, d}, still
/// fences {a, b, c} beside it, so c is named at 5 on {a, b, c} with nothing
/// pending. Recording {b, c, d} with {a, b, c} pending would drop the set a
/// candidate on {a, b, c} may already have opened and acknowledged on
/// (#1970).
#[test]
fn a_leader_elected_on_an_older_set_is_named_on_the_recorded_one() {
    let recorded = ShardAssignment {
        generation: 3,
        ..assignment()
    };
    let elected_c = elected("broker-c", &["broker-b", "broker-d"], 4, 2);
    let next = recorded
        .adopting_elected_set(&elected_c, &|_| true)
        .expect("adopted");
    assert_eq!(
        (next.leader.as_str(), next.generation),
        ("broker-c", 5),
        "{next:?}"
    );
    assert_eq!(
        next.nodes().collect::<BTreeSet<_>>(),
        recorded.nodes().collect::<BTreeSet<_>>()
    );
    assert_eq!(next.pending_set, None);
    assert_eq!(next.validate(), Ok(()));

    // Below the level, the elected set is recorded and this one pending.
    let older = recorded
        .adopting_elected_set_keeping_predated_set(&elected_c, &|_| true)
        .expect("adopted");
    assert_eq!(older.generation, 5);
    assert_eq!(older.replicas, vec!["broker-b", "broker-d"]);
    assert!(older.pending_set.is_some(), "{older:?}");

    // An elected set no older than the record is still recorded.
    let newer = recorded
        .adopting_elected_set(
            &elected("broker-c", &["broker-b", "broker-d"], 4, 3),
            &|_| true,
        )
        .expect("adopted");
    assert_eq!(newer, older);
}

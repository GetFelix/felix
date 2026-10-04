//! A lost leader is replaced only by a replica that holds its log.
use super::*;

/// A follower that holds the log, for the promotion tests.
struct CaughtUpNodes(BTreeSet<String>);

impl CaughtUp for CaughtUpNodes {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.0.contains(node_id)
    }
}

/// A lost leader is replaced by a follower that holds the log.
#[test]
fn a_caught_up_follower_is_promoted() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    // broker-a is gone.
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let caught_up = CaughtUpNodes(["broker-c".to_string()].into_iter().collect());

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);
    let (_, leader, _) = plan.to_place().next().expect("placed");
    assert_eq!(
        leader, "broker-c",
        "the caught-up follower must be promoted, not the other one",
    );
}

/// **The gate.** A follower that holds nothing is not promoted, however
/// eligible it looks: promoting it would serve an empty shard, which is the
/// data loss a failover is supposed to prevent.
///
/// And nothing else is promoted in its place. A node that has never seen the
/// shard is just as empty as an uncaught-up replica, so falling back to
/// ordinary scoring would defeat the gate rather than respect it — the shard
/// stays unplaced until something that holds the log can take it.
#[test]
fn a_follower_that_holds_nothing_is_not_promoted() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];

    for replica in ["broker-b", "broker-c"] {
        let existing = vec![assigned("orders", "broker-a", &[replica])];

        let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

        assert_eq!(
            plan.to_place().count(),
            0,
            "with {replica} holding nothing, the shard was placed anyway",
        );
    }
}

/// A node that was never a replica is never promoted, even if it reports being
/// caught up. Only the recorded replica set is promotable.
#[test]
fn only_a_recorded_replica_is_promotable() {
    let streams = vec![replicated_stream("orders", 1, 2)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];
    // broker-z is not in the replica set, but claims to be caught up.
    let existing = vec![assigned("orders", "broker-a", &["broker-b"])];
    let caught_up = CaughtUpNodes(
        ["broker-b".to_string(), "broker-z".to_string()]
            .into_iter()
            .collect(),
    );

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);
    let (_, leader, _) = plan.to_place().next().expect("placed");
    assert_eq!(
        leader, "broker-b",
        "promotion must come from the recorded replica set",
    );
}

/// A promoted follower is not left listed as its own follower.
#[test]
fn promotion_rebuilds_the_replica_set_without_the_new_leader() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let caught_up = CaughtUpNodes(["broker-b".to_string(), "broker-c".to_string()].into());

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);
    let (_, leader, replicas) = plan.to_place().next().expect("placed");
    assert!(
        !replicas.contains(&leader.to_string()),
        "{leader} was promoted and is still listed as a follower of itself",
    );
}

/// **A `Quorum` failover keeps the replica set, the dead leader in it.** The
/// new leader fences a majority of its set before it serves. With a spare
/// swapped in for the dead leader, the new leader and the spare would be that
/// majority, and a record acknowledged on the dead leader and the other
/// follower would be missing from both.
#[test]
fn a_quorum_failover_keeps_the_dead_leader_in_the_replica_set() {
    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let caught_up = CaughtUpNodes(["broker-c".to_string()].into());

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);
    let (_, leader, replicas) = plan.to_place().next().expect("placed");
    assert_eq!(leader, "broker-c");
    assert_eq!(
        replicas.iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([&"broker-a".to_string(), &"broker-b".to_string()]),
        "the spare took the dead leader's place in the fence's set",
    );
}

/// A staged copy was never counted toward an acknowledgement, so a `Quorum`
/// failover leaves it out along with the move it belonged to.
#[test]
fn a_quorum_failover_drops_a_staged_copy() {
    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let mut moving = assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"]);
    moving.successor = Some("broker-d".to_string());
    let caught_up = CaughtUpNodes(["broker-b".to_string()].into());

    let plan = plan(&streams, &[], &nodes, &[moving], &caught_up);
    let (_, leader, replicas) = plan.to_place().next().expect("placed");
    assert_eq!(leader, "broker-b");
    assert_eq!(
        replicas.iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([&"broker-a".to_string(), &"broker-c".to_string()]),
    );
}

/// A move's destination that was already a replica counted toward every
/// acknowledgement, so a `Quorum` failover that promotes another follower keeps
/// it. Dropped, the new leader's fence would need the dead leader's vote and
/// the shard would never serve again.
#[test]
fn a_quorum_failover_keeps_a_destination_that_was_already_a_replica() {
    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let mut moving = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    moving.successor = Some("broker-c".to_string());
    let caught_up = CaughtUpNodes(["broker-b".to_string(), "broker-c".to_string()].into());

    let plan = plan(&streams, &[], &nodes, &[moving], &caught_up);
    let (_, leader, replicas) = plan.to_place().next().expect("placed");
    assert_eq!(leader, "broker-b");
    assert_eq!(
        replicas.iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([&"broker-a".to_string(), &"broker-c".to_string()]),
        "the destination was dropped from the fence's set",
    );
}

/// A move's destination is never promoted on a `Quorum` shard, however caught
/// up its last report says it is. Its broker opens the shard as the move's
/// cut-over, without the promotion fence, and that report can predate a record
/// the dead leader acknowledged on its followers.
#[test]
fn a_quorum_failover_does_not_promote_a_move_destination() {
    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let mut moving = assigned("orders", "broker-a", &["broker-b", "broker-c", "broker-d"]);
    moving.successor = Some("broker-d".to_string());

    let only_destination = CaughtUpNodes(["broker-d".to_string()].into());
    let planned = plan(
        &streams,
        &[],
        &nodes,
        std::slice::from_ref(&moving),
        &only_destination,
    );
    assert_eq!(
        planned.to_place().count(),
        0,
        "the move's destination was promoted",
    );

    let both = CaughtUpNodes(["broker-b".to_string(), "broker-d".to_string()].into());
    let planned = plan(&streams, &[], &nodes, &[moving], &both);
    let (_, leader, _) = planned.to_place().next().expect("placed");
    assert_eq!(leader, "broker-b");
}

/// **A replicated shard is never handed to a node that does not hold it.**
///
/// The leader is gone and no replica is caught up. Placing the shard on a node
/// that has never seen it would serve an empty log at a new generation while
/// the records sat on the replicas — the failover would *be* the data loss, and
/// nothing downstream would report it as one.
#[test]
fn a_replicated_shard_with_no_caught_up_replica_is_left_unplaceable() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    // broker-a led it; b and c hold copies. broker-a is gone, and broker-z has
    // never seen this shard.
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

    assert_eq!(
        plan.to_place().count(),
        0,
        "a shard was placed on a node that does not hold its log",
    );
    assert!(
        plan.unplaceable()
            .any(|(_, why)| matches!(why, Unplaceable::NoCaughtUpReplica)),
        "the shard was dropped without saying why",
    );
}

/// And it is placed the moment a replica can take over, so the state above is
/// a pause rather than a dead end.
#[test]
fn the_shard_is_placed_as_soon_as_a_replica_is_caught_up() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let caught_up = CaughtUpNodes(["broker-b".to_string()].into_iter().collect());

    let plan = plan(&streams, &[], &nodes, &existing, &caught_up);

    let (_, leader, _) = plan.to_place().next().expect("placed");
    assert_eq!(leader, "broker-b");
}

/// **An unreplicated durable shard waits for its owner.** The owner holds the
/// only copy of the log, so placing the shard elsewhere would serve it empty
/// at a new generation. It stays unplaced until the owner returns or an
/// operator abandons the log (`abandon`).
#[test]
fn an_unreplicated_durable_shard_waits_for_its_owner() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let existing = vec![assigned("orders", "broker-a", &[])];
    for lifecycle in [NodeLifecycle::Down, NodeLifecycle::Left] {
        let nodes = vec![
            node("broker-a", lifecycle, None),
            node("broker-b", NodeLifecycle::Live, None),
        ];

        let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

        assert_eq!(
            plan.to_place().count(),
            0,
            "an unreplicated shard was placed on a node that does not hold its log ({lifecycle:?})",
        );
        let (_, why) = plan.unplaceable().next().expect("says why");
        assert_eq!(
            why,
            &Unplaceable::OwnerUnavailable {
                leader: "broker-a".to_string()
            }
        );
    }
}

/// The owner coming back is what ends the wait: it keeps the shard at the
/// same generation, with every record it held.
#[test]
fn a_returning_owner_keeps_its_unreplicated_shard() {
    let streams = vec![replicated_stream("orders", 1, 1)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &[])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

    assert_eq!(plan.kept(), 1);
}

/// A shard held only in memory has no log to lose, so it is placed again
/// straight away.
#[test]
fn an_unreplicated_ephemeral_shard_is_placed_after_its_node_is_lost() {
    let streams = vec![ephemeral_stream("orders", 1)];
    let nodes = vec![node("broker-b", NodeLifecycle::Live, None)];
    let existing = vec![assigned("orders", "broker-a", &[])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

    let (_, leader, _) = plan
        .to_place()
        .next()
        .expect("an ephemeral shard should still be placed");
    assert_eq!(leader, "broker-b");
}

/// A shard that has never been assigned is a first placement, not a failover,
/// so it is placed normally.
#[test]
fn a_shard_with_no_previous_assignment_is_placed_normally() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-z", NodeLifecycle::Live, None),
    ];

    let plan = plan(&streams, &[], &nodes, &[], &NothingCaughtUp);

    assert_eq!(plan.to_place().count(), 1);
}

/// A leader that is still live keeps the shard, caught-up replicas or not.
/// Nothing about this changes the ordinary path.
#[test]
fn a_live_leader_keeps_its_shard() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];

    let plan = plan(&streams, &[], &nodes, &existing, &NothingCaughtUp);

    assert_eq!(plan.kept(), 1);
    assert_eq!(plan.to_place().count(), 0);
}

/// A follower that holds the log, with a position, for the promotion tests.
struct ReplicasAt(std::collections::HashMap<String, u64>);

impl CaughtUp for ReplicasAt {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.0.contains_key(node_id)
    }

    fn reported_offset(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
        self.0.get(node_id).copied()
    }
}

/// **The replica holding the most is promoted**, not the one that scores best.
///
/// "Caught up" is only ever true of the tail it was measured against, so a
/// report made before the leader's last writes can call two replicas level when
/// one holds more. Preferring the higher offset picks the replica a
/// quorum-acknowledged record is guaranteed to be on — choosing by score would
/// discard the difference, and with it the record.
#[test]
fn the_furthest_ahead_replica_is_promoted() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];

    // Whichever scoring would have picked, the one at the higher offset wins.
    for (ahead, behind) in [("broker-b", "broker-c"), ("broker-c", "broker-b")] {
        let caught_up = ReplicasAt(
            [(ahead.to_string(), 100u64), (behind.to_string(), 50u64)]
                .into_iter()
                .collect(),
        );

        let plan = plan(&streams, &[], &nodes, &existing, &caught_up);

        let (_, leader, _) = plan.to_place().next().expect("placed");
        assert_eq!(
            leader, ahead,
            "{behind} was promoted over {ahead}, which held more",
        );
    }
}

/// Replicas level with each other fall back to the deterministic score, so the
/// choice stays a function of the shard and the cluster rather than of report
/// arrival order.
#[test]
fn replicas_at_the_same_offset_break_the_tie_deterministically() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let level: std::collections::HashMap<String, u64> = [
        ("broker-b".to_string(), 100u64),
        ("broker-c".to_string(), 100u64),
    ]
    .into_iter()
    .collect();

    let first = {
        let plan = plan(&streams, &[], &nodes, &existing, &ReplicasAt(level.clone()));
        plan.to_place().next().expect("placed").1.to_string()
    };
    let again = {
        let plan = plan(&streams, &[], &nodes, &existing, &ReplicasAt(level));
        plan.to_place().next().expect("placed").1.to_string()
    };

    assert_eq!(first, again);
}

/// A replica with no reported position is not preferred over one with a
/// position, and is not promoted at all unless it is also reported caught up.
#[test]
fn a_replica_with_no_reported_position_loses_to_one_with_a_position() {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    // Both are "caught up"; only broker-c has a position.
    struct OnlyOneKnown;
    impl CaughtUp for OnlyOneKnown {
        fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
            true
        }
        fn reported_offset(&self, _key: &ShardKey, node_id: &str) -> Option<u64> {
            (node_id == "broker-c").then_some(7)
        }
    }

    let plan = plan(&streams, &[], &nodes, &existing, &OnlyOneKnown);

    assert_eq!(plan.to_place().next().expect("placed").1, "broker-c");
}

/// A cache of one shard and three replicas.
fn cache_of(consistency: crate::model::ConsistencyLevel) -> Cache {
    Cache {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        display_name: "sessions".to_string(),
        shards: 1,
        replication_factor: 3,
        consistency,
    }
}

/// **A `Quorum` cache failover keeps the replica set, the dead leader in
/// it**, as a `Quorum` stream's does: the new leader fences a majority of its
/// set before it serves, and that majority has to meet every one the old
/// leader acknowledged a put or a counter add on. A `Leader` cache is not
/// fenced, and its followers are chosen afresh.
#[test]
fn a_quorum_cache_failover_keeps_the_dead_leader_in_the_replica_set() {
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let mut existing = assigned("sessions", "broker-a", &["broker-b", "broker-c"]);
    existing.key.kind = ShardKind::Cache;
    let caught_up = CaughtUpNodes(["broker-c".to_string()].into());

    let quorum = cache_of(crate::model::ConsistencyLevel::Quorum);
    let planned = plan(
        &[],
        &[quorum],
        &nodes,
        std::slice::from_ref(&existing),
        &caught_up,
    );
    let (_, leader, replicas) = planned.to_place().next().expect("placed");
    assert_eq!(leader, "broker-c");
    assert_eq!(
        replicas.iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([&"broker-a".to_string(), &"broker-b".to_string()]),
        "the spare took the dead leader's place in the fence's set",
    );

    let planned = plan(
        &[],
        &[cache_of(crate::model::ConsistencyLevel::Leader)],
        &nodes,
        &[existing],
        &caught_up,
    );
    let (_, leader, replicas) = planned.to_place().next().expect("placed");
    assert_eq!(leader, "broker-c");
    assert!(
        !replicas.contains(&"broker-a".to_string()),
        "a Leader cache kept its dead leader: {replicas:?}"
    );
}

/// A move's destination is never promoted on a `Quorum` cache: its broker
/// opened the shard as the move's cut-over, without the fence.
#[test]
fn a_quorum_cache_failover_does_not_promote_a_move_destination() {
    let nodes = vec![
        node("broker-b", NodeLifecycle::Live, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let mut moving = assigned(
        "sessions",
        "broker-a",
        &["broker-b", "broker-c", "broker-d"],
    );
    moving.key.kind = ShardKind::Cache;
    moving.successor = Some("broker-d".to_string());
    let quorum = cache_of(crate::model::ConsistencyLevel::Quorum);

    let planned = plan(
        &[],
        &[quorum],
        &nodes,
        &[moving],
        &CaughtUpNodes(["broker-d".to_string()].into()),
    );
    assert_eq!(
        planned.to_place().count(),
        0,
        "the move's destination was promoted",
    );
}

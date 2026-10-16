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
        incarnation: 0,
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

/// Liveness for the tests on a dead leader's last report: believed for
/// 2 x 1 500 + 500 ms.
fn report_liveness() -> crate::config::NodeLivenessConfig {
    crate::config::NodeLivenessConfig {
        heartbeat_interval_ms: 500,
        expiry_timeout_ms: 1_500,
        regrant_margin_ms: None,
        ..Default::default()
    }
}

const REPORTED_AT: u64 = 10_000;
const B_STORAGE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C_STORAGE: &str = "cccccccccccccccccccccccccccccccc";
const C_WIPED: &str = "dddddddddddddddddddddddddddddddd";

/// broker-a led `orders` at generation 3 and reported broker-c caught up at
/// [`REPORTED_AT`], under [`C_STORAGE`].
fn last_report() -> crate::model::ReplicaReport {
    crate::model::ReplicaReport {
        given_up: false,
        key: assigned("orders", "broker-a", &[]).key,
        generation: 3,
        caught_up: BTreeSet::from(["broker-c".to_string()]),
        offsets: BTreeMap::from([("broker-b".to_string(), 4), ("broker-c".to_string(), 10)]),
        reported_at_millis: REPORTED_AT,
        drained: false,
        leader_offset: Some(10),
        halted: Default::default(),
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        leader: None,
        node_incarnations: Default::default(),
        follower_storage: BTreeMap::from([
            ("broker-b".to_string(), B_STORAGE.to_string()),
            ("broker-c".to_string(), C_STORAGE.to_string()),
        ]),
    }
}

/// broker-a in `lifecycle`, last heard from at `leader_heartbeat`, and
/// broker-b and broker-c live, broker-c at `c_incarnation` and registered
/// with `c_storage`.
fn cluster_after_report(
    lifecycle: NodeLifecycle,
    leader_heartbeat: u64,
    c_incarnation: u64,
    c_storage: Option<&str>,
) -> Vec<Node> {
    let with = |id: &str, lifecycle, heartbeat, incarnation, storage: Option<&str>| {
        let mut node = node(id, lifecycle, None);
        node.status.last_heartbeat_at_millis = heartbeat;
        node.status.incarnation = incarnation;
        node.status.storage_id = storage.map(str::to_string);
        node
    };
    vec![
        with("broker-a", lifecycle, leader_heartbeat, 1, None),
        with(
            "broker-b",
            NodeLifecycle::Live,
            REPORTED_AT,
            2,
            Some(B_STORAGE),
        ),
        with(
            "broker-c",
            NodeLifecycle::Live,
            REPORTED_AT,
            c_incarnation,
            c_storage,
        ),
    ]
}

/// What placement reads of the report at `now`.
fn positions_after_report(nodes: &[Node], now: u64) -> ReplicaPositions {
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    ReplicaPositions::new(vec![last_report()], &report_liveness(), now).with_nodes(nodes, &existing)
}

/// What placement does with `orders` at `now`.
fn place_after_report(nodes: &[Node], now: u64) -> Option<String> {
    let streams = vec![replicated_stream("orders", 1, 3)];
    let existing = vec![assigned("orders", "broker-a", &["broker-b", "broker-c"])];
    let positions = positions_after_report(nodes, now);
    let plan = plan(&streams, &[], nodes, &existing, &positions);
    plan.to_place()
        .next()
        .map(|(_, leader, _)| leader.to_string())
}

fn report_ttl() -> u64 {
    super::super::replica_positions::report_ttl_millis(&report_liveness())
}

/// **A dead leader's last report stops ageing (#1250).** Nothing else will
/// ever report on the shard, so measured against now it would expire while
/// placement was slow, and the shard would stay unplaceable. Its age is
/// measured to the leader's last heartbeat instead.
#[test]
fn a_dead_leaders_report_promotes_past_the_ttl() {
    let nodes = cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 2, Some(C_STORAGE));
    // Long past the TTL by now, well within it of the last heartbeat.
    assert_eq!(
        place_after_report(&nodes, REPORTED_AT + report_ttl() * 10).as_deref(),
        Some("broker-c"),
        "the dead leader's report expired and the shard stayed unplaced",
    );
}

/// **A follower that only restarted still counts (#1266).** Its storage
/// identity survives the restart, so the report still describes its log,
/// however many times it registered since.
#[test]
fn a_restarted_follower_counts_past_the_ttl_of_a_dead_leader() {
    let nodes = cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 7, Some(C_STORAGE));
    assert_eq!(
        place_after_report(&nodes, REPORTED_AT + report_ttl() * 10).as_deref(),
        Some("broker-c"),
        "a follower that restarted on the same data directory was not promoted",
    );
}

/// **A follower whose storage changed is not promoted, even inside the
/// TTL (#1266).** It came back under a new identity, so it may hold none of
/// what the report says. With the promoted node's own log counted toward
/// the fence's majority, promoting it can lose an acknowledged record.
#[test]
fn a_follower_whose_storage_changed_is_not_promoted_inside_the_ttl() {
    let nodes = cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 3, Some(C_WIPED));
    let key = assigned("orders", "broker-a", &[]).key;
    for now in [REPORTED_AT + 1_000, REPORTED_AT + report_ttl() * 10] {
        let positions = positions_after_report(&nodes, now);
        assert!(!positions.is_caught_up(&key, "broker-c"), "at {now}");
        assert_eq!(positions.reported_offset(&key, "broker-c"), None);
        assert_eq!(positions.lag_records(&key, "broker-c"), None);
        assert_eq!(
            place_after_report(&nodes, now),
            None,
            "at {now}, broker-c was promoted on a report about its wiped log",
        );
    }
}

/// **A follower without an identity is read as before.** One that
/// registered none (a broker that predates them) counts while the report is
/// fresh, and never from a report held past its TTL for a dead leader.
#[test]
fn unverified_counts_within_ttl_but_never_frozen() {
    let nodes = cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 2, None);
    assert_eq!(
        place_after_report(&nodes, REPORTED_AT + 1_000).as_deref(),
        Some("broker-c"),
    );
    assert_eq!(
        place_after_report(&nodes, REPORTED_AT + report_ttl() * 10),
        None,
        "an unverified follower was promoted from a report past its TTL",
    );
}

/// **A live leader's report still expires.** One that heartbeats but stopped
/// reporting may have moved on, so its old report must not make a follower
/// look caught up: an operator move or a later failover would act on it.
/// This holds while its heartbeat is old too, as when expiry has not caught
/// up after a control-plane outage: only a leader marked down freezes it.
#[test]
fn a_live_leaders_old_report_still_expires() {
    let now = REPORTED_AT + report_ttl() + 1;
    let key = assigned("orders", "broker-a", &[]).key;
    for heartbeat in [now, REPORTED_AT + 200] {
        let nodes = cluster_after_report(NodeLifecycle::Live, heartbeat, 2, Some(C_STORAGE));
        let positions = positions_after_report(&nodes, now);
        assert!(
            !positions.is_caught_up(&key, "broker-c"),
            "a live leader heard from at {heartbeat} had its stale report believed",
        );
        assert_eq!(positions.reported_offset(&key, "broker-c"), None);
    }
}

/// [`last_report`] as a control plane that stamps reports records it:
/// made by broker-a at incarnation 1, with broker-b at 2 and broker-c at
/// `c_incarnation`.
fn stamped_report(c_incarnation: u64) -> crate::model::ReplicaReport {
    crate::model::ReplicaReport {
        leader: Some("broker-a".to_string()),
        node_incarnations: BTreeMap::from([
            ("broker-a".to_string(), 1),
            ("broker-b".to_string(), 2),
            ("broker-c".to_string(), c_incarnation),
        ]),
        ..last_report()
    }
}

/// What placement does with a `Leader` `orders`, held by `existing`, at `now`.
fn place_on(
    report: crate::model::ReplicaReport,
    nodes: &[Node],
    existing: ShardAssignment,
    now: u64,
) -> Option<String> {
    place_stream_on(
        replicated_stream("orders", 1, 3),
        report,
        nodes,
        existing,
        now,
    )
}

/// [`place_on`] for `stream`.
fn place_stream_on(
    stream: Stream,
    report: crate::model::ReplicaReport,
    nodes: &[Node],
    existing: ShardAssignment,
    now: u64,
) -> Option<String> {
    let streams = vec![stream];
    let existing = vec![existing];
    let positions =
        ReplicaPositions::new(vec![report], &report_liveness(), now).with_nodes(nodes, &existing);
    let plan = plan(&streams, &[], nodes, &existing, &positions);
    plan.to_place()
        .next()
        .map(|(_, leader, _)| leader.to_string())
}

/// **A leader that dies mid-move still freezes its report (#1250).** Each
/// step of a move writes a new generation under the same leader, so its last
/// report can be a step behind the assignment. It is still that leader's
/// report, and nothing else will report on the shard.
#[test]
fn a_leader_that_dies_mid_move_freezes_its_report() {
    let nodes = cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 2, Some(C_STORAGE));
    let mut stepped = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    stepped.generation += 1;
    assert_eq!(
        place_on(
            stamped_report(2),
            &nodes,
            stepped.clone(),
            REPORTED_AT + report_ttl() * 10
        )
        .as_deref(),
        Some("broker-c"),
        "the report from the step before expired and the shard stayed unplaced",
    );

    // A restart since the report is another process: its report ages.
    let mut restarted = nodes.clone();
    restarted[0].status.incarnation = 2;
    assert_eq!(
        place_on(
            stamped_report(2),
            &restarted,
            stepped,
            REPORTED_AT + report_ttl() * 10
        ),
        None,
        "a report from the leader's previous process was frozen",
    );
}

/// **A previous leader's report promotes nobody on a `Leader` shard.**
/// broker-a reported at generation 3 and broker-b was promoted at 4;
/// broker-b died before its first report. What broker-b acknowledged may be
/// on broker-b alone, so broker-a's view, however fresh, says nothing about
/// it.
#[test]
fn a_previous_leaders_report_promotes_nobody_on_a_leader_shard() {
    let mut nodes =
        cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 2, Some(C_STORAGE));
    nodes[1].status.lifecycle = NodeLifecycle::Down;
    let mut promoted = assigned("orders", "broker-b", &["broker-c"]);
    promoted.generation += 1;
    for now in [REPORTED_AT + 1_000, REPORTED_AT + report_ttl() * 10] {
        assert_eq!(
            place_on(stamped_report(2), &nodes, promoted.clone(), now),
            None,
            "at {now}, broker-c was promoted on the previous leader's report",
        );
    }
}

/// **An unverified follower that restarted since the report is not
/// counted.** Without an identity a restart is the only evidence it may have
/// lost its log, as when a wiped follower registers on a control plane that
/// drops identities, which clears the one it had registered.
#[test]
fn an_unverified_follower_that_restarted_since_the_report_is_not_counted() {
    let nodes = cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 3, None);
    let existing = assigned("orders", "broker-a", &["broker-b", "broker-c"]);
    let now = REPORTED_AT + 1_000;
    assert_eq!(
        place_on(stamped_report(2), &nodes, existing.clone(), now),
        None,
        "broker-c was counted from a report taken before it restarted",
    );
    assert_eq!(
        place_on(stamped_report(3), &nodes, existing, now).as_deref(),
        Some("broker-c"),
        "a report taken at broker-c's current incarnation did not count",
    );
}

/// **A previous leader's report still promotes on a `Quorum` shard.** The
/// new leader acknowledged nothing before its own first report, or a
/// majority holds it and the promoted leader's fence takes it, so refusing
/// would only leave the shard unplaced.
#[test]
fn a_previous_leaders_report_still_promotes_on_a_quorum_shard() {
    let mut nodes =
        cluster_after_report(NodeLifecycle::Down, REPORTED_AT + 200, 2, Some(C_STORAGE));
    nodes[1].status.lifecycle = NodeLifecycle::Down;
    let mut promoted = assigned("orders", "broker-b", &["broker-a", "broker-c"]);
    promoted.generation += 1;
    let stream = Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    };
    assert_eq!(
        place_stream_on(
            stream,
            stamped_report(2),
            &nodes,
            promoted,
            REPORTED_AT + 1_000
        )
        .as_deref(),
        Some("broker-c"),
        "a Quorum shard waited on a report its fence makes unnecessary",
    );
}

const A_STORAGE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const A_CRASHED: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

/// [`last_report`] naming the leader's own identity, as a leader that
/// reports it does.
fn report_naming_the_leader() -> crate::model::ReplicaReport {
    let mut report = last_report();
    report
        .follower_storage
        .insert("broker-a".to_string(), A_STORAGE.to_string());
    report
}

/// broker-a live and registered under `a_storage`, and both followers live
/// under the identities the report names.
fn live_leader_under(a_storage: &str) -> Vec<Node> {
    let mut nodes =
        cluster_after_report(NodeLifecycle::Live, REPORTED_AT + 200, 2, Some(C_STORAGE));
    nodes[0].status.storage_id = Some(a_storage.to_string());
    nodes
}

/// What placement decides for `orders`, replicated `factor` times, from
/// `report`, a second after it.
fn plan_from(nodes: &[Node], report: crate::model::ReplicaReport, factor: u32) -> Plan {
    let replicas: &[&str] = if factor > 1 {
        &["broker-b", "broker-c"]
    } else {
        &[]
    };
    let streams = vec![replicated_stream("orders", 1, factor)];
    let existing = vec![assigned("orders", "broker-a", replicas)];
    let positions = ReplicaPositions::new(vec![report], &report_liveness(), REPORTED_AT + 1_000)
        .with_nodes(nodes, &existing);
    plan(&streams, &[], nodes, &existing, &positions)
}

/// **A leader back on a new storage identity is replaced (#1295).** It
/// restarted after a crash that may have cost it records it acknowledged,
/// so it gives the shard up, and placement promotes a follower the report
/// names caught up, though the leader is live and heartbeating.
#[test]
fn a_leader_back_on_a_new_storage_identity_is_replaced() {
    let plan = plan_from(&live_leader_under(A_CRASHED), report_naming_the_leader(), 3);
    let (_, leader, replicas) = plan.to_place().next().expect("promoted");
    assert_eq!(leader, "broker-c");
    assert!(
        replicas.iter().any(|replica| replica == "broker-a"),
        "the old leader stays in the set to be copied again: {replicas:?}",
    );
}

/// The same leader under the identity it reported from keeps the shard.
#[test]
fn a_leader_on_the_identity_it_reported_from_keeps_the_shard() {
    let plan = plan_from(&live_leader_under(A_STORAGE), report_naming_the_leader(), 3);
    assert_eq!(plan.to_place().count(), 0);
    assert_eq!(plan.unplaceable().count(), 0);
}

/// **With nobody caught up the shard waits.** The leader will not lead from
/// its copy, and no follower holds the log, so placement writes nothing: in
/// particular no new generation naming the same leader, which would lead
/// from the short copy.
#[test]
fn a_replaced_leader_with_nobody_caught_up_waits() {
    let mut report = report_naming_the_leader();
    report.caught_up.clear();
    let plan = plan_from(&live_leader_under(A_CRASHED), report, 3);
    assert_eq!(plan.to_place().count(), 0);
    assert_eq!(plan.moves().count(), 0);
    assert!(
        plan.unplaceable()
            .any(|(_, why)| matches!(why, Unplaceable::NoCaughtUpReplica)),
    );
}

/// A report that does not name the leader's identity, from a broker that
/// predates it, is read as before.
#[test]
fn a_report_without_the_leaders_identity_keeps_the_leader() {
    let plan = plan_from(&live_leader_under(A_CRASHED), last_report(), 3);
    assert_eq!(plan.to_place().count(), 0);
}

/// An only copy is led whatever it lost: there is no other to promote.
#[test]
fn an_unreplicated_leader_back_on_a_new_identity_keeps_the_shard() {
    let plan = plan_from(&live_leader_under(A_CRASHED), report_naming_the_leader(), 1);
    assert_eq!(plan.to_place().count(), 0);
    assert_eq!(plan.unplaceable().count(), 0);
}

/// What broker-a reports once it came back under [`A_CRASHED`], gave the
/// shard up and measured both followers: given up, no tail of its own, its
/// position beside theirs, each under the identity and incarnation it is
/// registered at.
fn given_up_report(a: u64, b: u64, c: u64) -> crate::model::ReplicaReport {
    crate::model::ReplicaReport {
        caught_up: BTreeSet::new(),
        offsets: BTreeMap::from([
            ("broker-a".to_string(), a),
            ("broker-b".to_string(), b),
            ("broker-c".to_string(), c),
        ]),
        leader_offset: None,
        given_up: true,
        leader: Some("broker-a".to_string()),
        node_incarnations: BTreeMap::from([
            ("broker-a".to_string(), 1),
            ("broker-b".to_string(), 2),
            ("broker-c".to_string(), 2),
        ]),
        follower_storage: BTreeMap::from([
            ("broker-a".to_string(), A_CRASHED.to_string()),
            ("broker-b".to_string(), B_STORAGE.to_string()),
            ("broker-c".to_string(), C_STORAGE.to_string()),
        ]),
        ..last_report()
    }
}

/// **A leader that gave up is named again when no follower holds more than
/// it does (#1295).** Nobody else holds anything it lost, so waiting for an
/// operator gains nothing: placement names it at a new generation, on the
/// same set.
#[test]
fn a_leader_that_gave_up_with_no_follower_ahead_is_reclaimed() {
    let plan = plan_from(&live_leader_under(A_CRASHED), given_up_report(10, 4, 10), 3);
    let (_, leader, replicas) = plan.to_place().next().expect("reclaimed");
    assert_eq!(leader, "broker-a");
    assert_eq!(replicas, ["broker-b".to_string(), "broker-c".to_string()]);
}

/// A follower ahead of a leader that gave up holds what it lost, and the
/// probe names the furthest caught up: promoted, not reclaimed.
#[test]
fn a_follower_ahead_of_a_leader_that_gave_up_is_promoted() {
    let mut report = given_up_report(10, 4, 12);
    report.caught_up = BTreeSet::from(["broker-c".to_string()]);
    let plan = plan_from(&live_leader_under(A_CRASHED), report, 3);
    let (_, leader, _) = plan.to_place().next().expect("promoted");
    assert_eq!(leader, "broker-c");
}

/// **Only a fresh measurement of every follower, each under its registered
/// identity, reclaims.** A stale report can understate a follower holding
/// what the leader lost; one that misses a follower, or measured an older
/// copy of it, says nothing about what that follower holds now.
#[test]
fn a_reclaim_needs_every_follower_measured_since_the_crash() {
    let stale = || {
        let mut report = given_up_report(10, 4, 10);
        report.reported_at_millis = 0;
        report
    };
    let unmeasured = || {
        let mut report = given_up_report(10, 4, 10);
        report.offsets.remove("broker-c");
        report
    };
    let older_copy = || {
        let mut report = given_up_report(10, 4, 10);
        report
            .follower_storage
            .insert("broker-c".to_string(), C_WIPED.to_string());
        report
    };
    let older_process = || {
        let mut report = given_up_report(10, 4, 10);
        report.node_incarnations.insert("broker-b".to_string(), 1);
        report
    };
    for (what, report) in [
        ("stale", stale()),
        ("unmeasured", unmeasured()),
        ("older copy", older_copy()),
        ("older process", older_process()),
    ] {
        let plan = plan_from(&live_leader_under(A_CRASHED), report, 3);
        assert_eq!(plan.to_place().count(), 0, "{what}: reclaimed");
        assert!(
            plan.unplaceable()
                .any(|(_, why)| matches!(why, Unplaceable::NoCaughtUpReplica)),
            "{what}: the shard should wait",
        );
    }
}

/// **Only an explicit give-up counts (#1295).** A broker that predates the
/// flag sends a drained report without a tail for an ordinary drain; read as
/// a give-up, placement would replace a live leader on silence.
#[test]
fn an_old_drained_report_without_a_tail_is_not_a_give_up() {
    let mut report = given_up_report(10, 4, 10);
    report.given_up = false;
    report.drained = true;
    report
        .follower_storage
        .insert("broker-a".to_string(), A_STORAGE.to_string());
    let nodes = live_leader_under(A_STORAGE);
    let positions = ReplicaPositions::new(
        vec![report.clone()],
        &report_liveness(),
        REPORTED_AT + 1_000,
    )
    .with_nodes(
        &nodes,
        &[assigned("orders", "broker-a", &["broker-b", "broker-c"])],
    );
    assert!(!positions.leader_gave_up(&report.key, 3, "broker-a"));
    let plan = plan_from(&nodes, report, 3);
    assert_eq!(plan.to_place().count(), 0, "the live leader was replaced");
    assert_eq!(plan.unplaceable().count(), 0);
}

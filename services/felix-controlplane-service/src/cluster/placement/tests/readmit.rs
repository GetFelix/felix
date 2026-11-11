//! A member whose storage was replaced is a learner until its leader reports
//! it holding the start of the generation that names its new copy
//! (`FelixShard.tla`, the non-voting rejoin; docs/control-plane.md,
//! "Re-admitting a replaced copy").
use super::super::moves::READMITS_PER_PASS;
use super::reconciler::shard_zero;
use super::*;
use crate::model::{Namespace, ReplicaReport, Tenant};
use crate::store::memory::InMemoryStore;
use crate::store::{ControlPlaneStore, StoreConfig};

const FEATURES: [&str; 4] = [
    "generation_start",
    "majority_ack",
    "lease_free_reads",
    "joint_fence",
];

/// A storage id: 32 lowercase hex digits.
fn sid(n: u8) -> String {
    format!("{n:02x}").repeat(16)
}

fn registered(id: &str, storage: u8, lifecycle: NodeLifecycle) -> Node {
    let mut n = node(id, lifecycle, None);
    n.status.storage_id = Some(sid(storage));
    n.status.features = FEATURES.iter().map(|f| f.to_string()).collect();
    n
}

/// `{leader; replicas}` at generation 5 naming each member by `ids`.
fn named(leader: &str, replicas: &[&str], ids: &[(&str, u8)]) -> ShardAssignment {
    ShardAssignment {
        generation: 5,
        member_storage_ids: ids.iter().map(|(n, i)| (n.to_string(), sid(*i))).collect(),
        ..assigned("orders", leader, replicas)
    }
}

/// What `leader` reported at `generation`: `caught_up` followers at `offsets`
/// under the identities in `seen`, and `holds` holding the generation's start.
struct Said<'a> {
    generation: u64,
    leader: &'a str,
    offsets: &'a [(&'a str, u64)],
    seen: &'a [(&'a str, u8)],
    holds: &'a [&'a str],
}

impl Said<'_> {
    fn report(&self, incarnations: BTreeMap<String, u64>, at: u64) -> ReplicaReport {
        ReplicaReport {
            key: shard_zero(),
            generation: self.generation,
            caught_up: self.offsets.iter().map(|(n, _)| n.to_string()).collect(),
            offsets: self
                .offsets
                .iter()
                .map(|(n, o)| (n.to_string(), *o))
                .collect(),
            reported_at_millis: at,
            drained: false,
            leader_offset: Some(10),
            halted: Default::default(),
            counter_offsets: Default::default(),
            stream_incarnation: None,
            by_round: false,
            leader_incarnation: None,
            follower_storage: self
                .seen
                .iter()
                .map(|(n, i)| (n.to_string(), sid(*i)))
                .collect(),
            leader: Some(self.leader.to_string()),
            node_incarnations: incarnations,
            given_up: false,
            inherited_storage: Default::default(),
            joint_start_acked: None,
            refused: None,
            holds_start: self.holds.iter().map(|n| n.to_string()).collect(),
        }
    }
}

fn positions(
    said: &Said<'_>,
    nodes: &[Node],
    existing: &ShardAssignment,
    joint_fence: bool,
) -> ReplicaPositions {
    let incarnations = nodes
        .iter()
        .map(|n| (n.node_id.clone(), n.status.incarnation))
        .collect();
    ReplicaPositions::new(
        vec![said.report(incarnations, 1_000)],
        &Default::default(),
        1_000,
    )
    .with_nodes(nodes, std::slice::from_ref(existing))
    .with_joint_fence(joint_fence)
}

fn plan_for(
    nodes: &[Node],
    existing: &ShardAssignment,
    positions: &ReplicaPositions,
    joint_fence: bool,
) -> Decision {
    let stream = Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    };
    let policy = MovePolicy {
        joint_fence,
        ..MovePolicy::default()
    };
    let plan = plan_with(
        &[stream],
        &[],
        nodes,
        std::slice::from_ref(existing),
        positions,
        policy,
    );
    plan.shards[0].decision.clone()
}

/// **Failover never names a learner.** broker-a is gone; broker-c holds more
/// than broker-b but was wiped since the assignment named it, so it leads
/// nothing. Once the report shows it holding the generation's start it can.
#[test]
fn failover_skips_a_replaced_member_until_its_leader_shows_it_holding_the_start() {
    let nodes = vec![
        registered("broker-a", 1, NodeLifecycle::Down),
        registered("broker-b", 2, NodeLifecycle::Live),
        registered("broker-c", 33, NodeLifecycle::Live),
    ];
    let offsets = [("broker-b", 5), ("broker-c", 9)];
    let seen = [("broker-b", 2), ("broker-c", 33)];
    let leader_after = |existing: &ShardAssignment, holds: &[&str], joint_fence: bool| {
        let said = Said {
            generation: 5,
            leader: "broker-a",
            offsets: &offsets,
            seen: &seen,
            holds,
        };
        let positions = positions(&said, &nodes, existing, joint_fence);
        match plan_for(&nodes, existing, &positions, joint_fence) {
            Decision::Place(leader, _) => leader,
            other => panic!("expected a failover, got {other:?}"),
        }
    };

    // (a) The assignment names broker-c's old copy.
    let wiped = named(
        "broker-a",
        &["broker-b", "broker-c"],
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    assert_eq!(leader_after(&wiped, &[], true), "broker-b");
    assert_eq!(
        leader_after(&wiped, &["broker-c"], true),
        "broker-b",
        "holding the start of a generation that names the old copy is no admission"
    );

    // (b) It names the new copy and holds it as a learner.
    let learner = ShardAssignment {
        learners: ["broker-c".to_string()].into(),
        ..named(
            "broker-a",
            &["broker-b", "broker-c"],
            &[("broker-a", 1), ("broker-b", 2), ("broker-c", 33)],
        )
    };
    assert_eq!(leader_after(&learner, &[], true), "broker-b");
    assert_eq!(leader_after(&learner, &["broker-c"], true), "broker-c");

    // Without joint fences nothing is held back.
    assert_eq!(leader_after(&wiped, &[], false), "broker-c");
    assert_eq!(leader_after(&learner, &[], false), "broker-c");
}

/// **A given-up leader's probe admits a learner too.** broker-a led the
/// generation that re-admitted broker-c and crashed before a pass reported
/// broker-c holding its start, so its probe is the only report it can still
/// send. broker-b is behind, so broker-c is the one copy that can take over.
#[test]
fn a_given_up_leaders_probe_admits_a_learner_holding_the_start() {
    let nodes = vec![
        registered("broker-a", 1, NodeLifecycle::Live),
        registered("broker-b", 2, NodeLifecycle::Live),
        registered("broker-c", 33, NodeLifecycle::Live),
    ];
    let learner = ShardAssignment {
        learners: ["broker-c".to_string()].into(),
        ..named(
            "broker-a",
            &["broker-b", "broker-c"],
            &[("broker-a", 1), ("broker-b", 2), ("broker-c", 33)],
        )
    };
    let decision = |holds: &[&str]| {
        let said = Said {
            generation: 5,
            leader: "broker-a",
            offsets: &[("broker-a", 9), ("broker-b", 5), ("broker-c", 11)],
            seen: &[("broker-a", 1), ("broker-b", 2), ("broker-c", 33)],
            holds,
        };
        let incarnations = nodes
            .iter()
            .map(|n| (n.node_id.clone(), n.status.incarnation))
            .collect();
        let report = ReplicaReport {
            given_up: true,
            leader_offset: None,
            caught_up: ["broker-c".to_string()].into(),
            ..said.report(incarnations, 1_000)
        };
        let positions = ReplicaPositions::new(vec![report], &Default::default(), 1_000)
            .with_nodes(&nodes, std::slice::from_ref(&learner))
            .with_joint_fence(true);
        plan_for(&nodes, &learner, &positions, true)
    };

    assert!(
        matches!(
            decision(&[]),
            Decision::Unplaceable(Unplaceable::NoCaughtUpReplica)
        ),
        "{:?}",
        decision(&[])
    );
    match decision(&["broker-c"]) {
        Decision::Place(leader, _) => assert_eq!(leader, "broker-c"),
        other => panic!("expected broker-c promoted, got {other:?}"),
    }
}

/// **Known gap, pinned as it is today: a given-up leader at N+1 strands a
/// readmitted learner that is the furthest copy.** broker-b was elected at
/// generation 5 while the control plane was paused and went down. The control
/// plane, still naming broker-a, readmitted broker-c as a learner, then a late
/// report from broker-b collided and moved the shard to generation 6 naming
/// broker-b, which came back from an unclean stop and gave the shard up. Its
/// probe shows broker-c level with it and broker-a behind, but broker-c is a
/// learner and no leader ever writes generation 6, so it never holds a start
/// to leave that state; broker-a is behind and a reclaim needs every follower
/// behind. The shard stays without a leader (a sim stall).
///
/// A fix has to let placement promote broker-c here, either by taking the
/// probe's evidence of the elected generation it voted into, or by not moving
/// the shard to a leader that has no heartbeat. When it does, flip the final
/// assertion to expect `Decision::Place` of broker-c.
#[test]
fn known_gap_a_given_up_leader_leaves_the_furthest_learner_unpromotable() {
    let nodes = vec![
        registered("broker-a", 1, NodeLifecycle::Live),
        registered("broker-b", 2, NodeLifecycle::Live),
        registered("broker-c", 33, NodeLifecycle::Live),
    ];
    let collided = ShardAssignment {
        generation: 6,
        learners: ["broker-c".to_string()].into(),
        ..named(
            "broker-b",
            &["broker-a", "broker-c"],
            &[("broker-a", 1), ("broker-b", 2), ("broker-c", 33)],
        )
    };
    let said = Said {
        generation: 6,
        leader: "broker-b",
        offsets: &[("broker-a", 3), ("broker-b", 4), ("broker-c", 4)],
        seen: &[("broker-a", 1), ("broker-b", 2), ("broker-c", 33)],
        // Nobody wrote generation 6, so no follower holds its start.
        holds: &[],
    };
    let incarnations = nodes
        .iter()
        .map(|n| (n.node_id.clone(), n.status.incarnation))
        .collect();
    let report = ReplicaReport {
        given_up: true,
        leader_offset: None,
        caught_up: ["broker-c".to_string()].into(),
        ..said.report(incarnations, 1_000)
    };
    let positions = ReplicaPositions::new(vec![report], &Default::default(), 1_000)
        .with_nodes(&nodes, std::slice::from_ref(&collided))
        .with_joint_fence(true);
    let key = shard_zero();

    assert!(positions.is_learner(&key, "broker-c"));
    assert!(!positions.is_caught_up(&key, "broker-c"));
    assert!(!positions.is_caught_up(&key, "broker-a"));
    assert!(!positions.reclaimable(&key, "broker-b", &["broker-a", "broker-c"]));

    // Today's behaviour. After the fix this is `Place` of broker-c.
    match plan_for(&nodes, &collided, &positions, true) {
        Decision::Unplaceable(Unplaceable::NoCaughtUpReplica) => {}
        other => panic!("expected the stall, got {other:?}"),
    }
}

/// **A move's old-set majority does not count a learner.** A set above its
/// factor drops the first follower whose leaving leaves the rest at the
/// drained leader's tail. broker-b is a learner, so it does not count as at
/// the tail and broker-a cannot be the one to go; broker-b goes instead. Once
/// its leader shows it holding the start, broker-a goes as it would have.
#[test]
fn a_trim_does_not_count_a_learner_toward_the_rest_holding_the_tail() {
    let nodes = vec![
        registered("broker-a", 1, NodeLifecycle::Live),
        registered("broker-b", 22, NodeLifecycle::Live),
        registered("broker-c", 3, NodeLifecycle::Live),
    ];
    let offsets = [("broker-a", 100), ("broker-b", 100)];
    let seen = [("broker-a", 1), ("broker-b", 22)];
    let fenced = ShardAssignment {
        state: ShardState::Draining,
        learners: ["broker-b".to_string()].into(),
        ..named(
            "broker-c",
            &["broker-a", "broker-b"],
            &[("broker-a", 1), ("broker-b", 22), ("broker-c", 3)],
        )
    };
    let dropped = |joint_fence: bool, holds: &[&str]| {
        let said = Said {
            generation: 5,
            leader: "broker-c",
            offsets: &offsets,
            seen: &seen,
            holds,
        };
        let mut report = said.report(
            nodes.iter().map(|n| (n.node_id.clone(), 0)).collect(),
            1_000,
        );
        report.drained = true;
        report.leader_offset = Some(100);
        let positions = ReplicaPositions::new(vec![report], &Default::default(), 1_000)
            .with_nodes(&nodes, std::slice::from_ref(&fenced))
            .with_joint_fence(joint_fence);
        let stream = Stream {
            consistency: ConsistencyLevel::Quorum,
            ..replicated_stream("orders", 1, 2)
        };
        let policy = MovePolicy {
            joint_fence,
            ..MovePolicy::default()
        };
        let plan = plan_with(
            &[stream],
            &[],
            &nodes,
            std::slice::from_ref(&fenced),
            &positions,
            policy,
        );
        match &plan.shards[0].decision {
            Decision::Move(MoveStep::Trim { from }, _) => from.clone(),
            other => panic!("expected a trim, got {other:?}"),
        }
    };

    assert_eq!(dropped(true, &[]), "broker-b");
    assert_eq!(dropped(true, &["broker-b"]), "broker-a");
    assert_eq!(dropped(false, &[]), "broker-a");
}

/// Three brokers with storage ids 1 to 3, a durable `Quorum` stream of three
/// copies led by broker-b, and `joint_fence` finalized when asked.
pub(super) async fn cluster(joint_fence: bool) -> (InMemoryStore, ShardAssignment) {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: 1000,
        change_retention_max_rows: Some(1000),
    });
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            display_name: "NS".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            consistency: ConsistencyLevel::Quorum,
            ..replicated_stream("orders", 1, 3)
        })
        .await
        .expect("stream");
    for (i, id) in ["broker-a", "broker-b", "broker-c"].iter().enumerate() {
        let mut n = registered(id, i as u8 + 1, NodeLifecycle::Live);
        n.spec.advertise_addr = format!("10.0.0.4:{}", 7600 + i);
        store.register_node(n).await.expect("node");
    }
    if joint_fence {
        for feature in FEATURES {
            store.finalize_fleet_feature(feature).await.expect(feature);
        }
    }
    let assignment = store
        .put_shard_assignment(ShardAssignment {
            generation: 0,
            state: ShardState::Assigning,
            ..named(
                "broker-b",
                &["broker-a", "broker-c"],
                &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
            )
        })
        .await
        .expect("assignment");
    (store, assignment)
}

/// broker-c comes back on a wiped disk: registered again under storage id 33.
async fn wipe_c(store: &InMemoryStore) {
    let mut n = registered("broker-c", 33, NodeLifecycle::Live);
    n.spec.advertise_addr = "10.0.0.4:7602".to_string();
    store.register_node(n).await.expect("re-register");
}

/// What broker-b reports at `generation`: both followers caught up, broker-c
/// under its new identity, and `holds` holding the generation's start.
async fn b_reports(store: &InMemoryStore, generation: u64, holds: &[&str]) {
    let incarnations = store
        .list_nodes()
        .await
        .expect("nodes")
        .into_iter()
        .map(|n| (n.node_id, n.status.incarnation))
        .collect();
    let said = Said {
        generation,
        leader: "broker-b",
        offsets: &[("broker-a", 10), ("broker-c", 10)],
        seen: &[("broker-a", 1), ("broker-c", 33)],
        holds,
    };
    let at = store.now_millis().await.expect("clock");
    let stored = store
        .record_replica_report(said.report(incarnations, at), "broker-b")
        .await
        .expect("report");
    assert_eq!(stored, crate::store::ReportWrite::Stored);
}

async fn pass(store: &InMemoryStore) -> ShardAssignment {
    reconcile_once(store, &Default::default(), MovePolicy::default()).await;
    store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("assignment")
}

/// **The sequence, at the control plane.** broker-c was wiped while the
/// assignment names its old copy. Once its leader shows the new copy caught
/// up, the same set is written again naming the new copy, with broker-c a
/// learner. It leads nothing and counts toward nothing until the leader's
/// report at that generation shows it holding the start; then it can take
/// over from a leader and a follower that are gone.
#[tokio::test]
async fn a_wiped_member_is_readmitted_then_counts_once_it_holds_the_start() {
    for admit in [false, true] {
        let (store, first) = cluster(true).await;
        wipe_c(&store).await;

        // Nothing is written without the leader's word about the new copy.
        assert_eq!(pass(&store).await, first);

        b_reports(&store, first.generation, &[]).await;
        let readmitted = pass(&store).await;
        assert_eq!(readmitted.generation, first.generation + 1);
        assert_eq!(readmitted.leader, "broker-b");
        assert_eq!(readmitted.replicas, first.replicas);
        assert_eq!(readmitted.member_storage_ids["broker-c"], sid(33));
        assert_eq!(
            readmitted.learners,
            ["broker-c".to_string()].into(),
            "the new copy is a learner until it holds the start"
        );

        // Written once: the report that did it describes a generation gone.
        assert_eq!(pass(&store).await, readmitted);

        let holds: &[&str] = if admit { &["broker-c"] } else { &[] };
        b_reports(&store, readmitted.generation, holds).await;
        // The leader of the set naming the new copy showed it caught up:
        // that is the durable evidence a fence may trust it on. broker-a was
        // never replaced and gets none.
        let evidenced = store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("assignment");
        assert_eq!(
            evidenced.readmitted,
            vec![crate::model::Readmitted {
                node: "broker-c".to_string(),
                storage_id: sid(33),
                generation: readmitted.generation,
                offset: 10,
            }]
        );
        let readmitted = evidenced;
        let read = PlacementRead::load(&store, &Default::default())
            .await
            .expect("read");
        assert_eq!(read.positions.is_learner(&shard_zero(), "broker-c"), !admit);

        // The leader and broker-a are gone.
        for gone in ["broker-a", "broker-b"] {
            store
                .set_node_lifecycle(gone, NodeLifecycle::Down)
                .await
                .expect("down");
        }
        let after = pass(&store).await;
        if admit {
            assert_eq!(after.leader, "broker-c");
            assert_eq!(after.generation, readmitted.generation + 1);
            assert!(
                after.learners.is_empty(),
                "broker-c is no longer a learner: {after:?}"
            );
        } else {
            assert_eq!(after, readmitted, "a learner took over");
        }
    }
}

/// **A failed read keeps the learners.** An operator write names its members
/// against the assignment it replaces; a read that fails is not "no shard",
/// so the write fails rather than landing without broker-c as a learner.
#[tokio::test]
async fn an_operator_write_whose_read_fails_does_not_drop_the_learners() {
    let (store, first) = cluster(true).await;
    wipe_c(&store).await;
    b_reports(&store, first.generation, &[]).await;
    let readmitted = pass(&store).await;
    assert_eq!(readmitted.learners, ["broker-c".to_string()].into());

    let step = super::super::operator::FencedStep {
        step: OperatorStep {
            step: MoveStep::Readmit {
                node: "broker-c".to_string(),
            },
            assignment: ShardAssignment {
                generation: readmitted.generation + 1,
                ..readmitted.clone()
            },
            expected_generation: readmitted.generation,
            zones: None,
        },
        fence: store.placement_token().await.expect("token"),
    };
    store
        .faults
        .shard_reads
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let written = super::super::operator::write_operator_step(&store, &step).await;
    assert!(
        written.is_err(),
        "wrote without the previous set: {written:?}"
    );
    assert_eq!(pass(&store).await, readmitted);
}

/// **Without joint fences nothing changes.** The same wipe writes nothing,
/// holds no learner, and the failover may take the wiped member.
#[tokio::test]
async fn without_joint_fence_a_wiped_member_is_neither_readmitted_nor_held() {
    let (store, first) = cluster(false).await;
    wipe_c(&store).await;
    b_reports(&store, first.generation, &[]).await;

    assert_eq!(pass(&store).await, first);
    assert!(first.learners.is_empty());

    for gone in ["broker-a", "broker-b"] {
        store
            .set_node_lifecycle(gone, NodeLifecycle::Down)
            .await
            .expect("down");
    }
    let after = pass(&store).await;
    assert_eq!(after.leader, "broker-c");
    assert!(after.learners.is_empty());
}

/// **Re-admission waits for its evidence and for a quiet shard.** Only a
/// live leader whose report at the assignment's generation shows the new
/// copy caught up under the identity it is registered with writes the set
/// again, only while a majority of the set besides the new copy is live, and
/// not while a move, a drain or a pending set is in progress.
#[test]
fn a_readmission_needs_the_new_copy_caught_up_and_nothing_else_in_progress() {
    let nodes = |leader: NodeLifecycle| {
        vec![
            registered("broker-a", 1, NodeLifecycle::Live),
            registered("broker-b", 2, leader),
            registered("broker-c", 33, NodeLifecycle::Live),
        ]
    };
    let wiped = named(
        "broker-b",
        &["broker-a", "broker-c"],
        &[("broker-a", 1), ("broker-b", 2), ("broker-c", 3)],
    );
    let decided = |nodes: &[Node], existing: &ShardAssignment, seen_c: u8, caught_c: bool| {
        let offsets: &[(&str, u64)] = if caught_c {
            &[("broker-a", 10), ("broker-c", 10)]
        } else {
            &[("broker-a", 10)]
        };
        let said = Said {
            generation: 5,
            leader: "broker-b",
            offsets,
            seen: &[("broker-a", 1), ("broker-c", seen_c)],
            holds: &[],
        };
        let positions = positions(&said, nodes, existing, true);
        plan_for(nodes, existing, &positions, true)
    };
    let readmits = |decision: &Decision| matches!(decision, Decision::Move(MoveStep::Readmit { node }, _) if node == "broker-c");

    let live = nodes(NodeLifecycle::Live);
    assert!(readmits(&decided(&live, &wiped, 33, true)));
    assert!(
        !readmits(&decided(&live, &wiped, 33, false)),
        "not caught up"
    );
    assert!(
        !readmits(&decided(&live, &wiped, 3, true)),
        "the report still names the old copy"
    );
    assert!(
        !readmits(&decided(&nodes(NodeLifecycle::Down), &wiped, 33, true)),
        "the leader is down"
    );
    // With a down, b alone is 1 of 3, short of the 2 the next generation
    // needs, so the shard would stop.
    let a_down = vec![
        registered("broker-a", 1, NodeLifecycle::Down),
        registered("broker-b", 2, NodeLifecycle::Live),
        registered("broker-c", 33, NodeLifecycle::Live),
    ];
    assert!(
        !readmits(&decided(&a_down, &wiped, 33, true)),
        "broker-a is down"
    );
    for busy in [
        ShardAssignment {
            state: ShardState::Draining,
            ..wiped.clone()
        },
        ShardAssignment {
            joining: Some("broker-a".to_string()),
            ..wiped.clone()
        },
        ShardAssignment {
            pending_set: Some(crate::model::PendingSet {
                replicas: vec!["broker-a".to_string(), "broker-c".to_string()],
                generation: 5,
            }),
            ..wiped.clone()
        },
    ] {
        assert!(!readmits(&decided(&live, &busy, 33, true)), "{busy:?}");
    }

    // The write names the new copy and holds it as a learner: the decision
    // is the set unchanged, written at the next generation.
    let Decision::Move(_, written) = decided(&live, &wiped, 33, true) else {
        unreachable!()
    };
    assert_eq!(written.replicas, wiped.replicas);
    assert_eq!(written.leader, wiped.leader);
}

/// Every shard has broker-c caught up under a new identity.
struct AllReadmittable;

impl CaughtUp for AllReadmittable {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }

    fn readmittable(&self, _key: &ShardKey, _generation: u64, _leader: &str) -> Option<String> {
        Some("broker-c".to_string())
    }
}

/// **Re-admissions are paced.** Each is a new fenced term on its leader, so
/// a broker that comes back wiped under many shards is re-admitted into a
/// bounded number a pass, and into none while placement is paused.
#[test]
fn one_pass_readmits_a_bounded_number_and_none_while_paused() {
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    let shards = 3 * READMITS_PER_PASS as u32;
    let existing: Vec<ShardAssignment> = (0..shards)
        .map(|n| {
            let mut assignment = named("broker-b", &["broker-a", "broker-c"], &[]);
            assignment.key.shard = n;
            assignment
        })
        .collect();
    let stream = Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", shards, 3)
    };
    let readmitted = |paused: bool| {
        let policy = MovePolicy {
            joint_fence: true,
            paused,
            ..MovePolicy::default()
        };
        plan_with(
            std::slice::from_ref(&stream),
            &[],
            &nodes,
            &existing,
            &AllReadmittable,
            policy,
        )
        .moves()
        .filter(|(_, step, _)| matches!(step, MoveStep::Readmit { .. }))
        .count()
    };
    assert_eq!(readmitted(false), READMITS_PER_PASS);
    assert_eq!(readmitted(true), 0);
}

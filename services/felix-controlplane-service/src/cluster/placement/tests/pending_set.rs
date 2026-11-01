//! A cut-over that changes the replica set under `replica_election`: the new
//! set is recorded as pending, the old one stays the control plane's, and the
//! new one is taken only on the leader's report of its joint start
//! (`Swap` and `CpFlip` in docs/formal/FelixShard.tla). End to end against
//! the memory store, so the features are read from the store as a pass reads
//! them.
use super::reconciler::shard_zero;
use super::*;
use crate::model::{Namespace, PendingSet, ReplicaReport, Tenant};
use crate::store::memory::InMemoryStore;
use crate::store::{ControlPlaneStore, StoreConfig};

const ELECTION: [&str; 5] = [
    "generation_start",
    "majority_ack",
    "lease_free_reads",
    "joint_fence",
    "replica_election",
];

/// Brokers a to d offering the election features, a durable `Quorum` stream
/// of three copies, and a shard fenced for a move from a to b with c the
/// other follower. a is draining, so the cut-over drops it and tops the set
/// up with d: {a, b, c} becomes {b, c, d}.
async fn fenced_move(finalize: bool) -> (InMemoryStore, ShardAssignment) {
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
    for (i, id) in ["broker-a", "broker-b", "broker-c", "broker-d"]
        .iter()
        .enumerate()
    {
        let mut n = node(id, NodeLifecycle::Live, None);
        n.spec.advertise_addr = format!("10.0.0.4:{}", 7600 + i);
        n.status.features = ELECTION.iter().map(|f| f.to_string()).collect();
        store.register_node(n).await.expect("node");
    }
    if finalize {
        for feature in ELECTION {
            store.finalize_fleet_feature(feature).await.expect(feature);
        }
    }
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            state: ShardState::Draining,
            successor: Some("broker-b".to_string()),
            generation: 0,
            ..assigned("orders", "broker-a", &["broker-b", "broker-c"])
        })
        .await
        .expect("fenced move");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Draining)
        .await
        .expect("drain a");
    report(&store, &fenced, true, None).await;
    (store, fenced)
}

/// What `leader` reports at `assignment`'s generation, as the API stamps it.
async fn report(
    store: &InMemoryStore,
    assignment: &ShardAssignment,
    drained: bool,
    joint_start_acked: Option<u64>,
) {
    let node_incarnations = store
        .list_nodes()
        .await
        .expect("nodes")
        .into_iter()
        .map(|node| (node.node_id, node.status.incarnation))
        .collect();
    let written = store
        .record_replica_report(
            ReplicaReport {
                key: shard_zero(),
                generation: assignment.generation,
                caught_up: assignment
                    .nodes()
                    .filter(|node| **node != assignment.leader)
                    .cloned()
                    .collect(),
                offsets: Default::default(),
                reported_at_millis: store.now_millis().await.expect("clock"),
                drained,
                leader_offset: None,
                halted: Default::default(),
                counter_offsets: Default::default(),
                stream_incarnation: None,
                by_round: false,
                leader_incarnation: None,
                follower_storage: Default::default(),
                leader: Some(assignment.leader.clone()),
                node_incarnations,
                given_up: false,
                inherited_storage: Default::default(),
                joint_start_acked,
            },
            &assignment.leader,
        )
        .await
        .expect("report");
    assert_eq!(written, crate::store::ReportWrite::Stored);
}

async fn pass(store: &InMemoryStore) -> ShardAssignment {
    reconcile_once(store, &Default::default(), MovePolicy::default()).await;
    store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get")
}

fn set(assignment: &ShardAssignment) -> BTreeSet<&str> {
    assignment.nodes().map(String::as_str).collect()
}

#[tokio::test]
async fn a_cut_over_records_the_new_set_as_pending_and_takes_it_only_on_the_report() {
    let (store, fenced) = fenced_move(true).await;

    let cut_over = pass(&store).await;
    assert_eq!(cut_over.leader, "broker-b");
    assert_eq!(cut_over.generation, fenced.generation + 1);
    assert_eq!(
        set(&cut_over),
        set(&fenced),
        "the old set stays the control plane's"
    );
    let pending = cut_over
        .pending_set
        .clone()
        .expect("the new set is pending");
    assert_eq!(pending.generation, cut_over.generation);
    assert_eq!(
        pending.replicas.iter().collect::<BTreeSet<_>>(),
        ["broker-c", "broker-d"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .iter()
            .collect(),
    );

    // Nothing ends the change without the leader's word: no report, one
    // without the joint start, and one naming another set's generation.
    assert_eq!(pass(&store).await, cut_over);
    report(&store, &cut_over, false, None).await;
    assert_eq!(pass(&store).await, cut_over);
    report(&store, &cut_over, false, Some(pending.generation - 1)).await;
    assert_eq!(pass(&store).await, cut_over);

    report(&store, &cut_over, false, Some(pending.generation)).await;
    let taken = pass(&store).await;
    assert_eq!(taken.leader, "broker-b");
    assert_eq!(taken.replicas, pending.replicas);
    assert_eq!(taken.pending_set, None);
    assert_eq!(taken.generation, cut_over.generation + 1);

    // Taken once: the report that took it names a generation that is gone.
    assert_eq!(pass(&store).await, taken);
}

/// The joint start is read only from the assignment's leader at its own
/// generation: a report held from before a write that kept the set pending
/// does not take it.
#[tokio::test]
async fn a_joint_start_from_an_older_generation_does_not_take_the_set() {
    let (store, _) = fenced_move(true).await;
    let cut_over = pass(&store).await;
    let pending = cut_over.pending_set.clone().expect("pending");
    report(&store, &cut_over, false, Some(pending.generation)).await;
    // Another write that keeps the change pending, as adoption will, lands
    // before a pass reads the report.
    let later = store
        .put_shard_assignment_if(
            ShardAssignment {
                generation: 0,
                ..cut_over.clone()
            },
            Some(cut_over.generation),
            store.placement_token().await.expect("token"),
        )
        .await
        .expect("write");
    let crate::store::AssignmentWrite::Written(later) = later else {
        panic!("not written: {later:?}");
    };
    assert_eq!(pass(&store).await, later);
    report(&store, &later, false, Some(pending.generation)).await;
    assert_eq!(pass(&store).await.pending_set, None);
}

/// Without `replica_election` a cut-over writes what it always has: the new
/// set at once, and no pending set.
#[tokio::test]
async fn without_replica_election_a_cut_over_takes_the_new_set_at_once() {
    let (store, fenced) = fenced_move(false).await;
    let cut_over = pass(&store).await;
    assert_eq!(cut_over.leader, "broker-b");
    assert_eq!(cut_over.pending_set, None);
    assert_eq!(
        set(&cut_over),
        BTreeSet::from(["broker-b", "broker-c", "broker-d"])
    );
    assert_ne!(set(&cut_over), set(&fenced));
}

/// A cut-over that keeps the set has nothing to change jointly.
#[test]
fn a_cut_over_that_keeps_the_set_records_nothing_pending() {
    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 2)
    }];
    let nodes = live(&["broker-a", "broker-b"]);
    let fenced = ShardAssignment {
        state: ShardState::Draining,
        successor: Some("broker-b".to_string()),
        ..assigned("orders", "broker-a", &["broker-b"])
    };
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[fenced],
        &Drained,
        MovePolicy {
            replica_election: true,
            ..MovePolicy::default()
        },
    );
    match &plan.shards[0].decision {
        Decision::Move(MoveStep::CutOver { .. }, next) => {
            assert_eq!(next.leader, "broker-b");
            assert_eq!(next.replicas, ["broker-a"]);
            assert_eq!(next.pending_set, None);
        }
        other => panic!("expected a cut-over, got {other:?}"),
    }
}

/// A failover while the change is pending places the shard from the control
/// plane's own set, the old one, and writes no pending set: it abandons the
/// change.
#[test]
fn a_failover_abandons_a_pending_change() {
    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Live, None),
        node("broker-b", NodeLifecycle::Down, None),
        node("broker-c", NodeLifecycle::Live, None),
        node("broker-d", NodeLifecycle::Live, None),
    ];
    let pending = ShardAssignment {
        state: ShardState::Assigning,
        pending_set: Some(PendingSet {
            replicas: vec!["broker-c".to_string(), "broker-d".to_string()],
            generation: 3,
        }),
        ..assigned("orders", "broker-b", &["broker-a", "broker-c"])
    };
    let plan = plan_with(
        &streams,
        &[],
        &nodes,
        &[pending],
        &Drained,
        MovePolicy {
            replica_election: true,
            ..MovePolicy::default()
        },
    );
    match &plan.shards[0].decision {
        Decision::Place(leader, _) => {
            assert!(
                ["broker-a", "broker-c"].contains(&leader.as_str()),
                "{leader}"
            );
        }
        other => panic!("expected a failover, got {other:?}"),
    }
}

/// An operator can neither start a move nor cancel one while a change is
/// pending: only the report or a failover ends it.
#[test]
fn an_operator_cannot_move_or_cancel_while_a_change_is_pending() {
    let streams = vec![Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = live(&["broker-a", "broker-b", "broker-c", "broker-d"]);
    let existing = vec![ShardAssignment {
        state: ShardState::Assigning,
        pending_set: Some(PendingSet {
            replicas: vec!["broker-c".to_string(), "broker-d".to_string()],
            generation: 3,
        }),
        ..assigned("orders", "broker-b", &["broker-a", "broker-c"])
    }];
    let catalog = Catalog {
        streams: &streams,
        caches: &[],
        nodes: &nodes,
        existing: &existing,
        caught_up: &Drained,
        policy: MovePolicy::default(),
    };
    assert_eq!(
        start_move(&catalog, &shard_zero(), "broker-d"),
        Err(Refused::SetChangePending)
    );
    assert_eq!(
        cancel_move(&catalog, &shard_zero()),
        Err(Refused::SetChangePending)
    );
}

/// Every copy caught up, and every leader drained, at any generation.
struct Drained;

impl CaughtUp for Drained {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }

    fn is_drained(&self, _key: &ShardKey, _generation: u64) -> bool {
        true
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }
}

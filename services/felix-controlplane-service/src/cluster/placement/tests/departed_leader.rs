//! A fenced shard whose leader has deregistered is never handed back to it
//! (#1982). The move cuts over to a copy that holds the log, or the shard
//! waits for the leader's lease to run out and fails over.
use super::reconciler::{cluster, report, shard_zero};
use super::*;
use crate::model::{ConsistencyLevel, MoveReason};
use crate::store::ControlPlaneStore;

const NOW: u64 = 10_000_000_000;
const MINUTE: u64 = 60_000;

/// Reports as of `NOW` at generation 3, with the fence first seen at
/// `fenced_at`.
struct Fenced {
    caught_up: &'static [&'static str],
    drained: bool,
    fenced_at: Option<u64>,
}

impl CaughtUp for Fenced {
    fn is_caught_up(&self, _key: &ShardKey, node_id: &str) -> bool {
        self.caught_up.contains(&node_id)
    }

    fn is_drained(&self, _key: &ShardKey, generation: u64) -> bool {
        self.drained && generation == 3
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(NOW)
    }

    fn fenced_at_millis(&self, _key: &ShardKey, generation: u64) -> Option<u64> {
        self.fenced_at.filter(|_| generation == 3)
    }
}

/// A Quorum shard fenced to move from broker-a to broker-b. broker-a is
/// draining, and `departed` when it has deregistered inside its lease;
/// broker-b is live unless `successor_down`.
fn decide(reports: &Fenced, departed: bool, successor_down: bool) -> Decision {
    let streams = [Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = vec![
        node("broker-a", NodeLifecycle::Draining, None),
        node(
            "broker-b",
            if successor_down {
                NodeLifecycle::Down
            } else {
                NodeLifecycle::Live
            },
            None,
        ),
        node("broker-c", NodeLifecycle::Live, None),
    ];
    let fenced = ShardAssignment {
        state: ShardState::Draining,
        successor: Some("broker-b".to_string()),
        move_started_at_millis: Some(NOW - 10 * MINUTE),
        move_reason: Some(MoveReason::Drain),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c"])
    };
    let policy = MovePolicy {
        departed: if departed {
            BTreeSet::from(["broker-a".to_string()])
        } else {
            BTreeSet::new()
        },
        ..MovePolicy::default()
    };
    plan_with(&streams, &[], &nodes, &[fenced], reports, policy)
        .shards
        .remove(0)
        .decision
}

fn retaken_by_a(decision: &Decision) -> bool {
    matches!(decision, Decision::Move(_, assignment) if assignment.leader == "broker-a")
}

/// Past the fence timeout a draining leader serves again, but not one that
/// has deregistered.
#[test]
fn a_departed_leader_does_not_retake_a_timed_out_fence() {
    let reports = Fenced {
        caught_up: &[],
        drained: false,
        fenced_at: Some(NOW - DEFAULT_FENCE_TIMEOUT_MILLIS - 1),
    };
    assert!(retaken_by_a(&decide(&reports, false, false)));
    assert_eq!(
        decide(&reports, true, false),
        Decision::Waiting(Blocked::LeaderDeparted),
    );
}

/// The destination died before the leader drained: a draining leader takes
/// the shard back, a departed one does not.
#[test]
fn a_departed_leader_does_not_retake_when_the_destination_dies() {
    let reports = Fenced {
        caught_up: &[],
        drained: false,
        fenced_at: None,
    };
    assert!(retaken_by_a(&decide(&reports, false, true)));
    assert_eq!(
        decide(&reports, true, true),
        Decision::Waiting(Blocked::LeaderDeparted),
    );
}

/// Drained, with no other copy holding the log, the cut-over names the
/// leader again unless it has deregistered.
#[test]
fn a_departed_leader_is_not_the_cut_over_of_last_resort() {
    let reports = Fenced {
        caught_up: &[],
        drained: true,
        fenced_at: None,
    };
    assert!(retaken_by_a(&decide(&reports, false, false)));
    assert_eq!(
        decide(&reports, true, false),
        Decision::Waiting(Blocked::LeaderDeparted),
    );
}

/// The drain still finishes: a caught-up destination takes over from a
/// departed leader that drained.
#[test]
fn a_departed_leader_still_hands_over_to_its_destination() {
    let decision = decide(
        &Fenced {
            caught_up: &["broker-b"],
            drained: true,
            fenced_at: None,
        },
        true,
        false,
    );
    assert!(
        matches!(
            &decision,
            Decision::Move(MoveStep::CutOver { from, to }, _) if from == "broker-a" && to == "broker-b"
        ),
        "{decision:?}"
    );
}

/// Through the reconciler: a broker that deregistered inside its lease is
/// seen as departed, and its drained shard waits instead of going back to it.
#[tokio::test]
async fn the_reconciler_does_not_hand_a_departed_leader_its_shard_back() {
    let store = cluster(&["broker-x", "broker-y", "broker-z"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store.forget_deleted_names().await;
    store
        .create_stream(replicated_stream("orders", 1, 3))
        .await
        .expect("stream");
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            generation: 0,
            state: ShardState::Draining,
            successor: Some("broker-y".to_string()),
            move_reason: Some(MoveReason::Drain),
            ..assigned("orders", "broker-x", &["broker-y", "broker-z"])
        })
        .await
        .expect("fenced");
    // Drained, but no follower holds the log yet.
    report(&store, fenced.generation, &[], true).await;
    let now = store.now_millis().await.expect("clock");
    store
        .record_node_heartbeat("broker-x", 0, now)
        .await
        .expect("beat");
    store
        .set_node_lifecycle("broker-x", NodeLifecycle::Left)
        .await
        .expect("leave");

    reconcile_once(&store, &Default::default(), MovePolicy::default()).await;

    let after = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(after.generation, fenced.generation, "{after:?}");
    assert_eq!(after.state, ShardState::Draining, "{after:?}");
}

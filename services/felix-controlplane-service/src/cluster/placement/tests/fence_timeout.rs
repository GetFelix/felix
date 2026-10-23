//! A fenced leader that never reports drained gets its shard back once the
//! fence timeout passes, rather than holding the shard's writes forever.
use super::*;
use crate::model::{ConsistencyLevel, MoveReason};

const NOW: u64 = 10_000_000_000;
const MINUTE: u64 = 60_000;

/// Reports as of `NOW` at generation 3, every copy level, the leader never
/// drained, and the fence first seen at `fenced_at`.
struct Fenced {
    fenced_at: Option<u64>,
    drained: bool,
}

impl CaughtUp for Fenced {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
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

/// A Quorum shard on three brokers, fenced to move from broker-a to broker-b.
fn fenced_move() -> ShardAssignment {
    ShardAssignment {
        state: ShardState::Draining,
        successor: Some("broker-b".to_string()),
        move_started_at_millis: Some(NOW - 10 * MINUTE),
        move_reason: Some(MoveReason::Balance),
        ..assigned("orders", "broker-a", &["broker-b", "broker-c"])
    }
}

fn decide(reports: &dyn CaughtUp) -> Decision {
    let streams = [Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream("orders", 1, 3)
    }];
    let nodes = live(&["broker-a", "broker-b", "broker-c"]);
    plan_with(
        &streams,
        &[],
        &nodes,
        &[fenced_move()],
        reports,
        MovePolicy::default(),
    )
    .shards
    .remove(0)
    .decision
}

/// Past the fence timeout the leader takes the shard back at a new
/// generation, keeping every copy, and the move's start stays so the shard
/// waits behind others for its next one.
#[test]
fn a_leader_fenced_past_the_timeout_serves_again() {
    let decision = decide(&Fenced {
        fenced_at: Some(NOW - DEFAULT_FENCE_TIMEOUT_MILLIS - 1),
        drained: false,
    });
    let Decision::Move(step, assignment) = decision else {
        panic!("not given up: {decision:?}");
    };
    assert_eq!(
        step,
        MoveStep::FenceTimedOut {
            successor: Some("broker-b".to_string())
        }
    );
    assert_eq!(assignment.leader, "broker-a");
    assert_eq!(assignment.state, ShardState::Assigning);
    assert_eq!(assignment.successor, None);
    assert_eq!(assignment.move_reason, None);
    assert_eq!(assignment.move_started_at_millis, Some(NOW - 10 * MINUTE));
    assert_eq!(
        assignment.replicas,
        vec!["broker-b".to_string(), "broker-c".to_string()],
        "the destination was a follower already and stays one",
    );
}

/// Inside the timeout, or with no watch of the fence at all, the move waits
/// for the drained report.
#[test]
fn a_fence_inside_the_timeout_or_unwatched_waits() {
    for fenced_at in [Some(NOW - DEFAULT_FENCE_TIMEOUT_MILLIS), None] {
        assert_eq!(
            decide(&Fenced {
                fenced_at,
                drained: false,
            }),
            Decision::Waiting(Blocked::LeaderStopping),
            "{fenced_at:?}",
        );
    }
}

/// A drained report wins over the timeout: the move cuts over.
#[test]
fn a_drained_report_past_the_timeout_still_cuts_over() {
    let decision = decide(&Fenced {
        fenced_at: Some(NOW - 10 * DEFAULT_FENCE_TIMEOUT_MILLIS),
        drained: true,
    });
    assert!(
        matches!(decision, Decision::Move(MoveStep::CutOver { .. }, _)),
        "{decision:?}"
    );
}

async fn pass(
    store: &crate::store::memory::InMemoryStore,
    policy: &MovePolicy,
    progress: &mut super::super::progress::CopyProgress,
) -> ReconcileOutcome {
    let planned =
        super::super::reconciler::plan_pass(store, &Default::default(), policy.clone(), progress)
            .await
            .expect("plan");
    super::super::reconciler::apply_pass(
        store,
        &planned,
        &mut Default::default(),
        &Default::default(),
    )
    .await
}

/// Through the store: an instance that watches a fence across passes gives
/// it up once the timeout passes, and the write lands at the next generation.
#[tokio::test]
async fn a_watched_fence_is_given_up_through_the_store() {
    use super::reconciler::{cluster, report, shard_zero};
    use crate::store::ControlPlaneStore;
    let store = cluster(&["broker-x", "broker-y"]).await;
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the three-shard stream");
    store
        .create_stream(replicated_stream("orders", 1, 2))
        .await
        .expect("stream");
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            key: shard_zero(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Draining,
            successor: Some("broker-y".to_string()),
            joining: None,
            move_started_at_millis: None,
            move_reason: Some(MoveReason::Balance),
            incarnation: 0,
            fence_storage_ids: Default::default(),
        })
        .await
        .expect("fenced");
    report(&store, fenced.generation, &["broker-y"], false).await;

    let policy = MovePolicy {
        fence_timeout_millis: Some(1),
        ..MovePolicy::default()
    };
    let mut progress = Default::default();
    let first = pass(&store, &policy, &mut progress).await;
    assert_eq!(first.moved, 0, "the first pass only starts watching");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    report(&store, fenced.generation, &["broker-y"], false).await;
    let second = pass(&store, &policy, &mut progress).await;
    assert_eq!(second.moved, 1, "{second:?}");

    let after = store
        .get_shard_assignment(&shard_zero())
        .await
        .expect("get");
    assert_eq!(after.leader, "broker-x");
    assert_eq!(after.state, ShardState::Assigning);
    assert_eq!(after.successor, None);
    assert!(after.generation > fenced.generation);
}

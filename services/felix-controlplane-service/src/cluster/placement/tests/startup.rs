//! A cluster started all at once: new shards wait briefly for enough brokers
//! to hold every copy, and the copies a short shard is still missing are
//! added together when its log is small enough that there is nothing to pace.
use super::reconciler::cluster;
use super::*;
use crate::config::NodeLivenessConfig;
use crate::model::{MoveReason, StreamKey};
use crate::store::ControlPlaneStore;

/// When the first broker registered.
const T0: u64 = 10_000_000_000;

/// Reports as of `now`, at the generation `assigned` uses, with each
/// stream's leader tail.
struct Clock {
    now: u64,
    tails: BTreeMap<String, u64>,
}

impl Clock {
    fn at(now: u64) -> Self {
        Self {
            now,
            tails: BTreeMap::new(),
        }
    }

    fn tail(mut self, stream: &str, records: u64) -> Self {
        self.tails.insert(stream.to_string(), records);
        self
    }
}

impl CaughtUp for Clock {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        false
    }

    fn leader_offset(&self, key: &ShardKey) -> Option<u64> {
        self.tails.get(&key.stream).copied()
    }

    fn reported_generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn as_of_millis(&self) -> Option<u64> {
        Some(self.now)
    }
}

/// A live broker that first registered at `at`.
fn registered(id: &str, at: u64) -> Node {
    let mut node = node(id, NodeLifecycle::Live, None);
    node.status.registered_at_millis = at;
    node.status.last_heartbeat_at_millis = at;
    node
}

fn copies(plan: &Plan) -> Vec<usize> {
    plan.to_place()
        .map(|(_, _, replicas)| replicas.len() + 1)
        .collect()
}

/// **Started together.** Two of three brokers have registered when the first
/// pass runs: the new shards wait rather than go on two. The third registers
/// 120 ms in, and the next pass places every shard on all three, with no
/// restore left to run.
#[test]
fn brokers_registering_in_sequence_get_every_copy_at_first_placement() {
    let streams = [replicated_stream("orders", 11, 3)];
    let early = [registered("broker-a", T0), registered("broker-b", T0 + 50)];

    let held_plan = plan(&streams, &[], &early, &[], &Clock::at(T0 + 100));
    assert_eq!(
        held_plan.to_place().count(),
        0,
        "placed before the third broker"
    );
    let held: Vec<_> = held_plan.settling().collect();
    assert_eq!(held.len(), 11);
    assert!(
        held.iter()
            .all(|(_, live, until)| *live == 2 && *until == T0 + DEFAULT_PLACEMENT_SETTLE_MILLIS)
    );

    let all = [
        registered("broker-a", T0),
        registered("broker-b", T0 + 50),
        registered("broker-c", T0 + 120),
    ];
    let placed = plan(&streams, &[], &all, &[], &Clock::at(T0 + 150));
    assert_eq!(copies(&placed), vec![3; 11]);
}

/// **Bounded.** A third broker that never comes does not hold the shards up
/// past the window: at its end they are placed on the two that are live.
#[test]
fn a_missing_broker_holds_new_shards_only_for_the_window() {
    let streams = [replicated_stream("orders", 2, 3)];
    let nodes = [registered("broker-a", T0), registered("broker-b", T0 + 50)];

    let just_before = T0 + DEFAULT_PLACEMENT_SETTLE_MILLIS - 1;
    let plan_before = plan(&streams, &[], &nodes, &[], &Clock::at(just_before));
    assert_eq!(plan_before.settling().count(), 2);

    let at_end = T0 + DEFAULT_PLACEMENT_SETTLE_MILLIS;
    let plan_after = plan(&streams, &[], &nodes, &[], &Clock::at(at_end));
    assert_eq!(copies(&plan_after), [2, 2]);
}

/// **Enough brokers.** A single broker and a stream of one copy, or as many
/// live brokers as the factor, place at once however young the cluster is.
#[test]
fn a_young_cluster_with_enough_brokers_places_at_once() {
    let one = [registered("broker-a", T0)];
    let plan_one = plan(
        &[replicated_stream("orders", 2, 1)],
        &[],
        &one,
        &[],
        &Clock::at(T0 + 10),
    );
    assert_eq!(copies(&plan_one), [1, 1]);

    let two = [registered("broker-a", T0), registered("broker-b", T0)];
    let plan_two = plan(
        &[replicated_stream("orders", 1, 2)],
        &[],
        &two,
        &[],
        &Clock::at(T0 + 10),
    );
    assert_eq!(copies(&plan_two), [2]);
}

/// **An established cluster.** A broker down for good does not delay a
/// stream created later: the cluster's first broker registered long before,
/// so the new shards go on the brokers that are live.
#[test]
fn a_stream_created_while_a_broker_is_down_places_on_the_rest() {
    let mut down = registered("broker-c", T0);
    down.status.lifecycle = NodeLifecycle::Down;
    let nodes = [registered("broker-a", T0), registered("broker-b", T0), down];
    let plan = plan(
        &[replicated_stream("orders", 2, 3)],
        &[],
        &nodes,
        &[],
        &Clock::at(T0 + 60 * 60 * 1_000),
    );
    assert_eq!(copies(&plan), [2, 2]);
}

/// **Off.** With no window, new shards go on whatever is live, as before.
#[test]
fn no_window_places_new_shards_on_what_is_live() {
    let nodes = [registered("broker-a", T0), registered("broker-b", T0)];
    let plan = plan_with(
        &[replicated_stream("orders", 2, 3)],
        &[],
        &nodes,
        &[],
        &Clock::at(T0 + 10),
        MovePolicy {
            settle_millis: None,
            ..MovePolicy::default()
        },
    );
    assert_eq!(copies(&plan), [2, 2]);
}

/// Eleven one-shard streams, each led by broker-a with broker-b behind it:
/// how a cluster whose first placement saw two brokers stands.
fn short_shards() -> (Vec<Stream>, Vec<ShardAssignment>) {
    let names: Vec<String> = (0..11).map(|i| format!("s{i:02}")).collect();
    let streams = names
        .iter()
        .map(|name| replicated_stream(name, 1, 3))
        .collect();
    let existing = names
        .iter()
        .map(|name| assigned(name, "broker-a", &["broker-b"]))
        .collect();
    (streams, existing)
}

fn three() -> Vec<Node> {
    ["broker-a", "broker-b", "broker-c"]
        .iter()
        .map(|id| registered(id, 1))
        .collect()
}

fn restores(plan: &Plan) -> Vec<&str> {
    plan.moves()
        .filter(|(_, step, _)| matches!(step, MoveStep::Restore { .. }))
        .map(|(key, _, _)| key.stream.as_str())
        .collect()
}

/// **Late broker.** The third broker joins after the window, and every short
/// shard with a near-empty log gets its copy started in the same pass rather
/// than one per move slot.
#[test]
fn small_top_ups_all_start_in_one_pass() {
    let (streams, existing) = short_shards();
    let clock = streams
        .iter()
        .fold(Clock::at(T0), |clock, s| clock.tail(&s.stream, 0));
    let plan = plan(&streams, &[], &three(), &existing, &clock);
    assert_eq!(restores(&plan).len(), 11);
    assert_eq!(plan.waiting().count(), 0);
}

/// **History to copy.** A short shard whose leader holds more than a fence's
/// lag of records is a real copy, and still waits for a move slot; so does
/// one whose leader has not said what it holds.
#[test]
fn a_top_up_with_history_to_copy_waits_for_a_slot() {
    let (streams, existing) = short_shards();
    let clock = Clock::at(T0)
        .tail("s00", DEFAULT_FENCE_MAX_LAG_RECORDS + 1)
        .tail("s01", DEFAULT_FENCE_MAX_LAG_RECORDS + 1)
        .tail("s02", DEFAULT_FENCE_MAX_LAG_RECORDS);
    let plan = plan(&streams, &[], &three(), &existing, &clock);
    // s02 is small; of the rest, one gets the slot.
    let started = restores(&plan);
    assert_eq!(started, ["s00", "s02"]);
    assert_eq!(
        plan.waiting()
            .filter(|(_, blocked)| **blocked == Blocked::MoveLimit)
            .count(),
        9
    );
}

/// **No slot held.** A small top-up in flight leaves the move slot to a shard
/// that needs a real copy.
#[test]
fn a_small_top_up_in_flight_leaves_the_slot_free() {
    let (streams, mut existing) = short_shards();
    existing[0].replicas.push("broker-c".to_string());
    existing[0].joining = Some("broker-c".to_string());
    existing[0].move_reason = Some(MoveReason::Restore);
    existing[0].move_started_at_millis = Some(T0 - 1_000);
    let clock = Clock::at(T0)
        .tail("s00", 0)
        .tail("s01", DEFAULT_FENCE_MAX_LAG_RECORDS + 1);
    let plan = plan(&streams, &[], &three(), &existing, &clock);
    assert_eq!(restores(&plan), ["s01"]);
}

/// **Paused.** Pausing placement stops small top-ups too.
#[test]
fn a_paused_placement_starts_no_top_up() {
    let (streams, existing) = short_shards();
    let clock = streams
        .iter()
        .fold(Clock::at(T0), |clock, s| clock.tail(&s.stream, 0));
    let plan = plan_with(
        &streams,
        &[],
        &three(),
        &existing,
        &clock,
        MovePolicy {
            paused: true,
            ..MovePolicy::default()
        },
    );
    assert!(restores(&plan).is_empty());
    assert!(
        plan.waiting()
            .all(|(_, blocked)| *blocked == Blocked::Paused)
    );
}

/// **Through the store.** Two brokers registered just now and a stream of
/// eleven shards at three copies: a pass places nothing. The third broker
/// registers and the next pass places every shard on all three.
#[tokio::test]
async fn a_pass_after_the_last_broker_registers_places_every_copy() {
    let store = cluster(&[]).await;
    store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("drop the default stream");
    store
        .create_stream(replicated_stream("orders", 11, 3))
        .await
        .expect("stream");
    let register = |id: &'static str, port: u16| {
        let store = &store;
        async move {
            let now = store.now_millis().await.expect("clock");
            let mut node = registered(id, now);
            node.spec.advertise_addr = format!("10.0.0.6:{port}");
            store.register_node(node).await.expect("register");
        }
    };
    register("broker-a", 7600).await;
    register("broker-b", 7601).await;
    let liveness = NodeLivenessConfig::default();

    let outcome = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!((outcome.placed, outcome.settling), (0, 11));

    register("broker-c", 7602).await;
    let outcome = reconcile_once(&store, &liveness, MovePolicy::default()).await;
    assert_eq!((outcome.placed, outcome.settling), (11, 0));
    let assignments = store.list_shard_assignments().await.expect("list");
    assert_eq!(assignments.len(), 11);
    assert!(
        assignments
            .iter()
            .all(|assignment| assignment.replicas.len() == 2),
        "{assignments:?}"
    );
}

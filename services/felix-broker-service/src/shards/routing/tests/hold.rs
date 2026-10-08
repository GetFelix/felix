//! Writes that reach a broker while their shard moves are held until the
//! cut-over and then sent where it went, within the window and the cap.
use std::time::{Duration, Instant};

use super::*;
use crate::shards::lifecycle::{Action, ShardLifecycle};
use crate::shards::routing::hold::MoveHold;

/// One broker's routing state, driven the way the feed drives it.
struct Broker {
    lifecycle: ShardLifecycle,
    ingress: Arc<IngressRouter>,
    assignments: HashMap<ShardKey, ShardAssignment>,
    nodes: HashMap<String, NodeRef>,
}

impl Broker {
    fn new(node_id: &str, hold: MoveHold) -> Self {
        let router = Arc::new(ShardRouter::new(
            node_id,
            "us-west-2",
            RegionRouter::new("us-west-2".to_string()),
        ));
        let lifecycle = ShardLifecycle::new(node_id);
        let ingress = Arc::new(
            IngressRouter::new(router, Arc::clone(lifecycle.fence())).with_move_hold(hold),
        );
        Self {
            lifecycle,
            ingress,
            assignments: HashMap::new(),
            nodes: catalog(),
        }
    }

    /// Reconcile local state with `assignment` without publishing a view: the
    /// moment between the lifecycle acting and the feed publishing.
    fn observe(&mut self, assignment: ShardAssignment) {
        let key = assignment.key.clone();
        if let Action::Open { generation, .. } = self.lifecycle.observe(&key, Some(&assignment)) {
            self.lifecycle.opened(&key, generation);
        }
        self.assignments.insert(key, assignment);
    }

    /// Publish routes and the servable set together, as the feed does.
    fn publish(&self) {
        self.ingress.publish(
            routing_table_from(&self.assignments, &self.nodes),
            &self.nodes,
            self.lifecycle.servable(),
        );
    }

    fn apply(&mut self, assignment: ShardAssignment) {
        self.observe(assignment);
        self.publish();
    }
}

fn draining(shard: u32, leader: &str, generation: u64, successor: &str) -> ShardAssignment {
    ShardAssignment {
        state: "draining".to_string(),
        successor: Some(successor.to_string()),
        replicas: vec![successor.to_string()],
        ..assignment(shard, leader, generation)
    }
}

fn hold(window_ms: u64, max_held: usize) -> MoveHold {
    MoveHold::new(Duration::from_millis(window_ms), max_held)
}

fn forward_to_b(generation: u64) -> Dispatch {
    Dispatch::Forward {
        node_id: "broker-b".to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], 7002)),
        generation,
    }
}

/// Spawn a write's dispatch and give it time to finish if it is not held.
async fn dispatch_in_background(
    ingress: &Arc<IngressRouter>,
) -> tokio::task::JoinHandle<(Dispatch, bool)> {
    let ingress = Arc::clone(ingress);
    let task = tokio::spawn(async move {
        let (dispatch, fenced) = ingress.dispatch_write(&key(0)).await;
        (dispatch, fenced.is_some())
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    task
}

/// The old leader, fenced for a move, says why it will not take a write.
#[tokio::test]
async fn a_fenced_leader_says_the_shard_is_moving() {
    let mut a = Broker::new("broker-a", MoveHold::disabled());
    a.apply(assignment(0, "broker-a", 1));
    a.apply(draining(0, "broker-a", 2, "broker-b"));

    assert_eq!(
        a.ingress.dispatch(&key(0)),
        Dispatch::Unavailable(Reason::Moving)
    );
    assert_eq!(Reason::Moving.wire_name(), "moving");
}

/// The case the hold exists for: a publish to the old leader between the
/// fence and the cut-over waits, then goes to the new owner.
#[tokio::test]
async fn a_write_to_the_fenced_leader_follows_the_cut_over() {
    let mut a = Broker::new("broker-a", hold(5_000, 16));
    a.apply(assignment(0, "broker-a", 1));
    a.apply(draining(0, "broker-a", 2, "broker-b"));

    let write = dispatch_in_background(&a.ingress).await;
    assert!(!write.is_finished(), "a write to a moving shard is held");
    assert_eq!(a.ingress.move_hold().held(), 1);

    a.apply(assignment(0, "broker-b", 3));
    let (dispatch, fenced) = write.await.expect("dispatch");
    assert_eq!(dispatch, forward_to_b(3));
    assert!(!fenced, "a forward holds no local fence");
    assert_eq!(a.ingress.move_hold().held(), 0);
}

/// Any other broker that routes to the fenced leader waits as well, instead
/// of forwarding to a broker that will not take the write.
#[tokio::test]
async fn a_write_routed_to_a_fenced_leader_elsewhere_waits_for_the_cut_over() {
    let mut a = Broker::new("broker-a", hold(5_000, 16));
    a.apply(assignment(0, "broker-b", 1));
    a.apply(draining(0, "broker-b", 2, "broker-a"));

    let write = dispatch_in_background(&a.ingress).await;
    assert!(
        !write.is_finished(),
        "a write toward a moving shard is held"
    );

    // The shard moves here, and the write is served here.
    a.apply(assignment(0, "broker-a", 3));
    let (dispatch, fenced) = write.await.expect("dispatch");
    assert_eq!(dispatch, Dispatch::Local { generation: 3 });
    assert!(fenced, "a local write holds its place in the fence");
}

/// A drain moves the shard off a broker the catalog no longer counts as live,
/// so the fenced leader reads as unavailable rather than as somewhere to
/// forward. That is still a move, and the write waits for it.
#[tokio::test]
async fn a_write_to_a_draining_node_waits_for_the_cut_over() {
    let mut a = Broker::new("broker-a", hold(5_000, 16));
    a.apply(assignment(0, "broker-b", 1));
    a.nodes
        .insert("broker-b".to_string(), node("broker-b", 7002, false));
    a.apply(draining(0, "broker-b", 2, "broker-a"));
    assert!(matches!(
        a.ingress.dispatch(&key(0)),
        Dispatch::Unavailable(Reason::OwnerUnavailable(_))
    ));

    let write = dispatch_in_background(&a.ingress).await;
    assert!(
        !write.is_finished(),
        "a write toward a drained node is held"
    );

    a.apply(assignment(0, "broker-a", 3));
    let (dispatch, fenced) = write.await.expect("dispatch");
    assert_eq!(dispatch, Dispatch::Local { generation: 3 });
    assert!(fenced, "a local write holds its place in the fence");
}

/// The fence closes when the lifecycle acts, a moment before the feed
/// publishes the view that says so. A write routed in that moment would be
/// refused at its claim; it waits for the view instead.
#[tokio::test]
async fn a_write_that_meets_a_closed_fence_waits_for_the_new_routes() {
    let mut a = Broker::new("broker-a", hold(5_000, 16));
    a.apply(assignment(0, "broker-a", 1));
    a.observe(draining(0, "broker-a", 2, "broker-b"));
    assert_eq!(
        a.ingress.dispatch(&key(0)),
        Dispatch::Local { generation: 1 },
        "the view still says local",
    );

    let write = dispatch_in_background(&a.ingress).await;
    assert!(!write.is_finished(), "the closed fence holds the write");

    a.publish();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!write.is_finished(), "still moving");
    a.apply(assignment(0, "broker-b", 3));
    assert_eq!(write.await.expect("dispatch").0, forward_to_b(3));
}

/// A move that does not cut over within the window is refused as moving, so
/// the client retries rather than waits without bound.
#[tokio::test]
async fn a_move_that_outlasts_the_window_is_refused_as_moving() {
    let mut a = Broker::new("broker-a", hold(100, 16));
    a.apply(assignment(0, "broker-a", 1));
    a.apply(draining(0, "broker-a", 2, "broker-b"));

    let started = Instant::now();
    let (dispatch, fenced) = a.ingress.dispatch_write(&key(0)).await;
    assert_eq!(dispatch, Dispatch::Unavailable(Reason::Moving));
    assert!(fenced.is_none());
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "refused only once the window ran out",
    );
    assert_eq!(a.ingress.move_hold().held(), 0);
}

/// Each held write keeps its payload, so only so many wait; the rest are
/// refused at once.
#[tokio::test]
async fn writes_beyond_the_cap_are_refused_at_once() {
    let mut a = Broker::new("broker-a", hold(5_000, 1));
    a.apply(assignment(0, "broker-a", 1));
    a.apply(draining(0, "broker-a", 2, "broker-b"));

    let first = dispatch_in_background(&a.ingress).await;
    assert!(!first.is_finished());

    let started = Instant::now();
    let (second, _) = a.ingress.dispatch_write(&key(0)).await;
    assert_eq!(second, Dispatch::Unavailable(Reason::Moving));
    assert!(started.elapsed() < Duration::from_secs(1), "not held");

    a.apply(assignment(0, "broker-b", 3));
    assert_eq!(first.await.expect("dispatch").0, forward_to_b(3));
}

/// With holding off, a write to a moving shard is refused at once, as moving.
#[tokio::test]
async fn with_holding_off_a_moving_shard_is_refused_at_once() {
    let mut a = Broker::new("broker-a", MoveHold::disabled());
    a.apply(assignment(0, "broker-a", 1));
    a.apply(draining(0, "broker-a", 2, "broker-b"));

    let (dispatch, _) = a.ingress.dispatch_write(&key(0)).await;
    assert_eq!(dispatch, Dispatch::Unavailable(Reason::Moving));
}

/// The owner's side: a forward from a broker whose routes are newer than this
/// one's waits for this broker to catch up.
#[tokio::test]
async fn settling_waits_for_routes_to_reach_the_requested_generation() {
    let mut b = Broker::new("broker-b", hold(5_000, 16));
    b.apply(draining(0, "broker-a", 2, "broker-b"));

    let ingress = Arc::clone(&b.ingress);
    let settle = tokio::spawn(async move { ingress.settle(&key(0), 3).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!settle.is_finished(), "behind the requester, so it waits");

    b.apply(assignment(0, "broker-b", 3));
    tokio::time::timeout(Duration::from_secs(1), settle)
        .await
        .expect("settles once the routes catch up")
        .expect("settle");
    assert_eq!(
        b.ingress.dispatch(&key(0)),
        Dispatch::Local { generation: 3 }
    );
}

/// Nothing is held when nothing is moving.
#[tokio::test]
async fn a_shard_that_is_not_moving_is_dispatched_at_once() {
    let mut a = Broker::new("broker-a", hold(5_000, 16));
    a.apply(assignment(0, "broker-a", 1));

    let started = Instant::now();
    let (dispatch, fenced) = a.ingress.dispatch_write(&key(0)).await;
    assert_eq!(dispatch, Dispatch::Local { generation: 1 });
    assert!(fenced.is_some());
    a.ingress.settle(&key(0), 1).await;
    assert!(started.elapsed() < Duration::from_millis(100));
}

/// A write that reaches the broker that just took the shard, while it is
/// still fencing its replicas, waits for the fence instead of failing the
/// cut-over it arrived at.
#[tokio::test]
async fn a_write_to_a_shard_still_fencing_waits_for_the_fence() {
    let mut a = Broker::new("broker-a", hold(5_000, 16));
    a.lifecycle.fence_promotions();
    a.apply(assignment(0, "broker-a", 3));
    assert_eq!(a.ingress.fence().awaiting_promotion(&key(0)), Some(3));

    let write = dispatch_in_background(&a.ingress).await;
    assert!(!write.is_finished(), "a write to a fencing shard is held");

    assert!(a.lifecycle.fenced(&key(0), 3));
    a.publish();
    let (dispatch, fenced) = write.await.expect("dispatch");
    assert_eq!(dispatch, Dispatch::Local { generation: 3 });
    assert!(fenced, "a local write enters the fence");
}

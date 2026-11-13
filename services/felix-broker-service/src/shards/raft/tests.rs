use std::path::Path;

use felix_broker::StreamMetadata;
use felix_replication::raft::{
    AgreeingPeers, RaftReply, RaftRequest, RaftTransport, Refused, Role, RuntimeConfig,
};

use super::*;

pub(crate) const INCARNATION: u64 = 3;

pub(crate) fn key(stream: &str, shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

pub(crate) fn assigned(key: ShardKey, incarnation: u64) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: "broker-a".to_string(),
        replicas: vec!["broker-a".to_string()],
        generation: 1,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
    }
}

pub(crate) fn assignments(
    all: impl IntoIterator<Item = ShardAssignment>,
) -> HashMap<ShardKey, ShardAssignment> {
    let mut map = HashMap::default();
    for assignment in all {
        map.insert(assignment.key.clone(), assignment);
    }
    map
}

/// A broker that leaves durable streams to their engines, with the durable
/// stream `orders` at [`INCARNATION`] and the in-memory stream `scratch`.
pub(crate) async fn broker(root: &Path) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(root, config).expect("storage");
    let broker =
        Broker::new(felix_storage::EphemeralCache::new().into()).with_durable_storage(storage);
    broker.require_registered_streams();
    broker.leave_durable_streams_to_engines();
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                incarnation: INCARNATION,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("orders");
    broker
        .register_stream(
            "t1",
            "ns",
            "scratch",
            StreamMetadata {
                durable: false,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("scratch");
    Arc::new(broker)
}

pub(crate) fn shards(broker: &Arc<Broker>) -> Arc<RaftShards> {
    let runtime =
        GroupRuntime::start("broker-a", timers(), Arc::new(AgreeingPeers)).expect("runtime");
    RaftShards::new(Arc::clone(broker), runtime, "broker-a".to_string())
}

fn timers() -> RuntimeConfig {
    RuntimeConfig {
        election_min: Duration::from_millis(150),
        election_max: Duration::from_millis(300),
        heartbeat: Duration::from_millis(50),
        tick: Duration::from_millis(10),
        ..RuntimeConfig::default()
    }
}

pub(crate) async fn until(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// **A durable stream shard is taken from the hybrid and opened as a Raft
/// member on its incarnation's directory**, which the storage provider then
/// refuses to open: one shard never has two engines on its log.
#[tokio::test]
async fn a_durable_stream_shard_opens_as_a_raft_member() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut cache = key("orders", 0);
    cache.kind = ShardKind::Cache;
    let mut left = assignments([
        assigned(orders.clone(), INCARNATION),
        assigned(key("scratch", 0), 0),
        assigned(key("unknown", 0), 0),
        assigned(cache.clone(), 0),
    ]);

    shards.take(&mut left).await;

    // The in-memory stream and the cache are the hybrid's. The stream not
    // registered yet is nobody's this pass.
    let mut kept: Vec<_> = left.into_keys().collect();
    kept.sort();
    let mut expected = vec![key("scratch", 0), cache];
    expected.sort();
    assert_eq!(kept, expected);
    assert!(shards.engine_for(&key("unknown", 0)).is_none());
    assert!(shards.engine_for(&key("scratch", 0)).is_none());

    until("the member to lead its group of one", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.role == Role::Leader)
    })
    .await;
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;
    assert_eq!(shards.leading(&orders), shards.engine.open_term(&orders));
    assert!(shards.engine_for(&orders).is_some());

    let storage = broker.durable_storage().expect("storage");
    assert!(
        storage
            .open_stream_for("t1", "ns", "orders", 0, None, INCARNATION)
            .is_err(),
        "the provider opened a Raft member's directory"
    );
    assert!(
        broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .is_ok(),
        "the stream is not open on the member's log"
    );
    shards.runtime.shutdown().await;
}

/// **A deleted stream's Raft member is stopped and its log closed before
/// the directory is retired.** The provider never opened that log, so its
/// own close misses it, and the member would go on writing deleted files.
#[tokio::test]
async fn a_deleted_stream_closes_its_raft_log_before_the_directory_goes() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let log = shards.log_of(&orders).await.expect("open member");
    assert!(!log.is_closed());

    let removed = broker
        .remove_stream_from_catalog("t1", "ns", "orders", Some(INCARNATION))
        .await
        .expect("delete");

    assert!(removed);
    assert!(log.is_closed(), "the member's log outlived its directory");
    assert!(shards.runtime.status(&orders).is_none());
    assert!(shards.log_of(&orders).await.is_none());
    // The assignment is still in the feed: nothing is opened for a stream
    // that is gone.
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.runtime.status(&orders).is_none());
    shards.runtime.shutdown().await;
}

/// **A shard no longer assigned here is closed, and opens again from its
/// log when it comes back.**
#[tokio::test]
async fn a_shard_no_longer_assigned_is_closed_and_reopens() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let log = shards.log_of(&orders).await.expect("open member");
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;

    // Assigned to another broker only.
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    let mut left = assignments([elsewhere]);
    shards.take(&mut left).await;

    assert!(
        left.is_empty(),
        "a Raft shard held elsewhere went to the hybrid"
    );
    assert!(log.is_closed());
    assert!(shards.runtime.status(&orders).is_none());
    assert_eq!(shards.leading(&orders), None);

    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.runtime.status(&orders).is_some());
    assert!(
        broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .is_ok(),
        "the stream did not reopen on the member's log"
    );
    shards.runtime.shutdown().await;
}

pub(crate) fn engine(shards: &Arc<RaftShards>) -> crate::shards::routing::StreamEngine {
    let engine = crate::shards::routing::StreamEngine::default();
    engine.set_raft(Arc::clone(shards));
    engine
}

/// Routes that ask `shards` first and know every broker's address, with a
/// broker lease that was never granted: a Raft shard must not need it.
pub(crate) fn ingress(shards: &Arc<RaftShards>) -> Arc<crate::shards::routing::IngressRouter> {
    use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardRouter};
    let router = Arc::new(ShardRouter::new(
        shards.local.clone(),
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = BROKERS
        .iter()
        .map(|name| {
            let node = NodeRef {
                node_id: name.to_string(),
                advertise_addr: addr_of(name).expect("addr"),
                region: "us-west-2".to_string(),
                live: true,
            };
            (name.to_string(), node)
        })
        .collect();
    router.publish(RoutingTable::default(), &nodes);
    let ingress = crate::shards::routing::IngressRouter::new(router, Arc::default());
    ingress
        .fence()
        .bind_lease(Arc::new(crate::cluster::lease::LeaseState::new(
            Duration::from_secs(10),
        )));
    ingress.set_engine_shards(Arc::clone(shards) as Arc<dyn EngineShards>);
    Arc::new(ingress)
}

fn route(shards: &RaftShards, key: &ShardKey) -> Option<Dispatch> {
    EngineShards::dispatch(shards, key, &|_| None)
}

fn target(
    handle: felix_broker::StreamHandle,
    key: &ShardKey,
    term: u64,
) -> crate::serving::direct_publish::LocalTarget {
    crate::serving::direct_publish::LocalTarget {
        handle,
        shard: Some(key.clone()),
        generation: term,
        fenced: None,
    }
}

/// **A led term takes no write and is not routed here until the stream is
/// at the committed tail.** A batch the last term committed and this one
/// delivered after its own would reach a resuming reader twice.
#[tokio::test]
async fn a_led_term_is_served_only_after_the_stream_is_at_the_committed_tail() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let engine = engine(&shards);
    let orders = key("orders", 0);
    shards.stall_leading(true);
    let mut all = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut all).await;
    until("the member leads", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.role == Role::Leader)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let term = shards.runtime.status(&orders).expect("status").term;
    assert_eq!(shards.leading(&orders), None);
    assert_eq!(
        route(&shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let refused = engine
        .publish_on_raft(
            &broker,
            &handle,
            &orders,
            term,
            vec![bytes::Bytes::from_static(b"early")],
            Duration::from_secs(5),
        )
        .expect("a Raft shard");
    assert!(matches!(
        refused,
        Err(crate::shards::routing::RaftRefusal::NotLeading { .. })
    ));

    shards.stall_leading(false);
    until("the term is served", || {
        shards.leading(&orders) == Some(term)
    })
    .await;
    assert_eq!(
        route(&shards, &orders),
        Some(Dispatch::Local { generation: term })
    );
    EngineShards::shutdown(&*shards).await;
}

/// **A publish to a Raft shard is proposed to its group and answered at the
/// commit, and readers are bound by the commit index.** It never enters the
/// hybrid's fence or claims on the hybrid's log, which refuses the stream.
#[tokio::test]
async fn a_publish_on_a_raft_shard_commits_through_its_group() {
    use crate::serving::direct_publish::{DirectPublishError, publish_direct};
    use felix_replication::engine::Engine;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let engine = engine(&shards);
    let orders = key("orders", 0);
    let mut all = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut all).await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let payloads = [
        bytes::Bytes::from_static(b"one"),
        bytes::Bytes::from_static(b"two"),
    ];
    let timeout = Duration::from_secs(10);

    let first = publish_direct(
        &broker,
        None,
        &engine,
        target(handle.clone(), &orders, term),
        &payloads,
        None,
        timeout,
    )
    .await
    .expect("published")
    .expect("an offset");
    assert_eq!(
        engine.committed_bound(&orders),
        felix_broker::ReadBound::Committed(first + 2)
    );
    assert_eq!(engine.read_bound(None, &orders).ok(), Some(Some(first + 2)));

    // Routed at a term this broker does not serve: nothing is proposed.
    let stale = publish_direct(
        &broker,
        None,
        &engine,
        target(handle, &orders, term + 1),
        &payloads,
        None,
        timeout,
    )
    .await;
    assert!(matches!(stale, Err(DirectPublishError::NotWritten(_))));
    assert_eq!(
        engine.committed_bound(&orders),
        felix_broker::ReadBound::Committed(first + 2)
    );
    EngineShards::shutdown(&*shards).await;
}

/// **Stopping the broker stops every member and closes its log**, so the
/// storage flush that follows is not racing a group still writing.
#[tokio::test]
async fn shutdown_stops_the_members_and_closes_their_logs() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut all = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut all).await;
    assert!(shards.log_of(&orders).await.is_some());

    EngineShards::shutdown(&*shards).await;

    assert!(shards.log_of(&orders).await.is_none());
    assert_eq!(shards.runtime.status(&orders), None);
    assert_eq!(shards.leading(&orders), None);
    assert_eq!(route(&shards, &orders), None);
}

/// Carries Raft messages between the runtimes of one process. A broker
/// taken out of it is unreachable.
#[derive(Default)]
pub(crate) struct Loopback {
    runtimes: parking_lot::Mutex<HashMap<String, GroupRuntime>>,
}

#[async_trait::async_trait]
impl RaftTransport for Loopback {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let to = request.to().broker.to_string();
        let runtime = self.runtimes.lock().get(&to).cloned();
        match runtime {
            Some(runtime) => runtime.handle(request).await,
            None => Err(Refused::Unreachable(format!("broker {to} is down"))),
        }
    }
}

pub(crate) const BROKERS: [&str; 3] = ["broker-a", "broker-b", "broker-c"];

pub(crate) struct Member {
    pub(crate) name: &'static str,
    pub(crate) broker: Arc<Broker>,
    pub(crate) shards: Arc<RaftShards>,
    pub(crate) engine: Arc<crate::shards::routing::StreamEngine>,
    _root: tempfile::TempDir,
}

impl Member {
    /// A broker on `net` whose readers are bound the way startup binds them.
    async fn start(net: &Arc<Loopback>, name: &'static str) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        let broker = broker(root.path()).await;
        let runtime =
            GroupRuntime::start(name, timers(), Arc::clone(net) as Arc<dyn RaftTransport>)
                .expect("runtime");
        net.runtimes
            .lock()
            .insert(name.to_string(), runtime.clone());
        let shards = RaftShards::new(Arc::clone(&broker), runtime, name.to_string());
        let engine = Arc::new(engine(&shards));
        broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
            Arc::clone(&engine),
        )));
        Self {
            name,
            broker,
            shards,
            engine,
            _root: root,
        }
    }

    pub(crate) async fn publish(&self, term: u64, payload: &'static [u8]) -> u64 {
        use crate::serving::direct_publish::publish_direct;
        let orders = key("orders", 0);
        let handle = self
            .broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .expect("handle");
        publish_direct(
            &self.broker,
            None,
            &self.engine,
            target(handle, &orders, term),
            &[bytes::Bytes::from_static(payload)],
            None,
            Duration::from_secs(10),
        )
        .await
        .expect("published")
        .expect("an offset")
    }
}

/// `orders` on three brokers, placed with `broker-a` as leader.
fn replicated() -> ShardAssignment {
    let mut assignment = assigned(key("orders", 0), INCARNATION);
    assignment.replicas = BROKERS.iter().map(|name| name.to_string()).collect();
    assignment
}

/// Three brokers holding a member each, and the one whose term is served.
pub(crate) async fn trio() -> (Arc<Loopback>, Vec<Member>, usize) {
    let net = Arc::new(Loopback::default());
    let mut members = Vec::new();
    for name in BROKERS {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    let leader = served(&members).await;
    (net, members, leader)
}

async fn served(members: &[Member]) -> usize {
    let orders = key("orders", 0);
    let mut leader = None;
    until("a member serves its term", || {
        leader = members
            .iter()
            .position(|member| member.shards.leading(&orders).is_some());
        leader.is_some()
    })
    .await;
    leader.expect("a leader")
}

pub(crate) async fn stop(members: &[Member]) {
    for member in members {
        EngineShards::shutdown(&*member.shards).await;
    }
}

pub(crate) fn addr_of(name: &str) -> Option<SocketAddr> {
    let at = BROKERS.iter().position(|broker| *broker == name)?;
    Some(SocketAddr::from(([10, 0, 0, at as u8 + 1], 7001)))
}

/// **A follower sends a request to the leader its group elected, and a
/// broker with no member sends it to the placement's leader.** Neither
/// reads the hybrid's routes, which never saw the shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_off_the_leader_is_routed_to_it() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let follower = &members[(leader + 1) % 3];
    let to_leader = Dispatch::Forward {
        node_id: members[leader].name.to_string(),
        advertise_addr: addr_of(members[leader].name).expect("addr"),
        generation: term,
        incarnation: 0,
    };
    until("the follower knows its leader", || {
        EngineShards::dispatch(&*follower.shards, &orders, &addr_of) == Some(to_leader.clone())
    })
    .await;
    assert_eq!(
        route(&follower.shards, &orders),
        Some(Dispatch::Unavailable(Reason::OwnerUnavailable(
            members[leader].name.to_string()
        )))
    );

    let outside = Member::start(&net, "broker-d").await;
    assert_eq!(route(&outside.shards, &orders), None);
    let mut all = assignments([replicated()]);
    outside.shards.take(&mut all).await;
    assert!(
        all.is_empty(),
        "a Raft shard held elsewhere went to the hybrid"
    );
    assert!(outside.shards.engine_for(&orders).is_none());
    assert_eq!(
        EngineShards::dispatch(&*outside.shards, &orders, &addr_of),
        Some(Dispatch::Forward {
            node_id: "broker-a".to_string(),
            advertise_addr: addr_of("broker-a").expect("addr"),
            generation: 0,
            incarnation: 0,
        })
    );
    assert_eq!(
        route(&outside.shards, &orders),
        Some(Dispatch::Unavailable(Reason::OwnerUnavailable(
            "broker-a".to_string()
        )))
    );
    stop(&members).await;
    EngineShards::shutdown(&*outside.shards).await;
}

/// **A placed member that is not open yet answers not ready**, and never
/// falls through to the hybrid's routes.
#[tokio::test]
async fn a_shard_placed_here_with_no_member_open_is_not_ready() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .placed
        .lock()
        .insert(orders.clone(), "broker-a".to_string());

    assert_eq!(
        route(&shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    EngineShards::shutdown(&*shards).await;
}

pub(crate) async fn delivered(
    receiver: &mut felix_broker::SubscriptionReceiver,
) -> (u64, bytes::Bytes) {
    let got = tokio::time::timeout(Duration::from_secs(20), receiver.recv()).await;
    let envelope = got.expect("delivered in time").expect("subscription open");
    (
        envelope.base_offset().expect("an offset"),
        envelope.payloads()[0].clone(),
    )
}

/// **A Raft shard's subscribers on its leader get each record once it is
/// committed, at the offset the group gave it, and a follower's readers
/// are bound by its own member's commit index.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readers_of_a_raft_shard_get_what_committed() {
    use felix_replication::engine::Engine;

    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let follower = &members[(leader + 1) % 3];
    let (mut receiver, _guard) = {
        let subscription = members[leader].broker.subscribe("t1", "ns", "orders", 0);
        subscription.await.expect("subscribe").into_parts()
    };

    let first = members[leader].publish(term, b"one").await;
    let second = members[leader].publish(term, b"two").await;

    assert_eq!(second, first + 1);
    let one = (first, bytes::Bytes::from_static(b"one"));
    assert_eq!(delivered(&mut receiver).await, one);
    let two = (second, bytes::Bytes::from_static(b"two"));
    assert_eq!(delivered(&mut receiver).await, two);
    // What the follower's broker hands its readers, as startup wires it.
    let handle = follower
        .broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let committed = felix_broker::ReadBound::Committed(second + 1);
    until("the follower applies both", || {
        handle.read_bound() == committed
    })
    .await;
    assert_eq!(follower.engine.committed_bound(&orders), committed);
    assert_eq!(
        follower.engine.read_bound(None, &orders).ok(),
        Some(Some(second + 1))
    );
    stop(&members).await;
}

/// **When the leader stops, another member leads and takes publishes, and
/// nothing acknowledged before is lost**: the new leader's log holds every
/// acknowledged offset, so its first record lands above them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_leader_is_replaced_and_no_acked_record_is_lost() {
    use felix_replication::engine::Engine;

    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let mut acked = Vec::new();
    for payload in [b"a1", b"a2", b"a3"] {
        acked.push(members[leader].publish(term, payload).await);
    }
    let last = *acked.last().expect("acked");
    assert_eq!(acked, [last - 2, last - 1, last]);

    let old = members.remove(leader);
    net.runtimes.lock().remove(old.name);
    EngineShards::shutdown(&*old.shards).await;

    let next = served(&members).await;
    let next_term = members[next].shards.leading(&orders).expect("term");
    assert!(next_term > term);
    assert_eq!(
        route(&members[next].shards, &orders),
        Some(Dispatch::Local {
            generation: next_term
        })
    );
    let (mut receiver, _guard) = {
        let subscription = members[next]
            .broker
            .subscribe("t1", "ns", "orders", 0)
            .await;
        subscription.expect("subscribe").into_parts()
    };
    let after = members[next].publish(next_term, b"b1").await;
    assert!(after > last, "offset {after} reused an acknowledged one");
    assert_eq!(
        delivered(&mut receiver).await,
        (after, bytes::Bytes::from_static(b"b1"))
    );
    // The other survivor holds the acknowledged records and the new one.
    let other = &members[1 - next];
    until("the follower applies the new record", || {
        other.engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(after + 1)
    })
    .await;
    stop(&members).await;
}

/// **A leader that is deposed lets its subscribers go, and takes none
/// after.** Fanout comes from the leader's publishes, so a subscriber left
/// on a deposed broker would wait there and get nothing.
#[tokio::test]
async fn a_deposed_leader_lets_its_subscribers_go() {
    use crate::serving::quic::handlers::redirect::stopped_serving;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let ingress = ingress(&shards);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let (mut receiver, _guard) = {
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        subscription.expect("subscribe").into_parts()
    };
    // Served on the term alone: the broker's lease was never granted.
    assert!(stopped_serving(Some(&ingress), &orders).is_none());

    shards.leading.deposed(&orders, term);

    let ended = tokio::time::timeout(Duration::from_secs(20), receiver.recv()).await;
    assert!(
        ended.expect("ended in time").is_none(),
        "a record reached a deposed leader's subscriber"
    );
    assert!(
        stopped_serving(Some(&ingress), &orders).is_some(),
        "a subscriber registered after the deposal was kept"
    );
    EngineShards::shutdown(&*shards).await;
}

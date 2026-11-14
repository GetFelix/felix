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
    broker_of(root, 1).await
}

/// [`broker`], with `orders` asking for `replicas` brokers a shard.
pub(crate) async fn broker_of(root: &Path, replicas: u32) -> Arc<Broker> {
    broker_at(root, replicas, INCARNATION).await
}

/// [`broker_of`], with `orders` at `incarnation`.
async fn broker_at(root: &Path, replicas: u32, incarnation: u64) -> Arc<Broker> {
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
                incarnation,
                replication_factor: Some(replicas),
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

/// `broker`'s Raft shards, with its readers bound by them as startup does.
pub(crate) fn shards(broker: &Arc<Broker>) -> Arc<RaftShards> {
    let shards = unbound_shards(broker);
    broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
        Arc::new(engine(&shards)),
    )));
    shards
}

/// [`shards`] on a broker nothing tells what its groups committed.
fn unbound_shards(broker: &Arc<Broker>) -> Arc<RaftShards> {
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
        // Inside the shortest election timeout, as the runtime requires.
        lease: Duration::from_millis(100),
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

/// **A member placement drops keeps running.** Its group counts it as a
/// voter until a membership change says otherwise, and placement cannot make
/// one: closing it would take a vote from a group that still needs it.
#[tokio::test]
async fn a_member_placement_drops_keeps_serving() {
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
    let term = shards.leading(&orders);

    // Assigned to another broker only.
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    let mut left = assignments([elsewhere.clone()]);
    shards.take(&mut left).await;

    assert!(
        left.is_empty(),
        "a Raft shard held elsewhere went to the hybrid"
    );
    assert!(!log.is_closed(), "the dropped member's log was closed");
    assert_eq!(shards.leading(&orders), term);
    assert!(matches!(
        route(&shards, &orders),
        Some(Dispatch::Local { .. })
    ));
    let placed = BTreeSet::from(["broker-b"]);
    assert!(
        shards.off_placement(&orders, &placed),
        "a group whose voters placement does not name was not reported"
    );
    shards.runtime.shutdown().await;
}

/// **A member opened by an earlier run is opened again, whatever placement
/// says now.** A dropped voter that restarted would otherwise leave its
/// group one short for good.
#[tokio::test]
async fn a_dropped_member_reopens_after_a_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    {
        let broker = broker(root.path()).await;
        let shards = shards(&broker);
        // Never held here: nothing opens.
        shards.take(&mut assignments([elsewhere.clone()])).await;
        assert!(shards.runtime.status(&orders).is_none());
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
        until("the led term to be served", || {
            shards.leading(&orders).is_some()
        })
        .await;
        EngineShards::shutdown(&*shards).await;
    }
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    shards.take(&mut assignments([elsewhere])).await;
    until("the member to lead again", || {
        shards.leading(&orders).is_some()
    })
    .await;
    shards.runtime.shutdown().await;
}

/// **A failed read of whether a member was opened here, or a stream not yet
/// at the assignment's incarnation, is read again on the next pass.** Taken
/// for "never opened" for good, either would keep a dropped voter closed.
#[tokio::test]
async fn a_dropped_member_reopens_after_a_failed_read() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    {
        let broker = broker(root.path()).await;
        let shards = shards(&broker);
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
        until("the led term to be served", || {
            shards.leading(&orders).is_some()
        })
        .await;
        EngineShards::shutdown(&*shards).await;
    }
    let broker = broker(root.path()).await;
    let storage = broker.durable_storage().expect("storage");
    let (dir, _) = storage
        .claim_for_raft("t1", "ns", "orders", 0, INCARNATION)
        .expect("claimed");
    // A directory where the marker file should be fails the read.
    let marker = dir.join("engine");
    let raft = std::fs::read(&marker).expect("marker");
    std::fs::remove_file(&marker).expect("remove the marker");
    std::fs::create_dir(&marker).expect("unreadable marker");
    let shards = shards(&broker);
    shards.take(&mut assignments([elsewhere.clone()])).await;
    assert!(shards.runtime.status(&orders).is_none());
    std::fs::remove_dir(&marker).expect("readable again");
    std::fs::write(&marker, raft).expect("marker");

    storage.set_stream_incarnation("t1", "ns", "orders", Some(INCARNATION - 1));
    shards.take(&mut assignments([elsewhere.clone()])).await;
    assert!(shards.runtime.status(&orders).is_none());
    storage.set_stream_incarnation("t1", "ns", "orders", Some(INCARNATION));

    shards.take(&mut assignments([elsewhere])).await;
    until("the member to lead again", || {
        shards.leading(&orders).is_some()
    })
    .await;
    shards.runtime.shutdown().await;
}

/// **A led term is not served while the stream could not be moved to the
/// committed tail.** Here nothing tells the broker what the group committed,
/// so the move does nothing, and that is not taken for a stream at its tail.
#[tokio::test]
async fn a_term_is_not_served_while_nothing_says_what_committed() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = unbound_shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the member to lead its group of one", || {
        shards.engine.open_term(&orders).is_some()
    })
    .await;
    tokio::time::sleep(LEAD_RETRY * 10).await;
    assert_eq!(shards.leading(&orders), None);
    shards.runtime.shutdown().await;
}

/// How long a group that must not form is watched.
const NOT_FORMED_FOR: Duration = Duration::from_secs(1);

fn formed(member: &Member) -> bool {
    member
        .shards
        .runtime
        .status(&key("orders", 0))
        .is_some_and(|status| status.term > 0 || status.last_index.is_some())
}

/// **A set short of the stream's replica count forms no group, and the
/// full set forms one.** A group of the one broker placed first would
/// acknowledge on its own, and the brokers placed later would form the set
/// again on empty logs and overwrite what it acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_placement_forms_no_group() {
    let net = Arc::new(Loopback::default());
    let first = Member::start(&net, "broker-a").await;
    let orders = key("orders", 0);
    first
        .shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    tokio::time::sleep(NOT_FORMED_FOR).await;
    assert!(!formed(&first), "one broker of three formed a group");
    assert_eq!(first.shards.leading(&orders), None);
    assert_eq!(
        route(&first.shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );

    let mut members = vec![first];
    for name in ["broker-b", "broker-c"] {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    members[0]
        .shards
        .take(&mut assignments([replicated()]))
        .await;
    let leader = served(&members).await;
    let term = members[leader].shards.leading(&orders).expect("term");
    members[leader].publish(term, b"after the set filled").await;
    stop(&members).await;
}

/// **Only the lowest broker of the set forms the group, and only once every
/// other member has answered.** The others never form one, even when every
/// member answers them: two brokers forming at once could each be the lowest
/// of a different set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_forms_only_when_every_member_answers_the_lowest() {
    let net = Arc::new(Loopback::default());
    let orders = key("orders", 0);
    // The lowest broker still sees the short set, so it is open, answers,
    // and forms nothing itself.
    let lowest = Member::start(&net, "broker-a").await;
    lowest
        .shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let mut members = Vec::new();
    for name in ["broker-b", "broker-c"] {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    tokio::time::sleep(NOT_FORMED_FOR).await;
    assert!(
        !members.iter().any(formed) && !formed(&lowest),
        "a broker other than the lowest formed the group"
    );

    // The lowest broker with one member silent forms nothing either.
    let silent = net.runtimes.lock().remove("broker-c").expect("runtime");
    lowest.shards.take(&mut assignments([replicated()])).await;
    tokio::time::sleep(NOT_FORMED_FOR).await;
    assert!(!formed(&lowest), "formed with a member that never answered");
    assert!(!members.iter().any(formed));

    net.runtimes.lock().insert("broker-c".to_string(), silent);
    lowest.shards.take(&mut assignments([replicated()])).await;
    members.insert(0, lowest);
    served(&members).await;
    stop(&members).await;
}

/// **A broker placed on a shard whose group has formed does not form it
/// again.** It is the lowest broker of its set and its log is empty, but
/// the members it asks hold the group's entries. It serves nothing until a
/// membership change adds it, and the group keeps the members it formed with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_placed_on_a_formed_group_does_not_form_it_again() {
    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    members[leader].publish(term, b"acknowledged").await;

    // Placement replaces broker-a with a broker that sorts before the rest.
    let mut replaced = replicated();
    replaced.leader = "broker-0".to_string();
    replaced.replicas = vec![
        "broker-0".to_string(),
        "broker-b".to_string(),
        "broker-c".to_string(),
    ];
    let late = Member::start(&net, "broker-0").await;
    late.shards.take(&mut assignments([replaced.clone()])).await;
    for member in &members {
        member
            .shards
            .take(&mut assignments([replaced.clone()]))
            .await;
    }
    tokio::time::sleep(NOT_FORMED_FOR).await;

    assert!(!formed(&late), "a late broker formed the group again");
    assert_eq!(
        route(&late.shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    for member in &members {
        assert!(
            member.shards.runtime.status(&orders).is_some(),
            "{} closed its member",
            member.name
        );
    }
    let leader = served(&members).await;
    let term = members[leader].shards.leading(&orders).expect("term");
    members[leader].publish(term, b"still one group").await;
    members.push(late);
    stop(&members).await;
}

/// **A member of an earlier incarnation does not answer for a later one.**
/// The stream was recreated with a full set, and one broker's feed still
/// has the old incarnation, so its member there is blank. Taken as an
/// answer, the new group would append into the old incarnation's directory
/// on that broker and count it, and the broker drops that directory when
/// its feed catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_of_an_earlier_incarnation_does_not_answer_for_a_later_one() {
    let net = Arc::new(Loopback::default());
    let mut members = Vec::new();
    for name in ["broker-b", "broker-c"] {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    let recreated = INCARNATION + 1;
    let lowest = Member::start_at(&net, "broker-a", recreated).await;
    let mut assignment = replicated();
    assignment.incarnation = recreated;
    lowest.shards.take(&mut assignments([assignment])).await;
    tokio::time::sleep(NOT_FORMED_FOR).await;

    assert!(
        !formed(&lowest),
        "formed on blank answers from an earlier incarnation"
    );
    assert!(!members.iter().any(formed));
    members.push(lowest);
    stop(&members).await;
}

pub(crate) fn engine(shards: &Arc<RaftShards>) -> crate::shards::routing::StreamEngine {
    let engine = crate::shards::routing::StreamEngine::default();
    engine.set_raft(Arc::clone(shards));
    engine
}

/// Routes that ask `shards` first and know every broker's address, with a
/// broker lease that was never granted: a Raft shard must not need it.
pub(crate) fn ingress(shards: &Arc<RaftShards>) -> Arc<crate::shards::routing::IngressRouter> {
    routes(shards).1
}

/// [`ingress`], with the router under it.
pub(crate) fn routes(
    shards: &Arc<RaftShards>,
) -> (
    Arc<felix_router::ShardRouter>,
    Arc<crate::shards::routing::IngressRouter>,
) {
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
    let ingress = crate::shards::routing::IngressRouter::new(Arc::clone(&router), Arc::default());
    ingress
        .fence()
        .bind_lease(Arc::new(crate::cluster::lease::LeaseState::new(
            Duration::from_secs(10),
        )));
    ingress.set_engine_shards(Arc::clone(shards) as Arc<dyn EngineShards>);
    (router, Arc::new(ingress))
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

/// **A member's log directory is locked while the member is open**, so a
/// second broker started on the same data directory cannot recover and
/// write a log this one is writing. Closing the member lets it go.
#[tokio::test]
async fn an_open_member_holds_its_log_directory() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.log_of(&orders).await.is_some());
    let storage = broker.durable_storage().expect("storage");
    let (dir, label) = storage
        .claim_for_raft("t1", "ns", "orders", 0, INCARNATION)
        .expect("claimed");
    let config = LogConfig {
        raft_log: true,
        fsync_mode: FsyncMode::OnCommit,
        ..storage.config().clone()
    };

    let second = DiskLog::open(&dir, label.clone(), config.clone());
    assert!(
        matches!(second, Err(felix_storage::StorageError::Io(ref err))
            if err.kind() == std::io::ErrorKind::ResourceBusy),
        "a second open of an open member's directory was let in"
    );

    EngineShards::shutdown(&*shards).await;
    let reopened = DiskLog::open(&dir, label, config).expect("open after the member closed");
    reopened.close().await.expect("close");
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
        Self::start_at(net, name, INCARNATION).await
    }

    /// [`Self::start`], with `orders` at `incarnation`.
    async fn start_at(net: &Arc<Loopback>, name: &'static str, incarnation: u64) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        let broker = broker_at(root.path(), 3, incarnation).await;
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
    // The broker that forms the group last, so every member it asks is up.
    for name in BROKERS.iter().rev() {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    members.reverse();
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
    // Every acknowledged payload reads back from the new leader's log.
    let read = members[next]
        .broker
        .read_durable("t1", "ns", "orders", 0, acked[0], 1 << 20)
        .await
        .expect("read");
    let payloads: Vec<&[u8]> = read.iter().map(|record| &record.payload[..]).collect();
    assert_eq!(payloads, [&b"a1"[..], b"a2", b"a3", b"b1"]);
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

/// **A term led again straight after a deposal keeps its own subscribers.**
/// The deposed term's subscribers are ended by a task; the next term is not
/// served, so takes no subscriber, until that task is done.
#[tokio::test]
async fn a_term_led_again_keeps_its_subscribers() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let (mut old, _old_guard) = {
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        subscription.expect("subscribe").into_parts()
    };

    shards.stall_ends(true);
    shards.leading.deposed(&orders, term);
    shards.leading.opened(&orders, term + 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        shards.leading(&orders),
        None,
        "a term was served before the deposed term's subscribers were ended"
    );

    shards.stall_ends(false);
    until("the next term is served", || {
        shards.leading(&orders) == Some(term + 1)
    })
    .await;
    let ended = tokio::time::timeout(Duration::from_secs(20), old.recv()).await;
    assert!(ended.expect("ended in time").is_none());
    let (mut new, _new_guard) = {
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        subscription.expect("subscribe").into_parts()
    };
    let kept = tokio::time::timeout(Duration::from_millis(300), new.recv()).await;
    assert!(kept.is_err(), "the next term's subscriber was ended");
    EngineShards::shutdown(&*shards).await;
}

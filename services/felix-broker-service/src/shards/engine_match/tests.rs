use felix_broker::StreamMetadata;
use felix_replication::peer::PeerError;
use felix_wire::internal::InternalMessage;

use super::*;

/// Peers whose listeners offer what the map says. One not in it is down.
struct Fleet(parking_lot::Mutex<HashMap<String, PeerCapabilities>>);

impl PeerRequester for Fleet {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        unreachable!("the check only asks what a peer offers")
    }

    async fn capabilities(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> Result<PeerCapabilities, PeerError> {
        let offered = self.0.lock().get(node_id).copied();
        match offered {
            Some(offered) => Ok(offered),
            None => std::future::pending().await,
        }
    }
}

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn assigned(key: ShardKey, replicas: &[&str]) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: "broker-a".to_string(),
        replicas: replicas.iter().map(|id| id.to_string()).collect(),
        generation: 1,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation: 1,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
    }
}

/// `broker-a` on `raft` or the hybrid, with the durable stream `orders` and
/// the in-memory stream `scratch`, beside the peers in `fleet`.
async fn check(raft: bool, fleet: &[(&str, PeerCapabilities)]) -> Arc<EngineMatch<Fleet>> {
    // The directory outlives the test: nothing is opened in it.
    let root = tempfile::tempdir().expect("tempdir").keep();
    let storage = felix_broker::DurableStorage::open(&root, Default::default()).expect("storage");
    let broker =
        Broker::new(felix_storage::EphemeralCache::new().into()).with_durable_storage(storage);
    broker.leave_durable_streams_to_engines();
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    for (stream, durable) in [("orders", true), ("scratch", false)] {
        let metadata = StreamMetadata {
            durable,
            ..StreamMetadata::default()
        };
        broker
            .register_stream("t1", "ns", stream, metadata)
            .await
            .expect("stream");
    }
    let fleet = fleet
        .iter()
        .map(|(id, offered)| (id.to_string(), *offered))
        .collect();
    EngineMatch::new(
        Arc::new(broker),
        Arc::new(Fleet(parking_lot::Mutex::new(fleet))),
        Arc::new(|_: &str| Some(SocketAddr::from(([127, 0, 0, 1], 1)))),
        "broker-a".to_string(),
        raft,
        None,
    )
}

/// One pass over `orders` and `scratch`, both shared with `broker-b`.
/// Returns the streams left to this broker's engine.
async fn pass(check: &EngineMatch<Fleet>) -> Vec<String> {
    let mut assignments = HashMap::default();
    for stream in ["orders", "scratch"] {
        assignments.insert(
            key(stream),
            assigned(key(stream), &["broker-a", "broker-b"]),
        );
    }
    let taken = check.take(&mut assignments).await;
    // What was refused still counts toward its stream's width.
    assert_eq!(taken.len() + assignments.len(), 2);
    let mut left: Vec<String> = assignments.into_keys().map(|key| key.stream).collect();
    left.sort();
    left
}

fn refusal(check: &EngineMatch<Fleet>, stream: &str) -> Option<String> {
    match check.dispatch(&key(stream), &|_| None)? {
        Dispatch::Unavailable(Reason::OwnerUnavailable(why)) => Some(why),
        other => panic!("refused with {other:?}"),
    }
}

/// **A hybrid broker refuses a durable stream shard it shares with a Raft
/// broker.** Opened, it would acknowledge into a log of its own that the
/// Raft group never reads.
#[tokio::test]
async fn a_hybrid_broker_refuses_a_shard_shared_with_a_raft_broker() {
    let check = check(false, &[("broker-b", PeerCapabilities::RAFT)]).await;

    assert_eq!(pass(&check).await, ["scratch"]);
    let why = refusal(&check, "orders").expect("refused");
    assert!(why.contains("hybrid here, raft on broker-b"), "{why}");
    assert!(why.contains("FELIX_SHARD_ENGINE"), "{why}");
    assert_eq!(refusal(&check, "scratch"), None);
}

/// **A Raft broker refuses a durable stream shard it shares with a hybrid
/// broker**, one whose listener offers no Raft.
#[tokio::test]
async fn a_raft_broker_refuses_a_shard_shared_with_a_hybrid_broker() {
    let check = check(true, &[("broker-b", PeerCapabilities::NONE)]).await;

    assert_eq!(pass(&check).await, ["scratch"]);
    let why = refusal(&check, "orders").expect("refused");
    assert!(why.contains("raft here, hybrid on broker-b"), "{why}");
}

/// **Brokers on one engine refuse nothing, and neither does a broker whose
/// peer never answered**: a peer that is down says nothing about its engine.
#[tokio::test(start_paused = true)]
async fn a_fleet_on_one_engine_or_a_silent_peer_refuses_nothing() {
    for (raft, fleet) in [
        (true, vec![("broker-b", PeerCapabilities::RAFT)]),
        (false, vec![("broker-b", PeerCapabilities::NONE)]),
        (false, vec![]),
        (true, vec![]),
    ] {
        let check = check(raft, &fleet).await;
        assert_eq!(pass(&check).await, ["orders", "scratch"]);
        assert_eq!(refusal(&check, "orders"), None);
    }
}

/// **A refusal ends when the fleet agrees again**, as after the odd broker
/// restarts on the right engine.
#[tokio::test]
async fn a_refusal_ends_once_the_engines_match() {
    let check = check(false, &[("broker-b", PeerCapabilities::RAFT)]).await;
    assert_eq!(pass(&check).await, ["scratch"]);

    check
        .peers
        .0
        .lock()
        .insert("broker-b".to_string(), PeerCapabilities::NONE);
    // The pass after a restart asks again in the background.
    pass(&check).await;
    while check.engines.lock().get("broker-b") != Some(&false) {
        tokio::task::yield_now().await;
    }
    assert_eq!(pass(&check).await, ["orders", "scratch"]);
    assert_eq!(refusal(&check, "orders"), None);
}

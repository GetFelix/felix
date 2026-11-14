use felix_broker::StreamMetadata;
use felix_replication::peer::PeerError;
use felix_wire::internal::InternalMessage;

use super::*;

/// Peers whose listeners offer what the first map says. One not in it is
/// down. The second holds the peers with a live connection open, and the
/// count is how many dials were made.
struct Fleet(
    parking_lot::Mutex<HashMap<String, PeerCapabilities>>,
    parking_lot::Mutex<HashMap<String, PeerCapabilities>>,
    std::sync::atomic::AtomicUsize,
);

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
        self.2.fetch_add(1, Ordering::Relaxed);
        let offered = self.0.lock().get(node_id).copied();
        match offered {
            Some(offered) => Ok(offered),
            None => std::future::pending().await,
        }
    }

    fn connected_capabilities(&self, node_id: &str) -> Option<PeerCapabilities> {
        self.1.lock().get(node_id).copied()
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
        Arc::new(Fleet(
            parking_lot::Mutex::new(fleet),
            parking_lot::Mutex::default(),
            Default::default(),
        )),
        Arc::new(|_: &str| Some(SocketAddr::from(([127, 0, 0, 1], 1)))),
        "broker-a".to_string(),
        raft,
        None,
    )
}

/// One pass over `orders` and `scratch`, both shared with `broker-b`.
/// Returns the streams left to this broker's engine.
///
/// A pass asks without waiting, so [`settled`] comes between a change in the
/// fleet and the pass that shows it.
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

/// One pass to ask the peers, then wait for what `broker-b` runs to be
/// known as `raft`.
async fn settled(check: &EngineMatch<Fleet>, raft: bool) {
    pass(check).await;
    for _ in 0..1000 {
        if check.engines.lock().get("broker-b") == Some(&raft) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("broker-b never said which engine it runs");
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
    settled(&check, true).await;

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
    settled(&check, false).await;

    assert_eq!(pass(&check).await, ["scratch"]);
    let why = refusal(&check, "orders").expect("refused");
    assert!(why.contains("raft here, hybrid on broker-b"), "{why}");
}

/// **Brokers on one engine refuse nothing.**
#[tokio::test]
async fn a_fleet_on_one_engine_refuses_nothing() {
    for (raft, offered) in [
        (true, PeerCapabilities::RAFT),
        (false, PeerCapabilities::NONE),
    ] {
        let check = check(raft, &[("broker-b", offered)]).await;
        settled(&check, raft).await;
        assert_eq!(pass(&check).await, ["orders", "scratch"]);
        assert_eq!(refusal(&check, "orders"), None);
    }
}

/// **A peer not reached yet refuses nothing, and is asked on every pass
/// until it answers. The answer runs a pass, and that pass closes the
/// shard.** A peer that is down says nothing about its engine, and waiting
/// for it would keep a shard closed through an ordinary broker outage.
#[tokio::test(start_paused = true)]
async fn a_peer_reached_late_closes_the_shard_it_shares() {
    for raft in [false, true] {
        let check = check(raft, &[]).await;
        let wake = Arc::new(tokio::sync::Notify::new());
        check.wake_with(Arc::clone(&wake));
        assert_eq!(pass(&check).await, ["orders", "scratch"]);
        assert_eq!(refusal(&check, "orders"), None);

        // The peer comes up on the other engine. The ask already in flight
        // never returns, so the next one waits for it to give up.
        tokio::task::yield_now().await;
        let theirs = if raft {
            PeerCapabilities::NONE
        } else {
            PeerCapabilities::RAFT
        };
        check.peers.0.lock().insert("broker-b".to_string(), theirs);
        tokio::time::sleep(ASK_WITHIN + Duration::from_millis(1)).await;
        assert_eq!(pass(&check).await, ["orders", "scratch"]);

        tokio::time::timeout(Duration::from_secs(60), wake.notified())
            .await
            .expect("the answer runs a pass");
        assert_eq!(pass(&check).await, ["scratch"]);
        assert!(refusal(&check, "orders").is_some());
    }
}

/// **A refusal ends when the fleet agrees again**, as after the odd broker
/// restarts on the right engine.
#[tokio::test]
async fn a_refusal_ends_once_the_engines_match() {
    let check = check(false, &[("broker-b", PeerCapabilities::RAFT)]).await;
    settled(&check, true).await;
    assert_eq!(pass(&check).await, ["scratch"]);

    // It restarts on the hybrid, and replication connects to it again.
    check
        .peers
        .1
        .lock()
        .insert("broker-b".to_string(), PeerCapabilities::NONE);
    settled(&check, false).await;
    assert_eq!(pass(&check).await, ["orders", "scratch"]);
    assert_eq!(refusal(&check, "orders"), None);
}

/// **A peer whose engine is known is not dialled again.** One that just
/// restarted may still be at its old address in the catalog: a dial there
/// fails and starts a reconnect backoff that holds up a move onto it.
#[tokio::test]
async fn a_peer_whose_engine_is_known_is_not_dialled_again() {
    let check = check(false, &[("broker-b", PeerCapabilities::NONE)]).await;
    settled(&check, false).await;
    let dials = check.peers.2.load(Ordering::Relaxed);
    for _ in 0..3 {
        assert_eq!(pass(&check).await, ["orders", "scratch"]);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(
        check.peers.2.load(Ordering::Relaxed),
        dials,
        "broker-b was dialled again"
    );
}

//! Every user of the broker's engine holds the one the node built.
use std::time::Duration;

use super::*;
use crate::config::MembershipConfig;
use crate::serving::auth::BrokerAuth;
use crate::serving::kafka::BrokerCluster;
use crate::serving::mqtt::BrokerWrites;
use crate::serving::quic::ClusterContext;
use felix_broker::Broker;
use felix_storage::EphemeralCache;

/// **The client listeners, the Kafka and MQTT writes, forwarded writes, the
/// peer handler and the read bounds share one engine.** A second engine would
/// read different marks or never see the replica the peer listener sets.
#[tokio::test]
async fn every_component_holds_the_one_engine() {
    let config = BrokerConfig {
        membership: Some(MembershipConfig {
            node_id: "broker-a".to_string(),
            advertise_addr: "127.0.0.1:7001".to_string(),
            client_advertise_addr: None,
            kafka_advertise_addr: None,
            region: "local".to_string(),
            zone: None,
            region_bridges: Vec::new(),
            features: Default::default(),
        }),
        ..BrokerConfig::default()
    };
    let cluster = shard_state(&config);
    let (router, ingress, _, _) = cluster.as_ref().expect("shard state");
    let marks = Arc::new(QuorumMarks::new());
    let engine = Arc::new(shard_routing::StreamEngine::new(
        Some(Arc::clone(&marks)),
        Some(Arc::clone(ingress)),
    ));
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = Arc::new(BrokerAuth::new("http://localhost".to_string()));

    let (handler, _) = peer_handler(
        &config,
        router,
        &engine,
        &broker,
        &marks,
        &auth,
        felix_storage::StorageId::from_bytes([1; 16]),
        None,
        None,
    );
    assert!(Arc::ptr_eq(handler.engine(), &engine), "peer handler");
    assert!(
        Arc::ptr_eq(handler.forwarding().engine(), &engine),
        "forwarding handler"
    );

    let context = ClusterContext {
        engine: Arc::clone(&engine),
        ..ClusterContext::default()
    };
    assert!(Arc::ptr_eq(&context.engine, &engine), "cluster context");

    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Arc::clone(&engine),
        Duration::from_secs(1),
    );
    assert!(Arc::ptr_eq(writes.engine(), &engine), "mqtt writes");

    let kafka = BrokerCluster::new(
        auth,
        Arc::clone(&engine),
        None,
        "broker-a",
        "127.0.0.1:9092",
    )
    .expect("kafka cluster");
    assert!(Arc::ptr_eq(kafka.engine(), &engine), "kafka");

    // The same marks and shard routing as the ones the engine was built from.
    assert!(std::ptr::eq(engine.marks().expect("marks"), &*marks));
    assert!(Arc::ptr_eq(engine.serving().expect("ingress"), ingress));

    // The replica the peer handler was built with sits on this engine, so a
    // second one is refused.
    let replica = replication::ReplicaHandler::new(broker, Arc::clone(router));
    assert!(engine.set_replica(replica).is_err(), "replica");
}

/// **The restore gate runs only after the start's restore check has
/// answered, and its result reaches the members, the heartbeat and the
/// feed.** That check may rotate the storage id, which removes the mark log,
/// so the gate must not have it open before. Once the root passes, the
/// heartbeat reports it, the routing feed is woken and the member opens.
#[cfg(feature = "raft")]
#[tokio::test]
async fn the_raft_root_opens_after_the_restore_check_and_reaches_the_heartbeat() {
    use crate::cluster::membership::RootSequenceSlot;
    use crate::shards::raft::tests as raft;
    use crate::shards::routing::EngineShards;
    use crate::test_support::fake_link::FakeLink;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = raft::broker(root.path()).await;
    let (storage_id, _) =
        felix_storage::storage_identity::load_or_create(root.path(), || [7; 16]).expect("id");
    let members = raft::held_shards(&broker);
    let wake = Arc::new(tokio::sync::Notify::new());
    members.wake_with(Arc::clone(&wake));
    let shards = RaftShards {
        engine_shards: None,
        handler: None,
        members: Some(Arc::clone(&members)),
    };
    let (restore_checked, stop) = (CancellationToken::new(), CancellationToken::new());
    let slot = RootSequenceSlot::default();
    open_raft_root(
        &shards,
        RaftRoot {
            root: root.path().to_path_buf(),
            witness: None,
            restore_checked: restore_checked.clone(),
            // Answers 503: the control plane has no copy to give.
            link: Some(FakeLink::default()),
            node_id: "broker-a".to_string(),
            credential: Some(NodeCredential::new("token".to_string())),
            slot: slot.clone(),
            stop: stop.clone(),
        },
    );
    let orders = raft::key("orders", 0);
    let fed = || raft::assignments([raft::assigned(orders.clone(), raft::INCARNATION)]);

    tokio::time::sleep(Duration::from_millis(200)).await;
    members.take(&mut fed()).await;
    assert_eq!(slot.read(), None, "the gate ran before the restore check");
    assert!(members.engine_for(&orders).is_none());
    assert!(
        !root.path().join("raft-marks").exists(),
        "the mark log was opened before the restore check"
    );

    restore_checked.cancel();
    tokio::time::timeout(Duration::from_secs(10), wake.notified())
        .await
        .expect("the feed was not woken");
    assert_eq!(slot.read(), Some((storage_id, 0)));
    members.take(&mut fed()).await;
    assert!(members.engine_for(&orders).is_some());
    raft::until("the heartbeat to report the member's marks", || {
        slot.read().is_some_and(|(_, sequence)| sequence > 0)
    })
    .await;
    stop.cancel();
    members.shutdown().await;
}

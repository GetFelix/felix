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

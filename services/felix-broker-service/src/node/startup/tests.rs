//! What a start hands the quorum marks from its directory.
use super::*;

/// **A start hands the replica the ids a crash rotated away**, so a set
/// still naming one counts as this directory's own copy rather than a wipe.
/// Without it, a move whose other voter crashed this way stays fenced short.
#[tokio::test]
async fn a_start_hands_the_quorum_marks_its_rotated_ids() {
    let root = tempfile::tempdir().expect("tempdir");
    let old = felix_storage::StorageId::from_bytes([7; 16]);
    felix_storage::run_state::record_rotated_id(root.path(), old).expect("record");
    let durable = DurableStorageConfig {
        root: root.path().to_path_buf(),
        log: Default::default(),
        offload_dir: None,
    };
    let config = BrokerConfig::default();
    let stored = open_storage(&config, Some(durable), None).expect("open");
    let fleet = Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new()));
    let marks = Arc::new(QuorumMarks::new());
    let engine = Arc::new(crate::shards::routing::StreamEngine::new(
        Some(Arc::clone(&marks)),
        None,
    ));
    let _opened = wire_broker(&config, stored, &None, &fleet, &marks, &engine)
        .await
        .expect("wire");
    assert!(marks.rotated_own_copy(old), "the rotated id is this copy's");
    assert!(!marks.rotated_own_copy(felix_storage::StorageId::from_bytes([8; 16])));
}

/// **The committed-read bounds are built on the node's engine**, not on a
/// second one with its own marks and shard routing.
#[tokio::test]
async fn read_bounds_hold_the_nodes_engine() {
    let config = BrokerConfig {
        membership: Some(crate::config::MembershipConfig {
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
    let cluster = super::super::cluster::shard_state(&config);
    let (_, ingress, _, _) = cluster.as_ref().expect("shard state");
    let stored = open_storage(&config, None, None).expect("open");
    let fleet = Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new()));
    let marks = Arc::new(QuorumMarks::new());
    let engine = Arc::new(crate::shards::routing::StreamEngine::new(
        Some(Arc::clone(&marks)),
        Some(Arc::clone(ingress)),
    ));
    let opened = wire_broker(&config, stored, &cluster, &fleet, &marks, &engine)
        .await
        .expect("wire");
    assert_eq!(Arc::strong_count(&engine), 2, "the broker's read bounds");
    drop(opened);
}

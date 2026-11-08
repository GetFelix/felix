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
    let _opened = wire_broker(&config, stored, &None, &fleet, &marks)
        .await
        .expect("wire");
    assert!(marks.rotated_own_copy(old), "the rotated id is this copy's");
    assert!(!marks.rotated_own_copy(felix_storage::StorageId::from_bytes([8; 16])));
}

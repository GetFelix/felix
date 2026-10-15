//! Which logs a restore cuts, before any log is touched.

use super::*;

fn manifest() -> Manifest {
    serde_json::from_value(serde_json::json!({
        "format_version": 1,
        "name": "nightly",
        "taken_at_millis": 1,
        "metadata_version": 7,
        "shards": [
            {
                "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0,
                "kind": "stream", "leader": "broker-0", "generation": 3,
                "logs": { "records": 10, "group_cursors": 0, "group_dead_letters": 0 }
            },
            {
                "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 1,
                "kind": "stream", "leader": "broker-1", "generation": 2,
                "logs": { "records": 4 }
            },
            {
                "tenant_id": "t", "namespace": "ns", "name": "prices", "shard": 0,
                "kind": "cache", "leader": "broker-0", "generation": 1,
                "logs": { "records": 6, "counters": 2 }
            }
        ]
    }))
    .expect("manifest")
}

fn place(root: &Path, kind: LogKind, manifest: &Manifest, index: usize) {
    std::fs::create_dir_all(log_dir(root, kind, &manifest.shards[index])).expect("mkdir");
}

fn cuts(plan: &[Cut]) -> Vec<(String, LogKind, u64)> {
    plan.iter()
        .map(|cut| (cut.shard.label(), cut.kind, cut.offset))
        .collect()
}

#[test]
fn a_node_restores_the_shards_it_led_and_skips_empty_sidecars_it_never_had() {
    let root = tempfile::tempdir().expect("tempdir");
    let manifest = manifest();
    place(root.path(), LogKind::Stream, &manifest, 0);
    place(root.path(), LogKind::Cache, &manifest, 2);
    place(root.path(), LogKind::Counters, &manifest, 2);

    let plan = manifest.plan(Some("broker-0"), root.path()).expect("plan");
    assert_eq!(
        cuts(&plan),
        vec![
            ("t/ns/orders/0".to_string(), LogKind::Stream, 10),
            ("t/ns/prices/0".to_string(), LogKind::Cache, 6),
            ("t/ns/prices/0".to_string(), LogKind::Counters, 2),
        ]
    );
}

#[test]
fn a_log_the_point_says_has_records_must_be_in_the_copy() {
    let root = tempfile::tempdir().expect("tempdir");
    let manifest = manifest();
    place(root.path(), LogKind::Stream, &manifest, 0);
    // The cache's counters are missing from the copy.
    place(root.path(), LogKind::Cache, &manifest, 2);

    let err = manifest
        .plan(Some("broker-0"), root.path())
        .expect_err("incomplete copy");
    assert!(err.to_string().contains("Counters"), "{err}");
}

#[test]
fn without_a_node_only_the_shards_present_are_restored() {
    let root = tempfile::tempdir().expect("tempdir");
    let manifest = manifest();
    place(root.path(), LogKind::Stream, &manifest, 1);

    let plan = manifest.plan(None, root.path()).expect("plan");
    assert_eq!(
        cuts(&plan),
        vec![("t/ns/orders/1".to_string(), LogKind::Stream, 4)]
    );
}

#[test]
fn a_manifest_of_another_format_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("point.json");
    std::fs::write(&path, r#"{"format_version": 2, "name": "x", "shards": []}"#).expect("write");
    let err = Manifest::read(&path).expect_err("unknown format");
    assert!(err.to_string().contains("format version 2"), "{err}");
}

#[test]
fn a_stream_with_an_incarnation_is_found_in_that_incarnations_directory() {
    let root = tempfile::tempdir().expect("tempdir");
    let manifest: Manifest = serde_json::from_value(serde_json::json!({
        "format_version": 1,
        "name": "nightly",
        "shards": [{
            "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0,
            "kind": "stream", "leader": "broker-0", "generation": 3,
            "incarnation": 42, "logs": { "records": 10 }
        }]
    }))
    .expect("manifest");
    let key = felix_storage::log::ShardKey {
        tenant: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
    };
    // Only the deleted stream's directory: not the one the point describes.
    std::fs::create_dir_all(felix_storage::disk_log::layout::shard_dir(
        root.path(),
        &key,
    ))
    .expect("mkdir");
    assert!(manifest.plan(None, root.path()).expect("plan").is_empty());

    std::fs::create_dir_all(incarnation_dir(root.path(), &key, 42)).expect("mkdir");
    let plan = manifest.plan(None, root.path()).expect("plan");
    assert_eq!(
        cuts(&plan),
        vec![("t/ns/orders/0".to_string(), LogKind::Stream, 10)]
    );
}

fn durable_broker(root: &Path) -> (felix_broker::Broker, felix_broker::DurableStorage) {
    let storage =
        felix_broker::DurableStorage::open(root, felix_storage::log::LogConfig::default())
            .expect("open storage");
    let broker = felix_broker::Broker::new(Box::new(felix_storage::EphemeralCache::new()))
        .with_durable_storage(storage.clone());
    (broker, storage)
}

#[tokio::test]
async fn a_restore_gives_the_data_directory_a_new_storage_id() {
    // The restored logs are older than what peers last saw under the old id.
    let root = tempfile::tempdir().expect("tempdir");
    let before = crate::node::storage::storage_id(Some(root.path())).expect("create");
    let (broker, storage) = durable_broker(root.path());
    let restored = restore(
        &manifest(),
        Some("nobody"),
        root.path(),
        &broker,
        Some(storage),
    )
    .await
    .expect("restore");
    assert_eq!(restored, 0);
    let after = crate::node::storage::storage_id(Some(root.path())).expect("load");
    assert_ne!(after, before);
}

#[tokio::test]
async fn a_restore_that_fails_part_way_has_already_rotated_the_storage_id() {
    // The copy here is empty, so cutting it to 10 records fails after the
    // plan; some logs may already have been cut by then.
    let root = tempfile::tempdir().expect("tempdir");
    let before = crate::node::storage::storage_id(Some(root.path())).expect("create");
    let manifest = manifest();
    place(root.path(), LogKind::Stream, &manifest, 0);
    let (broker, storage) = durable_broker(root.path());
    restore(&manifest, None, root.path(), &broker, Some(storage))
        .await
        .expect_err("an empty copy is outside the point");
    let after = crate::node::storage::storage_id(Some(root.path())).expect("load");
    assert_ne!(after, before);
}

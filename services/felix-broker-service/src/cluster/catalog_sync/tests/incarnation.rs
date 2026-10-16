//! A stream's or cache's incarnation as the control plane sends it.

use super::super::wire::{Cache, CacheChange, CacheSnapshotResponse, Stream};

fn metadata(json: &str) -> felix_broker::StreamMetadata {
    serde_json::from_str::<Stream>(json)
        .expect("stream")
        .metadata()
        .expect("metadata")
}

#[test]
fn a_streams_incarnation_reaches_its_metadata() {
    let metadata = metadata(
        r#"{"tenant_id":"t","namespace":"ns","stream":"s","shards":1,"durable":true,
            "incarnation":4503599627370495}"#,
    );
    assert_eq!(metadata.incarnation, 4_503_599_627_370_495);
}

/// A control plane that predates incarnations, or a stream created before the
/// fleet finalized them, sends none.
#[test]
fn an_absent_incarnation_is_zero() {
    let metadata =
        metadata(r#"{"tenant_id":"t","namespace":"ns","stream":"s","shards":1,"durable":true}"#);
    assert_eq!(metadata.incarnation, 0);
}

#[test]
fn a_caches_incarnation_reaches_its_metadata() {
    let cache: Cache = serde_json::from_str(
        r#"{"tenant_id":"t","namespace":"ns","cache":"c","incarnation":4503599627370495}"#,
    )
    .expect("cache");
    assert_eq!(
        cache.metadata().expect("metadata").incarnation,
        4_503_599_627_370_495
    );
}

/// A control plane that predates cache incarnations sends none of the new
/// fields; each reads as what a cache was before them.
#[test]
fn omitted_cache_incarnation_fields_read_as_before() {
    let cache: Cache =
        serde_json::from_str(r#"{"tenant_id":"t","namespace":"ns","cache":"c"}"#).expect("cache");
    assert_eq!(cache.metadata().expect("metadata").incarnation, 0);

    let change: CacheChange = serde_json::from_str(
        r#"{"seq":3,"op":"deleted","key":{"tenant_id":"t","namespace":"ns","cache":"c"},
            "cache":null}"#,
    )
    .expect("change");
    assert_eq!(change.deleted_through, None);

    let snapshot: CacheSnapshotResponse =
        serde_json::from_str(r#"{"items":[],"next_seq":4}"#).expect("snapshot");
    assert!(snapshot.tombstones.is_empty());
}

#[test]
fn a_cache_delete_and_snapshot_name_what_they_delete() {
    let change: CacheChange = serde_json::from_str(
        r#"{"seq":3,"op":"deleted","key":{"tenant_id":"t","namespace":"ns","cache":"c"},
            "cache":{"tenant_id":"t","namespace":"ns","cache":"c","incarnation":5},
            "deleted_through":5}"#,
    )
    .expect("change");
    assert_eq!(change.deleted_through, Some(5));

    let snapshot: CacheSnapshotResponse = serde_json::from_str(
        r#"{"items":[],"next_seq":4,
            "tombstones":[{"tenant_id":"t","namespace":"ns","cache":"c","incarnation":5}]}"#,
    )
    .expect("snapshot");
    let tombstone = &snapshot.tombstones[0];
    assert_eq!(
        (
            tombstone.tenant_id.as_str(),
            tombstone.namespace.as_str(),
            tombstone.cache.as_str(),
            tombstone.incarnation
        ),
        ("t", "ns", "c", 5)
    );
}

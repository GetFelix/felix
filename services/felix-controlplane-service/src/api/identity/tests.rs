use axum::http::StatusCode;

use super::*;
use crate::api::types::{CacheSnapshotResponse, StreamSnapshotResponse};
use crate::store::identity::ClusterIdentity;

#[test]
fn the_route_answers_503_until_the_group_has_recorded_one() {
    let err = identity_response(GroupIdentity::Pending).expect_err("pending");
    assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn the_route_answers_the_recorded_identity() {
    let recorded = ClusterIdentity {
        group_id: "g-1".to_string(),
        shard_engine: "raft".to_string(),
        adopts_existing: false,
    };
    let answer = identity_response(GroupIdentity::Recorded(recorded)).expect("recorded");
    assert_eq!(
        serde_json::to_value(&answer).expect("json"),
        serde_json::json!({"group_id": "g-1", "shard_engine": "raft"})
    );
}

#[test]
fn memory_has_no_group_id_and_reports_the_configured_engine() {
    let answer = identity_response(GroupIdentity::NoGroup {
        shard_engine: "hybrid".to_string(),
    })
    .expect("memory");
    assert_eq!(
        serde_json::to_value(&answer).expect("json"),
        serde_json::json!({"shard_engine": "hybrid"})
    );
}

#[test]
fn snapshots_carry_group_id_only_when_recorded() {
    let without = serde_json::to_value(StreamSnapshotResponse {
        items: Vec::new(),
        next_seq: 3,
        tombstones: Vec::new(),
        group_id: None,
    })
    .expect("json");
    assert_eq!(without, serde_json::json!({"items": [], "next_seq": 3}));

    let with = serde_json::to_value(CacheSnapshotResponse {
        items: Vec::new(),
        next_seq: 3,
        tombstones: Vec::new(),
        group_id: Some("g-1".to_string()),
    })
    .expect("json");
    assert_eq!(
        with,
        serde_json::json!({"items": [], "next_seq": 3, "group_id": "g-1"})
    );
}

/// Change-feed pages carry the claim too, and an old peer sees the same
/// bytes while there is none.
#[test]
fn change_pages_carry_group_id_only_when_recorded() {
    use crate::api::types::{
        CacheChangesResponse, NamespaceChangesResponse, StreamChangesResponse,
        TenantChangesResponse,
    };
    let page = |group_id: Option<&str>| {
        let group_id = group_id.map(str::to_owned);
        [
            serde_json::to_value(StreamChangesResponse {
                items: Vec::new(),
                next_seq: 3,
                group_id: group_id.clone(),
            }),
            serde_json::to_value(CacheChangesResponse {
                items: Vec::new(),
                next_seq: 3,
                group_id: group_id.clone(),
            }),
            serde_json::to_value(TenantChangesResponse {
                items: Vec::new(),
                next_seq: 3,
                group_id: group_id.clone(),
            }),
            serde_json::to_value(NamespaceChangesResponse {
                items: Vec::new(),
                next_seq: 3,
                group_id,
            }),
        ]
    };
    for value in page(None) {
        assert_eq!(
            value.expect("json"),
            serde_json::json!({"items": [], "next_seq": 3})
        );
    }
    for value in page(Some("g-1")) {
        assert_eq!(
            value.expect("json"),
            serde_json::json!({"items": [], "next_seq": 3, "group_id": "g-1"})
        );
    }
}

#[test]
fn the_route_says_when_the_group_adopts_existing_data() {
    let recorded = ClusterIdentity {
        group_id: "g-1".to_string(),
        shard_engine: "hybrid".to_string(),
        adopts_existing: true,
    };
    let answer = identity_response(GroupIdentity::Recorded(recorded)).expect("recorded");
    assert_eq!(
        serde_json::to_value(&answer).expect("json"),
        serde_json::json!({"group_id": "g-1", "shard_engine": "hybrid", "adopts_existing": true})
    );
}

#[tokio::test]
async fn a_memory_store_answers_no_group() {
    let state = crate::test_support::app_state_ready(std::sync::Arc::new(
        crate::store::memory::InMemoryStore::new(crate::store::StoreConfig {
            changes_limit: 10,
            change_retention_max_rows: Some(10),
        })
        .with_shard_engine("raft"),
    ));
    assert_eq!(group_id(&state).await.expect("read"), None);
    assert_eq!(
        state.store.group_identity().await.expect("identity"),
        GroupIdentity::NoGroup {
            shard_engine: "raft".to_string()
        }
    );
}

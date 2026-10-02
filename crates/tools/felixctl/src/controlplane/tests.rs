use serde_json::json;

use super::*;
use crate::error::exit_for;

#[test]
fn a_page_yields_its_items_and_the_next_cursor() {
    let (items, next) =
        page_parts(json!({"items": [{"a": 1}], "next_cursor": "abc"})).expect("page");
    assert_eq!(items, [json!({"a": 1})]);
    assert_eq!(next.as_deref(), Some("abc"));

    let (_, next) = page_parts(json!({"items": []})).expect("page");
    assert_eq!(next, None);
    let (_, next) = page_parts(json!({"items": [], "next_cursor": null})).expect("page");
    assert_eq!(next, None);
}

#[test]
fn a_page_without_items_is_a_server_error() {
    let err = page_parts(json!({"tenants": []})).unwrap_err();
    assert_eq!(exit_for(&err), Exit::Server);
}

#[test]
fn rows_read_columns_by_pointer() {
    let node = json!({
        "node": {
            "node_id": "broker-1",
            "spec": {"client_addr": "10.0.0.1:5000", "region": "eu"},
            "status": {"lifecycle": "active"},
        },
        "placement": {"eligible": true, "heartbeat_age_ms": 12},
    });
    assert_eq!(
        rows(NODE_COLUMNS, &[node]),
        [["broker-1", "10.0.0.1:5000", "eu", "active", "true", "12"]]
    );
}

#[test]
fn a_missing_field_is_a_blank_cell() {
    assert_eq!(
        rows(TENANT_COLUMNS, &[json!({"tenant_id": "t1"})]),
        [["t1", ""]]
    );
}

#[test]
fn shard_rows_join_replicas() {
    let assignment = json!({
        "tenant_id": "t1", "namespace": "default", "stream": "orders", "kind": "stream",
        "shard": 0, "leader": "b1", "replicas": ["b1", "b2"], "generation": 3, "state": "active",
    });
    assert_eq!(
        rows(SHARD_COLUMNS, &[assignment]),
        [[
            "t1", "default", "orders", "stream", "0", "b1", "b1,b2", "3", "active"
        ]]
    );
}

#[test]
fn path_segments_are_percent_encoded() {
    assert_eq!(segment("orders"), "orders");
    assert_eq!(segment("a b/c"), "a%20b%2Fc");
    assert_eq!(segment("x.y-z_~"), "x.y-z_~");
}

#[test]
fn an_error_body_yields_its_message() {
    assert_eq!(
        error_message(r#"{"code":"forbidden","message":"missing tenant.manage"}"#),
        "missing tenant.manage"
    );
    assert_eq!(error_message("plain text\n"), "plain text");
}

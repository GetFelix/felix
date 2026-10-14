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

/// A control plane that answers each connection with the next of `answers`,
/// as `(status line, JSON body)`, and counts the requests it saw.
async fn scripted(
    answers: Vec<(&'static str, &'static str)>,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&seen);
    tokio::spawn(async move {
        for (status, body) in answers {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut request = vec![0u8; 8192];
            let _ = socket.read(&mut request).await;
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.expect("write");
            let _ = socket.shutdown().await;
        }
    });
    (format!("http://{addr}"), seen)
}

fn api(base: String) -> Api {
    Api {
        http: reqwest::Client::new(),
        base,
        token: None,
    }
}

const UNAVAILABLE: (&str, &str) = (
    "503 Service Unavailable",
    r#"{"code":"unavailable","message":"too many concurrent catalog writes; retry"}"#,
);

#[tokio::test]
async fn a_503_unavailable_is_retried() {
    let (base, seen) = scripted(vec![
        UNAVAILABLE,
        UNAVAILABLE,
        ("201 Created", r#"{"tenant_id":"t1"}"#),
    ])
    .await;
    let answer = api(base)
        .post("/v1/tenants", &[], Some(&json!({"tenant_id": "t1"})))
        .await
        .expect("the third attempt succeeds");
    assert!(answer.created);
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retries_of_a_503_unavailable_are_bounded() {
    let (base, seen) = scripted(vec![UNAVAILABLE; 10]).await;
    let err = api(base)
        .post("/v1/tenants", &[], Some(&json!({"tenant_id": "t1"})))
        .await
        .expect_err("the control plane stays unavailable");
    assert_eq!(exit_for(&err), Exit::Server);
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::SeqCst),
        UNAVAILABLE_RETRIES.len() + 1
    );
}

#[tokio::test]
async fn another_503_is_not_retried() {
    let (base, seen) = scripted(vec![
        (
            "503 Service Unavailable",
            r#"{"code":"upgrade_in_progress","message":"upgrade first"}"#,
        ),
        ("201 Created", "{}"),
    ])
    .await;
    api(base)
        .post("/v1/resources", &[], Some(&json!({})))
        .await
        .expect_err("not a retryable 503");
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
}

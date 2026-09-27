//! What an unauthenticated connection may do: small requests, and only for a
//! while.

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::ApiVersionsRequest;

use super::{Fixture, TENANT, TOKEN};
use crate::service::Settings;
use crate::service::connection::MAX_UNAUTHENTICATED_REQUEST_BYTES;

async fn secured_with_timeout(auth_timeout: Duration) -> Fixture {
    Fixture::build(
        Settings {
            cluster_id: "test-cluster".to_string(),
            auth_timeout,
            ..Settings::default()
        },
        Vec::new(),
    )
    .await
}

/// A length prefix with nothing behind it yet.
fn size_prefix(size: usize) -> BytesMut {
    let mut prefix = BytesMut::new();
    prefix.put_i32(size as i32);
    prefix
}

#[tokio::test]
async fn an_unauthenticated_connection_is_refused_a_large_request() {
    let fixture = Fixture::secured(&[]).await;
    let mut client = fixture.connect();
    client
        .write_raw(&size_prefix(MAX_UNAUTHENTICATED_REQUEST_BYTES + 1))
        .await;
    let closed = tokio::time::timeout(Duration::from_secs(5), client.read_frame())
        .await
        .expect("closed promptly");
    assert!(closed.is_none(), "the connection is closed, not answered");
}

#[tokio::test]
async fn an_authenticated_connection_may_send_a_large_request() {
    // Anonymous access counts as authenticated from the start.
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    client.write_raw(&size_prefix(1024 * 1024)).await;
    // The broker waits for the body rather than closing on the prefix.
    let waited = tokio::time::timeout(Duration::from_millis(300), client.read_frame()).await;
    assert!(waited.is_err(), "still open and reading the body");
}

#[tokio::test]
async fn an_unauthenticated_connection_is_closed_at_the_auth_deadline() {
    let fixture = secured_with_timeout(Duration::from_millis(200)).await;
    let mut client = fixture.connect();
    // Talking is not authenticating: ApiVersions is answered, and the
    // deadline still closes the connection.
    let versions = client.call(&ApiVersionsRequest::default(), 3).await;
    assert_eq!(versions.error_code, 0);
    let closed = tokio::time::timeout(Duration::from_secs(5), client.read_frame())
        .await
        .expect("closed by the deadline");
    assert!(closed.is_none());
}

#[tokio::test]
async fn a_body_trickled_past_the_deadline_is_cut_off() {
    let fixture = secured_with_timeout(Duration::from_millis(200)).await;
    let mut client = fixture.connect();
    // Declares a small body and never finishes it.
    client.write_raw(&size_prefix(64)).await;
    client.write_raw(&[0u8; 8]).await;
    let closed = tokio::time::timeout(Duration::from_secs(5), client.read_frame())
        .await
        .expect("closed by the deadline");
    assert!(closed.is_none());
}

#[tokio::test]
async fn an_authenticated_connection_outlives_the_deadline() {
    let fixture = secured_with_timeout(Duration::from_millis(200)).await;
    let mut client = fixture.connect();
    assert_eq!(client.login(TENANT, TOKEN).await, 0);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let versions = client.call(&ApiVersionsRequest::default(), 3).await;
    assert_eq!(versions.error_code, 0, "still served past the deadline");
}

use bytes::Bytes;
use kafka_protocol::messages::{MetadataRequest, SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::StrBytes;

use super::{Fixture, TENANT, TOKEN, topic_errors};
use crate::api::sasl::parse_plain;

const READ_ORDERS: &str = "stream.subscribe:stream:t1/orders/*";

#[test]
fn plain_messages_parse_per_rfc_4616() {
    assert_eq!(
        parse_plain(b"\0t1\0secret"),
        Some(("t1".to_string(), "secret".to_string()))
    );
    assert_eq!(
        parse_plain(b"t1\0t1\0secret"),
        Some(("t1".to_string(), "secret".to_string())),
        "authzid naming the same tenant"
    );
    assert_eq!(
        parse_plain(b"t2\0t1\0secret"),
        None,
        "acting as another tenant"
    );
    assert_eq!(parse_plain(b"\0t1"), None);
    assert_eq!(parse_plain(b"\0\0secret"), None);
    assert_eq!(parse_plain(b"\0t1\0"), None);
    assert_eq!(parse_plain(b"\0t1\0a\0b"), None);
}

#[tokio::test]
async fn a_valid_token_unlocks_the_streams_it_may_read() {
    let fixture = Fixture::secured(&[READ_ORDERS]).await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.stream("billing", "invoices", 1, true).await;
    let mut client = fixture.connect();

    // Before authenticating nothing is listed, and a named topic is refused.
    let all = client
        .call(&MetadataRequest::default().with_topics(None), 9)
        .await;
    assert!(all.topics.is_empty());
    let named = client
        .call(
            &MetadataRequest::default()
                .with_topics(Some(vec![super::metadata::topic("orders.created")])),
            9,
        )
        .await;
    assert_eq!(
        topic_errors(&named),
        vec![("orders.created".to_string(), 29)]
    );

    assert_eq!(client.login(TENANT, TOKEN).await, 0);
    let all = client
        .call(&MetadataRequest::default().with_topics(None), 9)
        .await;
    assert_eq!(
        topic_errors(&all),
        vec![("orders.created".to_string(), 0)],
        "billing is not readable with this token"
    );
    let named = client
        .call(
            &MetadataRequest::default()
                .with_topics(Some(vec![super::metadata::topic("billing.invoices")])),
            9,
        )
        .await;
    assert_eq!(
        topic_errors(&named),
        vec![("billing.invoices".to_string(), 29)]
    );
}

#[tokio::test]
async fn a_bad_token_is_refused_readably_and_the_connection_closes() {
    let fixture = Fixture::secured(&[READ_ORDERS]).await;
    let mut client = fixture.connect();
    assert_eq!(
        client.login(TENANT, "forged").await,
        58,
        "SASL_AUTHENTICATION_FAILED"
    );
    assert!(
        client.read_frame().await.is_none(),
        "closed after the refusal"
    );

    let mut client = fixture.connect();
    client
        .call(
            &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
            1,
        )
        .await;
    let refused = client
        .call(
            &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from_static(b"\0t1\0bad")),
            2,
        )
        .await;
    let message = refused.error_message.expect("a message").to_string();
    assert!(message.contains("token rejected"), "{message}");
    assert!(message.contains("tenant id as the username"), "{message}");
}

#[tokio::test]
async fn only_plain_is_offered_and_order_is_enforced() {
    let fixture = Fixture::secured(&[READ_ORDERS]).await;
    let mut client = fixture.connect();
    let handshake = client
        .call(
            &SaslHandshakeRequest::default()
                .with_mechanism(StrBytes::from_static_str("SCRAM-SHA-256")),
            1,
        )
        .await;
    assert_eq!(handshake.error_code, 33, "UNSUPPORTED_SASL_MECHANISM");
    assert_eq!(
        handshake
            .mechanisms
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>(),
        vec!["PLAIN"]
    );

    let mut client = fixture.connect();
    let early = client
        .call(
            &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from_static(b"\0t1\0x")),
            2,
        )
        .await;
    assert_eq!(early.error_code, 34, "ILLEGAL_SASL_STATE");
}

#[tokio::test]
async fn an_unauthenticated_fetch_is_refused_per_partition() {
    let fixture = Fixture::secured(&[READ_ORDERS]).await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["a"]).await;
    let mut client = fixture.connect();
    let response = client
        .call(
            &super::fetch::request(&[("orders.created", 0, 0)], 0, 1_000),
            11,
        )
        .await;
    assert_eq!(response.responses[0].partitions[0].error_code, 29);
}

#[tokio::test]
async fn a_v0_handshake_is_refused_because_its_exchange_is_unframed() {
    let fixture = Fixture::secured(&[READ_ORDERS]).await;
    let mut client = fixture.connect();
    let handshake = client
        .call(
            &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
            0,
        )
        .await;
    assert_eq!(handshake.error_code, 33);
}

/// The TLS client's certificate reaches the cluster with the token, so the
/// broker can refuse a token issued to someone else.
#[tokio::test]
async fn sasl_hands_the_client_certificate_to_the_cluster() {
    let fixture = Fixture::secured(&[READ_ORDERS]).await;
    let leaf = vec![0x30, 0x82, 0x01];
    let mut client = fixture.connect_as(
        vec![leaf.clone()],
        tokio_util::sync::CancellationToken::new(),
    );

    assert_eq!(client.login(TENANT, TOKEN).await, 0);
    let seen = fixture
        .cluster
        .peer_certs_seen
        .lock()
        .expect("lock")
        .clone();
    assert_eq!(
        seen,
        vec![vec![leaf]],
        "the certificate did not reach the cluster"
    );
}

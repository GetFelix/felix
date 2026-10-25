use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use felix_wire::Message;
use rustls::pki_types::{CertificateDer, pem::PemObject};

use felix_client::{CommitError, CommitOp};

use super::BareBroker;

/// A client refuses commit and state_get against a broker that advertised
/// nothing, and the broker is sent neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_sends_no_commit_to_a_broker_that_advertised_none() {
    let bare = BareBroker::start().expect("start the bare broker");
    let client = tokio::time::timeout(
        Duration::from_secs(10),
        felix_cluster::client::connect(bare.addr, "t1", "token"),
    )
    .await
    .expect("connect timed out")
    .expect("connect");

    let commit = tokio::time::timeout(
        Duration::from_secs(5),
        client.commit("t1", "default", b"k", vec![CommitOp::publish("s", "e")]),
    )
    .await
    .expect("a commit must be refused at the call, not left waiting")
    .expect_err("commit must be refused");
    assert!(
        matches!(
            commit.downcast_ref::<CommitError>(),
            Some(CommitError::Unsupported)
        ),
        "commit gave {commit:?}"
    );

    let state = tokio::time::timeout(
        Duration::from_secs(5),
        client.state_get("t1", "default", "s", b"k", "k"),
    )
    .await
    .expect("a state read must be refused at the call, not left waiting")
    .expect_err("state_get must be refused");
    assert!(
        matches!(
            state.downcast_ref::<CommitError>(),
            Some(CommitError::Unsupported)
        ),
        "state_get gave {state:?}"
    );

    assert_eq!(bare.requests.load(Ordering::SeqCst), 0);
}

/// The stub counts a raw commit and state read, so the zero asserted above
/// means something.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stub_counts_commit_and_state_get_frames() {
    let bare = BareBroker::start().expect("start the bare broker");
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(bare.cert_pem.as_bytes()).expect("parse cert"))
        .expect("trust cert");
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("client crypto"),
    ));
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).expect("endpoint");
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(bare.addr, "localhost")
        .expect("connect")
        .await
        .expect("handshake");
    let (mut send, _recv) = connection.open_bi().await.expect("open stream");

    let commit = Message::Commit {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "s".into(),
        entity_key: Bytes::from_static(b"k"),
        event: Bytes::from_static(b"e"),
        changes: Vec::new(),
        request_id: 1,
        expected_offset: None,
        shard: None,
    };
    let state_get = Message::StateGet {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "s".into(),
        entity_key: Bytes::from_static(b"k"),
        key: "k".into(),
        request_id: 2,
        shard: None,
    };
    for (message, expected) in [(commit, 1), (state_get, 2)] {
        let frame = message.encode().expect("encode");
        send.write_all(&frame.encode()).await.expect("send");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while bare.requests.load(Ordering::SeqCst) < expected {
            assert!(
                tokio::time::Instant::now() < deadline,
                "count never reached {expected}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(bare.requests.load(Ordering::SeqCst), expected);
    }
}

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use felix_wire::Message;
use rustls::pki_types::{CertificateDer, pem::PemObject};

use super::BareBroker;

/// A client refuses a broker that advertised none of the baseline, so no
/// commit, state read or trim can reach it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_refuses_a_broker_below_the_baseline() {
    let bare = BareBroker::start().expect("start the bare broker");
    let refused = tokio::time::timeout(
        Duration::from_secs(10),
        felix_cluster::client::connect(bare.addr, "t1", "token"),
    )
    .await
    .expect("connect timed out");
    assert!(
        refused.is_err(),
        "a broker below the baseline must be refused at connect"
    );
    assert_eq!(bare.requests.load(Ordering::SeqCst), 0);
}

/// The stub counts a raw commit, state read and trim, so the zero asserted above
/// means something.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stub_counts_commit_state_get_and_trim_frames() {
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
    let trim = Message::TrimBefore {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "s".into(),
        shard: 0,
        before: 1,
        expected_offset: 1,
        headers: None,
        request_id: 3,
    };
    for (message, expected) in [(commit, 1), (state_get, 2), (trim, 3)] {
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

/// What the other languages' client tests rely on: the broker serves a client
/// that presents the fixture's certificate and refuses one that does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mtls_stub_serves_only_a_client_with_its_certificate() {
    let (broker, client) = BareBroker::start_requiring_client_cert().expect("start the broker");
    let dir = tempfile::tempdir().expect("tempdir");
    let (cert_file, key_file, ca_file) = (
        dir.path().join("client.pem"),
        dir.path().join("client.key"),
        dir.path().join("ca.pem"),
    );
    std::fs::write(&cert_file, &client.cert_pem).expect("write cert");
    std::fs::write(&key_file, &client.key_pem).expect("write key");
    std::fs::write(&ca_file, &broker.cert_pem).expect("write ca");
    let roots = Arc::new(felix_client::root_store_from_pem_file(&ca_file).expect("roots"));

    let connect = |identity: Option<felix_client::ClientIdentity>| {
        let roots = Arc::clone(&roots);
        async move {
            let quinn = match identity {
                Some(identity) => {
                    felix_client::quic_client_config_with_identity(Some(roots), identity, false)
                }
                None => felix_client::quic_client_config(Some(roots), false),
            }
            .expect("quic config");
            let mut config = felix_client::ClientConfig::optimized_defaults(quinn);
            config.auth_tenant_id = Some("t1".to_string());
            config.auth_token = Some("token".to_string());
            tokio::time::timeout(
                Duration::from_secs(10),
                felix_client::Connection::connect(broker.addr, "localhost", config),
            )
            .await
            .expect("connect timed out")
        }
    };

    let identity =
        felix_client::ClientIdentity::from_pem_files(&cert_file, &key_file).expect("identity");
    connect(Some(identity))
        .await
        .expect("a client with the certificate connects");
    connect(None)
        .await
        .err()
        .expect("a client without a certificate is refused");
}

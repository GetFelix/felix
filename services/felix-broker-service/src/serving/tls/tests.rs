//! A broker serving a configured certificate: clients verify it against the
//! CA that issued it, rotation reaches the next handshake, and a client CA
//! turns client certificates on.

use std::path::{Path, PathBuf};
use std::time::Duration;

use felix_transport::{QuicClient, QuicConnection, QuicServer, TransportConfig};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use super::*;
use crate::config::ClientTlsFiles;

/// A CA on disk, and the certificates it issued.
struct Pki {
    dir: tempfile::TempDir,
    ca: rcgen::Issuer<'static, rcgen::KeyPair>,
    ca_path: PathBuf,
}

impl Pki {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let key = rcgen::KeyPair::generate().expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let cert = params.self_signed(&key).expect("ca cert");
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, cert.pem()).expect("write ca");
        Self {
            ca: rcgen::Issuer::new(params, key),
            dir,
            ca_path,
        }
    }

    /// Issue a certificate for `name` to `label.pem` / `label.key.pem`.
    fn issue(&self, label: &str, name: &str) -> (PathBuf, PathBuf) {
        let params = rcgen::CertificateParams::new(vec![name.to_string()]).expect("params");
        self.issue_with(label, params)
    }

    fn issue_with(&self, label: &str, params: rcgen::CertificateParams) -> (PathBuf, PathBuf) {
        let key = rcgen::KeyPair::generate().expect("key");
        let cert = params.signed_by(&key, &self.ca).expect("sign");
        let cert_path = self.dir.path().join(format!("{label}.pem"));
        let key_path = self.dir.path().join(format!("{label}.key.pem"));
        std::fs::write(&cert_path, cert.pem()).expect("write cert");
        std::fs::write(&key_path, key.serialize_pem()).expect("write key");
        (cert_path, key_path)
    }

    fn roots(&self) -> Arc<rustls::RootCertStore> {
        Arc::new(
            felix_common::tls::load_roots("CA", &self.ca_path.display().to_string())
                .expect("roots"),
        )
    }
}

fn files(cert: &Path, key: &Path, client_ca: Option<&Path>) -> ClientTlsConfig {
    ClientTlsConfig {
        files: Some(ClientTlsFiles {
            cert_path: cert.display().to_string(),
            key_path: key.display().to_string(),
            client_ca_path: client_ca.map(|path| path.display().to_string()),
        }),
        ..ClientTlsConfig::default()
    }
}

fn serve(tls: &ClientTls) -> Arc<QuicServer> {
    Arc::new(
        QuicServer::bind(
            "127.0.0.1:0".parse().expect("addr"),
            tls.quic_server_config().expect("server config"),
            TransportConfig::default(),
        )
        .expect("bind"),
    )
}

/// A client trusting `roots` only, optionally presenting a certificate.
fn client(roots: Arc<rustls::RootCertStore>, identity: Option<(&Path, &Path)>) -> QuicClient {
    client_offering(roots, identity, &[])
}

/// [`client`], offering `alpn`.
fn client_offering(
    roots: Arc<rustls::RootCertStore>,
    identity: Option<(&Path, &Path)>,
    alpn: &[&[u8]],
) -> QuicClient {
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("versions")
        .with_root_certificates(roots);
    let config = match identity {
        Some((cert, key)) => builder
            .with_client_auth_cert(
                CertificateDer::pem_file_iter(cert)
                    .expect("cert")
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .expect("cert"),
                rustls::pki_types::PrivateKeyDer::from_pem_file(key).expect("key"),
            )
            .expect("client cert"),
        None => builder.with_no_client_auth(),
    };
    let mut config = config;
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(config).expect("crypto");
    QuicClient::bind(
        "127.0.0.1:0".parse().expect("addr"),
        quinn::ClientConfig::new(Arc::new(crypto)),
        TransportConfig::default(),
    )
    .expect("client")
}

/// Connect, and return what each side made of it.
async fn handshake(
    server: &Arc<QuicServer>,
    client: &QuicClient,
    name: &str,
) -> (Result<QuicConnection>, Result<QuicConnection>) {
    let accepting = {
        let server = Arc::clone(server);
        tokio::spawn(async move { server.accept().await })
    };
    let addr = server.local_addr().expect("addr");
    let dialled = tokio::time::timeout(Duration::from_secs(5), client.connect(addr, name))
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("dial timed out")));
    let accepted = tokio::time::timeout(Duration::from_secs(5), accepting)
        .await
        .map(|joined| joined.expect("accept task"))
        .unwrap_or_else(|_| Err(anyhow::anyhow!("accept timed out")));
    (dialled, accepted)
}

#[tokio::test]
async fn a_client_verifies_the_configured_certificate_with_its_ca() {
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let tls = ClientTls::from_config(&files(&cert, &key, None)).expect("load");
    let server = serve(&tls);

    let trusting = client(pki.roots(), None);
    let (dialled, accepted) = handshake(&server, &trusting, "broker.felix.test").await;
    dialled.expect("a client trusting the CA was refused");
    accepted.expect("the broker refused a client trusting its CA");

    // The name is checked, not just the chain.
    let (dialled, _) = handshake(&server, &trusting, "someone-else.test").await;
    assert!(
        dialled.is_err(),
        "a certificate for another name was accepted"
    );

    // And a client that does not trust the CA refuses the broker.
    let other = Pki::new();
    let (dialled, _) = handshake(&server, &client(other.roots(), None), "broker.felix.test").await;
    assert!(
        dialled.is_err(),
        "a client trusting another CA accepted the broker"
    );
}

#[tokio::test]
async fn a_rotated_certificate_is_served_on_the_next_handshake() {
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let tls = ClientTls::from_config(&files(&cert, &key, None)).expect("load");
    let server = serve(&tls);
    let trusting = client(pki.roots(), None);

    let (first, _) = handshake(&server, &trusting, "broker.felix.test").await;
    let first = first.expect("first").peer_certificates().expect("certs");

    // Same paths, new material, then the check the reload timer makes.
    pki.issue("broker", "broker.felix.test");
    let reloading = tls.reloading.as_ref().expect("a configured cert reloads");
    assert!(
        reloading.reload().expect("reload"),
        "the rotation was missed"
    );

    // A fresh client: the first one may resume its session, and a resumed
    // session reports the certificate from the original handshake.
    let fresh = client(pki.roots(), None);
    let (second, _) = handshake(&server, &fresh, "broker.felix.test").await;
    let second = second.expect("second").peer_certificates().expect("certs");
    assert_ne!(first, second, "the rotated certificate was not served");
}

#[tokio::test]
async fn a_client_ca_refuses_clients_without_a_certificate_from_it() {
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let tls = ClientTls::from_config(&files(&cert, &key, Some(&pki.ca_path)))
        .expect("load with client ca");
    let server = serve(&tls);

    let (_, accepted) = handshake(&server, &client(pki.roots(), None), "broker.felix.test").await;
    assert!(
        accepted.is_err(),
        "a client with no certificate was accepted"
    );

    let outsider = Pki::new();
    let (outsider_cert, outsider_key) = outsider.issue("app", "app.felix.test");
    let (_, accepted) = handshake(
        &server,
        &client(pki.roots(), Some((&outsider_cert, &outsider_key))),
        "broker.felix.test",
    )
    .await;
    assert!(
        accepted.is_err(),
        "a certificate from another CA was accepted"
    );

    let (app_cert, app_key) = pki.issue("app", "app.felix.test");
    let (dialled, accepted) = handshake(
        &server,
        &client(pki.roots(), Some((&app_cert, &app_key))),
        "broker.felix.test",
    )
    .await;
    dialled.expect("dial with a client certificate");
    accepted.expect("a client certificate from the CA was refused");

    // The Kafka listener shares the requirement.
    tls.kafka_server_config().expect("kafka config");
}

#[test]
fn unreadable_material_fails_at_load_naming_the_variable() {
    let pki = Pki::new();
    let (cert, _) = pki.issue("broker", "broker.felix.test");
    let missing = pki.dir.path().join("missing.key.pem");
    let err = ClientTls::from_config(&files(&cert, &missing, None))
        .err()
        .expect("a missing key loaded");
    assert!(format!("{err:#}").contains("FELIX_TLS_KEY"), "{err:#}");
}

#[test]
fn the_generated_certificate_is_exported_when_asked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let export = dir.path().join("nested").join("broker-cert.pem");
    let config = ClientTlsConfig {
        cert_export: Some(export.display().to_string()),
        ..ClientTlsConfig::default()
    };
    let tls = ClientTls::from_config(&config).expect("generate");
    assert!(
        tls.reloading.is_none(),
        "nothing to reload for a generated cert"
    );
    let pem = std::fs::read_to_string(&export).expect("exported");
    assert!(pem.contains("BEGIN CERTIFICATE"));
}

#[tokio::test]
async fn the_client_alpn_is_selected_and_a_client_without_one_is_still_served() {
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let tls = ClientTls::from_config(&files(&cert, &key, None)).expect("load");
    let server = serve(&tls);

    let current = client_offering(pki.roots(), None, &[felix_wire::CLIENT_ALPN]);
    let (dialled, accepted) = handshake(&server, &current, "broker.felix.test").await;
    let accepted = accepted.expect("a client offering felix/1 was refused");
    assert_eq!(
        dialled.expect("dial").negotiated_protocol().as_deref(),
        Some(felix_wire::CLIENT_ALPN)
    );
    assert_eq!(
        accepted.negotiated_protocol().as_deref(),
        Some(felix_wire::CLIENT_ALPN)
    );

    // The compatibility path: clients from before ALPN offer none.
    let (dialled, accepted) =
        handshake(&server, &client(pki.roots(), None), "broker.felix.test").await;
    accepted.expect("a client offering no ALPN was refused");
    assert_eq!(dialled.expect("dial").negotiated_protocol(), None);

    // A broker's internal protocol has nothing in common with this port.
    let peer = client_offering(pki.roots(), None, &[b"felix-internal/1"]);
    let (dialled, _) = handshake(&server, &peer, "broker.felix.test").await;
    assert!(
        dialled.is_err(),
        "a peer's ALPN was accepted on the client port"
    );
}

#[tokio::test]
async fn requiring_alpn_refuses_a_client_that_offers_none() {
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let config = ClientTlsConfig {
        require_alpn: true,
        ..files(&cert, &key, None)
    };
    let server = serve(&ClientTls::from_config(&config).expect("load"));

    let (dialled, _) = handshake(&server, &client(pki.roots(), None), "broker.felix.test").await;
    assert!(dialled.is_err(), "a client without ALPN was served");
    let current = client_offering(pki.roots(), None, &[felix_wire::CLIENT_ALPN]);
    let (dialled, _) = handshake(&server, &current, "broker.felix.test").await;
    dialled.expect("a client offering felix/1 was refused");
}

/// The shipped client's own TLS config, not a hand-built one: with the offer
/// on it gets past `FELIX_TLS_REQUIRE_ALPN`, and with it off (the default,
/// for older brokers) it does not.
#[tokio::test]
async fn the_sdk_config_offering_alpn_is_served_when_alpn_is_required() {
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let config = ClientTlsConfig {
        require_alpn: true,
        ..files(&cert, &key, None)
    };
    let server = serve(&ClientTls::from_config(&config).expect("load"));
    let sdk = |offer_alpn| {
        QuicClient::bind(
            "127.0.0.1:0".parse().expect("addr"),
            felix_client::quic_client_config(Some(pki.roots()), offer_alpn).expect("sdk config"),
            TransportConfig::default(),
        )
        .expect("client")
    };

    let (dialled, accepted) = handshake(&server, &sdk(true), "broker.felix.test").await;
    accepted.expect("the SDK offering felix/1 was refused");
    assert_eq!(
        dialled.expect("dial").negotiated_protocol().as_deref(),
        Some(felix_wire::CLIENT_ALPN)
    );

    let (dialled, _) = handshake(&server, &sdk(false), "broker.felix.test").await;
    assert!(dialled.is_err(), "the SDK without ALPN was served");
}

#[tokio::test]
async fn a_token_subject_binds_to_the_client_certificate() {
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let tls = ClientTls::from_config(&files(&cert, &key, Some(&pki.ca_path))).expect("load");
    let server = serve(&tls);
    let (app_cert, app_key) = pki.issue("app", "orders.apps.felix.test");
    let (_, accepted) = handshake(
        &server,
        &client(pki.roots(), Some((&app_cert, &app_key))),
        "broker.felix.test",
    )
    .await;
    let certs = accepted
        .expect("accepted")
        .peer_certificates()
        .expect("the client's chain");

    check_subject_binding(Some(&certs), "t1", "orders.apps.felix.test").expect("its own name");
    let other = check_subject_binding(Some(&certs), "t1", "billing.apps.felix.test")
        .expect_err("another service's token");
    assert!(other.contains("not issued to"), "{other}");
    let unnameable =
        check_subject_binding(Some(&certs), "t1", "user@example.com").expect_err("not a name");
    assert!(unnameable.contains("not issued to"), "{unnameable}");
    // No certificate, nothing to bind to: a listener without a client CA.
    check_subject_binding(None, "t1", "anyone").expect("no certificate");
}

/// Control-plane principal ids are 64 hex characters: one label longer than
/// DNS allows, so no DNS SAN can carry them. They bind through the
/// `felix:principal:` URI SAN instead.
#[tokio::test]
async fn a_principal_id_binds_through_the_uri_san() {
    let principal = "3f".repeat(32);
    let pki = Pki::new();
    let (cert, key) = pki.issue("broker", "broker.felix.test");
    let tls = ClientTls::from_config(&files(&cert, &key, Some(&pki.ca_path))).expect("load");
    let server = serve(&tls);

    let mut params =
        rcgen::CertificateParams::new(vec!["orders.apps.felix.test".to_string()]).expect("params");
    params.subject_alt_names.push(rcgen::SanType::URI(
        format!("{PRINCIPAL_URI_PREFIX}{principal}")
            .try_into()
            .expect("uri"),
    ));
    let (app_cert, app_key) = pki.issue_with("principal", params);
    let certs = |cert: &Path, key: &Path| {
        let client = client(pki.roots(), Some((cert, key)));
        let server = Arc::clone(&server);
        async move {
            let (_, accepted) = handshake(&server, &client, "broker.felix.test").await;
            accepted
                .expect("accepted")
                .peer_certificates()
                .expect("the client's chain")
        }
    };

    let with_uri = certs(&app_cert, &app_key).await;
    check_subject_binding(Some(&with_uri), "t1", &principal).expect("the principal in its URI SAN");
    check_subject_binding(Some(&with_uri), "t1", "orders.apps.felix.test")
        .expect("its DNS name still binds");
    check_subject_binding(Some(&with_uri), "t1", &"4e".repeat(32))
        .expect_err("another principal's token");
    check_subject_binding(Some(&with_uri), "t1", "").expect_err("the bare prefix is no principal");

    let (plain_cert, plain_key) = pki.issue("plain", "orders.apps.felix.test");
    let without_uri = certs(&plain_cert, &plain_key).await;
    let refused = check_subject_binding(Some(&without_uri), "t1", &principal)
        .expect_err("no URI SAN, so no principal to bind to");
    assert!(refused.contains("not issued to"), "{refused}");
}

/// A gateway acting for many users presents each user's token over its own
/// certificate. A `felix:delegate:<tenant>` URI SAN lets that certificate
/// carry any subject of that tenant, and only that tenant.
#[test]
fn a_delegate_uri_binds_every_subject_of_its_tenant_only() {
    let key = rcgen::KeyPair::generate().expect("key");
    let mut params =
        rcgen::CertificateParams::new(vec!["gateway.apps.felix.test".to_string()]).expect("params");
    params.subject_alt_names.push(rcgen::SanType::URI(
        format!("{DELEGATE_URI_PREFIX}t1").try_into().expect("uri"),
    ));
    let gateway = vec![params.self_signed(&key).expect("cert").der().clone()];

    let alice = "3f".repeat(32);
    check_subject_binding(Some(&gateway), "t1", &alice).expect("a user of its tenant");
    check_subject_binding(Some(&gateway), "t1", "bob@example.com")
        .expect("any subject of its tenant");
    check_subject_binding(Some(&gateway), "t1", "gateway.apps.felix.test")
        .expect("its own name still binds");
    let other_tenant = check_subject_binding(Some(&gateway), "t2", &alice)
        .expect_err("a user of a tenant it does not act for");
    assert!(other_tenant.contains("not issued to"), "{other_tenant}");
    check_subject_binding(Some(&gateway), "", &alice).expect_err("no tenant is no delegation");

    // Without the URI, the same certificate refuses every other subject.
    let plain = vec![
        rcgen::CertificateParams::new(vec!["gateway.apps.felix.test".to_string()])
            .expect("params")
            .self_signed(&key)
            .expect("cert")
            .der()
            .clone(),
    ];
    let refused =
        check_subject_binding(Some(&plain), "t1", &alice).expect_err("a user's token, no URI");
    assert!(refused.contains("not issued to"), "{refused}");
}

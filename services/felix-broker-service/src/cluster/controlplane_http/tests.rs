//! An `https://` control plane with a private CA is reachable only once that
//! CA is trusted.

use std::sync::Arc;

use serial_test::serial;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

/// A one-route HTTPS server on loopback whose certificate, for `localhost`,
/// is issued by a fresh CA. Returns its URL and the CA's path.
async fn https_server(dir: &std::path::Path) -> (String, String) {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = params.self_signed(&ca_key).expect("ca");
    let ca = rcgen::Issuer::new(params, ca_key);
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, ca_cert.pem()).expect("write ca");

    let key = rcgen::KeyPair::generate().expect("key");
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("params")
        .signed_by(&key, &ca)
        .expect("sign");
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("versions")
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    )
    .expect("server config");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                let mut buf = [0u8; 1024];
                let _ = tls.read(&mut buf).await;
                let _ = tls
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                    )
                    .await;
                let _ = tls.shutdown().await;
            });
        }
    });
    (
        format!("https://localhost:{port}/"),
        ca_path.display().to_string(),
    )
}

#[tokio::test]
#[serial]
async fn a_private_control_plane_ca_is_trusted_only_once_configured() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (url, ca) = https_server(dir.path()).await;

    trust_ca(None).expect("system store only");
    assert!(
        builder()
            .build()
            .expect("client")
            .get(&url)
            .send()
            .await
            .is_err(),
        "a certificate from an untrusted CA was accepted"
    );

    trust_ca(Some(&ca)).expect("trust the CA");
    let response = builder()
        .build()
        .expect("client")
        .get(&url)
        .send()
        .await
        .expect("request");
    assert_eq!(response.text().await.expect("body"), "ok");

    trust_ca(None).expect("reset");
}

#[test]
#[serial]
fn an_unusable_bundle_fails_startup() {
    let dir = tempfile::tempdir().expect("tempdir");
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "").expect("write");
    let err = trust_ca(Some(&empty.display().to_string())).expect_err("empty bundle");
    assert!(
        format!("{err:#}").contains("FELIX_CONTROLPLANE_CA"),
        "{err:#}"
    );
    let err = trust_ca(Some("/nonexistent/ca.pem")).expect_err("missing bundle");
    assert!(
        format!("{err:#}").contains("FELIX_CONTROLPLANE_CA"),
        "{err:#}"
    );
}

/// A CA written to `dir/<name>.pem`, for issuing certificates.
fn ca(dir: &std::path::Path, name: &str) -> (String, rcgen::Issuer<'static, rcgen::KeyPair>) {
    let key = rcgen::KeyPair::generate().expect("ca key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let cert = params.self_signed(&key).expect("ca");
    let path = dir.join(format!("{name}.pem"));
    std::fs::write(&path, cert.pem()).expect("write ca");
    (path.display().to_string(), rcgen::Issuer::new(params, key))
}

/// A certificate for `san` from `issuer`, written to `dir/<name>.pem` and
/// `dir/<name>.key`.
fn issue(
    dir: &std::path::Path,
    name: &str,
    issuer: &rcgen::Issuer<'static, rcgen::KeyPair>,
    san: rcgen::SanType,
) -> ControlPlaneClientCert {
    let key = rcgen::KeyPair::generate().expect("key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
    params.subject_alt_names = vec![san];
    let cert = params.signed_by(&key, issuer).expect("sign");
    let cert_path = dir.join(format!("{name}.pem"));
    let key_path = dir.join(format!("{name}.key"));
    std::fs::write(&cert_path, cert.pem()).expect("write cert");
    std::fs::write(&key_path, key.serialize_pem()).expect("write key");
    ControlPlaneClientCert {
        cert_path: cert_path.display().to_string(),
        key_path: key_path.display().to_string(),
    }
}

/// The control plane's own API listener, requiring client certificates
/// from `client_ca`. Returns its URL and the CA its certificate chains to.
async fn mtls_control_plane(dir: &std::path::Path, client_ca: &str) -> (String, String) {
    use felix_controlplane_service::server::tls;

    let (server_ca, issuer) = ca(dir, "server-ca");
    let served = issue(
        dir,
        "server",
        &issuer,
        rcgen::SanType::DnsName("localhost".try_into().expect("dns")),
    );
    let config = felix_controlplane_service::config::ApiTlsConfig {
        cert_path: served.cert_path,
        key_path: served.key_path,
        client_ca_path: Some(client_ca.to_string()),
        bind_subject: false,
    };
    let server_config = tls::api_server_config(
        tls::load_api_identity(&config).expect("identity"),
        tls::load_api_client_roots(&config).expect("client roots"),
    )
    .expect("server config");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let router = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
    tokio::spawn(tls::serve_tls(
        "api",
        listener,
        router,
        server_config,
        tokio_util::sync::CancellationToken::new(),
    ));
    (format!("https://localhost:{port}/"), server_ca)
}

/// Against a control plane that requires client certificates, a broker
/// with `FELIX_CONTROLPLANE_CLIENT_CERT` and `_KEY` gets through, and one
/// without them is refused in the handshake.
#[tokio::test]
#[serial]
async fn the_configured_client_certificate_is_presented_to_the_control_plane() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (client_ca, clients) = ca(dir.path(), "client-ca");
    let (url, server_ca) = mtls_control_plane(dir.path(), &client_ca).await;
    let broker = issue(
        dir.path(),
        "broker",
        &clients,
        rcgen::SanType::URI("felix:principal:broker-1".try_into().expect("uri")),
    );
    trust_ca(Some(&server_ca)).expect("trust the control plane");
    let get = || async { builder().build().expect("client").get(&url).send().await };

    present_cert(None).expect("no certificate");
    assert!(
        get().await.is_err(),
        "served a broker without a certificate"
    );

    present_cert(Some(&broker)).expect("present the certificate");
    let response = get().await.expect("served with the certificate");
    assert_eq!(response.text().await.expect("body"), "ok");

    present_cert(None).expect("reset");
    trust_ca(None).expect("reset");
}

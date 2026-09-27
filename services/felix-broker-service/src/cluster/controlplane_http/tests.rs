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
        rustls::crypto::ring::default_provider(),
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

    trust_ca(None).expect("public roots only");
    assert!(
        client().get(&url).send().await.is_err(),
        "a certificate from an untrusted CA was accepted"
    );

    trust_ca(Some(&ca)).expect("trust the CA");
    let response = client().get(&url).send().await.expect("request");
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

//! The bootstrap listener follows rotation of its certificate and client CA,
//! and the API and bootstrap listeners keep their certificate when a rotated
//! one would not verify or drops a name.
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};

use super::{bootstrap_tls, load_api_identity};
use crate::config::{ApiTlsConfig, BootstrapTlsConfig};

struct Ca {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    pem: String,
    der: CertificateDer<'static>,
}

fn ca() -> Ca {
    let key = rcgen::KeyPair::generate().expect("ca key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let cert = params.self_signed(&key).expect("ca cert");
    Ca {
        pem: cert.pem(),
        der: cert.der().clone(),
        issuer: rcgen::Issuer::new(params, key),
    }
}

/// A leaf from `ca` for `name` ending at the start of `year`: (cert PEM,
/// key PEM, DER).
fn leaf(ca: &Ca, name: &str, year: i32) -> (String, String, CertificateDer<'static>) {
    let key = rcgen::KeyPair::generate().expect("key");
    let mut params = rcgen::CertificateParams::new(vec![name.to_string()]).expect("params");
    params.not_after = rcgen::date_time_ymd(year, 1, 1);
    let cert = params.signed_by(&key, &ca.issuer).expect("cert");
    (cert.pem(), key.serialize_pem(), cert.der().clone())
}

fn write_server(dir: &Path, ca: &Ca, year: i32) -> CertificateDer<'static> {
    write_server_for(dir, ca, "localhost", year)
}

fn write_server_for(dir: &Path, ca: &Ca, name: &str, year: i32) -> CertificateDer<'static> {
    let (cert, key, der) = leaf(ca, name, year);
    std::fs::write(dir.join("server.pem"), cert).expect("write cert");
    std::fs::write(dir.join("server.key"), key).expect("write key");
    der
}

fn config(dir: &Path) -> BootstrapTlsConfig {
    let path = |name: &str| dir.join(name).display().to_string();
    BootstrapTlsConfig {
        cert_path: path("server.pem"),
        key_path: path("server.key"),
        client_ca_path: path("client-ca.pem"),
    }
}

/// One handshake against `server` presenting a leaf from `client_ca`. The
/// server's leaf when both sides accept it.
async fn handshake(
    server: Arc<rustls::ServerConfig>,
    server_ca: &Ca,
    client_ca: &Ca,
) -> Result<CertificateDer<'static>, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let accept = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept");
        tokio_rustls::TlsAcceptor::from(server)
            .accept(tcp)
            .await
            .map(drop)
            .map_err(|err| err.to_string())
    });

    let mut roots = rustls::RootCertStore::empty();
    roots.add(server_ca.der.clone()).expect("root");
    let (cert, key, _) = leaf(client_ca, "bootstrap-client", 2099);
    let chain = rustls::pki_types::CertificateDer::pem_slice_iter(cert.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("chain");
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(key.as_bytes()).expect("key");
    let client = rustls::ClientConfig::builder_with_provider(super::provider())
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .expect("client config");
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let stream = tokio_rustls::TlsConnector::from(Arc::new(client))
        .connect(ServerName::try_from("localhost").expect("name"), tcp)
        .await
        .map_err(|err| err.to_string())?;
    accept.await.expect("accept task")?;
    Ok(stream.get_ref().1.peer_certificates().expect("server cert")[0].clone())
}

#[tokio::test]
async fn the_bootstrap_certificate_rotates_and_an_expired_one_is_refused() {
    let ca = ca();
    let dir = tempfile::tempdir().expect("dir");
    let first = write_server(dir.path(), &ca, 2099);
    std::fs::write(dir.path().join("client-ca.pem"), &ca.pem).expect("write ca");
    let (server, identity, _) = bootstrap_tls(&config(dir.path())).expect("tls");
    assert_eq!(handshake(Arc::clone(&server), &ca, &ca).await, Ok(first));

    let second = write_server(dir.path(), &ca, 2099);
    assert!(identity.reload().expect("rotated"));
    assert_eq!(
        handshake(Arc::clone(&server), &ca, &ca).await,
        Ok(second.clone())
    );

    write_server(dir.path(), &ca, 2020);
    identity
        .reload()
        .expect_err("an expired certificate is refused");
    assert_eq!(handshake(server, &ca, &ca).await, Ok(second));
}

#[tokio::test]
async fn the_bootstrap_client_ca_rotates() {
    let (old_ca, new_ca) = (ca(), ca());
    let dir = tempfile::tempdir().expect("dir");
    write_server(dir.path(), &old_ca, 2099);
    std::fs::write(dir.path().join("client-ca.pem"), &old_ca.pem).expect("write ca");
    let (server, _, client_ca) = bootstrap_tls(&config(dir.path())).expect("tls");
    assert!(
        handshake(Arc::clone(&server), &old_ca, &new_ca)
            .await
            .is_err(),
        "a client from the new CA before the bundle has it"
    );

    std::fs::write(
        dir.path().join("client-ca.pem"),
        format!("{}{}", old_ca.pem, new_ca.pem),
    )
    .expect("write ca");
    assert!(client_ca.reload().expect("rotated"));
    handshake(Arc::clone(&server), &old_ca, &new_ca)
        .await
        .expect("a client from the new CA");
    handshake(server, &old_ca, &old_ca)
        .await
        .expect("a client from the old CA");
}

/// Brokers dial the API and bootstrap listeners by a configured name, so a
/// rotated certificate that drops it is refused on both.
#[test]
fn the_api_and_bootstrap_listeners_refuse_a_rotated_certificate_for_another_name() {
    let ca = ca();
    let dir = tempfile::tempdir().expect("dir");
    write_server(dir.path(), &ca, 2099);
    std::fs::write(dir.path().join("client-ca.pem"), &ca.pem).expect("write ca");
    let (_, bootstrap, _) = bootstrap_tls(&config(dir.path())).expect("bootstrap tls");
    let api = load_api_identity(&ApiTlsConfig {
        cert_path: config(dir.path()).cert_path,
        key_path: config(dir.path()).key_path,
        client_ca_path: None,
        bind_subject: false,
    })
    .expect("api tls");

    write_server_for(dir.path(), &ca, "elsewhere.test", 2099);
    for (what, identity) in [("api", api), ("bootstrap", bootstrap)] {
        let err = identity
            .reload()
            .expect_err("a certificate dropping a name was taken");
        assert!(err.to_string().contains("localhost"), "{what}: {err}");
    }
}

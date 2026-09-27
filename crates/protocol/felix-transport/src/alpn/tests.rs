use std::sync::Arc;

use anyhow::{Context, Result};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};

use super::*;
use crate::{QuicClient, QuicServer, TransportConfig};

const PROTOCOL: &[u8] = b"felix/1";

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn identity() -> Result<(rustls::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let der = cert.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let tls = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(vec![der.clone()], key.into())?;
    Ok((tls, der))
}

fn client_tls(cert: CertificateDer<'static>, alpn: &[&[u8]]) -> Result<rustls::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let mut tls = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    Ok(tls)
}

/// The ClientHello a rustls client sends, without its TLS record header.
fn client_hello(alpn: &[&[u8]]) -> Result<Vec<u8>> {
    let (_, cert) = identity()?;
    let tls = client_tls(cert, alpn)?;
    let mut connection =
        rustls::ClientConnection::new(Arc::new(tls), ServerName::try_from("localhost")?)?;
    let mut record = Vec::new();
    connection.write_tls(&mut record)?;
    Ok(record[5..].to_vec())
}

#[test]
fn a_hello_with_alpn_is_recognised_whole_and_not_before() -> Result<()> {
    let hello = client_hello(&[PROTOCOL])?;
    assert_eq!(client_hello_offers_alpn(&hello), Hello::Offers(true));
    for cut in [0, 3, 4, hello.len() / 2, hello.len() - 1] {
        assert_eq!(
            client_hello_offers_alpn(&hello[..cut]),
            Hello::Incomplete,
            "a truncated hello must read as incomplete"
        );
    }
    Ok(())
}

#[test]
fn a_hello_without_alpn_offers_none() -> Result<()> {
    assert_eq!(
        client_hello_offers_alpn(&client_hello(&[])?),
        Hello::Offers(false)
    );
    Ok(())
}

#[test]
fn something_that_is_not_a_hello_is_left_to_rustls() {
    assert_eq!(
        client_hello_offers_alpn(&[2, 0, 0, 1, 0]),
        Hello::Unreadable
    );
    // A hello whose declared lengths overrun it.
    assert_eq!(
        client_hello_offers_alpn(&[1, 0, 0, 3, 3, 3, 3]),
        Hello::Unreadable
    );
}

/// Connect with `alpn` offered and return what was negotiated.
async fn negotiate(alpn: &[&[u8]]) -> Result<Option<Vec<u8>>> {
    let (tls, cert) = identity()?;
    let server_config = alpn_optional_server_config(tls, &[PROTOCOL])?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let accepted = tokio::spawn(async move { server.accept().await });
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls(cert, alpn)?)
        .context("client crypto")?;
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        quinn::ClientConfig::new(Arc::new(crypto)),
        transport,
    )?;
    let connection = client.connect(addr, "localhost").await;
    let accepted = accepted.await?;
    let connection = connection?;
    accepted?;
    Ok(connection.negotiated_protocol())
}

#[tokio::test]
async fn a_client_offering_the_protocol_gets_it() -> Result<()> {
    assert_eq!(
        negotiate(&[b"h3", PROTOCOL]).await?,
        Some(PROTOCOL.to_vec())
    );
    Ok(())
}

/// The compatibility path: a client built before ALPN offers nothing and is
/// still served.
#[tokio::test]
async fn a_client_offering_nothing_is_still_accepted() -> Result<()> {
    assert_eq!(negotiate(&[]).await?, None);
    Ok(())
}

#[tokio::test]
async fn a_client_offering_only_another_protocol_is_refused() -> Result<()> {
    let refused = negotiate(&[b"felix-internal/1"]).await;
    assert!(
        refused.is_err(),
        "a client offering only another protocol was served"
    );
    Ok(())
}

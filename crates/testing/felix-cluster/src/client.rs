//! Connecting a client to a broker in the harness.
//!
//! **Certificates are not verified.** Brokers generate a self-signed
//! certificate at startup and publish it nowhere, so a client in a separate
//! process has nothing to trust it against — the same gap the peer transport
//! documents, and the same answer until a real PKI exists. That is acceptable
//! here because every endpoint is loopback and started by this harness.
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use felix_client::{Client, ClientConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

/// Connect to a broker's client-facing port as `tenant_id`.
pub async fn connect(addr: SocketAddr, tenant_id: &str, token: &str) -> Result<Client> {
    let config = client_config(tenant_id, token)?;
    Client::connect(addr, "localhost", config)
        .await
        .with_context(|| format!("connect to broker at {addr}"))
}

/// Connect to whichever of `addrs` answers, the way an application with a seed
/// list would.
///
/// The server name is `"localhost"` for every broker, which is only sound
/// because this harness installs `AcceptAnyBroker` and never validates a
/// certificate — the name reaches the wire as SNI and is not checked against
/// anything. A deployment that verified certificates would need a name per
/// broker, or a certificate naming them all. `Client::connect_any` takes the
/// name as a parameter for exactly that reason; the hardcoding is the harness's,
/// not the client's.
pub async fn connect_any(addrs: &[SocketAddr], tenant_id: &str, token: &str) -> Result<Client> {
    let config = client_config(tenant_id, token)?;
    Client::connect_any(addrs, "localhost", config)
        .await
        .with_context(|| format!("connect to any of {addrs:?}"))
}

/// A client that reconnects to another broker when the one it is using fails.
pub async fn connect_cluster(
    addrs: &[SocketAddr],
    tenant_id: &str,
    token: &str,
) -> Result<felix_client::ClusterClient> {
    let config = client_config(tenant_id, token)?;
    felix_client::ClusterClient::connect(addrs, "localhost", config)
        .await
        .with_context(|| format!("connect a cluster client to any of {addrs:?}"))
}

fn client_config(tenant_id: &str, token: &str) -> Result<ClientConfig> {
    let mut tls = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .context("client protocol versions")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyBroker))
        .with_no_client_auth();
    // The client-facing role negotiates no ALPN, which is what keeps it out of
    // the internal listener. Setting one here would change that.
    tls.alpn_protocols.clear();

    let quinn = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("client crypto")?,
    ));
    let mut config = ClientConfig::optimized_defaults(quinn);
    config.auth_tenant_id = Some(tenant_id.to_string());
    config.auth_token = Some(token.to_string());

    Ok(config)
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

#[derive(Debug)]
struct AcceptAnyBroker;

impl ServerCertVerifier for AcceptAnyBroker {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

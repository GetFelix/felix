//! Client TLS for a cluster whose brokers present self-signed certificates.
//!
//! Brokers generate a certificate at startup and publish it nowhere, so a
//! remote client has nothing to verify against — the same gap the cluster
//! harness documents, with the same answer until a real PKI exists (#127
//! territory). The perf suite runs inside a private VNet the deployer owns,
//! which is what makes accepting any certificate tolerable *here*; the flag
//! that enables it is named for what it does, and there is no quiet default.

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_client::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

pub(crate) fn client_config(tenant_id: &str, token: &str) -> Result<ClientConfig> {
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(rustls::ALL_VERSIONS)
    .context("client protocol versions")?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AcceptAnyBroker))
    .with_no_client_auth();
    // The client-facing role negotiates no ALPN; setting one would aim this at
    // the internal listener, which refuses it.
    tls.alpn_protocols.clear();

    let quinn = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("client crypto")?,
    ));
    // Env-aware, not `optimized_defaults`: the instrument has to be able to
    // sweep the client's own knobs. Built on the defaults, so an unset
    // environment measures exactly what a default client does -- but
    // FELIX_PUB_CONN_POOL, FELIX_PUB_SHARDING and FELIX_PUBLISH_INFLIGHT_BYTES
    // silently did nothing here before, which made three runs of a perf
    // session measure the configuration they were meant to be varying (#553).
    let mut config =
        ClientConfig::from_env_or_yaml(quinn, None).context("build the loadgen's client config")?;
    config.auth_tenant_id = Some(tenant_id.to_string());
    config.auth_token = Some(token.to_string());
    // Say what was actually configured, once. A run that believes it swept a
    // knob it never set is worse than one that did not try, and that is exactly
    // what #553 produced -- so the instrument states its own configuration and
    // the result log carries the proof.
    static ANNOUNCED: std::sync::Once = std::sync::Once::new();
    ANNOUNCED.call_once(|| {
        eprintln!(
            "loadgen client config: conn_pool={} streams_per_conn={} sharding={:?} \
             inflight_bytes={} chunk_bytes={} queue_depth={}",
            config.publish_conn_pool,
            config.publish_streams_per_conn,
            config.publish_sharding,
            config.publish_inflight_bytes,
            config.publish_chunk_bytes,
            config.publish_queue_depth,
        );
    });
    Ok(config)
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
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests;

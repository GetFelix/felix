//! The TLS side of a client's QUIC connection.
//!
//! [`ClientConfig`](crate::ClientConfig) takes a `quinn::ClientConfig`, and the
//! ALPN list is fixed inside it once built, so whether a client offers
//! `felix/1` is decided here rather than on `ClientConfig`.
//!
//! Offering is opt-in. A broker from before ALPN support configures none on its
//! client listener, and QUIC refuses a handshake where the client offered a
//! protocol and the server picked none (RFC 9001 §8.1). Offering by default
//! would lock this client out of every such broker.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_platform_verifier::BuilderVerifierExt;

/// A client certificate chain and its private key, for a broker that asks
/// clients to authenticate with one.
///
/// Debug output names the certificates but never shows the key.
pub struct ClientIdentity {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl ClientIdentity {
    /// An identity from a certificate chain, leaf first, and the leaf's key.
    pub fn new(chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Self {
        Self { chain, key }
    }

    /// Read the chain and key from PEM files.
    ///
    /// `cert` holds the leaf certificate and any intermediates, leaf first.
    /// `key` holds one PKCS#8, PKCS#1 or SEC1 private key. Fails, naming the
    /// file, when either cannot be read or `cert` holds no certificate.
    pub fn from_pem_files(cert: impl AsRef<Path>, key: impl AsRef<Path>) -> Result<Self> {
        let (cert, key) = (cert.as_ref(), key.as_ref());
        let chain = read_certificates(cert)?;
        let key = PrivateKeyDer::from_pem_file(key)
            .with_context(|| format!("read a private key from {}", key.display()))?;
        Ok(Self { chain, key })
    }
}

impl std::fmt::Debug for ClientIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientIdentity")
            .field("certificates", &self.chain.len())
            .finish_non_exhaustive()
    }
}

/// Build the QUIC client config for a Felix broker.
///
/// The broker's certificate is verified against `roots`, or against the
/// platform trust store when `roots` is `None`. TLS 1.3 only, as QUIC requires.
///
/// With `offer_alpn`, the client offers `felix/1`
/// ([`felix_wire::CLIENT_ALPN`]). A broker with `FELIX_TLS_REQUIRE_ALPN=true`
/// serves only clients that do; a broker that predates ALPN refuses them. A
/// current broker without that setting serves either.
///
/// [`quic_client_config_with_identity`] presents a client certificate. For
/// anything else, build the `rustls::ClientConfig` yourself and set its
/// `alpn_protocols` to `vec![felix_wire::CLIENT_ALPN.to_vec()]` to offer
/// `felix/1`.
pub fn quic_client_config(
    roots: Option<Arc<RootCertStore>>,
    offer_alpn: bool,
) -> Result<quinn::ClientConfig> {
    quic_config(rustls_config(roots, None, offer_alpn)?)
}

/// [`quic_client_config`], presenting `identity` to a broker that asks for a
/// client certificate.
///
/// Fails when the key does not match the leaf certificate or is of a kind
/// rustls cannot sign with.
pub fn quic_client_config_with_identity(
    roots: Option<Arc<RootCertStore>>,
    identity: ClientIdentity,
    offer_alpn: bool,
) -> Result<quinn::ClientConfig> {
    quic_config(rustls_config(roots, Some(identity), offer_alpn)?)
}

/// Trust roots read from a PEM file of CA certificates, for the `roots`
/// argument of [`quic_client_config`].
///
/// Fails, naming the file, when it cannot be read, holds no certificate, or
/// holds one that is not a usable trust anchor. An empty store would refuse
/// every broker, which is better reported here than as a handshake failure.
pub fn root_store_from_pem_file(path: impl AsRef<Path>) -> Result<RootCertStore> {
    let path = path.as_ref();
    let mut roots = RootCertStore::empty();
    for cert in read_certificates(path)? {
        roots
            .add(cert)
            .with_context(|| format!("trust a certificate from {}", path.display()))?;
    }
    Ok(roots)
}

/// The rustls half of [`quic_client_config`]; what `quinn`'s own
/// `with_root_certificates` and `try_with_platform_verifier` build, plus ALPN
/// and an optional client certificate.
pub(crate) fn rustls_config(
    roots: Option<Arc<RootCertStore>>,
    identity: Option<ClientIdentity>,
    offer_alpn: bool,
) -> Result<rustls::ClientConfig> {
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .context("TLS 1.3")?;
    let builder = match roots {
        Some(roots) => builder.with_root_certificates(roots),
        None => builder
            .with_platform_verifier()
            .context("the platform trust store")?,
    };
    let mut config = match identity {
        None => builder.with_no_client_auth(),
        Some(ClientIdentity { chain, key }) => builder
            .with_client_auth_cert(chain, key)
            .context("use the client certificate")?,
    };
    config.enable_early_data = true;
    if offer_alpn {
        config.alpn_protocols = vec![felix_wire::CLIENT_ALPN.to_vec()];
    }
    Ok(config)
}

fn quic_config(tls: rustls::ClientConfig) -> Result<quinn::ClientConfig> {
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("QUIC client TLS config")?;
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .with_context(|| format!("read certificates from {}", path.display()))?;
    anyhow::ensure!(
        !certs.is_empty(),
        "{} holds no certificates",
        path.display()
    );
    Ok(certs)
}

#[cfg(test)]
mod tests;

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

use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::RootCertStore;
use rustls_platform_verifier::BuilderVerifierExt;

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
/// To present a client certificate or otherwise customise TLS, build the
/// `rustls::ClientConfig` yourself and set its `alpn_protocols` to
/// `vec![felix_wire::CLIENT_ALPN.to_vec()]` to offer `felix/1`.
pub fn quic_client_config(
    roots: Option<Arc<RootCertStore>>,
    offer_alpn: bool,
) -> Result<quinn::ClientConfig> {
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config(roots, offer_alpn)?)
            .context("QUIC client TLS config")?;
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

/// The rustls half of [`quic_client_config`]; what `quinn`'s own
/// `with_root_certificates` and `try_with_platform_verifier` build, plus ALPN.
pub(crate) fn rustls_config(
    roots: Option<Arc<RootCertStore>>,
    offer_alpn: bool,
) -> Result<rustls::ClientConfig> {
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .context("TLS 1.3")?;
    let mut config = match roots {
        Some(roots) => builder.with_root_certificates(roots).with_no_client_auth(),
        None => builder
            .with_platform_verifier()
            .context("the platform trust store")?
            .with_no_client_auth(),
    };
    config.enable_early_data = true;
    if offer_alpn {
        config.alpn_protocols = vec![felix_wire::CLIENT_ALPN.to_vec()];
    }
    Ok(config)
}

#[cfg(test)]
mod tests;

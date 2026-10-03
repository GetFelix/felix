//! The bootstrap listener: its token, and the optional mTLS in front of it.
use std::net::SocketAddr;

use anyhow::{Result, anyhow};
use serde::Deserialize;

#[derive(Clone)]
pub struct BootstrapConfig {
    pub enabled: bool,
    pub bind_addr: SocketAddr,
    pub token: Option<String>,
    /// The token being rotated out. Still accepted, so a rotation is two
    /// rolling deploys (add the new token, then drop this) with no window in
    /// which some instances refuse a token others require.
    pub previous_token: Option<String>,
    /// When set, the bootstrap listener terminates TLS and refuses any client
    /// that does not present a certificate signed by `client_ca_path`.
    pub tls: Option<BootstrapTlsConfig>,
    /// Serve `POST .../dev-token`, which mints a token for any principal of an
    /// initialized tenant with no identity provider. For local development
    /// only: refused unless the bootstrap listener is on loopback.
    pub dev_tokens: bool,
}

/// Whether each token is set, never the token.
impl std::fmt::Debug for BootstrapConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shown = |token: &Option<String>| token.as_ref().map(|_| "<redacted>");
        f.debug_struct("BootstrapConfig")
            .field("enabled", &self.enabled)
            .field("bind_addr", &self.bind_addr)
            .field("token", &shown(&self.token))
            .field("previous_token", &shown(&self.previous_token))
            .field("tls", &self.tls)
            .field("dev_tokens", &self.dev_tokens)
            .finish()
    }
}

impl BootstrapConfig {
    /// Tokens the bootstrap endpoint accepts, current first.
    pub fn accepted_tokens(&self) -> Vec<String> {
        self.token
            .iter()
            .chain(self.previous_token.iter())
            .cloned()
            .collect()
    }
}

/// mTLS for the bootstrap listener: all three or nothing, because a TLS
/// bootstrap endpoint that skips client verification would look secured while
/// still letting anyone on the network present the token.
#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapTlsConfig {
    /// PEM certificate chain the listener presents.
    pub cert_path: String,
    /// PEM private key for `cert_path`.
    pub key_path: String,
    /// PEM CA bundle; only clients holding a certificate signed by it may
    /// reach the bootstrap API at all.
    pub client_ca_path: String,
}

/// Bootstrap mTLS from the environment: all three variables or none.
///
/// A partial set is an error rather than "TLS off", because an operator who set
/// two of the three believed the listener was secured.
pub(super) fn bootstrap_tls_from_env() -> Result<Option<BootstrapTlsConfig>> {
    let cert = std::env::var("FELIX_BOOTSTRAP_TLS_CERT").ok();
    let key = std::env::var("FELIX_BOOTSTRAP_TLS_KEY").ok();
    let client_ca = std::env::var("FELIX_BOOTSTRAP_TLS_CLIENT_CA").ok();
    match (cert, key, client_ca) {
        (None, None, None) => Ok(None),
        (Some(cert_path), Some(key_path), Some(client_ca_path)) => Ok(Some(BootstrapTlsConfig {
            cert_path,
            key_path,
            client_ca_path,
        })),
        _ => Err(anyhow!(
            "bootstrap TLS needs all of FELIX_BOOTSTRAP_TLS_CERT, \
             FELIX_BOOTSTRAP_TLS_KEY, and FELIX_BOOTSTRAP_TLS_CLIENT_CA"
        )),
    }
}

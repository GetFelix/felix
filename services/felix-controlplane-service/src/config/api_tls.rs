//! TLS on the API listener: the certificate it serves, re-read on rotation.
use anyhow::{Result, bail};
use serde::Deserialize;

/// The certificate and key the API listener serves. Both or neither.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiTlsConfig {
    /// PEM certificate chain, leaf first.
    pub cert_path: String,
    /// PEM private key for the leaf.
    pub key_path: String,
}

/// `FELIX_CONTROLPLANE_TLS_CERT` and `FELIX_CONTROLPLANE_TLS_KEY`.
///
/// One without the other is an error rather than "TLS off": whoever set it
/// believed the API was served over TLS.
pub(super) fn api_tls_from_env() -> Result<Option<ApiTlsConfig>> {
    let read = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };
    match (
        read("FELIX_CONTROLPLANE_TLS_CERT"),
        read("FELIX_CONTROLPLANE_TLS_KEY"),
    ) {
        (None, None) => Ok(None),
        (Some(cert_path), Some(key_path)) => Ok(Some(ApiTlsConfig {
            cert_path,
            key_path,
        })),
        (cert, _) => bail!(
            "API TLS is partly configured: {} is not set; set both \
             FELIX_CONTROLPLANE_TLS_CERT and FELIX_CONTROLPLANE_TLS_KEY, or neither",
            if cert.is_none() {
                "FELIX_CONTROLPLANE_TLS_CERT"
            } else {
                "FELIX_CONTROLPLANE_TLS_KEY"
            }
        ),
    }
}

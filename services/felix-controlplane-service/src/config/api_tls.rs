//! TLS on the API listener: the certificate it serves, re-read on rotation,
//! and the CA client certificates must chain to.
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
    /// When set, every client must present a certificate chaining to this PEM
    /// bundle, or the handshake is refused. Read once at startup.
    #[serde(default)]
    pub client_ca_path: Option<String>,
    /// A bearer token must be issued to the client's certificate: a
    /// `felix:principal:<sub>` URI SAN, or a DNS/IP SAN the subject matches.
    /// Needs `client_ca_path`.
    #[serde(default)]
    pub bind_subject: bool,
}

impl ApiTlsConfig {
    /// Refuse a binding nothing can satisfy.
    pub(super) fn validate(&self) -> Result<()> {
        // Binding to a certificate nobody is asked for would check nothing
        // while reading as a control.
        if self.bind_subject && self.client_ca_path.is_none() {
            bail!(
                "FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT (tls.bind_subject) is set \
                 without FELIX_CONTROLPLANE_TLS_CLIENT_CA (tls.client_ca_path): clients \
                 present no certificate to bind their token to"
            );
        }
        Ok(())
    }
}

/// `FELIX_CONTROLPLANE_TLS_CERT`, `FELIX_CONTROLPLANE_TLS_KEY`,
/// `FELIX_CONTROLPLANE_TLS_CLIENT_CA` and
/// `FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT`.
///
/// Half a configuration is an error rather than "TLS off": whoever set it
/// believed the API was served over TLS.
pub(super) fn api_tls_from_env() -> Result<Option<ApiTlsConfig>> {
    let read = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };
    let bind_subject = match read("FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT").as_deref() {
        None | Some("0" | "false" | "no") => false,
        Some("1" | "true" | "yes") => true,
        Some(other) => bail!(
            "FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT must be true or false, not {other:?}"
        ),
    };
    let tls = match (
        read("FELIX_CONTROLPLANE_TLS_CERT"),
        read("FELIX_CONTROLPLANE_TLS_KEY"),
        read("FELIX_CONTROLPLANE_TLS_CLIENT_CA"),
    ) {
        (None, None, None) if !bind_subject => return Ok(None),
        (Some(cert_path), Some(key_path), client_ca_path) => ApiTlsConfig {
            cert_path,
            key_path,
            client_ca_path,
            bind_subject,
        },
        (None, None, Some(_)) => bail!(
            "FELIX_CONTROLPLANE_TLS_CLIENT_CA is set without FELIX_CONTROLPLANE_TLS_CERT and \
             FELIX_CONTROLPLANE_TLS_KEY: client certificates can only be verified by a \
             listener that serves TLS"
        ),
        (None, None, None) => bail!(
            "FELIX_CONTROLPLANE_TLS_CLIENT_CERT_BIND_SUBJECT is set without \
             FELIX_CONTROLPLANE_TLS_CLIENT_CA: clients present no certificate to bind \
             their token to"
        ),
        (cert, _, _) => bail!(
            "API TLS is partly configured: {} is not set; set both \
             FELIX_CONTROLPLANE_TLS_CERT and FELIX_CONTROLPLANE_TLS_KEY, or neither",
            if cert.is_none() {
                "FELIX_CONTROLPLANE_TLS_CERT"
            } else {
                "FELIX_CONTROLPLANE_TLS_KEY"
            }
        ),
    };
    tls.validate()?;
    Ok(Some(tls))
}

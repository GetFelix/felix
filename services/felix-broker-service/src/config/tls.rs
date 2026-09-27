//! The client-facing certificate (`FELIX_TLS_*`) and the CA brokers trust for
//! an `https://` control plane (`FELIX_CONTROLPLANE_CA`).
//!
//! What each setting does, and the Helm wiring, is in `docs/broker-config.md`
//! under "Client TLS".

use anyhow::{Result, bail};
use serde::Serialize;

/// The certificate the QUIC and Kafka listeners present to clients.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ClientTlsConfig {
    /// Certificate files to serve. `None` generates a self-signed `localhost`
    /// certificate at startup, which is for development only.
    pub files: Option<ClientTlsFiles>,
    /// Refuse to start on the generated certificate (`FELIX_TLS_REQUIRE_CERT`).
    pub require_cert: bool,
    /// Where to write the generated certificate for clients to trust
    /// (`FELIX_TLS_CERT_EXPORT`). Only meaningful for the generated one.
    pub cert_export: Option<String>,
    /// Refuse QUIC clients that do not negotiate the `felix/1` ALPN
    /// (`FELIX_TLS_REQUIRE_ALPN`). Off by default, because clients built
    /// before ALPN offer none and are otherwise still served.
    pub require_alpn: bool,
    /// When a client presents a certificate, it must be issued to its token's
    /// subject: a `felix:principal:<sub>` URI SAN, or a DNS/IP SAN
    /// (`FELIX_TLS_CLIENT_CERT_BIND_SUBJECT`). QUIC listeners, and the Kafka
    /// listener when it serves TLS.
    /// Needs `FELIX_TLS_CLIENT_CA`: without it no client presents one.
    pub bind_subject: bool,
}

/// PEM files for the client-facing listeners. The certificate and key are
/// re-read when they change; the client CA is read once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClientTlsFiles {
    /// Certificate chain, leaf first (`FELIX_TLS_CERT`).
    pub cert_path: String,
    /// Private key for the leaf (`FELIX_TLS_KEY`).
    pub key_path: String,
    /// When set, every client must present a certificate chaining to this
    /// bundle (`FELIX_TLS_CLIENT_CA`).
    pub client_ca_path: Option<String>,
}

impl ClientTlsConfig {
    /// Read `FELIX_TLS_CERT`, `FELIX_TLS_KEY`, `FELIX_TLS_CLIENT_CA`,
    /// `FELIX_TLS_REQUIRE_CERT`, `FELIX_TLS_CERT_EXPORT`,
    /// `FELIX_TLS_REQUIRE_ALPN` and `FELIX_TLS_CLIENT_CERT_BIND_SUBJECT`.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`Self::from_env`] over any source of variables.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let get = |name: &str| {
            lookup(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let files = match (
            get("FELIX_TLS_CERT"),
            get("FELIX_TLS_KEY"),
            get("FELIX_TLS_CLIENT_CA"),
        ) {
            (None, None, None) => None,
            (Some(cert_path), Some(key_path), client_ca_path) => Some(ClientTlsFiles {
                cert_path,
                key_path,
                client_ca_path,
            }),
            // Half a configuration is refused rather than read as "TLS off":
            // whoever set one of these believed the listener was using it.
            (None, None, Some(_)) => bail!(
                "FELIX_TLS_CLIENT_CA is set without FELIX_TLS_CERT and FELIX_TLS_KEY: \
                 client certificates can only be verified by a listener serving a \
                 configured certificate"
            ),
            (cert, _, _) => bail!(
                "client TLS is partly configured: {} is not set; set both \
                 FELIX_TLS_CERT and FELIX_TLS_KEY, or neither",
                if cert.is_none() {
                    "FELIX_TLS_CERT"
                } else {
                    "FELIX_TLS_KEY"
                }
            ),
        };
        let flag = |name: &str| -> Result<bool> {
            match get(name).as_deref() {
                None | Some("0" | "false" | "no") => Ok(false),
                Some("1" | "true" | "yes") => Ok(true),
                Some(other) => bail!("{name} must be true or false, not {other:?}"),
            }
        };
        Ok(Self {
            files,
            require_cert: flag("FELIX_TLS_REQUIRE_CERT")?,
            cert_export: get("FELIX_TLS_CERT_EXPORT"),
            require_alpn: flag("FELIX_TLS_REQUIRE_ALPN")?,
            bind_subject: flag("FELIX_TLS_CLIENT_CERT_BIND_SUBJECT")?,
        })
    }

    /// Refuse the combinations that cannot do what they say.
    pub(super) fn validate(&self) -> Result<()> {
        match &self.files {
            None if self.require_cert => bail!(
                "FELIX_TLS_REQUIRE_CERT is set but FELIX_TLS_CERT and FELIX_TLS_KEY are not: \
                 this broker would serve a self-signed certificate no client can verify"
            ),
            // The export exists so clients can trust a certificate nothing
            // else names. A configured leaf is not a trust root for a client:
            // they trust the CA that issued it.
            Some(_) if self.cert_export.is_some() => bail!(
                "FELIX_TLS_CERT_EXPORT is set with FELIX_TLS_CERT: the export only \
                 applies to the generated development certificate; clients should \
                 trust the CA that issued FELIX_TLS_CERT"
            ),
            // Binding to a certificate nobody is asked for would check
            // nothing while reading as a control.
            files
                if self.bind_subject
                    && files
                        .as_ref()
                        .is_none_or(|files| files.client_ca_path.is_none()) =>
            {
                bail!(
                    "FELIX_TLS_CLIENT_CERT_BIND_SUBJECT is set without FELIX_TLS_CLIENT_CA: \
                     clients present no certificate to bind their token to"
                )
            }
            _ => Ok(()),
        }
    }
}

/// `FELIX_CONTROLPLANE_CA`: a PEM bundle trusted, in addition to the public
/// roots, when the control plane is reached over `https://`.
pub(super) fn controlplane_ca_from_env() -> Option<String> {
    std::env::var("FELIX_CONTROLPLANE_CA")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// A CA for the control plane over a plain `http://` URL is a deployment that
/// believes its control-plane traffic is verified when it is not even
/// encrypted.
pub(super) fn validate_controlplane_ca(ca: Option<&str>, url: Option<&str>) -> Result<()> {
    if let (Some(_), Some(url)) = (ca, url)
        && !url.trim_start().starts_with("https://")
    {
        bail!(
            "FELIX_CONTROLPLANE_CA is set but FELIX_CONTROLPLANE_URL ({url}) is not https://: \
             the CA would never be used and control-plane traffic would be plain HTTP"
        );
    }
    Ok(())
}

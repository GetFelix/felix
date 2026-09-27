//! The certificate clients see, on the QUIC listeners and the Kafka listener.
//!
//! Two modes, chosen by [`ClientTlsConfig`]:
//!
//! - **Configured**, when `FELIX_TLS_CERT` and `FELIX_TLS_KEY` are set. The
//!   files are re-read on a timer and a rotation reaches the next handshake;
//!   connections already up keep theirs. With `FELIX_TLS_CLIENT_CA`, every
//!   client must also present a certificate chaining to that bundle.
//! - **Generated**, otherwise: a fresh self-signed `localhost` certificate
//!   per start. Only a client handed this exact certificate can verify it, and
//!   it changes on every restart, so it is for development. Startup says so,
//!   and `FELIX_TLS_REQUIRE_CERT` refuses it.

use std::sync::Arc;

use anyhow::{Context, Result};
use felix_common::tls::{IdentityFiles, ReloadingIdentity};
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::server::ResolvesServerCert;
use tokio_util::sync::CancellationToken;

use crate::config::ClientTlsConfig;

/// The client-facing identity, shared by every client listener so a rotation
/// reaches all of them at once.
pub(crate) struct ClientTls {
    resolver: Arc<dyn ResolvesServerCert>,
    /// Present when the certificate comes from files and can rotate.
    reloading: Option<Arc<ReloadingIdentity>>,
    /// Present when clients must authenticate with a certificate.
    client_roots: Option<Arc<rustls::RootCertStore>>,
    /// Refuse a QUIC client that negotiates no ALPN.
    require_alpn: bool,
}

impl ClientTls {
    /// Load the configured certificate, or generate the development one.
    pub(crate) fn from_config(config: &ClientTlsConfig) -> Result<Self> {
        let Some(files) = &config.files else {
            let mut generated = Self::generated(config.cert_export.as_deref())?;
            generated.require_alpn = config.require_alpn;
            return Ok(generated);
        };
        let identity = Arc::new(
            ReloadingIdentity::load(
                IdentityFiles {
                    cert_path: files.cert_path.clone(),
                    cert_var: "FELIX_TLS_CERT",
                    key_path: files.key_path.clone(),
                    key_var: "FELIX_TLS_KEY",
                },
                provider(),
            )
            .context("load the client-facing TLS certificate")?,
        );
        let client_roots = files
            .client_ca_path
            .as_deref()
            .map(|path| felix_common::tls::load_roots("FELIX_TLS_CLIENT_CA", path))
            .transpose()?
            .map(Arc::new);
        tracing::info!(
            cert = %files.cert_path,
            client_ca = files.client_ca_path.as_deref().unwrap_or("none"),
            "client listeners serve the configured certificate",
        );
        Ok(Self {
            resolver: Arc::clone(&identity) as Arc<dyn ResolvesServerCert>,
            reloading: Some(identity),
            client_roots,
            require_alpn: config.require_alpn,
        })
    }

    /// The QUIC listeners' server config.
    ///
    /// Selects the `felix/1` ALPN for a client that offers it and refuses one
    /// that offers only other protocols, such as a broker's internal one. A
    /// client that offers none is accepted unless `FELIX_TLS_REQUIRE_ALPN` is
    /// set: clients built before ALPN existed send none.
    pub(crate) fn quic_server_config(&self) -> Result<quinn::ServerConfig> {
        let mut config = self
            .rustls_config(&[&rustls::version::TLS13])
            .context("client QUIC TLS config")?;
        if self.client_roots.is_none() {
            // What quinn's own single-certificate config sets: QUIC allows
            // only 0 or u32::MAX, and this keeps 0-RTT available.
            config.max_early_data_size = u32::MAX;
        }
        if !self.require_alpn {
            return felix_transport::alpn_optional_server_config(
                config,
                &[felix_wire::CLIENT_ALPN],
            )
            .context("client QUIC crypto");
        }
        config.alpn_protocols = vec![felix_wire::CLIENT_ALPN.to_vec()];
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(config)
            .context("client QUIC crypto")?;
        Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
    }

    /// The Kafka listener's server config. TLS 1.2 stays on because Kafka
    /// clients in the wild still negotiate it.
    pub(crate) fn kafka_server_config(&self) -> Result<Arc<rustls::ServerConfig>> {
        Ok(Arc::new(
            self.rustls_config(rustls::ALL_VERSIONS)
                .context("kafka TLS config")?,
        ))
    }

    /// Re-read the certificate files until `shutdown`, when they are files.
    pub(crate) fn spawn_reload(&self, shutdown: &CancellationToken) {
        if let Some(identity) = &self.reloading {
            let identity = Arc::clone(identity);
            let shutdown = shutdown.clone();
            drop(tokio::spawn(async move {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = identity.reload_periodically("client") => {}
                }
            }));
        }
    }

    fn generated(export: Option<&str>) -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .context("generate the development certificate")?;
        let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let key = rustls::sign::CertifiedKey::from_der(
            vec![cert.cert.der().clone()],
            key.into(),
            &provider(),
        )
        .context("the generated certificate and key")?;
        // A generated certificate nothing can name is a certificate only a
        // client that skips verification can use, which is how "just disable
        // TLS verification in dev" becomes a habit. Writing it out gives every
        // client -- including the ones that are not Rust -- a CA file to trust.
        if let Some(path) = export {
            export_certificate(&cert.cert.pem(), path)?;
        }
        tracing::warn!(
            "client listeners serve a GENERATED self-signed certificate for `localhost`, new on \
             every start: clients cannot verify this broker, so their tokens go to whoever \
             answers. For anything but development set FELIX_TLS_CERT and FELIX_TLS_KEY, and \
             FELIX_TLS_REQUIRE_CERT=true to refuse to start without them",
        );
        Ok(Self {
            resolver: Arc::new(rustls::sign::SingleCertAndKey::from(key)),
            reloading: None,
            client_roots: None,
            require_alpn: false,
        })
    }

    fn rustls_config(
        &self,
        versions: &[&'static rustls::SupportedProtocolVersion],
    ) -> Result<rustls::ServerConfig> {
        let builder = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(versions)?;
        Ok(match &self.client_roots {
            Some(roots) => {
                let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::clone(roots),
                    provider(),
                )
                .build()
                .context("build the client certificate verifier")?;
                let mut config = builder
                    .with_client_cert_verifier(verifier)
                    .with_cert_resolver(Arc::clone(&self.resolver));
                // A resumed session carries the client identity it resumes,
                // so a rotated or revoked client certificate would go on
                // being accepted. Same choice as the peer listener.
                config.send_tls13_tickets = 0;
                config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
                config
            }
            None => builder
                .with_no_client_auth()
                .with_cert_resolver(Arc::clone(&self.resolver)),
        })
    }
}

/// The URI subject alternative name that binds a certificate to a principal:
/// `felix:principal:<sub>`, with the token's `sub` verbatim.
pub(crate) const PRINCIPAL_URI_PREFIX: &str = "felix:principal:";

/// Whether a token's `subject` is a principal the client's certificate was
/// issued to, when the client presented one.
///
/// The chain was verified in the handshake; this binds the certificate to
/// the principal the token claims, so a stolen token is no use without the
/// key of a certificate issued to the same principal. The certificate binds a
/// subject if its SANs include the URI `felix:principal:<subject>` (exact
/// match), or, for a human-readable subject, a DNS or IP name the subject
/// matches the way a server name would, wildcards included. Control-plane
/// principal ids are 64 hex characters, one label longer than DNS allows,
/// so they can only bind through the URI. A client with no certificate
/// passes, since only a listener with `FELIX_TLS_CLIENT_CA` asks for one and
/// that listener refuses the handshake without it.
pub(crate) fn check_subject_binding(
    peer_certs: Option<&[rustls::pki_types::CertificateDer<'_>]>,
    subject: &str,
) -> std::result::Result<(), String> {
    let Some(leaf) = peer_certs.and_then(|certs| certs.first()) else {
        return Ok(());
    };
    // The messages name no values: they end up in logs, and the subject comes
    // from the token.
    if carries_principal_uri(leaf, subject)? {
        return Ok(());
    }
    let refused = || {
        "the client certificate is not issued to the token's subject: no matching \
         felix:principal URI, DNS or IP name"
            .to_string()
    };
    let Ok(name) = rustls::pki_types::ServerName::try_from(subject) else {
        return Err(refused());
    };
    let cert = webpki::EndEntityCert::try_from(leaf)
        .map_err(|err| format!("the client certificate does not parse: {err}"))?;
    cert.verify_is_valid_for_subject_name(&name)
        .map_err(|_| refused())
}

/// Whether `leaf` has the URI SAN `felix:principal:<subject>`.
fn carries_principal_uri(
    leaf: &rustls::pki_types::CertificateDer<'_>,
    subject: &str,
) -> std::result::Result<bool, String> {
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|err| format!("the client certificate does not parse: {err}"))?;
    let sans = cert
        .subject_alternative_name()
        .map_err(|err| format!("the client certificate's SANs do not parse: {err}"))?;
    Ok(sans.is_some_and(|sans| {
        sans.value.general_names.iter().any(|name| {
            matches!(
                name,
                x509_parser::extensions::GeneralName::URI(uri)
                    if uri.strip_prefix(PRINCIPAL_URI_PREFIX) == Some(subject)
            )
        })
    }))
}

/// Same provider as the peer transport; see `peer::tls::provider`.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    crate::peer::tls::provider()
}

/// Write the generated certificate where a client can trust it from.
///
/// Fails startup rather than warning: a deployment that asked for the export
/// is a deployment whose clients are configured to read it, and coming up
/// without it produces connection failures whose cause is nowhere near the
/// symptom.
fn export_certificate(pem: &str, path: &str) -> Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create the directory for FELIX_TLS_CERT_EXPORT {path}"))?;
    }
    std::fs::write(path, pem).with_context(|| format!("write the broker certificate to {path}"))?;
    tracing::info!(
        path,
        "wrote the broker's self-signed certificate for clients to trust"
    );
    Ok(())
}

#[cfg(test)]
mod tests;

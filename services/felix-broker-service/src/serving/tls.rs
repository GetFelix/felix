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
}

impl ClientTls {
    /// Load the configured certificate, or generate the development one.
    pub(crate) fn from_config(config: &ClientTlsConfig) -> Result<Self> {
        let Some(files) = &config.files else {
            return Self::generated(config.cert_export.as_deref());
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
        })
    }

    /// The QUIC listeners' server config.
    pub(crate) fn quic_server_config(&self) -> Result<quinn::ServerConfig> {
        let mut config = self
            .rustls_config(&[&rustls::version::TLS13])
            .context("client QUIC TLS config")?;
        if self.client_roots.is_none() {
            // What quinn's own single-certificate config sets: QUIC allows
            // only 0 or u32::MAX, and this keeps 0-RTT available.
            config.max_early_data_size = u32::MAX;
        }
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

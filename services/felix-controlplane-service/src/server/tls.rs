//! TLS termination for the control plane's HTTP listeners.
//!
//! The API listener serves TLS when [`crate::config::ApiTlsConfig`] is set,
//! with a certificate re-read on rotation. The bootstrap listener hands out
//! admin-equivalent power, and a shared token is one factor, so when
//! [`crate::config::BootstrapTlsConfig`] is set it refuses the TLS handshake
//! itself to any client that does not present a certificate signed by the
//! configured CA: an unauthenticated caller never reaches the router.
//!
//! Material is plain PEM files, the format operators already have.
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
// The PEM parsing that used to be rustls-pemfile's job; that crate is
// unmaintained (RUSTSEC-2025-0134) and pki-types absorbed the functionality.
use rustls::pki_types::pem::PemObject;
use tokio_util::sync::CancellationToken;

use crate::config::{ApiTlsConfig, BootstrapTlsConfig};

/// The API listener's rustls config over `identity`, which the caller keeps
/// to reload.
pub fn api_server_config(
    identity: Arc<felix_common::tls::ReloadingIdentity>,
) -> Result<Arc<rustls::ServerConfig>> {
    let config = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .context("API TLS protocol versions")?
        .with_no_client_auth()
        .with_cert_resolver(identity);
    Ok(Arc::new(config))
}

/// Read the API certificate and key. Fails startup on unreadable material.
pub fn load_api_identity(tls: &ApiTlsConfig) -> Result<Arc<felix_common::tls::ReloadingIdentity>> {
    Ok(Arc::new(felix_common::tls::ReloadingIdentity::load(
        felix_common::tls::IdentityFiles {
            cert_path: tls.cert_path.clone(),
            cert_var: "FELIX_CONTROLPLANE_TLS_CERT",
            key_path: tls.key_path.clone(),
            key_var: "FELIX_CONTROLPLANE_TLS_KEY",
        },
        provider(),
    )?))
}

/// Named explicitly: more than one rustls crypto provider is linked into
/// this binary, so there is no unambiguous process default to rely on.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Build the listener's rustls config: serve `cert_path`/`key_path`, require a
/// client certificate signed by `client_ca_path`.
///
/// Fails at startup rather than at the first connection — a bootstrap listener
/// that comes up with unreadable key material is a misconfiguration, not a
/// runtime condition to retry.
pub fn load_server_config(tls: &BootstrapTlsConfig) -> Result<Arc<rustls::ServerConfig>> {
    let provider = provider();

    let certs = load_pem_certs(&tls.cert_path)
        .with_context(|| format!("read bootstrap TLS certificate {}", tls.cert_path))?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_file(&tls.key_path)
        .with_context(|| format!("read bootstrap TLS key {}", tls.key_path))?;

    let mut roots = rustls::RootCertStore::empty();
    for cert in load_pem_certs(&tls.client_ca_path)
        .with_context(|| format!("read bootstrap client CA {}", tls.client_ca_path))?
    {
        roots.add(cert).context("add bootstrap client CA root")?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .context("build bootstrap client verifier")?;

    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("bootstrap TLS protocol versions")?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("bootstrap TLS certificate/key")?;
    Ok(Arc::new(config))
}

fn load_pem_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let certs =
        rustls::pki_types::CertificateDer::pem_file_iter(path)?.collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(!certs.is_empty(), "no certificates in {path}");
    Ok(certs)
}

/// Serve `router` over TLS until `shutdown` fires, then let accepted
/// connections finish. `name` labels the log lines.
///
/// A failed handshake ends that connection and nothing else: the listener's
/// job during a probe or scan is to keep serving legitimate clients.
pub async fn serve_tls(
    name: &'static str,
    listener: tokio::net::TcpListener,
    router: Router,
    tls: Arc<rustls::ServerConfig>,
    shutdown: CancellationToken,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        // Transient accept errors (EMFILE, resets) starve the
                        // loop if it exits; log and go on accepting.
                        tracing::warn!(error = %err, listener = name, "accept failed");
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let app = router.clone();
                let shutdown = shutdown.clone();
                connections.spawn(async move {
                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(tls_stream) => tls_stream,
                        Err(err) => {
                            // With client verification on, this is the
                            // refusal doing its job.
                            tracing::debug!(%peer, error = %err, listener = name, "TLS handshake refused");
                            return;
                        }
                    };
                    let service = hyper_util::service::TowerToHyperService::new(app);
                    let builder =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                    let conn = builder.serve_connection(TokioIo::new(tls_stream), service);
                    tokio::pin!(conn);
                    let result = tokio::select! {
                        result = conn.as_mut() => result,
                        _ = shutdown.cancelled() => {
                            // Keep-alive connections idle between requests are
                            // the common case; without this nudge each would
                            // hold the drain until its peer's idle timeout.
                            conn.as_mut().graceful_shutdown();
                            conn.as_mut().await
                        }
                    };
                    if let Err(err) = result {
                        tracing::debug!(%peer, error = %err, listener = name, "connection ended with error");
                    }
                });
            }
        }
    }

    // In-flight requests get to finish; the caller's drain budget bounds how
    // long this is allowed to take before the whole task is aborted.
    while connections.join_next().await.is_some() {}
}

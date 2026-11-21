//! TLS termination for the control plane's HTTP listeners.
//!
//! The API listener serves TLS when [`crate::config::ApiTlsConfig`] is set,
//! and with a client CA set it
//! refuses the handshake to a client without a certificate from that CA. The bootstrap listener hands out
//! admin-equivalent power, and a shared token is one factor, so when
//! [`crate::config::BootstrapTlsConfig`] is set it refuses the TLS handshake
//! itself to any client that does not present a certificate signed by the
//! configured CA: an unauthenticated caller never reaches the router.
//!
//! Material is plain PEM files, the format operators already have. Every
//! listener re-reads its certificate, key and client CA every
//! [`felix_common::tls::RELOAD_INTERVAL`]; a rotation reaches the next
//! handshake.
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio_util::sync::CancellationToken;

use crate::auth::cert_binding::PeerCertificates;
use crate::config::{ApiTlsConfig, BootstrapTlsConfig};

/// The API listener's rustls config over `identity`, which the caller keeps
/// to reload. With `client_ca`, the handshake requires a client certificate
/// chaining to it.
pub fn api_server_config(
    identity: Arc<felix_common::tls::ReloadingIdentity>,
    client_ca: Option<Arc<ReloadingClientCa>>,
) -> Result<Arc<rustls::ServerConfig>> {
    match client_ca {
        Some(ca) => mtls_server_config(identity, ca),
        None => Ok(Arc::new(
            rustls::ServerConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .context("API TLS protocol versions")?
                .with_no_client_auth()
                .with_cert_resolver(identity),
        )),
    }
}

/// Read `FELIX_CONTROLPLANE_TLS_CLIENT_CA`, when set. Fails startup on an
/// unreadable or empty bundle.
pub fn load_api_client_roots(tls: &ApiTlsConfig) -> Result<Option<Arc<ReloadingClientCa>>> {
    tls.client_ca_path
        .as_deref()
        .map(|path| ReloadingClientCa::load("FELIX_CONTROLPLANE_TLS_CLIENT_CA", path))
        .transpose()
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

/// Passed explicitly so the listener never depends on an installed process
/// default.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// The bootstrap listener's rustls config: serve `cert_path`/`key_path`,
/// require a client certificate signed by `client_ca_path`. All three files
/// are re-read every [`felix_common::tls::RELOAD_INTERVAL`] until `shutdown`.
///
/// Fails at startup rather than at the first connection: a bootstrap listener
/// that comes up with unreadable key material is a misconfiguration, not a
/// runtime condition to retry.
pub fn bootstrap_server_config(
    tls: &BootstrapTlsConfig,
    shutdown: &CancellationToken,
) -> Result<Arc<rustls::ServerConfig>> {
    let identity = Arc::new(
        felix_common::tls::ReloadingIdentity::load(
            felix_common::tls::IdentityFiles {
                cert_path: tls.cert_path.clone(),
                cert_var: "FELIX_BOOTSTRAP_TLS_CERT",
                key_path: tls.key_path.clone(),
                key_var: "FELIX_BOOTSTRAP_TLS_KEY",
            },
            provider(),
        )
        .context("bootstrap TLS")?,
    );
    let client_ca = ReloadingClientCa::load("FELIX_BOOTSTRAP_TLS_CLIENT_CA", &tls.client_ca_path)?;
    spawn_reload(
        "bootstrap",
        Arc::clone(&identity),
        Some(Arc::clone(&client_ca)),
        shutdown,
    );
    mtls_server_config(identity, client_ca)
}

/// A rustls server config that presents `identity` and requires a client
/// certificate `verifier` accepts, with no session resumption: a resumed
/// session skips the client certificate check, so a rotated-out or revoked
/// certificate would go on being accepted.
pub(crate) fn mtls_server_config(
    identity: Arc<felix_common::tls::ReloadingIdentity>,
    verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
) -> Result<Arc<rustls::ServerConfig>> {
    let mut config = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(identity);
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    Ok(Arc::new(config))
}

/// Re-read `identity`, and `client_ca` when given, every
/// [`felix_common::tls::RELOAD_INTERVAL`] until `shutdown`.
pub(crate) fn spawn_reload(
    what: &'static str,
    identity: Arc<felix_common::tls::ReloadingIdentity>,
    client_ca: Option<Arc<ReloadingClientCa>>,
    shutdown: &CancellationToken,
) {
    let shutdown = shutdown.clone();
    drop(tokio::spawn(async move {
        let ca = async {
            match client_ca {
                Some(ca) => ca.reload_periodically(what).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = identity.reload_periodically(what) => {}
            _ = ca => {}
        }
    }));
}

/// A client CA bundle re-read on a timer, and the verifier built from it.
///
/// A bundle that does not read or parse keeps the current one, as a
/// half-written certificate does.
pub struct ReloadingClientCa {
    var: &'static str,
    path: String,
    current: arc_swap::ArcSwap<ClientTrust>,
}

struct ClientTrust {
    bundle: Vec<rustls::pki_types::CertificateDer<'static>>,
    roots: Arc<rustls::RootCertStore>,
    verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
}

impl ReloadingClientCa {
    /// Read the bundle. Fails startup on an unreadable or empty one.
    pub fn load(var: &'static str, path: &str) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            var,
            path: path.to_string(),
            current: arc_swap::ArcSwap::from_pointee(client_trust(var, path)?),
        }))
    }

    /// The roots currently trusted.
    pub fn roots(&self) -> Arc<rustls::RootCertStore> {
        Arc::clone(&self.current.load().roots)
    }

    /// Re-read the bundle. `Ok(true)` when it changed.
    pub fn reload(&self) -> Result<bool> {
        let fresh = client_trust(self.var, &self.path)?;
        let changed = fresh.bundle != self.current.load().bundle;
        if changed {
            self.current.store(Arc::new(fresh));
        }
        Ok(changed)
    }

    /// Check the bundle every [`felix_common::tls::RELOAD_INTERVAL`], for
    /// ever. Callers race it against their shutdown.
    pub async fn reload_periodically(self: Arc<Self>, what: &'static str) {
        let mut ticker = tokio::time::interval(felix_common::tls::RELOAD_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match self.reload() {
                Ok(true) => tracing::info!(ca = %self.path, "{what} client CA rotated"),
                Ok(false) => {}
                Err(err) => tracing::warn!(
                    error = %format!("{err:#}"),
                    "could not reload the {what} client CA; keeping the current one",
                ),
            }
        }
    }
}

impl std::fmt::Debug for ReloadingClientCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadingClientCa")
            .field("path", &self.path)
            .finish()
    }
}

fn client_trust(var: &'static str, path: &str) -> Result<ClientTrust> {
    let bundle = felix_common::tls::load_pem_certs(var, path)?;
    let roots = Arc::new(felix_common::tls::load_roots(var, path)?);
    let verifier =
        rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::clone(&roots), provider())
            .build()
            .with_context(|| format!("build the client verifier for {var}"))?;
    Ok(ClientTrust {
        bundle,
        roots,
        verifier,
    })
}

impl rustls::server::danger::ClientCertVerifier for ReloadingClientCa {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    // Hints are optional, and a borrow of the current bundle cannot outlive
    // the load that would produce it.
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
        self.current
            .load()
            .verifier
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.current
            .load()
            .verifier
            .verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.current
            .load()
            .verifier
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.current.load().verifier.supported_verify_schemes()
    }
}

/// When a connection must close because a certificate it was opened under
/// ends: given the client's chain, a Unix time, or `None` for never.
pub type CloseAt =
    Arc<dyn Fn(&[rustls::pki_types::CertificateDer<'static>]) -> Option<i64> + Send + Sync>;

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
    close_at: Option<CloseAt>,
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
                let close_at = close_at.clone();
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
                    // The chain verified in the handshake, for the routes
                    // that bind a token to it.
                    let peer_certs = tls_stream.get_ref().1.peer_certificates().map(|certs| {
                        PeerCertificates(certs.iter().map(|cert| cert.clone().into_owned()).collect())
                    });
                    // The handshake checked the client's certificate once;
                    // the connection must not outlive it.
                    let ends_at = close_at
                        .zip(peer_certs.as_ref())
                        .and_then(|(close_at, certs)| close_at(&certs.0));
                    let app = match peer_certs {
                        Some(certs) => app.layer(axum::Extension(certs)),
                        None => app,
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
                        () = sleep_until(ends_at) => {
                            tracing::info!(%peer, listener = name, "closing a connection whose certificate has ended");
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

async fn sleep_until(at: Option<i64>) {
    match at {
        Some(at) => felix_common::tls::sleep_until_unix(at).await,
        None => std::future::pending().await,
    }
}

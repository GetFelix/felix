//! The MQTT listener: accepting connections, TLS, and the checks that need the
//! broker around them.
//!
//! The protocol work is in the `felix-mqtt` crate. This module is what only
//! the broker service knows: how tokens are verified and how many connections
//! it will carry. It runs on a tokio runtime of its own, so a reconnect storm
//! of TLS handshakes spends the listener's threads and not the ones serving
//! QUIC (`docs/mqtt-design.md`, "Isolation and limits").

mod rate;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use felix_mqtt::{AuthError, Authenticator, MqttService, Principal};
use tokio::net::{TcpListener, TcpSocket};
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use self::rate::RateGate;
use crate::config::MqttListenerConfig;
use crate::serving::auth::{BrokerAuth, CredentialRefused};
use crate::serving::limits::PerIpLimiter;
use crate::serving::revocation::Revoked;
use felix_authz::AuthzError;

/// A TLS handshake that has not finished by now is abandoned, so a client
/// that connects and sends nothing does not hold a connection slot.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections the kernel queues while the accept rate holds new ones back.
const LISTEN_BACKLOG: u32 = 4096;
/// Pause after a failed `accept` (out of descriptors, say) so the loop does
/// not spin.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(50);

/// [`Authenticator`] for this broker: the same token check as every other
/// client listener.
pub struct BrokerMqttAuth {
    auth: Arc<BrokerAuth>,
}

impl BrokerMqttAuth {
    pub fn new(auth: Arc<BrokerAuth>) -> Self {
        Self { auth }
    }
}

#[async_trait]
impl Authenticator for BrokerMqttAuth {
    async fn authenticate(
        &self,
        tenant_id: &str,
        token: &str,
        peer_certs: &[Vec<u8>],
    ) -> Result<Principal, AuthError> {
        let certs: Vec<rustls::pki_types::CertificateDer<'_>> = peer_certs
            .iter()
            .map(|der| rustls::pki_types::CertificateDer::from(der.as_slice()))
            .collect();
        let peer = (!certs.is_empty()).then_some(certs.as_slice());
        match self.auth.authenticate_peer(tenant_id, token, peer).await {
            Ok(context) => {
                let revocable = context.revocable;
                Ok(Principal::new(tenant_id)
                    .with_expiry(context.expires_at)
                    .with_revocation(move || {
                        let revocable = revocable.clone();
                        Box::pin(async move { revocable.revoked().await })
                    }))
            }
            Err(err) if err.is::<Revoked>() => {
                Err(AuthError::Rejected("credential revoked".to_string()))
            }
            Err(err) => {
                if let Some(authz) = err.downcast_ref::<AuthzError>() {
                    return Err(AuthError::Rejected(format!("token rejected: {authz}")));
                }
                if let Some(refused) = err.downcast_ref::<CredentialRefused>() {
                    return Err(AuthError::Rejected(refused.to_string()));
                }
                // The control plane being unreachable. Its error text can
                // carry its URL, so it is logged and not sent to the client.
                // Debug, not warn: a client can make this fire at the connect
                // rate, and `auth_unavailable` is the counter to alert on.
                tracing::debug!(error = %err, "mqtt authentication could not verify a token");
                Err(AuthError::Unavailable)
            }
        }
    }
}

/// A bound MQTT listener, not yet accepting.
pub struct MqttListener {
    runtime: OwnedRuntime,
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    service: MqttService,
    max_connections: usize,
    per_ip: Arc<PerIpLimiter>,
    connect_rate: u32,
}

impl MqttListener {
    /// Bind the listener and build the runtime it will run on. `tls` is
    /// required when the config asks for TLS.
    pub fn bind(
        config: &MqttListenerConfig,
        tls: Option<Arc<rustls::ServerConfig>>,
        auth: Arc<dyn Authenticator>,
    ) -> Result<Self> {
        let tls = match (config.tls, tls) {
            (true, Some(server)) => Some(TlsAcceptor::from(server)),
            (true, None) => anyhow::bail!("FELIX_MQTT_TLS is on but no certificate was given"),
            (false, _) => None,
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads)
            .thread_name("felix-mqtt")
            .enable_all()
            .build()
            .context("build the MQTT runtime")?;
        // Owned from here, so a failed bind drops it without panicking inside
        // the broker's own runtime.
        let runtime = OwnedRuntime(Some(runtime));
        // The socket registers with the runtime that will accept on it.
        let listener = {
            let _enter = runtime.handle().enter();
            let socket = if config.listen.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }?;
            socket.set_reuseaddr(true)?;
            socket
                .bind(config.listen)
                .and_then(|()| socket.listen(LISTEN_BACKLOG))
                .with_context(|| format!("bind the MQTT listener on {}", config.listen))?
        };
        Ok(Self {
            runtime,
            listener,
            tls,
            service: MqttService::new(auth, config.settings.clone()),
            max_connections: config.max_connections,
            per_ip: PerIpLimiter::new(config.max_connections_per_ip),
            connect_rate: config.connect_rate,
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Start accepting on the listener's own runtime and return its accept
    /// task. Connections are served there too, tracked on `connections` so a
    /// drain waits for them; the runtime is shut down once they are done.
    /// `ready` is waited for first, when given, like the other listeners wait
    /// for the broker to know its streams.
    pub fn spawn(
        self,
        shutdown: CancellationToken,
        connections: TaskTracker,
        ready: Option<CancellationToken>,
    ) -> tokio::task::JoinHandle<()> {
        let Self {
            runtime,
            listener,
            tls,
            service,
            max_connections,
            per_ip,
            connect_rate,
        } = self;
        let accept = Accept {
            listener,
            tls,
            service,
            slots: Arc::new(Semaphore::new(max_connections)),
            max_connections,
            per_ip,
            rate: RateGate::new(connect_rate),
        };
        let task = runtime.handle().spawn({
            let shutdown = shutdown.clone();
            let connections = connections.clone();
            async move {
                if let Some(ready) = ready {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return,
                        _ = ready.cancelled() => {}
                    }
                }
                accept.serve(shutdown, connections).await;
            }
        });
        // The runtime lives until every connection it carries is done. Dropped
        // from any context this only signals the threads to stop.
        tokio::spawn(async move {
            connections.wait().await;
            drop(runtime);
        });
        task
    }
}

/// Shuts the runtime down without waiting, which is safe to do from inside
/// another runtime's task, where a plain drop would panic.
struct OwnedRuntime(Option<Runtime>);

impl OwnedRuntime {
    fn handle(&self) -> &tokio::runtime::Handle {
        self.0
            .as_ref()
            .expect("runtime is held until dropped")
            .handle()
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

struct Accept {
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    service: MqttService,
    slots: Arc<Semaphore>,
    max_connections: usize,
    per_ip: Arc<PerIpLimiter>,
    rate: RateGate,
}

impl Accept {
    async fn serve(mut self, shutdown: CancellationToken, connections: TaskTracker) {
        loop {
            // Held back before `accept`, so a storm waits in the kernel's
            // queue and not in this process.
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = self.rate.admit() => {}
            }
            let accepted = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                accepted = self.listener.accept() => accepted,
            };
            let (socket, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    tracing::warn!(error = %err, "mqtt accept failed");
                    tokio::time::sleep(ACCEPT_ERROR_PAUSE).await;
                    continue;
                }
            };
            // Per address first, so one host at its cap is refused without
            // taking a slot from the shared pool even briefly.
            let Some(per_ip) = self.per_ip.try_acquire(peer.ip()) else {
                tracing::debug!(%peer, max = self.per_ip.max(), "mqtt per-address connection limit reached; closing");
                metrics::counter!("felix_mqtt_refused_total", "reason" => "per_ip_limit")
                    .increment(1);
                continue;
            };
            let Ok(slot) = Arc::clone(&self.slots).try_acquire_owned() else {
                tracing::warn!(%peer, max = self.max_connections, "mqtt connection limit reached; closing");
                metrics::counter!("felix_mqtt_refused_total", "reason" => "connection_limit")
                    .increment(1);
                continue;
            };
            let _ = socket.set_nodelay(true);
            let tls = self.tls.clone();
            let service = self.service.clone();
            let shutdown = shutdown.clone();
            connections.spawn(async move {
                let _slot = slot;
                let _per_ip = per_ip;
                match tls {
                    None => {
                        service
                            .serve_connection(socket, Vec::new(), shutdown)
                            .await;
                    }
                    Some(tls) => {
                        let handshake = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => return,
                            handshake = tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, tls.accept(socket)) => handshake,
                        };
                        match handshake {
                            Ok(Ok(stream)) => {
                                // Kept for CONNECT, which binds the token to
                                // the certificate when the broker is set to.
                                let peer_certs = stream
                                    .get_ref()
                                    .1
                                    .peer_certificates()
                                    .map(|chain| chain.iter().map(|der| der.to_vec()).collect())
                                    .unwrap_or_default();
                                service.serve_connection(stream, peer_certs, shutdown).await;
                            }
                            Ok(Err(err)) => {
                                tracing::debug!(%peer, error = %err, "mqtt TLS handshake failed");
                                metrics::counter!("felix_mqtt_refused_total", "reason" => "tls_handshake")
                                    .increment(1);
                            }
                            Err(_) => {
                                tracing::debug!(%peer, "mqtt TLS handshake timed out");
                                metrics::counter!("felix_mqtt_refused_total", "reason" => "tls_handshake")
                                    .increment(1);
                            }
                        }
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests;

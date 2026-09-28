//! Who may speak Raft to this member, and how this member proves it is one.
//!
//! The Raft routes can replace the entire metadata store (`propose` of an
//! `ImportState`), so they are served on their own listener and every request
//! must carry the cluster's peer token and cluster id. The token is the
//! cluster-admin credential: peers, and the operator's migration tool, hold
//! it; brokers and API clients never do. Optional mTLS on top means a
//! connection that cannot present a certificate from the cluster CA never
//! reaches the router at all.
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Carries the sender's cluster id on every peer request.
pub const CLUSTER_ID_HEADER: &str = "x-felix-raft-cluster-id";

/// How this member authenticates Raft traffic, both directions.
#[derive(Clone)]
pub struct PeerSecurity {
    /// Names the group. A member only talks to peers that name the same one,
    /// and refuses to start on a data dir recorded under another.
    pub cluster_id: String,
    /// The shared peer secret. `None` only when the operator explicitly opted
    /// out (`FELIX_RAFT_INSECURE_PEERS`); config validation enforces that.
    pub token: Option<String>,
    /// mTLS for the peer listener and the peer client.
    pub tls: Option<PeerTls>,
}

impl std::fmt::Debug for PeerSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerSecurity")
            .field("cluster_id", &self.cluster_id)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("tls", &self.tls)
            .finish()
    }
}

/// PEM files for peer mTLS. Every member presents `cert_path` as both server
/// and client, and trusts only certificates signed by `ca_path`.
#[derive(Debug, Clone)]
pub struct PeerTls {
    pub cert_path: String,
    pub key_path: String,
    pub ca_path: String,
}

impl PeerSecurity {
    /// URL scheme for reaching a peer.
    pub fn scheme(&self) -> &'static str {
        if self.tls.is_some() { "https" } else { "http" }
    }

    /// An HTTP client that authenticates as a peer on every request.
    pub fn client(&self, timeout: Duration) -> Result<reqwest::Client> {
        let mut headers = HeaderMap::new();
        headers.insert(
            CLUSTER_ID_HEADER,
            HeaderValue::from_str(&self.cluster_id).context("cluster id is not a valid header")?,
        );
        if let Some(token) = &self.token {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("peer token is not a valid header")?;
            value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, value);
        }
        let mut builder = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(headers);
        if let Some(tls) = &self.tls {
            let ca = std::fs::read(&tls.ca_path)
                .with_context(|| format!("read raft peer CA {}", tls.ca_path))?;
            let mut identity = std::fs::read(&tls.cert_path)
                .with_context(|| format!("read raft peer certificate {}", tls.cert_path))?;
            identity.push(b'\n');
            identity.extend(
                std::fs::read(&tls.key_path)
                    .with_context(|| format!("read raft peer key {}", tls.key_path))?,
            );
            // Only the cluster CA: a peer certificate from a public CA must not
            // be enough to receive the log.
            builder = builder.tls_certs_only(
                reqwest::Certificate::from_pem_bundle(&ca).context("parse raft peer CA")?,
            );
            builder = builder.identity(
                reqwest::Identity::from_pem(&identity).context("parse raft peer identity")?,
            );
        }
        builder.build().context("build raft peer client")
    }

    /// Server-side TLS for the peer listener, when mTLS is configured.
    pub(super) fn server_tls(&self) -> Result<Option<std::sync::Arc<rustls::ServerConfig>>> {
        self.tls
            .as_ref()
            .map(|tls| {
                crate::server::tls::load_server_config(&crate::config::BootstrapTlsConfig {
                    cert_path: tls.cert_path.clone(),
                    key_path: tls.key_path.clone(),
                    client_ca_path: tls.ca_path.clone(),
                })
                .context("raft peer TLS")
            })
            .transpose()
    }
}

/// Refuse any request that does not name this cluster and carry the token.
///
/// The cluster id is checked first and answered with 403 rather than 401:
/// a peer from another group holds a valid token for *its* group, and the
/// operator reading the log needs to know the members are cross-wired rather
/// than that a secret is wrong.
pub(super) async fn require_peer(
    State(security): State<std::sync::Arc<PeerSecurity>>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    let cluster = headers
        .get(CLUSTER_ID_HEADER)
        .and_then(|value| value.to_str().ok());
    if cluster != Some(security.cluster_id.as_str()) {
        metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => "cluster_id")
            .increment(1);
        tracing::warn!(
            offered = cluster.unwrap_or("<none>"),
            expected = %security.cluster_id,
            "raft peer request for another cluster refused"
        );
        return (StatusCode::FORBIDDEN, "wrong raft cluster id").into_response();
    }
    if let Some(expected) = &security.token {
        let offered = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or("");
        if !crate::api::bootstrap::constant_time_eq(offered.as_bytes(), expected.as_bytes()) {
            metrics::counter!("felix_meta_raft_peer_rejected_total", "reason" => "token")
                .increment(1);
            tracing::warn!("raft peer request with a missing or wrong peer token refused");
            return (StatusCode::UNAUTHORIZED, "raft peer token required").into_response();
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests;

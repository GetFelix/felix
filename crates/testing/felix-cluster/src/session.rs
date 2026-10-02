//! How a second terminal finds a running cluster.
//!
//! `felix-cluster up` holds a cluster in one process. Everything it needs to be
//! talked to — the broker addresses and a credential — lives in that process and
//! nowhere else, so a `publish` or `subscribe` in another window has nothing to
//! connect to. This writes it down.
//!
//! **The file contains a bearer token**, so it is written with owner-only
//! permissions and lives in the temp directory. It is a local development
//! harness whose brokers listen on loopback with dev certificates; the token is
//! no more sensitive than the cluster it opens, and that cluster disappears when
//! the process does.
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// A running cluster, as another process needs to see it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub control_plane: String,
    pub tenant_id: String,
    pub namespace: String,
    /// Publishes and subscribes. Stream permissions only.
    pub client_token: String,
    /// Reads membership and shard ownership, so a client can say which broker
    /// owns what.
    pub admin_token: String,
    pub nodes: Vec<SessionNode>,
}

impl Session {
    /// Write the session to `path`, readable by its owner only.
    pub fn write(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec_pretty(self).context("encode session")?;
        std::fs::write(path, body).with_context(|| format!("write {}", path.display()))?;
        restrict(path)?;
        Ok(())
    }

    /// Read a session written by [`Session::write`].
    pub fn read(path: &Path) -> Result<Self> {
        let body = std::fs::read(path).with_context(|| {
            format!(
                "no cluster session at {} -- start one with `task cluster:up`",
                path.display()
            )
        })?;
        serde_json::from_slice(&body).context("decode session")
    }

    /// The broker named `node_id`, if the session lists one.
    pub fn node(&self, node_id: &str) -> Option<&SessionNode> {
        self.nodes.iter().find(|node| node.node_id == node_id)
    }

    /// Remove the session file, but only if it still describes this cluster.
    ///
    /// Two clusters share one path, so the second to start overwrites the
    /// first's session. Without this check the second to *stop* then deletes a
    /// file describing a cluster that is still running, leaving it alive and
    /// unreachable — the CLI reports no session while three brokers keep
    /// serving.
    pub fn remove_if_ours(&self, path: &Path) {
        match Self::read(path) {
            Ok(current) if current.control_plane == self.control_plane => {
                let _ = std::fs::remove_file(path);
            }
            // Someone else's, or already gone. Either way not ours to delete.
            _ => {}
        }
    }
}

/// One broker in a [`Session`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionNode {
    pub node_id: String,
    pub client_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    /// The broker's generated certificate, for a client to trust.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
}

/// Where `up` leaves the file and the other commands look for it.
///
/// A fixed path, so a second window needs nothing copied into it.
pub fn default_path() -> PathBuf {
    std::env::temp_dir().join("felix-cluster.json")
}

/// A session already on disk whose cluster is still answering.
///
/// Starting a second cluster is allowed — the tests do it — but it takes over
/// the session file, and the first cluster becomes unreachable through the CLI
/// while still holding its ports. Worth saying out loud rather than letting it
/// be discovered.
pub async fn live_session(path: &Path) -> Option<Session> {
    let session = Session::read(path).ok()?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .no_proxy()
        .build()
        .ok()?;
    let url = format!("{}/v1/nodes", session.control_plane);
    let response = client
        .get(&url)
        .bearer_auth(&session.admin_token)
        .send()
        .await
        .ok()?;
    response.status().is_success().then_some(session)
}

/// Owner-only, because the file holds a bearer token.
#[cfg(unix)]
fn restrict(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict {}", path.display()))
}

#[cfg(not(unix))]
fn restrict(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests;

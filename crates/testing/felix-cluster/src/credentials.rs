//! The credentials the harness presents, minted when they are used.
//!
//! The harness holds the tenant's signing keys, so it never has to keep a token
//! around. A token minted once at startup expires while a long-running
//! `felix-cluster up` is still holding the cluster, and from then on every call
//! the harness and its brokers make to the control plane is refused, teardown
//! included.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use felix_controlplane_service::auth::felix_token::{
    BROKER_AUDIENCE, CONTROLPLANE_AUDIENCE, TenantSigningKeys, mint_token_for,
};
use tokio_util::task::AbortOnDropHandle;

/// How long each minted token lives.
pub const TOKEN_TTL: Duration = Duration::from_secs(3600);

/// Mints every credential the harness, its brokers and its clients use.
#[derive(Clone)]
pub struct Credentials {
    keys: TenantSigningKeys,
    tenant_id: String,
    ttl: Duration,
    /// Each broker's token file, rewritten by [`Self::renew_node_tokens`].
    node_token_files: Arc<Mutex<BTreeMap<String, PathBuf>>>,
}

impl Credentials {
    pub(crate) fn new(keys: TenantSigningKeys, tenant_id: &str, ttl: Duration) -> Self {
        Self {
            keys,
            tenant_id: tenant_id.to_string(),
            ttl,
            node_token_files: Default::default(),
        }
    }

    /// How long a token minted now stays valid.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// How often anything holding a token on disk should replace it: half a
    /// lifetime, so a missed renewal still leaves time for the next.
    pub fn renew_interval(&self) -> Duration {
        self.ttl / 2
    }

    /// Presented to brokers over QUIC: publish, subscribe, read and write caches.
    ///
    /// Stream and cache permissions only. A broker rejects a whole token if one
    /// of its actions is not client-facing.
    pub fn client_token(&self) -> String {
        let tenant_id = &self.tenant_id;
        self.mint(
            "p:harness-client",
            vec![
                format!("stream.publish:stream:{tenant_id}/*/*"),
                format!("stream.subscribe:stream:{tenant_id}/*/*"),
                format!("cache.read:cache:{tenant_id}/*/*"),
                format!("cache.write:cache:{tenant_id}/*/*"),
            ],
            BROKER_AUDIENCE,
        )
    }

    /// Presented to the control plane's HTTP API: create metadata, and read
    /// membership and ownership.
    pub fn admin_token(&self) -> String {
        let tenant_id = &self.tenant_id;
        self.mint(
            "p:harness-admin",
            vec![
                format!("tenant.manage:tenant:{tenant_id}"),
                format!("ns.manage:namespace:{tenant_id}/*"),
                format!("stream.manage:stream:{tenant_id}/*/*"),
                format!("cache.manage:cache:{tenant_id}/*/*"),
                "node.view:cluster:*".to_string(),
            ],
            CONTROLPLANE_AUDIENCE,
        )
    }

    /// [`Self::admin_token`], plus listing every tenant, which needs
    /// `tenant.manage` over the whole cluster rather than one tenant.
    pub fn cluster_admin_token(&self) -> String {
        let tenant_id = &self.tenant_id;
        self.mint(
            "p:harness-cluster-admin",
            vec![
                "tenant.manage:cluster:*".to_string(),
                format!("ns.manage:namespace:{tenant_id}/*"),
                format!("stream.manage:stream:{tenant_id}/*/*"),
                format!("cache.manage:cache:{tenant_id}/*/*"),
                "node.view:cluster:*".to_string(),
            ],
            CONTROLPLANE_AUDIENCE,
        )
    }

    /// Changes any node's membership, and reads it. Draining a broker is a
    /// write, which [`Self::admin_token`] cannot do.
    pub fn operator_token(&self) -> String {
        self.mint(
            "p:harness-operator",
            vec![
                "node.manage:cluster:*".to_string(),
                "node.view:cluster:*".to_string(),
            ],
            CONTROLPLANE_AUDIENCE,
        )
    }

    /// Presented to brokers by an operator inspecting what they hold
    /// (`shard_inspect`). Cluster scope, and nothing a tenant could grant.
    pub fn inspector_token(&self) -> String {
        self.mint(
            "p:harness-inspector",
            vec!["node.view:cluster:*".to_string()],
            BROKER_AUDIENCE,
        )
    }

    /// Redrives and discards dead letters, which a consumer's
    /// `stream.subscribe` does not allow.
    pub fn group_operator_token(&self) -> String {
        let tenant_id = &self.tenant_id;
        self.mint(
            "p:harness-group-operator",
            vec![format!("group.manage:stream:{tenant_id}/*/*")],
            BROKER_AUDIENCE,
        )
    }

    /// May subscribe, may not publish. For asserting that a forwarded publish
    /// is still authorized at the owner.
    pub fn subscribe_only_token(&self) -> String {
        let tenant_id = &self.tenant_id;
        self.mint(
            "p:harness-reader",
            vec![format!("stream.subscribe:stream:{tenant_id}/*/*")],
            BROKER_AUDIENCE,
        )
    }

    /// One broker's credential: manage its own membership, and read the
    /// cluster's assignments and node catalog.
    ///
    /// Scoped to `node:{node_id}`, as a deployment would scope it, so a broker
    /// presenting it for another node is refused.
    pub fn node_token(&self, node_id: &str) -> String {
        self.mint(
            &format!("p:{node_id}"),
            vec![
                format!("node.manage:node:{node_id}"),
                "node.view:cluster:*".to_string(),
            ],
            CONTROLPLANE_AUDIENCE,
        )
    }

    /// Write `node_id`'s token to `path` and keep it current there.
    ///
    /// The broker re-reads the file on a timer, so rewriting it is how a
    /// broker that outlives one token gets the next.
    pub(crate) fn write_node_token(&self, node_id: &str, path: &Path) -> Result<()> {
        std::fs::write(path, self.node_token(node_id))
            .with_context(|| format!("write node token to {}", path.display()))?;
        self.node_token_files
            .lock()
            .expect("node token files lock")
            .insert(node_id.to_string(), path.to_path_buf());
        Ok(())
    }

    /// Rewrite every broker's token file with a fresh token.
    pub(crate) fn renew_node_tokens(&self) {
        let files = self
            .node_token_files
            .lock()
            .expect("node token files lock")
            .clone();
        for (node_id, path) in files {
            // A broker removed for good takes its data directory with it.
            if let Err(err) = std::fs::write(&path, self.node_token(&node_id)) {
                tracing::debug!(node_id, error = %err, "could not renew node token");
            }
        }
    }

    /// Renew every broker's token file each [`Self::renew_interval`], until
    /// the handle is dropped.
    pub(crate) fn spawn_node_token_renewal(&self) -> AbortOnDropHandle<()> {
        let credentials = self.clone();
        AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                tokio::time::sleep(credentials.renew_interval()).await;
                credentials.renew_node_tokens();
            }
        }))
    }

    fn mint(&self, principal_id: &str, perms: Vec<String>, audience: &str) -> String {
        // Signs with keys generated and validated when the control plane
        // started, so a failure here is a bug rather than a condition.
        mint_token_for(
            &self.keys,
            &self.tenant_id,
            principal_id,
            perms,
            self.ttl,
            audience,
        )
        .unwrap_or_else(|err| panic!("mint {principal_id} token: {err}"))
    }
}

#[cfg(test)]
mod tests;

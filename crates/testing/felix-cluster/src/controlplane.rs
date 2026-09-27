//! The control plane the harness runs.
//!
//! In process, on a real TCP port, so the brokers it starts reach it over HTTP
//! exactly as they would in a deployment. It is in process for one reason: a
//! broker needs a credential, and the only way to obtain one today is an OIDC
//! token exchange against a real identity provider. Holding the store here lets
//! the harness mint node and client tokens directly with the tenant's signing
//! keys — the same thing `services/felix-broker-service/tests/membership_lifecycle.rs` does,
//! and the same reason.
//!
//! The consequence is worth stating plainly: **the control plane is not under
//! test as a process.** Its router, store, placement, and HTTP contract all are.
//! What is not exercised is its `main`, its own configuration, and its shutdown.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::felix_token::TenantSigningKeys;
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};
use tokio_util::sync::CancellationToken;

use crate::ports;

/// Liveness tuned for a harness: brokers are local, so a lapsed heartbeat means
/// a broker that actually stopped rather than a slow network. Short windows make
/// a stopped broker observable in seconds instead of tens of seconds, which is
/// what failure tests wait on.
const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 200,
    expiry_timeout_ms: 1_000,
    sweep_interval_ms: 100,
    // Placement is driven explicitly by the harness, so this only matters as a
    // backstop for anything the harness does not step itself.
    shard_reconcile_interval_ms: 500,
};

/// A running control plane, and the keys to mint credentials against it.
pub struct ControlPlane {
    pub base_url: String,
    pub store: Arc<InMemoryStore>,
    keys: TenantSigningKeys,
    placement_wakes: Arc<felix_controlplane_service::cluster::placement::PlacementWakes>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl ControlPlane {
    /// Start the control plane, holding the signing keys it will issue against.
    pub async fn start(tenant_id: &str) -> Result<Self> {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(10_000),
        }));
        let keys = felix_controlplane_service::auth::keys::generate_signing_keys()
            .context("generate tenant signing keys")?;
        store
            .create_tenant(felix_controlplane_service::model::Tenant {
                tenant_id: tenant_id.to_string(),
                display_name: tenant_id.to_string(),
            })
            .await
            .context("seed tenant")?;
        store
            .set_tenant_signing_keys(tenant_id, keys.clone())
            .await
            .context("seed tenant signing keys")?;

        let addr = ports::free_tcp()?;
        Self::serve(store, keys, addr).await
    }

    /// Stop this control plane, stay down for `downtime`, and start a new one
    /// on the same address over the same store.
    ///
    /// The new instance is a restart in every sense brokers can see: their
    /// connections are cut, requests fail while it is down, and its expiry
    /// sweep starts fresh against heartbeat stamps as old as the outage.
    pub async fn restart(self, downtime: Duration) -> Result<Self> {
        let store = Arc::clone(&self.store);
        let keys = self.keys.clone();
        let addr = self
            .base_url
            .trim_start_matches("http://")
            .parse()
            .context("parse control plane address")?;
        self.shutdown().await;
        tokio::time::sleep(downtime).await;
        Self::serve(store, keys, addr).await
    }

    /// Serve `store` on `addr`, with the expiry sweep running.
    async fn serve(
        store: Arc<InMemoryStore>,
        keys: TenantSigningKeys,
        addr: std::net::SocketAddr,
    ) -> Result<Self> {
        let state = AppState {
            region: Region {
                region_id: "local".to_string(),
                display_name: "Local".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: false,
                tiered_storage: false,
                bridges: false,
            },
            store: Arc::clone(&store)
                as Arc<dyn felix_controlplane_service::store::ControlPlaneAuthStore + Send + Sync>,
            oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(
            ),
            bootstrap_enabled: false,
            bootstrap_tokens: Vec::new(),
            node_liveness: LIVENESS,
            readiness: std::sync::Arc::new(
                felix_controlplane_service::api::readiness::Readiness::new(std::sync::Arc::new(
                    felix_controlplane_service::api::readiness::AlwaysReady,
                )),
            ),
            in_flight: Default::default(),
            placement_wakes: Default::default(),
            move_policy: Default::default(),
        };
        let placement_wakes = Arc::clone(&state.placement_wakes);

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind control plane on {addr}"))?;
        let addr = listener
            .local_addr()
            .context("read control plane address")?;
        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state).into_make_service())
                .with_graceful_shutdown(async move { serve_shutdown.cancelled().await })
                .await;
        });

        // Expiry has to run, or a stopped broker stays `live` forever and no
        // failure test can observe it leaving.
        let expiry = felix_controlplane_service::cluster::membership::spawn_expiry_sweep(
            Arc::clone(&store)
                as Arc<dyn felix_controlplane_service::store::ControlPlaneStore + Send + Sync>,
            LIVENESS,
            felix_controlplane_service::raft::LeadershipGate::Always,
            shutdown.clone(),
        );
        // Owned by the same token; nothing waits on it separately.
        drop(expiry);

        Ok(Self {
            base_url: format!("http://{addr}"),
            store,
            keys,
            placement_wakes,
            shutdown,
            task,
        })
    }

    /// Create `tenant_id` and bind this control plane's signing keys to it.
    ///
    /// Straight into the store rather than through the API: creating a tenant
    /// over HTTP takes an operator credential, and an operator is minted from
    /// a tenant -- the one this creates. Keys go on after the row, because
    /// creation resets them, and every token minted here verifies against
    /// whatever the store holds when the request arrives.
    pub async fn seed_tenant(&self, tenant_id: &str) -> Result<()> {
        match self
            .store
            .create_tenant(felix_controlplane_service::model::Tenant {
                tenant_id: tenant_id.to_string(),
                display_name: tenant_id.to_string(),
            })
            .await
        {
            Ok(_) | Err(felix_controlplane_service::store::StoreError::Conflict(_)) => {}
            Err(err) => return Err(err).context("seed tenant"),
        }
        self.store
            .set_tenant_signing_keys(tenant_id, self.keys.clone())
            .await
            .context("seed tenant signing keys")
    }

    /// A credential for one broker: manage its own membership, and read the
    /// cluster's assignments and node catalog.
    ///
    /// Scoped to `node:{node_id}` on purpose, which is what a real deployment
    /// would hand it — a broker presenting this for another node is refused.
    pub fn node_token(&self, tenant_id: &str, node_id: &str) -> Result<String> {
        felix_controlplane_service::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            &format!("p:{node_id}"),
            vec![
                format!("node.manage:node:{node_id}"),
                // Assignments and `/v1/nodes` both require this; without it a
                // broker cannot learn who owns what, or where they are.
                "node.view:cluster:*".to_string(),
            ],
            Duration::from_secs(3600),
        )
        .context("mint node token")
    }

    /// A credential for a client publishing and subscribing in `tenant_id`.
    ///
    /// Stream permissions only. A broker validates every action in a token it is
    /// presented, and rejects the whole token if one is not a client-facing
    /// action — so a cluster-scoped permission here would make this credential
    /// unusable for its actual purpose.
    pub fn client_token(&self, tenant_id: &str) -> Result<String> {
        felix_controlplane_service::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:harness-client",
            vec![
                format!("stream.publish:stream:{tenant_id}/*/*"),
                format!("stream.subscribe:stream:{tenant_id}/*/*"),
                format!("cache.read:cache:{tenant_id}/*/*"),
                format!("cache.write:cache:{tenant_id}/*/*"),
            ],
            Duration::from_secs(3600),
        )
        .context("mint client token")
    }

    /// A credential for the harness itself, against the control plane's HTTP
    /// API: create metadata, and read membership and ownership.
    ///
    /// Separate from [`Self::client_token`] because the two are presented to
    /// different services, which accept different actions.
    pub fn admin_token(&self, tenant_id: &str) -> Result<String> {
        felix_controlplane_service::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:harness-admin",
            vec![
                format!("tenant.manage:tenant:{tenant_id}"),
                format!("ns.manage:namespace:{tenant_id}/*"),
                format!("stream.manage:stream:{tenant_id}/*/*"),
                format!("cache.manage:cache:{tenant_id}/*/*"),
                "node.view:cluster:*".to_string(),
            ],
            Duration::from_secs(3600),
        )
        .context("mint admin token")
    }

    /// A credential that may operate consumer groups: redrive and discard dead
    /// letters, which a consumer's `stream.subscribe` does not allow.
    pub fn group_operator_token(&self, tenant_id: &str) -> Result<String> {
        felix_controlplane_service::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:harness-group-operator",
            vec![format!("group.manage:stream:{tenant_id}/*/*")],
            Duration::from_secs(3600),
        )
        .context("mint group operator token")
    }

    /// A credential that may subscribe but not publish.
    ///
    /// For asserting that a forwarded publish is still authorized: a check done
    /// only at the ingress broker, or done there and then trusted by the owner,
    /// would let this through.
    pub fn subscribe_only_token(&self, tenant_id: &str) -> Result<String> {
        felix_controlplane_service::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:harness-reader",
            vec![format!("stream.subscribe:stream:{tenant_id}/*/*")],
            Duration::from_secs(3600),
        )
        .context("mint subscribe-only token")
    }

    /// A credential that can change any node's membership.
    ///
    /// Separate from [`Self::admin_token`], which only reads: draining a broker
    /// is a write, and the control plane requires `node.manage` over a scope
    /// containing the node. A test that moves a shard needs this; nothing else
    /// should. It also reads, so a test can list the moves it made.
    pub fn operator_token(&self, tenant_id: &str) -> Result<String> {
        felix_controlplane_service::auth::felix_token::mint_token(
            &self.keys,
            tenant_id,
            "p:harness-operator",
            vec![
                "node.manage:cluster:*".to_string(),
                "node.view:cluster:*".to_string(),
            ],
            Duration::from_secs(3600),
        )
        .context("mint operator token")
    }

    /// Place any unassigned shard onto a live broker.
    ///
    /// Driven explicitly rather than waited for: the reconciler runs on a timer,
    /// and a harness that slept for one would be timing-dependent in exactly the
    /// way the acceptance criteria rule out.
    ///
    /// Reads the reports leaders sent to the API from the same store, judged
    /// by this harness's own liveness settings: the report TTL is derived from
    /// them, and the defaults would give a report a 20-second life against a
    /// cluster tuned to notice a dead broker in one.
    pub async fn place_shards(
        &self,
    ) -> felix_controlplane_service::cluster::placement::ReconcileOutcome {
        self.place_shards_with(
            felix_controlplane_service::cluster::placement::MovePolicy::default(),
        )
        .await
    }

    /// Step placement once under an explicit move policy.
    pub async fn place_shards_with(
        &self,
        policy: felix_controlplane_service::cluster::placement::MovePolicy,
    ) -> felix_controlplane_service::cluster::placement::ReconcileOutcome {
        felix_controlplane_service::cluster::placement::reconcile_once(
            self.store.as_ref(),
            &LIVENESS,
            policy,
        )
        .await
    }

    /// Run placement the way a deployment does: on `interval`, and whenever a
    /// report a move waits on arrives. Off unless a test asks, because most
    /// tests step placement themselves.
    pub fn run_placement(&self, interval: Duration) {
        drop(
            felix_controlplane_service::cluster::placement::spawn_reconciler(
                Arc::clone(&self.store)
                    as Arc<dyn felix_controlplane_service::store::ControlPlaneStore + Send + Sync>,
                LIVENESS,
                felix_controlplane_service::cluster::placement::MovePolicy::default(),
                interval,
                "felix-cluster".to_string(),
                felix_controlplane_service::raft::LeadershipGate::Always,
                Arc::clone(&self.placement_wakes),
                self.shutdown.clone(),
            ),
        );
    }

    /// Stop serving and cut every connection brokers hold to it.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        // Aborted, not drained. A graceful shutdown keeps serving the keep-alive
        // connections brokers already hold, so their heartbeats would go on
        // succeeding and the control plane would not be gone in any sense a
        // broker could detect -- which is the whole point when this is used as a
        // fault rather than a teardown.
        self.task.abort();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.task).await;
    }
}

//! The control plane, in process, for the simulator.
//!
//! [`SimControlPlane`] runs the real control plane router over the in-memory
//! store and answers requests by calling the router directly, with no socket.
//! Brokers reach it through [`SimLink`], which sends the same requests
//! `HttpControlPlaneLink` does.
//!
//! The router has no switch to turn auth off, so the sim holds an operator
//! token of its own and puts it on every request. Whatever bearer a caller
//! passes is ignored.
//!
//! Faults ([`CpFault`]) can be set at any time and apply to the next call:
//!
//! - `Slow(d)`: the call sleeps `d` on tokio time before it reaches the router.
//! - `Paused`: the call parks. It stays parked while the fault holds and
//!   proceeds once the fault clears, as a resumed process answers a request
//!   it had already read. A caller with a timeout gives up first, as it would
//!   on a hung process; the router never sees that call.
//! - `Down`: the call fails at once, as a refused connection does. Calls
//!   parked by `Paused` fail too, as a process that dies drops its sockets.
//!   Going down also forgets the expiry sweep's memory, so a control plane
//!   that comes back waits a full window before expiring anyone, as a
//!   restarted one does.
//!
//! The sim does not spawn the control plane's background loops. The harness
//! calls [`SimControlPlane::step`] at [`CpConfig::sweep_interval`] and
//! [`CpConfig::reconcile_interval`] (or [`sweep`](SimControlPlane::sweep) and
//! [`reconcile`](SimControlPlane::reconcile) on their own cadences), and runs
//! a pass whenever [`SimControlPlane::pass_requested`] resolves. A paused or
//! down control plane is not stepped. Placement writes wake the
//! `assignment_changes` long-polls, as the real loop's do.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::Router;
use axum::http::{Method, Request, header};
use felix_common::fleet;
use felix_controlplane_service::api::readiness::{AlwaysReady, Readiness};
use felix_controlplane_service::api::types::{
    CacheCreateRequest, FeatureFlags, FleetFeaturesResponse, FleetFinalizeResponse,
    NamespaceCreateRequest, NodeRegistrationRequest, Region, ShardAssignmentSnapshotResponse,
    StreamCreateRequest, TenantCreateRequest,
};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::felix_token::{CONTROLPLANE_AUDIENCE, mint_token_for};
use felix_controlplane_service::cluster::membership::{SilenceWatch, SweepGrace, expire_observed};
use felix_controlplane_service::cluster::placement::{
    MovePolicy, PlacementWakes, ReconcileOutcome, reconcile_once_waking,
};
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, Stream, StreamKind,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};
use felix_replication::controlplane_link::{ControlPlaneLink, LinkResponse};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::watch;
use tower::ServiceExt;

/// The tenant the sim's own operator token belongs to.
const OPERATOR_TENANT: &str = "sim-operator";
/// Long enough that no run outlives it. Tokens are checked against the real
/// clock, not simulated time.
const TOKEN_TTL: Duration = Duration::from_secs(10 * 365 * 24 * 3600);

/// How the control plane is behaving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpFault {
    Healthy,
    /// Every call waits this long, then goes through.
    Slow(Duration),
    /// Every call parks until the fault changes.
    Paused,
    /// Every call fails at once.
    Down,
}

/// The control plane's own timing and placement settings.
#[derive(Debug, Clone, Default)]
pub struct CpConfig {
    pub liveness: NodeLivenessConfig,
    pub moves: MovePolicy,
}

impl CpConfig {
    /// How often the real control plane sweeps for silent nodes.
    pub fn sweep_interval(&self) -> Duration {
        Duration::from_millis(self.liveness.sweep_interval_ms)
    }

    /// How often the real control plane reconciles shard placement.
    pub fn reconcile_interval(&self) -> Duration {
        Duration::from_millis(self.liveness.shard_reconcile_interval_ms)
    }

    /// The heartbeat cadence brokers are told to keep.
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.liveness.heartbeat_interval_ms)
    }

    /// How long a node may stay silent before the sweep may expire it.
    pub fn silence_before_down(&self) -> Duration {
        Duration::from_millis(self.liveness.silence_before_down_ms())
    }
}

/// What one [`SimControlPlane::step`] did.
#[derive(Debug, Clone)]
pub struct StepOutcome {
    /// Nodes the sweep marked down.
    pub expired: usize,
    pub placement: ReconcileOutcome,
}

/// The expiry sweep's state, which a real process keeps in memory.
struct Sweep {
    watch: SilenceWatch,
    grace: SweepGrace,
    /// The `restarts` count this state was built for.
    restarts: u64,
}

struct Inner {
    router: Router,
    store: Arc<InMemoryStore>,
    config: CpConfig,
    operator_token: String,
    fault: watch::Sender<CpFault>,
    /// Times the control plane went down, so the sweep can start over.
    restarts: AtomicU64,
    /// Shared with the router, so a placement write wakes the long-polls it
    /// serves, and a node's registration or report asks for a pass.
    wakes: Arc<PlacementWakes>,
    sweep: tokio::sync::Mutex<Sweep>,
    tenant_tokens: Mutex<BTreeMap<String, String>>,
}

/// The real control plane router over the in-memory store, with faults.
/// Cloning shares the same control plane.
#[derive(Clone)]
pub struct SimControlPlane {
    inner: Arc<Inner>,
}

impl SimControlPlane {
    pub async fn new(config: CpConfig) -> Result<Self> {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: 1000,
            change_retention_max_rows: Some(1000),
        }));
        store
            .create_tenant(felix_controlplane_service::model::Tenant {
                tenant_id: OPERATOR_TENANT.to_string(),
                display_name: "Sim operator".to_string(),
            })
            .await
            .context("create the operator tenant")?;
        let keys = felix_controlplane_service::auth::keys::generate_signing_keys()?;
        store
            .set_tenant_signing_keys(OPERATOR_TENANT, keys.clone())
            .await
            .context("store the operator keys")?;
        let operator_token = mint_token_for(
            &keys,
            OPERATOR_TENANT,
            "sim",
            [
                "node.manage:cluster:*",
                "node.view:cluster:*",
                "tenant.manage:cluster:*",
            ]
            .map(String::from)
            .to_vec(),
            TOKEN_TTL,
            CONTROLPLANE_AUDIENCE,
        )?;

        let dyn_store: Arc<
            dyn felix_controlplane_service::store::ControlPlaneAuthStore + Send + Sync,
        > = Arc::clone(&store) as _;
        let wakes = Arc::new(PlacementWakes::default());
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
            store: dyn_store,
            oidc_validator: Default::default(),
            bootstrap_enabled: false,
            bootstrap_tokens: Vec::new(),
            node_liveness: config.liveness.clone(),
            readiness: Arc::new(Readiness::new(Arc::new(AlwaysReady))),
            in_flight: Default::default(),
            placement_wakes: Arc::clone(&wakes),
            move_policy: config.moves.clone(),
            new_cluster: Default::default(),
        };
        let sweep = Sweep {
            watch: SilenceWatch::default(),
            grace: SweepGrace::new(Duration::from_millis(config.liveness.expiry_timeout_ms)),
            restarts: 0,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                router: build_router(state),
                store,
                config,
                operator_token,
                fault: watch::channel(CpFault::Healthy).0,
                restarts: AtomicU64::new(0),
                wakes,
                sweep: tokio::sync::Mutex::new(sweep),
                tenant_tokens: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    pub fn config(&self) -> &CpConfig {
        &self.inner.config
    }

    /// The store behind the router, for assertions.
    pub fn store(&self) -> &Arc<InMemoryStore> {
        &self.inner.store
    }

    /// Change how the control plane behaves from the next call on.
    pub fn set_fault(&self, fault: CpFault) {
        if fault == CpFault::Down {
            self.inner.restarts.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.fault.send_replace(fault);
    }

    pub fn fault(&self) -> CpFault {
        *self.inner.fault.borrow()
    }

    /// A link a broker can use in place of the HTTP one.
    pub fn link(&self) -> SimLink {
        SimLink { cp: self.clone() }
    }

    /// One request, through the fault gate and then the router. `Err` means no
    /// answer (down); any status is `Ok`.
    pub async fn request(
        &self,
        method: Method,
        path_and_query: &str,
        body: Option<serde_json::Value>,
    ) -> Result<LinkResponse> {
        self.send(method, path_and_query, body, &self.inner.operator_token)
            .await
    }

    async fn send(
        &self,
        method: Method,
        path_and_query: &str,
        body: Option<serde_json::Value>,
        token: &str,
    ) -> Result<LinkResponse> {
        self.gate().await?;
        let mut request = Request::builder()
            .method(method)
            .uri(path_and_query)
            .header(header::AUTHORIZATION, format!("Bearer {token}"));
        let body = match body {
            Some(body) => {
                request = request.header(header::CONTENT_TYPE, "application/json");
                axum::body::Body::from(serde_json::to_vec(&body)?)
            }
            None => axum::body::Body::empty(),
        };
        let request = request.body(body)?;
        let response = self
            .inner
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|never| match never {})?;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        Ok(LinkResponse { status, body })
    }

    /// Waits out the current fault. `Paused` re-checks whenever the fault
    /// changes, so a call parked behind a pause meets whatever replaces it.
    async fn gate(&self) -> Result<()> {
        let mut changes = self.inner.fault.subscribe();
        loop {
            let fault = *changes.borrow_and_update();
            match fault {
                CpFault::Healthy => return Ok(()),
                CpFault::Slow(delay) => {
                    tokio::time::sleep(delay).await;
                    return Ok(());
                }
                CpFault::Down => bail!("connection refused: the control plane is down"),
                CpFault::Paused => {
                    if changes.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
            }
        }
    }

    /// One expiry sweep, as the real loop runs it. `None` while paused or down.
    pub async fn sweep(&self) -> Option<usize> {
        if !self.steppable() {
            return None;
        }
        let inner = &self.inner;
        let mut sweep = inner.sweep.lock().await;
        let restarts = inner.restarts.load(Ordering::SeqCst);
        if sweep.restarts != restarts {
            *sweep = Sweep {
                watch: SilenceWatch::default(),
                grace: SweepGrace::new(Duration::from_millis(
                    inner.config.liveness.expiry_timeout_ms,
                )),
                restarts,
            };
        }
        let now = inner.store.now_millis().await.ok()?;
        let at = tokio::time::Instant::now();
        let Sweep { watch, grace, .. } = &mut *sweep;
        if !grace.may_sweep(at) {
            // Watched from the first tick, so the window has been running
            // when the grace ends.
            if let Ok(nodes) = inner.store.list_nodes().await {
                watch.observe(&nodes, at);
            }
            return Some(0);
        }
        Some(expire_observed(inner.store.as_ref(), &inner.config.liveness, now, watch, at).await)
    }

    /// One placement pass. `None` while paused or down.
    pub async fn reconcile(&self) -> Option<ReconcileOutcome> {
        if !self.steppable() {
            return None;
        }
        Some(
            reconcile_once_waking(
                self.inner.store.as_ref(),
                &self.inner.config.liveness,
                self.inner.config.moves.clone(),
                &self.inner.wakes,
            )
            .await,
        )
    }

    /// Resolves when the router asked for a placement pass, as a node
    /// registering or reporting does. The real loop runs one at once, outside
    /// its timer; the harness does the same by calling
    /// [`reconcile`](Self::reconcile) when this resolves.
    pub async fn pass_requested(&self) {
        self.inner.wakes.pass_requested().await;
    }

    /// One sweep and one placement pass. `None` while paused or down.
    pub async fn step(&self) -> Option<StepOutcome> {
        let expired = self.sweep().await?;
        let placement = self.reconcile().await?;
        Some(StepOutcome { expired, placement })
    }

    fn steppable(&self) -> bool {
        matches!(self.fault(), CpFault::Healthy | CpFault::Slow(_))
    }

    /// Create a tenant. Already existing is fine.
    pub async fn create_tenant(&self, tenant_id: &str) -> Result<()> {
        let body = TenantCreateRequest {
            tenant_id: tenant_id.to_string(),
            display_name: tenant_id.to_string(),
        };
        let response = self
            .request(
                Method::POST,
                "/v1/tenants",
                Some(serde_json::to_value(body)?),
            )
            .await?;
        expect_created(&response, "create tenant")
    }

    /// Create a namespace in an existing tenant. Already existing is fine.
    pub async fn create_namespace(&self, tenant_id: &str, namespace: &str) -> Result<()> {
        let body = NamespaceCreateRequest {
            namespace: namespace.to_string(),
            display_name: namespace.to_string(),
        };
        let response = self
            .as_tenant(
                tenant_id,
                Method::POST,
                &format!("/v1/tenants/{tenant_id}/namespaces"),
                Some(serde_json::to_value(body)?),
            )
            .await?;
        expect_created(&response, "create namespace")
    }

    /// Create a durable stream with quorum acknowledgement, `shards` shards
    /// each held by `replication_factor` brokers. Returns the stream as the
    /// control plane stored it.
    pub async fn create_durable_stream(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shards: u32,
        replication_factor: u32,
    ) -> Result<Stream> {
        let body = StreamCreateRequest {
            stream: stream.to_string(),
            kind: StreamKind::Stream,
            shards,
            replication_factor,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Quorum,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: true,
            region: None,
            mount: None,
            routing: None,
        };
        let response = self
            .as_tenant(
                tenant_id,
                Method::POST,
                &format!("/v1/tenants/{tenant_id}/namespaces/{namespace}/streams"),
                Some(serde_json::to_value(body)?),
            )
            .await?;
        if !response.status.is_success() {
            bail!("create stream: {} {}", response.status, response.text());
        }
        response.json()
    }

    /// Create a cache with quorum acknowledgement, `shards` shards each held
    /// by `replication_factor` brokers.
    pub async fn create_quorum_cache(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shards: u32,
        replication_factor: u32,
    ) -> Result<()> {
        let body = CacheCreateRequest {
            cache: cache.to_string(),
            display_name: cache.to_string(),
            shards,
            replication_factor,
            consistency: ConsistencyLevel::Quorum,
        };
        let response = self
            .as_tenant(
                tenant_id,
                Method::POST,
                &format!("/v1/tenants/{tenant_id}/namespaces/{namespace}/caches"),
                Some(serde_json::to_value(body)?),
            )
            .await?;
        if !response.status.is_success() {
            bail!("create cache: {} {}", response.status, response.text());
        }
        Ok(())
    }

    /// A request carrying a token for `tenant_id`, which the router checks
    /// against that tenant's own keys.
    async fn as_tenant(
        &self,
        tenant_id: &str,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<LinkResponse> {
        let cached = self.inner.tenant_tokens.lock().get(tenant_id).cloned();
        let token = match cached {
            Some(token) => token,
            None => {
                let keys = self
                    .inner
                    .store
                    .get_tenant_signing_keys(tenant_id)
                    .await
                    .with_context(|| format!("keys of tenant {tenant_id}"))?;
                let token = mint_token_for(
                    &keys,
                    tenant_id,
                    "sim",
                    [
                        format!("ns.manage:tenant:{tenant_id}"),
                        format!("stream.manage:tenant:{tenant_id}"),
                        format!("cache.manage:tenant:{tenant_id}"),
                    ]
                    .to_vec(),
                    TOKEN_TTL,
                    CONTROLPLANE_AUDIENCE,
                )?;
                self.inner
                    .tenant_tokens
                    .lock()
                    .insert(tenant_id.to_string(), token.clone());
                token
            }
        };
        self.send(method, path, body, &token).await
    }

    /// Finalize a fleet feature as an operator would. A feature that is only
    /// used alongside others is refused until those are enabled; they are
    /// finalized first.
    pub async fn finalize_feature(&self, feature: &str) -> Result<FleetFinalizeResponse> {
        let preview = self
            .request(
                Method::POST,
                &format!("/v1/fleet/features/{feature}/finalize?dry_run=true"),
                None,
            )
            .await?;
        if preview.status.is_success() {
            let preview: FleetFinalizeResponse = preview.json()?;
            for required in &preview.requires {
                Box::pin(self.finalize_feature(required)).await?;
            }
        }
        let response = self
            .request(
                Method::POST,
                &format!("/v1/fleet/features/{feature}/finalize"),
                None,
            )
            .await?;
        if !response.status.is_success() {
            bail!(
                "finalize {feature}: {} {}",
                response.status,
                response.text()
            );
        }
        response.json()
    }

    /// Finalize `joint_fence`, then `replica_election`, and what they need,
    /// once every serving broker has registered with [`broker_features`].
    pub async fn finalize_election(&self) -> Result<()> {
        self.finalize_feature(fleet::JOINT_FENCE.name()).await?;
        self.finalize_feature(fleet::REPLICA_ELECTION.name())
            .await?;
        Ok(())
    }

    /// Which fleet features the brokers support and which are enabled.
    pub async fn fleet_features(&self) -> Result<FleetFeaturesResponse> {
        let response = self
            .request(Method::GET, "/v1/fleet/features", None)
            .await?;
        if !response.status.is_success() {
            bail!("fleet features: {} {}", response.status, response.text());
        }
        response.json()
    }

    /// Every shard's assignment as the control plane has it.
    pub async fn assignments(&self) -> Result<ShardAssignmentSnapshotResponse> {
        let response = self
            .request(Method::GET, "/v1/shard-assignments/snapshot", None)
            .await?;
        if !response.status.is_success() {
            bail!(
                "assignment snapshot: {} {}",
                response.status,
                response.text()
            );
        }
        response.json()
    }
}

fn expect_created(response: &LinkResponse, what: &str) -> Result<()> {
    // 409 is "already exists" for all three create calls.
    if response.status.is_success() || response.status == axum::http::StatusCode::CONFLICT {
        Ok(())
    } else {
        Err(anyhow!("{what}: {} {}", response.status, response.text()))
    }
}

/// The feature names a broker that offers `ELECTION` reports when it
/// registers: everything the build implements, `lease_free_reads` included,
/// plus the capability reports and the two election features.
pub fn broker_features() -> BTreeSet<String> {
    fleet::IMPLEMENTED
        .iter()
        .chain([
            &fleet::PEER_ELECTION,
            &fleet::JOINT_FENCE,
            &fleet::REPLICA_ELECTION,
            &fleet::START_HOLDERS,
            &fleet::NON_VOTING,
        ])
        .map(|feature| feature.name().to_string())
        .collect()
}

/// A registration for broker `node_id` listening on `advertise_addr`, reporting
/// [`broker_features`].
pub fn registration(node_id: &str, advertise_addr: &str) -> NodeRegistrationRequest {
    NodeRegistrationRequest {
        node_id: node_id.to_string(),
        advertise_addr: advertise_addr.to_string(),
        client_addr: None,
        kafka_addr: None,
        region: "local".to_string(),
        zone: None,
        labels: BTreeMap::new(),
        capacity: Default::default(),
        features: broker_features(),
        storage_id: None,
    }
}

/// [`ControlPlaneLink`] over a [`SimControlPlane`].
#[derive(Clone)]
pub struct SimLink {
    cp: SimControlPlane,
}

impl SimLink {
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
        timeout: Option<Duration>,
    ) -> Result<LinkResponse> {
        let call = self.cp.request(method, path, body);
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, call)
                .await
                .map_err(|_| anyhow!("request timed out after {timeout:?}"))?,
            None => call.await,
        }
    }
}

fn json(body: &impl Serialize) -> Result<Option<serde_json::Value>> {
    Ok(Some(serde_json::to_value(body)?))
}

impl ControlPlaneLink for SimLink {
    fn endpoint(&self) -> String {
        "sim://controlplane".to_string()
    }

    async fn register(
        &self,
        _bearer: &str,
        body: &(impl Serialize + Sync),
    ) -> Result<LinkResponse> {
        self.call(Method::POST, "/v1/nodes", json(body)?, None)
            .await
    }

    async fn node(&self, _bearer: &str, node_id: &str) -> Result<LinkResponse> {
        self.call(Method::GET, &format!("/v1/nodes/{node_id}"), None, None)
            .await
    }

    async fn heartbeat(
        &self,
        _bearer: &str,
        node_id: &str,
        body: &(impl Serialize + Sync),
        timeout: Duration,
    ) -> Result<LinkResponse> {
        self.call(
            Method::POST,
            &format!("/v1/nodes/{node_id}/heartbeat"),
            json(body)?,
            Some(timeout),
        )
        .await
    }

    async fn lifecycle(&self, _bearer: &str, node_id: &str, action: &str) -> Result<LinkResponse> {
        self.call(
            Method::POST,
            &format!("/v1/nodes/{node_id}/{action}"),
            None,
            None,
        )
        .await
    }

    async fn node_catalog_page(
        &self,
        _bearer: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<LinkResponse> {
        let path = match cursor {
            Some(cursor) => format!("/v1/nodes?cursor={}", query_value(cursor)),
            None => "/v1/nodes".to_string(),
        };
        self.call(Method::GET, &path, None, None).await
    }

    async fn assignment_snapshot(
        &self,
        _bearer: Option<&str>,
        timeout: Duration,
    ) -> Result<LinkResponse> {
        self.call(
            Method::GET,
            "/v1/shard-assignments/snapshot",
            None,
            Some(timeout),
        )
        .await
    }

    async fn assignment_changes(
        &self,
        _bearer: Option<&str>,
        since: u64,
        wait: Duration,
        timeout: Duration,
    ) -> Result<LinkResponse> {
        self.call(
            Method::GET,
            &format!(
                "/v1/shard-assignments/changes?since={since}&wait_ms={}",
                wait.as_millis()
            ),
            None,
            Some(timeout),
        )
        .await
    }

    async fn replica_status(
        &self,
        _bearer: Option<&str>,
        node_id: &str,
        body: &(impl Serialize + Sync),
    ) -> Result<LinkResponse> {
        self.call(
            Method::POST,
            &format!("/v1/nodes/{node_id}/replica-status"),
            json(body)?,
            None,
        )
        .await
    }
}

/// A cursor percent-encoded as reqwest's `.query()` would, so the router
/// decodes it back to the same text.
fn query_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests;

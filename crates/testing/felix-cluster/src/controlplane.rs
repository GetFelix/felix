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
//!
//! It runs on its own multi-thread runtime rather than the test's. Broker
//! leases are renewed only by heartbeats answered within a few hundred
//! milliseconds, and a test's runtime (often single-threaded) also drives every
//! client the test opens, so sharing it lets test load lapse a lease.
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

use crate::credentials::{Credentials, TOKEN_TTL};
use crate::ports;

/// `policy` without the settle window. A test starts the brokers it wants
/// before it places, so a wall-clock wait for more would only make a test
/// that places on fewer than a stream's factor slower and timing-dependent.
fn unsettled(
    policy: felix_controlplane_service::cluster::placement::MovePolicy,
) -> felix_controlplane_service::cluster::placement::MovePolicy {
    felix_controlplane_service::cluster::placement::MovePolicy {
        settle_millis: None,
        ..policy
    }
}

/// Liveness tuned for a harness: brokers are local, so a lapsed heartbeat means
/// a broker that actually stopped rather than a slow network. Short windows make
/// a stopped broker observable in seconds instead of tens of seconds, which is
/// what failure tests wait on.
const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 200,
    expiry_timeout_ms: 1_000,
    regrant_margin_ms: None,
    sweep_interval_ms: 100,
    // Placement is driven explicitly by the harness, so this only matters as a
    // backstop for anything the harness does not step itself.
    shard_reconcile_interval_ms: 500,
};

/// How long [`Runtime::stop`] waits for the runtime's threads to exit.
const RUNTIME_STOP: Duration = Duration::from_secs(5);

/// A running control plane, and the keys to mint credentials against it.
pub struct ControlPlane {
    pub base_url: String,
    pub store: Arc<InMemoryStore>,
    keys: TenantSigningKeys,
    placement_wakes: Arc<felix_controlplane_service::cluster::placement::PlacementWakes>,
    /// The interval [`Self::run_placement`] was given, so a restart runs
    /// placement again: the reconciler stops with the instance it ran on.
    placement: std::sync::Mutex<Option<Duration>>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    runtime: Runtime,
}

impl ControlPlane {
    /// Start the control plane, holding the signing keys it will issue against.
    pub async fn start(tenant_id: &str) -> Result<Self> {
        crate::cluster::capture_control_plane();
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

    /// Where this control plane listens.
    pub(crate) fn addr(&self) -> Result<std::net::SocketAddr> {
        self.base_url
            .trim_start_matches("http://")
            .parse()
            .context("parse control plane address")
    }

    /// Stop this control plane, stay down for `downtime`, and start a new one
    /// on the same address over the same store.
    ///
    /// The new instance is a restart in every sense brokers can see: their
    /// connections are cut, requests fail while it is down, and its expiry
    /// sweep starts fresh against heartbeat stamps as old as the outage.
    pub async fn restart(self, downtime: Duration) -> Result<Self> {
        let stopped = self.stop().await?;
        tokio::time::sleep(downtime).await;
        stopped.start().await
    }

    /// Stop serving, keeping what a restart needs.
    pub(crate) async fn stop(self) -> Result<StoppedControlPlane> {
        let stopped = StoppedControlPlane {
            store: Arc::clone(&self.store),
            keys: self.keys.clone(),
            addr: self.addr()?,
            placement: *self.placement.lock().expect("placement lock"),
        };
        self.shutdown().await;
        Ok(stopped)
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

        let runtime = Runtime::new()?;
        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let sweep_store = Arc::clone(&store);
        let (task, addr) = runtime
            .handle()
            .spawn(async move {
                let listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("bind control plane on {addr}"))?;
                let addr = listener
                    .local_addr()
                    .context("read control plane address")?;
                let sweep_shutdown = serve_shutdown.clone();
                let task = tokio::spawn(async move {
                    let _ = axum::serve(listener, build_router(state).into_make_service())
                        .with_graceful_shutdown(async move { serve_shutdown.cancelled().await })
                        .await;
                });

                // Expiry has to run, or a stopped broker stays `live` forever
                // and no failure test can observe it leaving.
                let expiry = felix_controlplane_service::cluster::membership::spawn_expiry_sweep(
                    sweep_store
                        as Arc<
                            dyn felix_controlplane_service::store::ControlPlaneStore + Send + Sync,
                        >,
                    LIVENESS,
                    felix_controlplane_service::raft::LeadershipGate::Always,
                    sweep_shutdown,
                );
                // Owned by the same token; nothing waits on it separately.
                drop(expiry);
                anyhow::Ok((task, addr))
            })
            .await
            .context("start control plane")??;

        Ok(Self {
            base_url: format!("http://{addr}"),
            store,
            keys,
            placement_wakes,
            placement: std::sync::Mutex::new(None),
            shutdown,
            task,
            runtime,
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

    /// Credentials for `tenant_id`, signed with this control plane's keys.
    ///
    /// They stay valid across [`Self::restart`], which keeps the keys.
    pub fn credentials(&self, tenant_id: &str) -> Credentials {
        Credentials::new(self.keys.clone(), tenant_id, TOKEN_TTL)
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

    /// What a placement pass would decide if `down` had stopped heartbeating,
    /// from the reports held now. Nothing is written.
    pub async fn plan_if_down(
        &self,
        down: &str,
    ) -> Result<felix_controlplane_service::cluster::placement::Plan> {
        use felix_controlplane_service::cluster::placement::{MovePolicy, PlacementRead};
        use felix_controlplane_service::model::NodeLifecycle;

        let mut read = PlacementRead::load(self.store.as_ref(), &LIVENESS)
            .await
            .context("read placement")?;
        for node in read.nodes.iter_mut().filter(|node| node.node_id == down) {
            node.status.lifecycle = NodeLifecycle::Down;
        }
        Ok(read.plan(unsettled(MovePolicy::default())))
    }

    /// Step placement once under an explicit move policy.
    pub async fn place_shards_with(
        &self,
        policy: felix_controlplane_service::cluster::placement::MovePolicy,
    ) -> felix_controlplane_service::cluster::placement::ReconcileOutcome {
        felix_controlplane_service::cluster::placement::reconcile_once(
            self.store.as_ref(),
            &LIVENESS,
            unsettled(policy),
        )
        .await
    }

    /// Shards with replicas whose leader has not yet reported a caught-up
    /// replica at the shard's current generation, by name.
    ///
    /// Such a shard cannot fail over: with no report, placement cannot tell a
    /// replica holding the log from one that does not, and refuses to guess.
    pub async fn unreported_shards(&self) -> Result<Vec<String>> {
        let assignments = self
            .store
            .list_shard_assignments()
            .await
            .context("list shard assignments")?;
        let reports: std::collections::HashMap<_, _> = self
            .store
            .list_replica_reports()
            .await
            .context("list replica reports")?
            .into_iter()
            .map(|report| (report.key.clone(), report))
            .collect();
        Ok(assignments
            .iter()
            .filter(|assignment| !assignment.replicas.is_empty())
            .filter(|assignment| {
                !reports.get(&assignment.key).is_some_and(|report| {
                    report.generation == assignment.generation && !report.caught_up.is_empty()
                })
            })
            .map(|assignment| {
                format!(
                    "{}/{} generation {}",
                    assignment.key.stream, assignment.key.shard, assignment.generation
                )
            })
            .collect())
    }

    /// The last replica report placement holds for a stream's shard, if any.
    pub async fn replica_report(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<Option<felix_controlplane_service::model::ReplicaReport>> {
        self.replica_report_of(
            felix_controlplane_service::model::ShardKind::Stream,
            tenant_id,
            namespace,
            stream,
            shard,
        )
        .await
    }

    /// [`Self::replica_report`] for a shard of either kind.
    pub async fn replica_report_of(
        &self,
        kind: felix_controlplane_service::model::ShardKind,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        shard: u32,
    ) -> Result<Option<felix_controlplane_service::model::ReplicaReport>> {
        Ok(self
            .store
            .list_replica_reports()
            .await
            .context("list replica reports")?
            .into_iter()
            .find(|report| {
                report.key.kind == kind
                    && report.key.tenant_id == tenant_id
                    && report.key.namespace == namespace
                    && report.key.stream == name
                    && report.key.shard == shard
            }))
    }

    /// Run placement the way a deployment does: on `interval`, and whenever a
    /// report a move waits on arrives. Off unless a test asks, because most
    /// tests step placement themselves. Runs again after a
    /// [`Self::restart`].
    pub fn run_placement(&self, interval: Duration) {
        *self.placement.lock().expect("placement lock") = Some(interval);
        let _runtime = self.runtime.handle().enter();
        drop(
            felix_controlplane_service::cluster::placement::spawn_reconciler(
                Arc::clone(&self.store)
                    as Arc<dyn felix_controlplane_service::store::ControlPlaneStore + Send + Sync>,
                LIVENESS,
                unsettled(felix_controlplane_service::cluster::placement::MovePolicy::default()),
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
        // Stopping the runtime also drops the connection tasks `axum::serve`
        // spawned, which aborting the serve task alone leaves running.
        self.runtime.stop().await;
    }
}

/// The control plane's runtime, stopped without blocking when dropped.
///
/// Dropping a `tokio::runtime::Runtime` blocks, and panics inside async code,
/// which is where a `Cluster` is usually dropped.
struct Runtime(Option<tokio::runtime::Runtime>);

impl Runtime {
    fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("felix-cluster-controlplane")
            .enable_all()
            .build()
            .context("build control plane runtime")?;
        Ok(Self(Some(runtime)))
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        self.0.as_ref().expect("runtime is running").handle()
    }

    /// Shut down every task and wait for the worker threads to exit.
    async fn stop(mut self) {
        if let Some(runtime) = self.0.take() {
            let _ =
                tokio::task::spawn_blocking(move || runtime.shutdown_timeout(RUNTIME_STOP)).await;
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// A control plane that has been stopped with its state kept, as a crashed
/// one whose database survives: [`Self::start`] brings it back on the same
/// address over the same store and keys.
pub(crate) struct StoppedControlPlane {
    store: Arc<InMemoryStore>,
    keys: TenantSigningKeys,
    addr: std::net::SocketAddr,
    placement: Option<Duration>,
}

impl StoppedControlPlane {
    /// Serve again, and run placement again if it was running.
    pub(crate) async fn start(self) -> Result<ControlPlane> {
        let control_plane = ControlPlane::serve(self.store, self.keys, self.addr).await?;
        if let Some(interval) = self.placement {
            control_plane.run_placement(interval);
        }
        Ok(control_plane)
    }
}

#[cfg(test)]
mod tests;

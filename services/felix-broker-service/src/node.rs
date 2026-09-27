//! Running a broker node: startup order, readiness gating and the shutdown drain.
//!
//! ## Process lifecycle
//! - The broker starts long-running background tasks (QUIC accept loop, the
//!   optional Kafka listener, metrics server, and optional control-plane sync).
//! - The process remains alive until the provided shutdown future completes. In
//!   production that is SIGTERM or SIGINT: SIGTERM is what Kubernetes, systemd, and
//!   `docker stop` send, so handling only SIGINT would abort in-flight work on every
//!   rolling update.
//! - Shutdown then runs a bounded drain, in order: readiness goes false so load
//!   balancers stop routing here, a clustered broker hands its shards to others
//!   for up to `FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS` while it keeps serving, the
//!   listener keeps admitting for
//!   `FELIX_SHUTDOWN_PREDRAIN_MS` (off by default) while they notice, then stops,
//!   in-flight connections finish, and finally the metrics server stops. Anything
//!   still running when `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` expires is force-cancelled
//!   and named in a warning. See `felix_common::lifecycle`.
//!
//! Startup is a sequence of steps in `run_with_shutdown`, each in its own
//! submodule. Their order is load-bearing, and the comments at each step say
//! why.

mod cluster;
mod handoff;
mod listeners;
mod membership;
pub(crate) mod peer_dispatch;
mod shutdown;
mod storage;
mod sync;

use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use felix_authz::TenantKeyCache;
use felix_common::lifecycle::Readiness;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::cluster::{controlplane_http, credential};
use crate::config::{self, DurableStorageConfig};
use crate::replication;
use crate::serving::auth::{BrokerAuth, ControlPlaneKeyStore, TenantCatalog};
use crate::serving::tls::ClientTls;

/// Start the broker and run until the provided `shutdown` future resolves.
///
/// This indirection makes the process lifecycle explicit and testable:
/// - In production, `shutdown` is SIGTERM or SIGINT.
/// - In tests, callers can pass a bounded timer or a oneshot receiver.
///
/// The function is responsible for spawning background tasks and ensuring they are
/// cancelled when shutdown is requested.
pub async fn run_with_shutdown<F>(shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    // Observability is initialized first so any subsequent startup logs/metrics are captured.
    let metrics_handle = crate::observability::init_observability("felix-broker");
    // A `FELIX_*` name nothing reads is a typo, and a typo is a default quietly
    // taking effect. Reported after logging is up so the warning is actually
    // seen, and as a warning rather than a refusal: an orchestrator may inject
    // variables meant for something else, and refusing to start over one is
    // worse than the mistake it guards against.
    for warning in felix_common::env_registry::unrecognised_warnings() {
        tracing::warn!("{warning}");
    }

    // Configuration is resolved from environment variables (and optionally a YAML file).
    // Keep this early so the remainder of startup is entirely driven by `config`.
    let config = config::BrokerConfig::from_env_or_yaml()?;
    // Before any control-plane client exists: they are all built trusting it.
    controlplane_http::trust_ca(config.controlplane_ca.as_deref())?;
    // Size the QUIC I/O runtime pool before ANY endpoint is built: the pool is
    // created on the first one and a tokio runtime cannot be resized after.
    // This has to sit above the peer pool below, which binds a client endpoint
    // long before the client listeners are bound -- placing it next to those
    // listeners is too late, and the symptom is silent: every driver lands on
    // one thread and throughput simply does not improve.
    felix_transport::plan_server_endpoints(config.server_endpoints());
    // Lifecycle coordination. `readiness` gates `/ready`; the two tokens separate
    // "stop admitting connections" from "stop serving probes", because the metrics
    // endpoint has to outlive the drain — that is how an operator watches the drain
    // happen. `connections` tracks per-connection tasks so the drain can wait for
    // in-flight work instead of aborting it.
    // Durable brokers start *not* ready. Their streams do not exist until the
    // control plane has been applied and each durable log recovered, and an
    // instance that reports ready before then invites an orchestrator to route
    // traffic at streams that are, from any client's point of view, missing.
    // An in-memory broker has nothing to recover and keeps the old behaviour.
    let durable_storage_configured = DurableStorageConfig::from_env()?.is_some();
    let gate_readiness_on_sync = durable_storage_configured && config.controlplane_url.is_some();
    let readiness = if gate_readiness_on_sync {
        Readiness::starting()
    } else {
        Readiness::ready()
    };
    let accept_shutdown = CancellationToken::new();
    // Cancelled the moment shutdown begins, so startup work in flight can tell
    // "not ready yet" from "no longer ready".
    let draining = CancellationToken::new();
    // Cancelled once the catalog is applied and every durable log recovered.
    // Both readiness and the accept loop wait on it, so a durable broker
    // neither advertises itself nor answers a direct client before its streams
    // exist.
    let seeded = CancellationToken::new();
    // Cancelled once the catalog is applied, whether or not readiness waits on
    // it. Until then auth cannot tell an unknown tenant from one not yet
    // synced, so it lets both through to the JWKS fetch.
    let catalog_seeded = CancellationToken::new();

    let cluster = cluster::shard_state(&config);
    let ingress_router = cluster
        .as_ref()
        .map(|(_, ingress, _, _)| Arc::clone(ingress));

    // This broker's authority to serve the shards it leads. Created here rather
    // than inside the membership task because the accept loop is built first and
    // the publish path reads it. Starts conservative and invalid: authority
    // arrives with the first accepted heartbeat, never before.
    let lease = config
        .membership
        .as_ref()
        .map(|_| Arc::new(membership::initial_lease()));
    // Every write path enters the shard fence, so that is where the lease is
    // enforced; see `shards::lifecycle::fence`.
    if let (Some(ingress), Some(lease)) = (&ingress_router, &lease) {
        ingress.fence().bind_lease(Arc::clone(lease));
    }

    // Shared between the replication driver, which advances it, and the publish
    // path, which waits on it for `Quorum` streams.
    let quorum_marks = Arc::new(replication::quorum::QuorumMarks::new());
    // What replication has stopped for. Read by the admin listing, because the
    // metric is a bare count: a label per shard is a label per stream per
    // tenant, and a halt is useless to act on without knowing which replica.
    let halted_replicas = Arc::new(replication::halted::HaltedReplicas::new());
    // Empty until the first catalog refresh fills it, which is the honest
    // answer in the meantime: this broker has not yet been told where any
    // client may connect.
    let client_endpoints = Arc::new(crate::cluster::client_endpoints::ClientEndpoints::new());
    let peer_shutdown = CancellationToken::new();
    let cluster::Peers {
        tls: peer_tls,
        pool: peers,
    } = cluster::connect_peers(&config, &peer_shutdown)?;
    let sync_shutdown = CancellationToken::new();
    let metrics_shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    tracing::info!(
        lanes = config.subscriber_writer_lanes.max(1),
        queue_bound = config.subscriber_lane_queue_depth.max(1),
        queue_mode = ?config.subscriber_lane_queue_policy,
        shard = ?config.subscriber_lane_shard,
        single_writer_per_conn = config.subscriber_single_writer_per_conn,
        "sub egress lanes ENABLED"
    );

    let (broker, durable_storage) = storage::open(&config)?;
    // Readers of a replicated `Quorum` stream see only what its quorum mark
    // has passed; see `replication::quorum::committed_bound`.
    if let Some(ingress) = &ingress_router {
        broker.set_read_bounds(Arc::new(replication::quorum::CommittedReads::new(
            Arc::clone(&quorum_marks),
            Arc::clone(ingress),
        )));
    }
    tracing::info!("broker started");
    let controlplane_url = config
        .controlplane_url
        .clone()
        .context("FELIX_CONTROLPLANE_URL must be set for auth")?;

    // Start the Prometheus metrics HTTP server. This is separate from QUIC traffic and
    // intentionally lightweight so metrics remain available even under load.
    let metrics_task = {
        let metrics_shutdown = metrics_shutdown.clone();
        tokio::spawn(crate::observability::serve_metrics(
            metrics_handle,
            config.metrics_bind,
            readiness.clone(),
            Arc::clone(&halted_replicas),
            async move { metrics_shutdown.cancelled().await },
        ))
    };

    // Loaded before anything binds, so unreadable key material fails startup.
    let client_tls = ClientTls::from_config(&config.client_tls)?;
    client_tls.spawn_reload(&accept_shutdown);
    let quic_servers = listeners::bind(&config, &client_tls)?;
    let broker = Arc::new(broker);
    let key_store =
        ControlPlaneKeyStore::new(controlplane_url, Arc::new(TenantKeyCache::default()))
            .with_tenant_catalog(TenantCatalog::new(
                Arc::clone(&broker),
                catalog_seeded.clone(),
            ));
    let auth = Arc::new(BrokerAuth::with_key_store(Arc::new(key_store)));
    // One set for every client listener, so the per-address cap and each
    // tenant's quota are per broker rather than per socket.
    let limits = crate::serving::limits::ListenerLimits::from_config(&config);
    crate::observability::tenants::set_max_tenants(config.limits.tenant_metrics_max);
    let kafka = listeners::bind_kafka(
        &config,
        &client_tls,
        &broker,
        &auth,
        listeners::KafkaClusterView {
            ingress: &ingress_router,
            client_endpoints: &client_endpoints,
            lease: &lease,
            quorum_marks: &quorum_marks,
            quotas: &limits.tenant_rates,
        },
    )
    .await?;
    let mut accept_tasks = listeners::spawn_accept_loops(
        &quic_servers,
        listeners::AcceptLoops {
            broker: &broker,
            config: &config,
            auth: &auth,
            accept_shutdown: &accept_shutdown,
            connections: &connections,
            seeded: &seeded,
            gate_readiness_on_sync,
            ingress_router: &ingress_router,
            peers: &peers,
            lease: &lease,
            quorum_marks: &quorum_marks,
            client_endpoints: &client_endpoints,
            limits: &limits,
        },
    );
    if let Some(kafka) = kafka {
        accept_tasks.push(listeners::spawn_kafka(
            kafka,
            &accept_shutdown,
            &connections,
            &seeded,
            gate_readiness_on_sync,
        ));
    }

    // One holder, shared by every control-plane caller: the metadata sync
    // here, and membership below. A refresh swaps what is inside it, so a
    // caller handed it at startup keeps presenting a current token for the
    // life of the process rather than the one token it was given.
    let credential = (!config.controlplane_token.is_empty())
        .then(|| credential::NodeCredential::new(config.controlplane_token.clone()));
    let (seeded_tx, seeded_rx) = tokio::sync::oneshot::channel();
    let controlplane_task = sync::spawn_catalog_sync(
        &config,
        &broker,
        &credential,
        &sync_shutdown,
        gate_readiness_on_sync,
        seeded_tx,
        &catalog_seeded,
    );
    sync::spawn_readiness_flip(
        gate_readiness_on_sync,
        &readiness,
        &draining,
        &seeded,
        seeded_rx,
    );

    let membership_client = crate::cluster::controlplane_client::build()?;
    let (membership, credential_refresh) = match membership::spawn(
        &config,
        &membership_client,
        gate_readiness_on_sync,
        &seeded,
        &lease,
        &credential,
        &sync_shutdown,
    ) {
        Some(joined) => (Some(joined.membership), joined.credential_refresh),
        None => (None, None),
    };
    // Its own token, so a stopping broker can stop taking forwarded writes
    // while it still ships to followers over the pool.
    let peer_listener_shutdown = peer_shutdown.child_token();
    let peer_task = cluster::bind_peer_listener(
        &config,
        &cluster,
        &broker,
        &quorum_marks,
        &auth,
        &peer_tls,
        &peer_listener_shutdown,
    )?;
    let shard_tasks = cluster::spawn_shard_tasks(cluster::ShardTaskDeps {
        config: &config,
        cluster: &cluster,
        durable_storage: &durable_storage,
        membership_client: &membership_client,
        credential: &credential,
        client_endpoints: &client_endpoints,
        peers: &peers,
        broker: &broker,
        quorum_marks: &quorum_marks,
        halted_replicas: &halted_replicas,
        sync_shutdown: &sync_shutdown,
    });

    // Only while the shard watch runs is there anything to hand off, and
    // anything to tell this broker when it is done.
    let ownership = shard_tasks
        .as_ref()
        .and(cluster.as_ref())
        .map(|(_, _, _, ownership)| Arc::clone(ownership));
    let running = shutdown::Running {
        config,
        ownership,
        readiness,
        draining,
        accept_shutdown,
        connections,
        peers,
        peer_task,
        peer_shutdown,
        peer_listener_shutdown,
        accept_tasks,
        membership_client,
        credential,
        membership,
        credential_refresh,
        shard_tasks,
        sync_shutdown,
        controlplane_task,
        durable_storage,
        metrics_task,
        metrics_shutdown,
    };
    let membership_rejected = running.wait(shutdown).await;
    running.drain(membership_rejected).await
}

#[cfg(test)]
mod tests;

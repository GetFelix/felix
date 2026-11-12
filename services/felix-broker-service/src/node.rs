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

mod cache_expiry;
mod cluster;
mod from_view;
mod handoff;
mod listeners;
mod membership;
pub(crate) mod peer_dispatch;
mod shutdown;
#[cfg(feature = "sim")]
pub mod sim;
mod startup;
pub(crate) mod storage;
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
use crate::serving::auth::{BrokerAuth, ControlPlaneKeyStore, TenantCatalog};
use crate::serving::mqtt::{BrokerMounts, BrokerWrites};
use crate::serving::tls::ClientTls;
use crate::shards::routing as shard_routing;
use felix_replication as replication;

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
    // Parsed once: what readiness gates on is what storage opens.
    let durable_config = DurableStorageConfig::from_env()?;
    // A broker in memory loses every ballot and record at a restart, so it
    // has nothing to attest: it fences and answers as one that predates
    // counting by attestation.
    let mut config = config;
    if durable_config.is_none()
        && let Some(peer) = config.peer_transport.as_mut()
    {
        peer.unified_fence = false;
    }
    let durable_root = durable_config.as_ref().map(|durable| durable.root.clone());
    let durable_storage_configured = durable_root.is_some();
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
    // Cancelled once every listener is bound. Readiness waits on it too: a
    // bind that fails after `/ready` said yes looks like a crash, not a
    // start-up to retry.
    let listening = CancellationToken::new();

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
    // What replication has stopped for. Read by the admin listing, because the
    // metric is a bare count: a label per shard is a label per stream per
    // tenant, and a halt is useless to act on without knowing which replica.
    let halted_replicas = Arc::new(replication::halted::HaltedReplicas::new());
    // Each led shard's cursors and fence as replication last left them, for an
    // operator's `shard_inspect`.
    let shard_status = Arc::new(replication::status::ShardStatusBoard::new());
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

    // Bound before the wait below so a taken port fails startup at once, and
    // so `/live` answers while the broker waits on the control plane or on
    // recovery: a liveness probe would otherwise restart it in a loop. The
    // full server takes this socket over once the broker is built.
    let metrics_listener = std::net::TcpListener::bind(config.metrics_bind)
        .and_then(|listener| listener.set_nonblocking(true).map(|()| listener))
        .with_context(|| format!("bind metrics listener on {}", config.metrics_bind))?;
    let starting_probes =
        crate::observability::serve_starting(&metrics_listener, metrics_handle.clone())
            .context("answer probes on the metrics listener")?;
    // One holder, shared by every control-plane caller: the startup lookup
    // and the metadata sync here, and membership below. A refresh swaps what
    // is inside it, so a caller handed it at startup keeps presenting a
    // current token for the life of the process rather than the one token it
    // was given.
    let credential = (!config.controlplane_token.is_empty())
        .then(|| credential::NodeCredential::new(config.controlplane_token.clone()));
    let membership_client = crate::cluster::controlplane_client::build()?;
    let credential_refresh = membership::keep_credential_current(
        &config,
        &membership_client,
        &credential,
        &sync_shutdown,
    );
    // Before any store opens: a data directory restored from a snapshot is
    // older than the node's last registration and must not keep its id
    // (#1334). Nothing is open yet, so a stop while waiting leaves the
    // directory as it was. A start from the view defers that check to when
    // the control plane answers, and serves no client before it.
    let link = config.controlplane_url.as_deref().map(|base_url| {
        replication::controlplane_link::HttpControlPlaneLink::new(
            membership_client.clone(),
            base_url,
        )
    });
    let mut shutdown = Box::pin(shutdown);
    let witnesses = storage::witness_dir();
    let start = tokio::select! {
        start = membership::start(
            &config,
            durable_root.as_deref(),
            witnesses.as_deref(),
            &link,
            &credential,
        ) => start?,
        () = shutdown.as_mut() => {
            tracing::info!("stopped before the control plane answered; nothing was opened");
            sync_shutdown.cancel();
            peer_shutdown.cancel();
            return Ok(());
        }
    };
    let (registered, view) = match start {
        membership::Start::Asked(registered) => (registered, None),
        membership::Start::FromView(view) => (None, Some(*view)),
    };
    let mut stored = startup::open_storage(&config, durable_config, registered.as_ref())?;
    let own_kept = stored.kept;
    stored.kept = stored.kept.attested(view.is_some());
    let storage_id = stored.storage_id;
    // Beginning the run may have rotated the id the view was registered
    // under, and a new id registers before it acts: wait for the control
    // plane after all, before anything is bound or seeded.
    let view = match (view, &durable_root) {
        (Some(view), Some(root)) => tokio::select! {
            kept = membership::keep_view_if_opened_under(
                view,
                root,
                storage_id,
                &config,
                &link,
                &credential,
            ) => {
                let (view, replaced) = kept?;
                stored.replaced_storage |= replaced;
                view
            }
            () = shutdown.as_mut() => {
                // Nothing was acknowledged under the new id, so a stop here
                // is as safe as a crash.
                tracing::info!("stopped before the control plane answered");
                sync_shutdown.cancel();
                peer_shutdown.cancel();
                return Ok(());
            }
        },
        (view, _) => view,
    };
    // A start that kept its view attests as kept only once its restore check
    // has run against the control plane's record (`CountsForgotten`,
    // `TrustsRegistered` in `docs/formal/FelixShardUnified.tla`).
    stored.kept.deferred = view.is_some();
    // Which cross-broker features the whole fleet has, held by anything that
    // changes how brokers treat each other; membership keeps it current. A
    // broker outside a cluster has no fleet and enables nothing. A start
    // from the view begins with the view's features enabled but not adopted,
    // so a feature it lacks stays undecided until registration answers.
    let reported = config
        .membership
        .as_ref()
        .map(|membership| membership.features.clone())
        .unwrap_or_default();
    let fleet = Arc::new(match &view {
        Some(view) => felix_common::fleet::FleetGate::seeded(reported, &view.fleet),
        None => felix_common::fleet::FleetGate::new(reported),
    });
    // Shared between the replication driver, which advances it, and the publish
    // path, which waits on it for `Quorum` streams. The fleet decides how it
    // counts: see `QuorumMarks::own_generation_only`.
    let quorum_marks = Arc::new(replication::quorum::QuorumMarks::with_fleet(Arc::clone(
        &fleet,
    )));
    // One engine for every user: client listeners, the Kafka and MQTT writes,
    // forwarded writes, the read bounds and the peer handler. Its replica is
    // set when the peer listener binds.
    let engine = Arc::new(shard_routing::StreamEngine::new(
        Some(Arc::clone(&quorum_marks)),
        ingress_router.clone(),
    ));
    let startup::Opened {
        broker,
        durable_storage,
        storage_id,
        unclean,
    } = startup::wire_broker(&config, stored, &cluster, &fleet, &quorum_marks, &engine).await?;
    gate_record_generations(&config, &broker, &fleet);
    tracing::info!("broker started");
    let controlplane_url = config
        .controlplane_url
        .clone()
        .context("FELIX_CONTROLPLANE_URL must be set for auth")?;

    // Start the Prometheus metrics HTTP server. This is separate from QUIC traffic and
    // intentionally lightweight so metrics remain available even under load.
    // A start from the view registers its streams and caches at the
    // incarnations they had, so their logs open, and takes its routes, before
    // any shard task or peer listener runs. Neither cancels `catalog_seeded`
    // or `seeded`: auth and readiness wait for the control plane's own sync,
    // whose snapshots then replace all of it.
    let provisional = match (&view, &cluster) {
        (Some(view), Some((_, _, _, ownership))) => {
            let provisional =
                crate::cluster::catalog_sync::seed_provisional(&broker, &view.catalog).await;
            // Read by the routing feed's first tick, which comes at once.
            ownership.write().await.reset(view.routes.clone());
            provisional
        }
        _ => crate::cluster::catalog_sync::Provisional::default(),
    };
    // Client listeners and forwarded client requests wait on this. Without a
    // view it is `seeded`; after a start from the view it is cancelled only
    // once this process has registered too.
    // The restore check a start from the view deferred: the catalog sync and
    // the replica reports wait for it; membership runs it.
    let membership::Gates {
        clients,
        restore_checked,
    } = membership::Gates::new(view.is_some(), &seeded);
    startup::bind_read_check(
        &config,
        &cluster,
        &peers,
        &broker,
        &quorum_marks,
        crate::config::quorum_reads_by_lease,
    )?;
    starting_probes.stop().await;
    let metrics_listener = tokio::net::TcpListener::from_std(metrics_listener)
        .context("serve the metrics listener")?;
    let metrics_task = {
        let metrics_shutdown = metrics_shutdown.clone();
        tokio::spawn(crate::observability::serve_metrics(
            metrics_handle,
            metrics_listener,
            readiness.clone(),
            Arc::clone(&halted_replicas),
            crate::observability::backup::BackupOffsets {
                node_id: config.membership.as_ref().map(|m| m.node_id.clone()),
                broker: Arc::clone(&broker),
                ingress: ingress_router.clone(),
                marks: Arc::clone(&quorum_marks),
            },
            async move { metrics_shutdown.cancelled().await },
        ))
    };

    // Loaded before anything binds, so unreadable key material fails startup.
    let client_tls = ClientTls::from_config(&config.client_tls)?;
    client_tls.spawn_reload(&accept_shutdown);
    let quic_servers = listeners::bind(&config, &client_tls)?;
    let key_store = Arc::new(
        ControlPlaneKeyStore::new(controlplane_url, Arc::new(TenantKeyCache::default()))
            .with_tenant_catalog(TenantCatalog::new(
                Arc::clone(&broker),
                catalog_seeded.clone(),
            )),
    );
    // Keeps cached keys current so no request waits on the control plane.
    tokio::spawn(Arc::clone(&key_store).renew_until(sync_shutdown.clone()));
    let auth = Arc::new(
        BrokerAuth::with_key_store(Arc::clone(&key_store))
            .with_subject_binding(config.client_tls.bind_subject)
            .with_lease_tokens(crate::serving::lease::LeaseTokens::from_env()?.map(Arc::new)),
    );
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
            engine: &engine,
            client_endpoints: &client_endpoints,
            quotas: &limits.tenant_rates,
        },
    )
    .await?;
    let mqtt_writes = BrokerWrites::new(
        Arc::clone(&broker),
        Arc::clone(&engine),
        std::time::Duration::from_millis(config.publish_quorum_timeout_ms.max(1)),
    )
    .with_quotas(Arc::clone(&limits.tenant_rates));
    let mqtt_writes = match &peers {
        Some(pool) => mqtt_writes.with_forwarding(Arc::clone(pool), config.forward_budget()),
        None => mqtt_writes,
    };
    let mqtt = listeners::bind_mqtt(
        &client_tls,
        &auth,
        BrokerMounts::new(Arc::clone(&broker), ingress_router.clone()),
        mqtt_writes,
    )?;
    let mut accept_tasks = listeners::spawn_accept_loops(
        &quic_servers,
        listeners::AcceptLoops {
            broker: &broker,
            config: &config,
            auth: &auth,
            accept_shutdown: &accept_shutdown,
            connections: &connections,
            seeded: &clients,
            gate_readiness_on_sync,
            engine: &engine,
            peers: &peers,
            lease: &lease,
            shard_status: &shard_status,
            client_endpoints: &client_endpoints,
            limits: &limits,
        },
    );
    if let Some(kafka) = kafka {
        accept_tasks.push(listeners::spawn_kafka(
            kafka,
            &accept_shutdown,
            &connections,
            &clients,
            gate_readiness_on_sync,
        ));
    }
    if let Some(mqtt) = mqtt {
        accept_tasks.push(listeners::spawn_mqtt(
            mqtt,
            &accept_shutdown,
            &connections,
            &clients,
            gate_readiness_on_sync,
        ));
    }

    tokio::spawn(cache_expiry::run(
        Arc::clone(&broker),
        ingress_router.clone(),
        cache_expiry::INTERVAL,
        sync_shutdown.clone(),
    ));
    // Set at each registration. The peer listener's handshake must tell "not
    // registered yet" from incarnation 0 and learn when it changes.
    let (registered, registered_watch) = tokio::sync::watch::channel(None);
    let (seeded_tx, seeded_rx) = tokio::sync::oneshot::channel();
    let controlplane_task = sync::spawn_catalog_sync(
        &config,
        &broker,
        &credential,
        &sync_shutdown,
        gate_readiness_on_sync,
        seeded_tx,
        &catalog_seeded,
        &key_store,
        &restore_checked,
        provisional,
    );
    sync::spawn_readiness_flip(
        gate_readiness_on_sync,
        &readiness,
        &draining,
        &seeded,
        seeded_rx,
        &listening,
        view.as_ref()
            .map(|_| (clients.clone(), registered_watch.clone())),
    );

    // Filled by the shard tasks, carried by the heartbeat.
    let suspects = Arc::new(felix_replication::suspicion::Suspects::default());
    // Set at each registration, read by the replica reports.
    let incarnation = Arc::new(std::sync::atomic::AtomicU64::new(0));
    // Its own client: the heartbeat runs on a runtime of its own, and a pooled
    // connection belongs to the runtime that opened it.
    let heartbeat_link = config
        .membership
        .as_ref()
        .and(config.controlplane_url.as_deref())
        .map(|base_url| {
            replication::controlplane_link::HttpControlPlaneLink::new(
                crate::cluster::controlplane_client::build()
                    .expect("the control-plane client built once already"),
                base_url,
            )
        });
    let view_registered = registered_watch.clone();
    // Shared by membership and the view's writer, so a view membership forgets
    // is not written back.
    let view_file = durable_storage.as_ref().map(|storage| {
        crate::cluster::controlplane_view::ViewFile::new(storage.root().to_path_buf())
    });
    let membership = membership::spawn(
        &config,
        heartbeat_link,
        true,
        gate_readiness_on_sync,
        &seeded,
        &lease,
        &fleet,
        &credential,
        &suspects,
        &incarnation,
        registered,
        storage_id,
        durable_storage.as_ref().map(|storage| storage.root()),
        view_file.clone(),
        view.as_ref().map(|_| {
            let marks = Arc::clone(&quorum_marks);
            membership::deferred_check(&restore_checked, own_kept, move |kept| {
                // Readmits first: setting kept is what lets a readmission
                // count (`QuorumMarks::knows_kept_ballots`).
                marks.set_readmits(kept.readmits);
                marks.set_kept_ballots(kept.ballots);
            })
        }),
        &sync_shutdown,
    );
    // Its own token, so a stopping broker can stop taking forwarded writes
    // while it still ships to followers over the pool.
    let peer_listener_shutdown = peer_shutdown.child_token();
    let peer_listener = cluster::bind_peer_listener(
        &config,
        &cluster,
        &engine,
        &broker,
        &quorum_marks,
        &auth,
        &peer_tls,
        peers.as_ref().map(|pool| pool.known_capabilities().clone()),
        storage_id,
        registered_watch,
        unclean.clone(),
        config
            .controlplane_url
            .as_deref()
            .map(|base_url| (&membership_client, base_url, &credential)),
        view.as_ref().map(|_| clients.clone()),
        // Peers suspect a broker that cannot serve clients yet, so they stand
        // for the shards it leads instead of waiting on it. After a start from
        // the view that lasts until it has registered.
        gate_readiness_on_sync.then_some(&clients),
        &peer_listener_shutdown,
    )?;
    let shard_tasks = cluster::spawn_shard_tasks(cluster::ShardTaskDeps {
        config: &config,
        cluster: &cluster,
        durable_storage: &durable_storage,
        link: &link,
        credential: &credential,
        client_endpoints: &client_endpoints,
        peers: &peers,
        broker: &broker,
        engine: &engine,
        quorum_marks: &quorum_marks,
        halted_replicas: &halted_replicas,
        shard_status: &shard_status,
        fleet: &fleet,
        suspects: &suspects,
        incarnation: &incarnation,
        storage_id: durable_storage.as_ref().map(|_| storage_id),
        unclean: &unclean,
        prefetch: peer_listener.prefetch,
        nodes: view.as_ref().map(from_view::nodes).unwrap_or_default(),
        reports_held: view.as_ref().map(|_| restore_checked.clone()),
        leads_after: view.as_ref().map(|_| clients.clone()),
        sync_shutdown: &sync_shutdown,
    });

    // Only where a restart could start from it: a cluster member with a data
    // directory whose routes come from the shard watch.
    let controlplane_view = match (&config.membership, &cluster, &view_file, &shard_tasks) {
        (Some(membership), Some((router, _, _, ownership)), Some(view_file), Some(_)) => {
            Some(crate::cluster::controlplane_view::spawn(
                crate::cluster::controlplane_view::Sources {
                    node_id: membership.node_id.clone(),
                    storage_id,
                    registered: view_registered,
                    fleet: Arc::clone(&fleet),
                    catalog_seeded: catalog_seeded.clone(),
                    ownership: Arc::clone(ownership),
                    ownership_changes: ownership.read().await.changes(),
                    router: Arc::clone(router),
                    broker: Arc::clone(&broker),
                },
                view_file.clone(),
            ))
        }
        _ => None,
    };

    // After the last bind: anything bound below here could fail after ready.
    listening.cancel();

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
        peer_task: peer_listener.task,
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
        controlplane_view,
        storage: shutdown::Storage {
            broker: Arc::clone(&broker),
            run: durable_storage
                .as_ref()
                .map(|storage| (storage.root().to_path_buf(), storage_id)),
        },
        metrics_task,
        metrics_shutdown,
    };
    let membership_rejected = running.wait(shutdown).await;
    running.drain(membership_rejected).await
}

/// A v9 segment, or a committed vote, is a format an older build refuses, so
/// a cluster member writes one only once its fleet has finalized
/// `record_generations`. Asked when a log rolls, so the gate may be set after
/// the logs open. A lone broker has no fleet and stays on the older formats.
fn gate_record_generations(
    config: &config::BrokerConfig,
    broker: &felix_broker::Broker,
    fleet: &Arc<felix_common::fleet::FleetGate>,
) {
    if config.membership.is_some() {
        let fleet = Arc::clone(fleet);
        broker.record_generations_when(move || {
            fleet.supports(felix_common::fleet::RECORD_GENERATIONS)
        });
    }
}

#[cfg(test)]
mod tests;

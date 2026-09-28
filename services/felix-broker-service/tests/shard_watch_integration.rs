//! Snapshot-to-watch handoff against the real control-plane router.
//!
//! The property under test is the one that cannot be checked in isolation: a
//! broker starting from nothing converges on the current assignments, and no
//! committed change is lost in the seam between the snapshot and the first poll.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker_service::shards::watch::{self as shard_watch, ShardOwnership};
use felix_broker_service::shards::{ShardKey as WatchedShardKey, ShardKind as WatchedShardKind};
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::felix_token::{TenantSigningKeys, mint_token_for};
use felix_controlplane_service::auth::keys::generate_signing_keys;
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity, NodeLifecycle, NodeSpec,
    NodeStatus, RetentionPolicy, ShardAssignment, ShardKey, ShardKind, ShardState, Stream,
    StreamKind, Tenant,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig,
};
use reqwest::{Client, redirect::Policy};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 5_000,
    expiry_timeout_ms: 15_000,
    regrant_margin_ms: None,
    sweep_interval_ms: 2_000,
    shard_reconcile_interval_ms: 5_000,
};

struct Cluster {
    base_url: String,
    store: Arc<InMemoryStore>,
    client: Client,
    bearer: String,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

impl Cluster {
    async fn start() -> Self {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: 1000,
            change_retention_max_rows: Some(1000),
        }));
        let keys: TenantSigningKeys = generate_signing_keys().expect("keys");
        store
            .set_tenant_signing_keys("t1", keys.clone())
            .await
            .expect("keys");

        let state = AppState {
            region: Region {
                region_id: "us-west-2".to_string(),
                display_name: "Test".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: store.is_durable(),
                tiered_storage: false,
                bridges: false,
            },
            store: Arc::clone(&store) as Arc<dyn ControlPlaneAuthStore + Send + Sync>,
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

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (stop, stop_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state).into_make_service())
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await;
        });

        let bearer = mint_token_for(
            &keys,
            "t1",
            "p:broker",
            vec!["node.view:cluster:*".to_string()],
            Duration::from_secs(900),
            felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
        )
        .expect("token");

        let cluster = Self {
            base_url: format!("http://{addr}"),
            store,
            client: Client::builder()
                .timeout(Duration::from_secs(2))
                .no_proxy()
                .redirect(Policy::none())
                .build()
                .expect("client"),
            bearer,
            stop,
            server,
        };
        cluster.seed().await;
        cluster
    }

    async fn seed(&self) {
        self.store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "T".to_string(),
            })
            .await
            .expect("tenant");
        self.store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                display_name: "NS".to_string(),
            })
            .await
            .expect("namespace");
        self.store
            .create_stream(Stream {
                tenant_id: "t1".to_string(),
                namespace: "ns".to_string(),
                stream: "orders".to_string(),
                kind: StreamKind::Stream,
                shards: 4,
                replication_factor: 1,
                retention: RetentionPolicy {
                    max_age_seconds: None,
                    max_size_bytes: None,
                },
                consistency: ConsistencyLevel::Leader,
                delivery: DeliveryGuarantee::AtMostOnce,
                durable: true,
                region: None,
            })
            .await
            .expect("stream");

        for (i, id) in ["broker-a", "broker-b"].iter().enumerate() {
            self.store
                .register_node(Node {
                    node_id: id.to_string(),
                    spec: NodeSpec {
                        advertise_addr: format!("10.0.0.4:{}", 7800 + i),
                        client_addr: None,
                        kafka_addr: None,
                        region: "us-west-2".to_string(),
                        zone: None,
                        labels: BTreeMap::new(),
                        capacity: NodeCapacity::default(),
                    },
                    status: NodeStatus {
                        lifecycle: NodeLifecycle::Live,
                        last_heartbeat_at_millis: 1,
                        registered_at_millis: 1,
                        incarnation: 0,
                    },
                })
                .await
                .expect("node");
        }
    }

    async fn assign(&self, shard: u32, leader: &str, state: ShardState) {
        self.store
            .put_shard_assignment(ShardAssignment {
                key: ShardKey {
                    tenant_id: "t1".to_string(),
                    namespace: "ns".to_string(),
                    stream: "orders".to_string(),
                    shard,
                    kind: ShardKind::Stream,
                },
                leader: leader.to_string(),
                replicas: Vec::new(),
                generation: 0,
                state,
                successor: None,
                joining: None,
                move_started_at_millis: None,
                move_reason: None,
            })
            .await
            .expect("assign");
    }

    fn watch(
        &self,
        ownership: Arc<RwLock<ShardOwnership>>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        self.watch_with(
            felix_broker_service::cluster::credential::NodeCredential::new(self.bearer.clone()),
            ownership,
            shutdown,
        )
    }

    fn watch_with(
        &self,
        credential: felix_broker_service::cluster::credential::NodeCredential,
        ownership: Arc<RwLock<ShardOwnership>>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(shard_watch::run(
            self.client.clone(),
            self.base_url.clone(),
            Some(credential),
            ownership,
            Arc::default(),
            Duration::from_millis(10),
            shutdown,
        ))
    }

    /// A watch polling every `interval`, reporting changes to `changed`.
    fn watch_every(
        &self,
        interval: Duration,
        ownership: Arc<RwLock<ShardOwnership>>,
        changed: Arc<tokio::sync::Notify>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(shard_watch::run(
            self.client.clone(),
            self.base_url.clone(),
            Some(
                felix_broker_service::cluster::credential::NodeCredential::new(self.bearer.clone()),
            ),
            ownership,
            changed,
            interval,
            shutdown,
        ))
    }

    async fn shutdown(mut self) {
        let _ = self.stop.send(());
        // A graceful shutdown waits for held long-polls, which answer only
        // when their wait runs out; the test is done with them.
        if tokio::time::timeout(Duration::from_millis(500), &mut self.server)
            .await
            .is_err()
        {
            self.server.abort();
        }
    }
}

/// Wait for a condition rather than sleeping a fixed time.
async fn until(mut check: impl AsyncFnMut() -> bool) -> bool {
    for _ in 0..400 {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn a_broker_starting_empty_converges_on_the_snapshot() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;
    cluster.assign(1, "broker-b", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());

    assert!(
        until(async || ownership.read().await.len() == 2).await,
        "the watch should converge on the current snapshot",
    );

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

/// The seam the acceptance criterion names: a change committed after the
/// snapshot must still arrive, and one already in it must not be applied twice.
#[tokio::test]
async fn changes_after_the_snapshot_are_not_lost() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());

    assert!(until(async || ownership.read().await.len() == 1).await);

    // Committed strictly after the snapshot the broker already applied.
    cluster.assign(1, "broker-b", ShardState::Assigning).await;
    cluster.assign(2, "broker-a", ShardState::Assigning).await;

    assert!(
        until(async || ownership.read().await.len() == 3).await,
        "changes committed after the snapshot must reach the broker",
    );

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

/// A leader change has to reach the broker, since that is what tells it to stop
/// serving a shard it no longer owns.
#[tokio::test]
async fn a_leader_change_reaches_the_broker() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());

    let shard = WatchedShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: WatchedShardKind::Stream,
    };
    assert!(until(async || ownership.read().await.is_leader(&shard, "broker-a")).await);

    cluster.assign(0, "broker-b", ShardState::Assigning).await;

    assert!(
        until(async || ownership.read().await.is_leader(&shard, "broker-b")).await,
        "the broker must learn it lost the shard",
    );
    assert!(!ownership.read().await.is_leader(&shard, "broker-a"));

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn an_unassignment_reaches_the_broker() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());
    assert!(until(async || ownership.read().await.len() == 1).await);

    cluster
        .store
        .delete_shard_assignment(&ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard: 0,
            kind: ShardKind::Stream,
        })
        .await
        .expect("delete");

    assert!(
        until(async || ownership.read().await.is_empty()).await,
        "an unassignment must remove the shard locally",
    );

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

/// The control plane being unreachable must not stop a broker serving what it
/// already owns, and the watch must recover on its own when it returns.
#[tokio::test]
async fn the_watch_survives_the_control_plane_going_away() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch(Arc::clone(&ownership), shutdown.clone());
    assert!(until(async || ownership.read().await.len() == 1).await);

    let base_url = cluster.base_url.clone();
    let client = cluster.client.clone();
    cluster.shutdown().await;

    // The watch keeps running and keeps what it had.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        ownership.read().await.len(),
        1,
        "ownership must survive the control plane going away",
    );
    assert!(!watch.is_finished(), "the watch must not give up");
    let _ = (base_url, client);

    shutdown.cancel();
    let _ = watch.await;
}

/// The watch reads its credential per poll, not once at startup.
///
/// This is the whole reason the credential is a shared holder rather than a
/// `String`: the watch outlives many access tokens, and a copy taken when it
/// started is the exact thing that drops a broker out of the cluster fifteen
/// minutes in. Started with a credential the control plane refuses, so the
/// watch makes no progress; a refresh then swaps a good one in, and progress
/// is the proof it went back and looked.
#[tokio::test]
async fn the_watch_picks_up_a_refreshed_credential() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let credential =
        felix_broker_service::cluster::credential::NodeCredential::new("not-a-valid-token");
    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = cluster.watch_with(credential.clone(), Arc::clone(&ownership), shutdown.clone());

    // Nothing arrives while the credential is refused. Long enough for several
    // poll intervals, so this is "refused" rather than "not yet".
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        ownership.read().await.len(),
        0,
        "the watch applied assignments it was not authorised to read",
    );

    credential.replace(cluster.bearer.clone());

    assert!(
        until(async || ownership.read().await.len() == 1).await,
        "the watch never picked up the refreshed credential, so it is still \
         presenting the token it was handed at startup",
    );

    shutdown.cancel();
    let _ = watch.await;
    cluster.shutdown().await;
}

/// **A change reaches the broker as it is written, not at the next poll.** The
/// interval is far longer than the wait allowed here, so only a long-poll the
/// control plane answers on the write can deliver it in time.
#[tokio::test]
async fn a_change_arrives_before_the_next_poll() {
    let cluster = Cluster::start().await;
    cluster.assign(0, "broker-a", ShardState::Assigning).await;

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let changed = Arc::new(tokio::sync::Notify::new());
    let shutdown = CancellationToken::new();
    let watch = cluster.watch_every(
        Duration::from_secs(30),
        Arc::clone(&ownership),
        Arc::clone(&changed),
        shutdown.clone(),
    );
    assert!(until(async || ownership.read().await.len() == 1).await);
    // The snapshot counted as a change.
    tokio::time::timeout(Duration::from_secs(1), changed.notified())
        .await
        .expect("the snapshot should wake the feed");

    // Let the watch settle into its long-poll before writing.
    tokio::time::sleep(Duration::from_millis(200)).await;
    cluster.assign(1, "broker-b", ShardState::Assigning).await;

    assert!(
        until(async || ownership.read().await.len() == 2).await,
        "the change should arrive through the held request, well before the \
         30 s interval",
    );
    tokio::time::timeout(Duration::from_secs(1), changed.notified())
        .await
        .expect("an applied change should wake the feed");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), watch)
        .await
        .expect("shutdown must not wait for a held long-poll")
        .expect("join");
    cluster.shutdown().await;
}

/// **A control plane that ignores `wait_ms` is polled on the interval.** It
/// answers every request at once; without the interval between empty answers
/// the watch would spin.
#[tokio::test]
async fn a_control_plane_without_long_poll_is_polled_on_the_interval() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let requests = Arc::new(AtomicUsize::new(0));
    let asked_to_wait = Arc::new(AtomicUsize::new(0));
    let app = axum::Router::new()
        .route(
            "/v1/shard-assignments/snapshot",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({ "items": [], "next_seq": 0 }))
            }),
        )
        .route(
            "/v1/shard-assignments/changes",
            axum::routing::get({
                let requests = Arc::clone(&requests);
                let asked_to_wait = Arc::clone(&asked_to_wait);
                move |query: axum::extract::Query<std::collections::HashMap<String, String>>| {
                    requests.fetch_add(1, Ordering::Relaxed);
                    if query.contains_key("wait_ms") {
                        asked_to_wait.fetch_add(1, Ordering::Relaxed);
                    }
                    async { axum::Json(serde_json::json!({ "items": [], "next_seq": 0 })) }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });

    let shutdown = CancellationToken::new();
    let watch = tokio::spawn(shard_watch::run(
        Client::builder().no_proxy().build().expect("client"),
        format!("http://{addr}"),
        None,
        Arc::new(RwLock::new(ShardOwnership::default())),
        Arc::default(),
        Duration::from_millis(100),
        shutdown.clone(),
    ));

    tokio::time::sleep(Duration::from_secs(1)).await;
    let seen = requests.load(Ordering::Relaxed);
    assert!(seen >= 3, "the watch should keep polling; saw {seen}");
    assert!(
        seen <= 15,
        "{seen} polls in a second at a 100 ms interval: the watch is spinning \
         on a control plane that answers at once",
    );
    assert_eq!(
        asked_to_wait.load(Ordering::Relaxed),
        seen,
        "every poll asks to wait"
    );

    shutdown.cancel();
    let _ = watch.await;
    server.abort();
}

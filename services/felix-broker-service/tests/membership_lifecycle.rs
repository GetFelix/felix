//! Broker membership end to end, against the real control-plane router.
//!
//! A stub would let the two services drift: the broker could send a field the
//! control plane ignores, or read one it never sends, and every test would still
//! pass. Running the real router means the HTTP contract is under test too.
//!
//! Run with `cargo test -p felix-broker-service --test membership_lifecycle`.
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use felix_broker_service::cluster::membership::{self, MembershipError};
use felix_broker_service::config::MembershipConfig;
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::NodeLifecycle;
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};
use reqwest::{Client, redirect::Policy};
use tokio_util::sync::CancellationToken;

const LIVENESS: NodeLivenessConfig = NodeLivenessConfig {
    heartbeat_interval_ms: 20,
    expiry_timeout_ms: 60,
    regrant_margin_ms: None,
    sweep_interval_ms: 10,
    shard_reconcile_interval_ms: 5_000,
};

struct Cluster {
    base_url: String,
    store: Arc<InMemoryStore>,
    client: Client,
    /// A credential scoped to `broker-a`, which is the broker these tests are.
    token: String,
    keys: felix_controlplane_service::auth::felix_token::TenantSigningKeys,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

impl Cluster {
    async fn start() -> Self {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: 1000,
            change_retention_max_rows: Some(1000),
        }));
        let keys = felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys");
        store
            .set_tenant_signing_keys("t1", keys.clone())
            .await
            .expect("keys");
        // Scoped to this broker's own identity, which is what a real deployment
        // would hand it. `broker-b` cases below rely on that being a real limit.
        let token = felix_controlplane_service::auth::felix_token::mint_token_for(
            &keys,
            "t1",
            "p:broker-a",
            vec!["node.manage:node:broker-a".to_string()],
            Duration::from_secs(900),
            felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
        )
        .expect("token");
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

        Self {
            base_url: format!("http://{addr}"),
            store,
            token,
            keys,
            client: Client::builder()
                .timeout(Duration::from_secs(2))
                .no_proxy()
                .redirect(Policy::none())
                .build()
                .expect("client"),
            stop,
            server,
        }
    }

    /// A credential covering every node, for cases that are not about scope.
    fn fleet_token(&self) -> String {
        felix_controlplane_service::auth::felix_token::mint_token_for(
            &self.keys,
            "t1",
            "p:operator",
            vec!["node.manage:cluster:*".to_string()],
            Duration::from_secs(900),
            felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
        )
        .expect("token")
    }

    async fn lifecycle(&self, node_id: &str) -> NodeLifecycle {
        self.store
            .get_node(node_id)
            .await
            .expect("node should be registered")
            .status
            .lifecycle
    }

    async fn shutdown(self) {
        let _ = self.stop.send(());
        let _ = self.server.await;
    }
}

fn config(node_id: &str, port: u16) -> MembershipConfig {
    MembershipConfig {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.4:{port}"),
        client_advertise_addr: None,
        kafka_advertise_addr: None,
        region: "us-west-2".to_string(),
        zone: None,
        region_bridges: Vec::new(),
        features: Default::default(),
    }
}

/// Boot must leave exactly one live record, and a restart must reuse the same
/// identity rather than adding a second one.
#[tokio::test]
async fn boot_produces_one_live_record_and_a_restart_reuses_it() {
    let cluster = Cluster::start().await;

    let first = membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-a", 7001),
        &felix_broker_service::cluster::credential::NodeCredential::new(cluster.token.clone()),
    )
    .await
    .expect("register");
    assert_eq!(first.incarnation, 0);
    assert_eq!(cluster.lifecycle("broker-a").await, NodeLifecycle::Live);

    // Same identity, new process.
    let second = membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-a", 7001),
        &felix_broker_service::cluster::credential::NodeCredential::new(cluster.token.clone()),
    )
    .await
    .expect("re-register");
    assert_eq!(
        second.incarnation, 1,
        "a restart takes the next incarnation"
    );

    let nodes = cluster.store.list_nodes().await.expect("list");
    assert_eq!(
        nodes.len(),
        1,
        "a restart must not create a second identity"
    );

    cluster.shutdown().await;
}

/// Graceful shutdown has to be distinguishable from a crash, or placement
/// cannot tell "this broker meant to go" from "this broker vanished".
#[tokio::test]
async fn graceful_shutdown_leaves_rather_than_expiring() {
    let cluster = Cluster::start().await;
    membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-a", 7001),
        &felix_broker_service::cluster::credential::NodeCredential::new(cluster.token.clone()),
    )
    .await
    .expect("register");

    membership::shutdown_membership(
        &cluster.client,
        &cluster.base_url,
        "broker-a",
        &cluster.token,
    )
    .await;

    assert_eq!(cluster.lifecycle("broker-a").await, NodeLifecycle::Left);
    cluster.shutdown().await;
}

/// A broker that is killed never says anything, so silence is the only signal.
#[tokio::test]
async fn an_abrupt_stop_is_detected_by_expiry() {
    let cluster = Cluster::start().await;
    let registered = membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-a", 7001),
        &felix_broker_service::cluster::credential::NodeCredential::new(cluster.token.clone()),
    )
    .await
    .expect("register");
    assert_eq!(cluster.lifecycle("broker-a").await, NodeLifecycle::Live);

    // The process is gone; nothing more arrives. Drive the sweep at a time past
    // the timeout instead of waiting for one.
    let registered_at = cluster
        .store
        .get_node("broker-a")
        .await
        .expect("get")
        .status
        .last_heartbeat_at_millis;
    let expired = felix_controlplane_service::cluster::membership::expire_once(
        cluster.store.as_ref(),
        &LIVENESS,
        registered_at + LIVENESS.silence_before_down_ms() + 1,
    )
    .await;

    assert_eq!(expired, 1);
    assert_eq!(cluster.lifecycle("broker-a").await, NodeLifecycle::Down);
    assert_eq!(registered.incarnation, 0);

    cluster.shutdown().await;
}

/// Two brokers cannot both be reachable at one address, so the second must be
/// refused rather than silently shadowing the first.
#[tokio::test]
async fn a_duplicate_advertised_address_is_refused_terminally() {
    let cluster = Cluster::start().await;
    membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-a", 7001),
        &felix_broker_service::cluster::credential::NodeCredential::new(cluster.token.clone()),
    )
    .await
    .expect("register");

    // Cluster-scoped on purpose: this test is about the duplicate address, and a
    // node-scoped token would fail authorisation first and prove nothing.
    let fleet_token = cluster.fleet_token();
    let err = membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-b", 7001),
        &felix_broker_service::cluster::credential::NodeCredential::new(fleet_token.clone()),
    )
    .await
    .expect_err("should be refused");
    assert!(
        matches!(err, MembershipError::Rejected(_)),
        "a wrong address stays wrong, so this must not be retried: {err:?}",
    );

    cluster.shutdown().await;
}

/// The whole loop: register, heartbeat, and stay live across several expiry
/// windows without anything else intervening.
#[tokio::test]
async fn heartbeats_keep_a_broker_live_past_its_expiry_window() {
    let cluster = Cluster::start().await;
    let serving = CancellationToken::new();
    serving.cancel();
    let shutdown = CancellationToken::new();

    let task = membership::spawn(
        cluster.client.clone(),
        cluster.base_url.clone(),
        config("broker-a", 7001),
        felix_broker_service::cluster::credential::NodeCredential::new(cluster.token.clone()),
        serving,
        shutdown.clone(),
        std::sync::Arc::new(felix_broker_service::cluster::lease::LeaseState::new(
            Duration::from_secs(30),
        )),
        std::sync::Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        std::sync::Arc::default(),
    );

    // Wait for registration to land.
    for _ in 0..200 {
        if cluster.store.get_node("broker-a").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Sweep repeatedly across more than one expiry window. A live broker must
    // survive every pass.
    for _ in 0..12 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        felix_controlplane_service::cluster::membership::expire_once(
            cluster.store.as_ref(),
            &LIVENESS,
            felix_controlplane_service::clock::now_millis(),
        )
        .await;
    }

    assert_eq!(cluster.lifecycle("broker-a").await, NodeLifecycle::Live);
    assert_eq!(task.consecutive_failures.load(Ordering::Acquire), 0);

    shutdown.cancel();
    let _ = task.handle.await;
    cluster.shutdown().await;
}

/// End to end, through the real broker client and the real control plane: a
/// broker's own credential cannot be turned on another broker.
///
/// This is the hole that was open — anyone who could reach the control plane
/// could deregister any broker in the fleet.
#[tokio::test]
async fn a_brokers_credential_cannot_deregister_another_broker() {
    let cluster = Cluster::start().await;

    // broker-b exists, registered by an operator.
    let fleet_token = cluster.fleet_token();
    membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-b", 7002),
        &felix_broker_service::cluster::credential::NodeCredential::new(fleet_token.clone()),
    )
    .await
    .expect("register broker-b");
    assert_eq!(cluster.lifecycle("broker-b").await, NodeLifecycle::Live);

    // broker-a holds only its own credential.
    let err = membership::deregister(
        &cluster.client,
        &cluster.base_url,
        "broker-b",
        &cluster.token,
    )
    .await
    .expect_err("broker-a must not deregister broker-b");
    assert!(err.to_string().contains("403"), "{err}");

    assert_eq!(
        cluster.lifecycle("broker-b").await,
        NodeLifecycle::Live,
        "the attempt must leave broker-b untouched",
    );

    cluster.shutdown().await;
}

/// A broker with no credential at all gets nowhere, and finds out at
/// registration rather than silently running as a non-member.
#[tokio::test]
async fn an_unauthenticated_broker_cannot_register() {
    let cluster = Cluster::start().await;

    let err = membership::register(
        &cluster.client,
        &cluster.base_url,
        &config("broker-a", 7001),
        &felix_broker_service::cluster::credential::NodeCredential::new(""),
    )
    .await
    .expect_err("should be refused");
    assert!(
        matches!(err, MembershipError::Rejected(_)),
        "a missing credential is terminal, not something to retry: {err:?}",
    );

    cluster.shutdown().await;
}

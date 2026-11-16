//! The floor reporter against the real control-plane router, so the request
//! body, the auth scope and the store's floor rule are checked together.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::IntoResponse;
use felix_controlplane_service::api::types::{FeatureFlags, Region};
use felix_controlplane_service::api::{AppState, build_router};
use felix_controlplane_service::auth::felix_token;
use felix_controlplane_service::config::NodeLivenessConfig;
use felix_controlplane_service::model::{
    self as model, ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity,
    NodeLifecycle, NodeSpec, NodeStatus, RetentionPolicy, SealReport, SealedSideLog,
    ShardAssignment, ShardState, Stream, StreamKind, Tenant,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::{
    AssignmentWrite, AuthStore, ControlPlaneStore, StoreConfig,
};
use felix_replication::controlplane_link::HttpControlPlaneLink;
use felix_replication::raft::LogPoint;

use super::{at, key};
use crate::shards::raft::floor::{Floors, tick};
use crate::shards::{ShardKey, ShardKind};

const TENANT: &str = "t1";
const COPY_X: &str = "000102030405060708090a0b0c0d0e0f";
const COPY_Y: &str = "101112131415161718191a1b1c1d1e1f";

struct ControlPlane {
    store: Arc<InMemoryStore>,
    link: HttpControlPlaneLink,
    token: String,
    /// While set, POST .../raft-floors answers 409.
    refuse: Arc<AtomicBool>,
    /// POST .../raft-floors requests that reached the layer.
    posts: Arc<AtomicUsize>,
}

impl ControlPlane {
    async fn start() -> Self {
        let store = Arc::new(InMemoryStore::new(StoreConfig {
            changes_limit: 1000,
            change_retention_max_rows: Some(1000),
        }));
        let keys = felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys");
        store
            .set_tenant_signing_keys(TENANT, keys.clone())
            .await
            .expect("keys");
        let token = felix_token::mint_token_for(
            &keys,
            TENANT,
            "p:broker-x",
            vec!["node.manage:node:broker-x".to_string()],
            Duration::from_secs(900),
            felix_token::CONTROLPLANE_AUDIENCE,
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
            node_liveness: NodeLivenessConfig {
                heartbeat_interval_ms: 1_000,
                expiry_timeout_ms: 60_000,
                regrant_margin_ms: None,
                sweep_interval_ms: 60_000,
                shard_reconcile_interval_ms: 60_000,
            },
            readiness: Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
                Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
            )),
            in_flight: Default::default(),
            placement_wakes: Default::default(),
            move_policy: Default::default(),
            new_cluster: Default::default(),
        };

        let refuse = Arc::new(AtomicBool::new(false));
        let posts = Arc::new(AtomicUsize::new(0));
        let (layer_refuse, layer_posts) = (Arc::clone(&refuse), Arc::clone(&posts));
        // A memory store has no metadata level; the real level-38 409 is covered
        // by the CP's raft_floors_wait_for_every_member_known_at_level_38.
        let router = build_router(state).layer(axum::middleware::from_fn(
            move |request: Request, next: Next| {
                let (refuse, posts) = (Arc::clone(&layer_refuse), Arc::clone(&layer_posts));
                async move {
                    let floors = request.method() == axum::http::Method::POST
                        && request.uri().path().ends_with("/raft-floors");
                    if floors {
                        posts.fetch_add(1, Ordering::SeqCst);
                        if refuse.load(Ordering::SeqCst) {
                            return felix_controlplane_service::api::error::api_conflict(
                                "conflict",
                                "a Raft offset floor report needs every control-plane member \
                                 at metadata level 38",
                            )
                            .into_response();
                        }
                    }
                    next.run(request).await
                }
            },
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router.into_make_service()).await;
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .no_proxy()
            .build()
            .expect("client");
        Self {
            store,
            link: HttpControlPlaneLink::new(client, &format!("http://{addr}")),
            token,
            refuse,
            posts,
        }
    }

    async fn tick(&self, floors: &mut Floors, led: Vec<(ShardKey, u64, LogPoint)>) {
        tick(floors, led, &self.link, "broker-x", Some(&self.token)).await;
    }

    async fn floor(&self, key: &model::ShardKey) -> Option<(u64, u64, u64)> {
        self.store
            .get_shard_assignment(key)
            .await
            .expect("assignment")
            .raft_floor
            .map(|floor| (floor.incarnation, floor.index, floor.term))
    }
}

fn node(node_id: &str, port: u16) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: format!("10.0.0.4:{port}"),
            client_addr: None,
            kafka_addr: None,
            region: "us-west-2".to_string(),
            zone: None,
            labels: BTreeMap::new(),
            capacity: NodeCapacity {
                max_shards: Some(64),
                weight: 1,
            },
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 1_700_000_000_000,
            registered_at_millis: 1_699_000_000_000,
            incarnation: 0,
            features: Default::default(),
            storage_id: None,
        },
    }
}

fn written(write: AssignmentWrite) -> ShardAssignment {
    match write {
        AssignmentWrite::Written(stored) => stored,
        other => panic!("expected a write, got {other:?}"),
    }
}

/// Stream shard 0 of "a", led by broker-x with broker-y as its replica, sealed
/// and switched to Raft with both copies voting. As the CP's own switch
/// contract builds it.
async fn switched(store: &InMemoryStore) -> ShardAssignment {
    store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Floors".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: TENANT.to_string(),
            namespace: "ns".to_string(),
            display_name: "Floors".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: TENANT.to_string(),
            namespace: "ns".to_string(),
            stream: "a".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
            routing: Default::default(),
            incarnation: 0,
            mount: None,
        })
        .await
        .expect("stream");
    for (i, id) in ["broker-x", "broker-y"].iter().enumerate() {
        store
            .register_node(node(id, 7500 + i as u16))
            .await
            .expect("register");
    }

    let key = key("a");
    let mut wanted = ShardAssignment {
        key: model::ShardKey {
            tenant_id: key.tenant_id,
            namespace: key.namespace,
            stream: key.stream,
            shard: key.shard,
            kind: model::ShardKind::Stream,
        },
        leader: "broker-x".to_string(),
        replicas: vec!["broker-y".to_string()],
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: BTreeMap::from([("broker-x".to_string(), COPY_X.to_string())]),
        learners: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: None,
        raft_copy_ids: Default::default(),
        raft_target: None,
        raft_floor: None,
        raft_ordered: None,
    };
    store
        .put_shard_assignment(wanted.clone())
        .await
        .expect("assign");
    wanted.state = ShardState::Active;
    let active = store.put_shard_assignment(wanted).await.expect("activate");
    let order = written(
        store
            .seal_shard(&active.key, active.generation)
            .await
            .expect("seal"),
    );
    let side = SealedSideLog {
        start: 0,
        end: 3,
        digest: "cd".repeat(32),
    };
    let report = SealReport {
        generation: order.generation,
        offset: 40,
        start: 2,
        digest: "ab".repeat(32),
        dead_letters: side.clone(),
        cursors: side,
        voters: BTreeMap::from([
            ("broker-x".to_string(), COPY_X.to_string()),
            ("broker-y".to_string(), COPY_Y.to_string()),
        ]),
    };
    let shard = written(
        store
            .accept_seal_report(&order.key, report, "broker-x")
            .await
            .expect("report"),
    );
    assert!(shard.switched());
    shard
}

/// Refused with a 409, the floor is not stored and stays owed; the first
/// accepted tick stores it even though the group was idle, and only movement
/// sends again. An untracked cache key in the batch costs nothing.
#[tokio::test]
async fn the_control_plane_stores_the_floor_once_it_stops_refusing() {
    let cp = ControlPlane::start().await;
    let shard = switched(&cp.store).await;
    let incarnation = shard.incarnation;
    let cache = ShardKey {
        kind: ShardKind::Cache,
        ..key("orders")
    };
    let led = |index, term| {
        vec![
            (key("a"), incarnation, at(term, index)),
            (cache.clone(), 0, at(1, 3)),
        ]
    };
    let mut floors = Floors::default();

    cp.refuse.store(true, Ordering::SeqCst);
    cp.tick(&mut floors, led(5, 1)).await;
    assert_eq!(cp.posts.load(Ordering::SeqCst), 1);
    assert_eq!(cp.floor(&shard.key).await, None);
    assert_eq!(floors.batch().len(), 2, "both entries are still owed");

    // Idle: nothing committed since the refused tick.
    cp.refuse.store(false, Ordering::SeqCst);
    cp.tick(&mut floors, led(5, 1)).await;
    assert_eq!(cp.posts.load(Ordering::SeqCst), 2);
    assert_eq!(cp.floor(&shard.key).await, Some((incarnation, 5, 1)));

    cp.tick(&mut floors, led(5, 1)).await;
    assert_eq!(cp.posts.load(Ordering::SeqCst), 2, "nothing moved");

    cp.tick(&mut floors, led(9, 2)).await;
    assert_eq!(cp.posts.load(Ordering::SeqCst), 3);
    assert_eq!(cp.floor(&shard.key).await, Some((incarnation, 9, 2)));
}

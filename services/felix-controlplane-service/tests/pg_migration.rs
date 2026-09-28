#![cfg(feature = "pg-tests")]
//! The migration, run for real: a populated Postgres control plane exported
//! through the store traits, imported into a Raft group as one proposed
//! command over the actual HTTP route the tool uses, and the two stores
//! compared where it matters — records, sequence heads, and the auth state
//! a tenant's tokens depend on.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_controlplane_service::auth::keys::generate_signing_keys;
use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity, NodeLifecycle, NodeSpec,
    NodeStatus, RetentionPolicy, ShardAssignment, ShardKey, ShardKind, ShardState, Stream,
    StreamKind, Tenant,
};
use felix_controlplane_service::raft::{AppStateMachine, NodeId, RaftHandle, RaftSettings};
use felix_controlplane_service::store::export::export_state_from;
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::command::{
    MetaCommand, decode_result, encode_command,
};
use felix_controlplane_service::store::raft::state_machine::MetadataStateMachine;
use felix_controlplane_service::store::{
    AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig,
};
use testcontainers::clients::Cli;

mod common;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// The container a test started, held so it is removed when the test ends.
///
/// `None` when `FELIX_TEST_DATABASE_URL` pointed at an existing database, in
/// which case there is nothing here to clean up.
type PgGuard =
    Option<testcontainers::Container<'static, testcontainers_modules::postgres::Postgres>>;

async fn postgres_store() -> Option<(
    felix_controlplane_service::store::postgres::PostgresStore,
    String,
    PgGuard,
)> {
    let mut container: PgGuard = None;
    let base_url = match std::env::var("FELIX_TEST_DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            if !docker_available() {
                eprintln!("skipping pg_migration: docker not available");
                return None;
            }
            // The client is leaked on purpose — it is a docker CLI wrapper
            // holding no container — so the container below can be `'static`
            // and travel back to the caller. The *container* is not leaked:
            // that is what removes it, and its anonymous volume, on drop.
            let docker: &'static Cli = Box::leak(Box::new(Cli::default()));
            // The module's default tag is Postgres 11; pin what `task pg:up` runs.
            let image = testcontainers::RunnableImage::from(
                testcontainers_modules::postgres::Postgres::default(),
            )
            .with_tag("16-alpine")
            .with_container_name(felix_test_container_name());
            let started = docker.run(image);
            let port = started.get_host_port_ipv4(5432);
            container = Some(started);
            format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres")
        }
    };

    let schema = common::unique_schema("felix_migrate");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await
        {
            Ok(pool) => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    r#"CREATE SCHEMA IF NOT EXISTS "{schema}""#
                )))
                .execute(&pool)
                .await
                .expect("create schema");
                pool.close().await;
                break;
            }
            Err(err) => {
                assert!(Instant::now() < deadline, "postgres never came up: {err}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let url = format!("{base_url}{separator}options=-csearch_path%3D{schema}");
    let store = felix_controlplane_service::store::postgres::PostgresStore::connect(
        &felix_controlplane_service::config::PostgresConfig {
            url: url.clone(),
            max_connections: 4,
            connect_timeout_ms: 5_000,
            acquire_timeout_ms: 5_000,
        },
        StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        },
    )
    .await
    .expect("connect and migrate");
    Some((store, url, container))
}

/// A control plane's worth of state, with enough churn that the sequence
/// heads are not trivially zero.
async fn seed(store: &(dyn ControlPlaneAuthStore + Send + Sync)) {
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_tenant(Tenant {
            tenant_id: "t-doomed".to_string(),
            display_name: "Doomed".to_string(),
        })
        .await
        .expect("tenant");
    store.delete_tenant("t-doomed").await.expect("churn");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            display_name: "NS One".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: Some(3_600),
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: true,
            region: None,
        })
        .await
        .expect("stream");
    store
        .register_node(Node {
            node_id: "broker-1".to_string(),
            spec: NodeSpec {
                advertise_addr: "10.0.0.4:7000".to_string(),
                client_addr: None,
                kafka_addr: None,
                region: "local".to_string(),
                zone: None,
                labels: BTreeMap::new(),
                capacity: NodeCapacity {
                    max_shards: Some(64),
                    weight: 1,
                },
            },
            status: NodeStatus {
                lifecycle: NodeLifecycle::Live,
                last_heartbeat_at_millis: 1_000,
                registered_at_millis: 1_000,
                incarnation: 0,
            },
        })
        .await
        .expect("node");
    store
        .put_shard_assignment(ShardAssignment {
            key: ShardKey {
                tenant_id: "t1".to_string(),
                namespace: "ns1".to_string(),
                stream: "orders".to_string(),
                shard: 0,
                kind: ShardKind::Stream,
            },
            leader: "broker-1".to_string(),
            replicas: Vec::new(),
            generation: 0,
            state: ShardState::Assigning,
            successor: None,
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
        })
        .await
        .expect("assignment");
    store
        .set_tenant_signing_keys("t1", generate_signing_keys().expect("keys"))
        .await
        .expect("keys");
    store
        .set_tenant_auth_bootstrapped("t1", true)
        .await
        .expect("flag");
}

#[tokio::test]
async fn a_postgres_control_plane_migrates_into_a_raft_group() {
    let Some((pg, _url, _container)) = postgres_store().await else {
        return;
    };
    seed(&pg).await;

    // Export exactly as the tool does: through the traits, heads carried.
    let exported = export_state_from(&pg).await.expect("export");

    // A single-member Raft group with the real HTTP propose route — the
    // wire path `migrate import` uses.
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(1_000),
    }));
    let machine = Arc::new(MetadataStateMachine::new(inner));
    let mut settings = RaftSettings::new(1, dir.path().into(), peer_security());
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(150), Duration::from_millis(300));
    let handle = RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
        .await
        .expect("start");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    handle
        .initialize(BTreeMap::from([(1 as NodeId, addr.to_string())]))
        .await
        .expect("initialize");
    let deadline = Instant::now() + Duration::from_secs(10);
    while handle.status().leader.is_none() {
        assert!(Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let command = encode_command(&MetaCommand::ImportState {
        state: Box::new(exported),
        overwrite: false,
    });
    let response = peer_security()
        .client(Duration::from_secs(30))
        .expect("peer client")
        .post(format!("http://{addr}/internal/raft/propose"))
        .body(command)
        .send()
        .await
        .expect("propose");
    assert!(response.status().is_success(), "{}", response.status());
    decode_result(&response.bytes().await.expect("body"))
        .expect("decodes")
        .expect("import succeeds");

    let raft_store = machine.store();

    // Records match, entity by entity.
    let pg_streams = pg.stream_snapshot().await.expect("pg streams");
    let raft_streams = raft_store.stream_snapshot().await.expect("raft streams");
    assert_eq!(pg_streams.items.len(), raft_streams.items.len());
    assert_eq!(
        pg_streams.next_seq, raft_streams.next_seq,
        "stream feed head"
    );

    let pg_tenants = pg.tenant_snapshot().await.expect("pg tenants");
    let raft_tenants = raft_store.tenant_snapshot().await.expect("raft tenants");
    assert_eq!(pg_tenants.items.len(), raft_tenants.items.len());
    assert_eq!(
        pg_tenants.next_seq, raft_tenants.next_seq,
        "the churn (create + delete) must be reflected in the head, not lost"
    );

    let pg_shards = pg.shard_assignment_snapshot().await.expect("pg shards");
    let raft_shards = raft_store
        .shard_assignment_snapshot()
        .await
        .expect("raft shards");
    assert_eq!(pg_shards.items.len(), raft_shards.items.len());
    assert_eq!(pg_shards.next_seq, raft_shards.next_seq);
    assert_eq!(
        pg_shards.items[0].generation, raft_shards.items[0].generation,
        "generations survive: a broker's staleness checks keep working"
    );

    // A broker checkpointed at the head continues without a resnapshot.
    let at_head = raft_store
        .shard_assignment_changes(pg_shards.next_seq)
        .await
        .expect("changes");
    assert!(at_head.items.is_empty());
    assert_eq!(at_head.next_seq, pg_shards.next_seq);

    // Auth state a tenant's tokens depend on.
    let pg_keys = pg.get_tenant_signing_keys("t1").await.expect("pg keys");
    let raft_keys = raft_store
        .get_tenant_signing_keys("t1")
        .await
        .expect("raft keys");
    assert_eq!(pg_keys.current.kid, raft_keys.current.kid);
    assert!(
        raft_store
            .tenant_auth_is_bootstrapped("t1")
            .await
            .expect("flag")
    );

    let _ = handle.shutdown().await;
    server.abort();
}

/// The tool's export half, run as the real binary against the real database.
#[tokio::test]
async fn the_cli_exports_a_snapshot_the_import_side_accepts() {
    let Some((pg, url, _container)) = postgres_store().await else {
        return;
    };
    seed(&pg).await;

    let out = tempfile::tempdir().expect("tempdir");
    let out_file = out.path().join("state.json");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_felix-controlplane"))
        .args(["migrate", "export-postgres"])
        .arg(&out_file)
        .env("FELIX_CONTROLPLANE_POSTGRES_URL", &url)
        .output()
        .expect("run export");
    assert!(
        output.status.success(),
        "export failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let bytes = std::fs::read(&out_file).expect("read export");
    let state: felix_controlplane_service::store::export::ExportedState =
        serde_json::from_slice(&bytes).expect("the file parses as an exported state");
    assert!(state.summary().contains("1 streams"), "{}", state.summary());
}

/// A name a cleanup can recognise as ours.
///
/// testcontainers sets no labels, so without this the only thing separating a
/// leftover test database from one an operator is running is the image tag —
/// which is not enough to delete on. Unique per container, since a fixed name
/// would collide between concurrent runs.
fn felix_test_container_name() -> String {
    format!(
        "felix-test-pg-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

/// Peer credentials every member of a test group shares.
fn peer_security() -> felix_controlplane_service::raft::PeerSecurity {
    felix_controlplane_service::raft::PeerSecurity {
        cluster_id: "test-cluster".to_string(),
        token: Some("0123456789abcdef0123456789abcdef".to_string()),
        tls: None,
    }
}

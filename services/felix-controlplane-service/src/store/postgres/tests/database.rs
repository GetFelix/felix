//! Postgres store unit tests with real DB integration.
//!
//! Exercise the Postgres-backed store with real SQL to verify schema, migrations,
//! and CRUD/change-log behavior.
//!
//! - Tests use a dedicated schema to avoid cross-test contamination.
//! - Migrations are run once per schema to minimize test time.
//! - Reset logic truncates tables to a clean baseline.
//!
//! - Database URLs may contain credentials; tests should not log them.
//! - Docker-based Postgres is used only in test environments.
//!
//! - Tests are serialized to avoid shared schema races.
//! - OnceCell guards ensure container/migration setup occurs once.
//!
//! Run with `cargo test -p felix-controlplane-service --features pg-tests postgres_store_full_roundtrip`.
//!
//! These tests live in a separate module so coverage is attributed to the production
//! `postgres.rs` implementation without inflating that file's line counts.
#![cfg(feature = "pg-tests")]

use std::time::Duration;

use serial_test::serial;
use sqlx::AssertSqlSafe;
use sqlx::Connection;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use testcontainers::clients::Cli;
use testcontainers::core::Container;
use testcontainers_modules::postgres::Postgres;

use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::keys::generate_signing_keys;
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::config;
use crate::model::{
    Cache, CacheKey, CachePatchRequest, ConsistencyLevel, DeliveryGuarantee, Namespace,
    NamespaceKey, RetentionPolicy, Stream, StreamKey, StreamKind, StreamPatchRequest, Tenant,
};
use crate::store::postgres::PostgresStore;
use crate::store::{AuthStore, ControlPlaneStore, StoreConfig};

struct PgContainer {
    url: String,
    _container: Container<'static, Postgres>,
}

static PG_CONTAINER: tokio::sync::OnceCell<PgContainer> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static PG_MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

static MIGRATOR: Migrator = sqlx::migrate!();

fn docker_available() -> bool {
    // We probe docker availability to decide whether to spin up a container.
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

async fn wait_for_postgres(url: &str, timeout: Duration) -> Result<(), sqlx::Error> {
    // Poll until Postgres is ready or the timeout is reached to avoid flaky startup.
    let start = tokio::time::Instant::now();
    loop {
        let attempt = tokio::time::timeout(
            Duration::from_secs(5),
            PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(3))
                .connect(url),
        )
        .await;
        match attempt {
            Ok(Ok(pool)) => {
                let _ = pool.close().await;
                return Ok(());
            }
            Ok(Err(err)) => {
                if start.elapsed() >= timeout {
                    return Err(err);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(_) => {
                if start.elapsed() >= timeout {
                    return Err(sqlx::Error::PoolTimedOut);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn pg_url() -> Option<String> {
    // Prefer explicitly configured URLs to avoid using docker in CI unless needed.
    if let Ok(url) = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
    {
        return Some(url);
    }
    if !docker_available() {
        // We skip tests gracefully when docker is unavailable.
        eprintln!("skipping pg-tests: docker not available");
        return None;
    }
    let container = PG_CONTAINER
        .get_or_try_init(|| async {
            // Use a single long-lived container to keep tests deterministic and fast.
            let docker = Box::leak(Box::new(Cli::default()));
            // The module's default tag is Postgres 11, which predates the
            // generated columns migration 0009 uses; pin the version the
            // supported deployment path (`task pg:up`) runs.
            let container = docker.run(
                testcontainers::RunnableImage::from(Postgres::default()).with_tag("16-alpine"),
            );
            let port = container.get_host_port_ipv4(5432);
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            wait_for_postgres(&url, Duration::from_secs(30)).await?;
            Ok::<_, sqlx::Error>(PgContainer {
                url,
                _container: container,
            })
        })
        .await
        .ok()?;
    Some(container.url.clone())
}

async fn test_schema_name() -> String {
    // Unique schema per test run avoids collisions across processes.
    PG_SCHEMA
        .get_or_init(|| async {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("felix_test_{}_{}", std::process::id(), nanos)
        })
        .await
        .clone()
}

fn url_with_schema(base_url: &str, schema: &str) -> String {
    // We pass search_path via connection options to isolate test schema.
    let encoded = format!("-csearch_path%3D{}", schema);
    if base_url.contains('?') {
        format!("{base_url}&options={encoded}")
    } else {
        format!("{base_url}?options={encoded}")
    }
}

async fn ensure_schema(base_url: &str) -> Result<String, sqlx::Error> {
    // Create the schema once so subsequent tests can reuse it.
    let schema = test_schema_name().await;
    let schema_clone = schema.clone();
    PG_SCHEMA_READY
        .get_or_try_init(|| async move {
            let mut conn = sqlx::PgConnection::connect(base_url).await?;
            let create_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, schema_clone);
            // A schema name cannot be a bind parameter, and this one is generated from our own
            // pid and clock in `test_schema_name` — it never carries external input.
            sqlx::query(AssertSqlSafe(create_sql))
                .execute(&mut conn)
                .await?;
            Ok::<_, sqlx::Error>(())
        })
        .await?;
    Ok(schema)
}

async fn run_migrations_once(url: &str) -> Result<(), sqlx::Error> {
    // Migrations are expensive; run them once per schema to reduce flakiness.
    PG_MIGRATED
        .get_or_try_init(|| async move {
            let mut conn = sqlx::PgConnection::connect(url).await?;
            MIGRATOR.run(&mut conn).await?;
            conn.close().await?;
            Ok::<_, sqlx::Error>(())
        })
        .await?;
    Ok(())
}

async fn reset_db(url: &str, schema: &str) -> Result<(), sqlx::Error> {
    // This reset ensures each test starts from a clean slate.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(url)
        .await?;

    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(format!("{schema}.tenant_changes"))
        .fetch_one(&pool)
        .await?;
    if exists.is_none() {
        // If tables don't exist yet, there's nothing to truncate.
        pool.close().await;
        return Ok(());
    }

    let schema_ident = format!(r#""{}""#, schema);
    let truncate = format!(
        "TRUNCATE {schema_ident}.tenant_changes, {schema_ident}.namespace_changes, \
         {schema_ident}.stream_changes, {schema_ident}.cache_changes, {schema_ident}.streams, \
         {schema_ident}.caches, {schema_ident}.namespaces, {schema_ident}.idp_issuers, \
         {schema_ident}.rbac_policies, {schema_ident}.rbac_groupings, \
         {schema_ident}.tenant_signing_keys, {schema_ident}.nodes, \
         {schema_ident}.node_changes, {schema_ident}.shard_assignments, \
         {schema_ident}.shard_assignment_changes, {schema_ident}.tenants, \
         {schema_ident}.fleet_features RESTART IDENTITY CASCADE",
    );
    // Same as `ensure_schema`: the interpolated identifier is our own generated schema name,
    // which cannot be passed as a bind parameter.
    sqlx::query(AssertSqlSafe(truncate)).execute(&pool).await?;
    // A one-row table: reset, not truncated, so the row the migration made
    // stays.
    let unpause = format!("UPDATE {schema_ident}.placement_settings SET moves_paused = false");
    sqlx::query(AssertSqlSafe(unpause)).execute(&pool).await?;
    let unlease = format!(
        "UPDATE {schema_ident}.placement_lease SET holder = NULL, expires_at = NULL, token = 0"
    );
    sqlx::query(AssertSqlSafe(unlease))
        .execute(&pool)
        .await
        .map(|_| ())
}

/// Node records are authoritative state, so a new store over the same database
/// must find them exactly as they were -- including the observed status a
/// restarting broker cannot reconstruct for itself.
#[tokio::test]
#[serial]
async fn a_reconnected_store_still_sees_registered_nodes() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 10_000,
        acquire_timeout_ms: 10_000,
    };
    let store_config = StoreConfig {
        changes_limit: config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: None,
    };

    let before = {
        let store =
            PostgresStore::connect_without_migrations(&pg_cfg, store_config.clone()).await?;
        let node = store
            .register_node(crate::store::contract::nodes::node("broker-a", 7001))
            .await?;
        store
            .record_node_heartbeat("broker-a", 0, 1_800_000_000_000)
            .await?;
        store.node_snapshot().await?;
        drop(store);
        node
    };

    // A different store handle over the same database, which is what a restart
    // of the control plane looks like from here.
    let store = PostgresStore::connect_without_migrations(&pg_cfg, store_config).await?;
    let after = store.get_node("broker-a").await?;

    assert_eq!(after.node_id, before.node_id);
    assert_eq!(after.spec, before.spec);
    assert_eq!(
        after.status.registered_at_millis,
        before.status.registered_at_millis
    );
    assert_eq!(after.status.incarnation, before.status.incarnation);
    assert_eq!(
        after.status.last_heartbeat_at_millis, 1_800_000_000_000,
        "the last observed heartbeat survives, so expiry can judge staleness",
    );

    // The changefeed survives too, so a client resuming after the restart is
    // not forced to re-bootstrap.
    let changes = store.node_changes(0).await?;
    assert!(changes.items.iter().any(|c| c.node_id == "broker-a"));
    Ok(())
}

/// Assignments are authoritative state, so a new store over the same database
/// must find them exactly as they were, generation included.
#[tokio::test]
#[serial]
async fn a_reconnected_store_still_sees_shard_assignments() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 10_000,
        acquire_timeout_ms: 10_000,
    };
    let store_config = StoreConfig {
        changes_limit: config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: None,
    };

    let before = {
        let store =
            PostgresStore::connect_without_migrations(&pg_cfg, store_config.clone()).await?;
        let store = std::sync::Arc::new(store);
        crate::store::contract::shards::seed_for_restart(store.as_ref()).await;
        let assigned = crate::store::contract::shards::assign_for_restart(store.as_ref()).await;
        drop(store);
        assigned
    };

    // A different store handle over the same database: a control-plane restart.
    let store = PostgresStore::connect_without_migrations(&pg_cfg, store_config).await?;
    let after = store.get_shard_assignment(&before.key).await?;
    assert_eq!(after, before, "the assignment survives unchanged");

    // The changefeed survives too, so a broker resuming after the restart is not
    // forced to re-bootstrap its ownership view.
    let changes = store.shard_assignment_changes(0).await?;
    assert!(changes.items.iter().any(|c| c.key == before.key));
    Ok(())
}

/// The placement suite, as two instances: two stores, each with its own
/// pool, over one database.
#[tokio::test]
#[serial]
async fn satisfies_the_placement_contract() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let mut instances = Vec::new();
    for _ in 0..2 {
        let store: std::sync::Arc<dyn crate::store::ControlPlaneStore> = std::sync::Arc::new(
            PostgresStore::connect_without_migrations(
                &config::PostgresConfig {
                    url: url.clone(),
                    max_connections: 5,
                    connect_timeout_ms: 10_000,
                    acquire_timeout_ms: 10_000,
                },
                StoreConfig {
                    changes_limit: config::DEFAULT_CHANGES_LIMIT,
                    change_retention_max_rows: None,
                },
            )
            .await?,
        );
        instances.push(store);
    }
    let (a, b) = (instances[0].clone(), instances[1].clone());
    crate::store::contract::placement::run_placement_contract(a.clone(), b.clone()).await;
    crate::store::contract::placement::run_expiring_lease_contract(a, b).await;
    Ok(())
}

/// The same shard suite the memory store runs, so parity is enforced.
#[tokio::test]
#[serial]
async fn satisfies_the_shard_store_contract() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let store = PostgresStore::connect_without_migrations(
        &config::PostgresConfig {
            url,
            max_connections: 5,
            connect_timeout_ms: 10_000,
            acquire_timeout_ms: 10_000,
        },
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;

    let store = std::sync::Arc::new(store);
    crate::store::contract::shards::run_shard_contract(store.clone()).await;
    crate::store::contract::shards::run_shard_concurrency_contract(store.clone()).await;
    crate::store::contract::shards::run_node_delete_race_contract(store.clone(), 300).await;
    crate::store::contract::refresh_tokens::run_refresh_contract(store.clone()).await;
    crate::store::contract::rbac::run_rbac_contract(store.clone()).await;
    crate::store::contract::pagination::run_pagination_contract(store).await;
    Ok(())
}

/// The same signing-key suite the memory store runs. A pool wider than the
/// default so the racing callers really hold separate connections, as they
/// would on separate control-plane instances.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn satisfies_the_signing_key_contract() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let store = PostgresStore::connect_without_migrations(
        &config::PostgresConfig {
            url,
            max_connections: 16,
            connect_timeout_ms: 10_000,
            acquire_timeout_ms: 10_000,
        },
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;

    crate::store::contract::signing_keys::run_signing_key_contract(std::sync::Arc::new(store))
        .await;
    Ok(())
}

/// The property `node_change_seq` exists to provide: a second writer cannot take
/// a seq until the first has committed.
///
/// Without it -- with the `BIGSERIAL` the other change tables use -- writer 2
/// takes seq 1 and commits while writer 1 still holds seq 0 uncommitted. A
/// snapshot in that window reads `MAX(seq) + 1 = 2`, so when writer 1 finally
/// commits, seq 0 is below every consumer's resume point and is never
/// delivered. Serialising the allocation is what removes that window, and this
/// asserts the serialisation rather than trusting it.
#[tokio::test]
#[serial]
async fn a_node_change_seq_is_not_handed_out_until_the_holder_commits() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    const ALLOCATE: &str =
        "UPDATE node_change_seq SET next_seq = next_seq + 1 RETURNING next_seq - 1";

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;

    let mut holder = pool.begin().await?;
    let first: i64 = sqlx::query_scalar(ALLOCATE).fetch_one(&mut *holder).await?;

    // Spawned rather than timed out in place: dropping a sqlx future does not
    // cancel the statement the server is already running, so the contender has
    // to be the same attempt that eventually succeeds.
    let mut contender = tokio::spawn({
        let pool = pool.clone();
        async move {
            let mut tx = pool.begin().await?;
            let seq: i64 = sqlx::query_scalar(ALLOCATE).fetch_one(&mut *tx).await?;
            tx.commit().await?;
            Ok::<i64, sqlx::Error>(seq)
        }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(750), &mut contender)
            .await
            .is_err(),
        "a second writer took a seq while {first} was still uncommitted",
    );

    holder.commit().await?;

    let second = tokio::time::timeout(Duration::from_secs(5), contender)
        .await
        .expect("the second writer should proceed once the first commits")??;

    assert_eq!(
        second,
        first + 1,
        "seq order must follow commit order with no gap",
    );
    pool.close().await;
    Ok(())
}

/// The same suite the memory store runs, so parity is enforced rather than
/// assumed. Skips silently when no database is configured, like every other
/// test in this file.
#[tokio::test]
#[serial]
async fn satisfies_the_node_store_contract() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let store = PostgresStore::connect_without_migrations(
        &config::PostgresConfig {
            url,
            max_connections: 5,
            connect_timeout_ms: 10_000,
            acquire_timeout_ms: 10_000,
        },
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;

    let store = std::sync::Arc::new(store);
    crate::store::contract::nodes::run_node_contract(store.clone()).await;
    crate::store::contract::nodes::run_node_concurrency_contract(store.clone()).await;
    crate::store::contract::nodes::run_fleet_contract(store).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn postgres_store_full_roundtrip() -> anyhow::Result<()> {
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let schema = ensure_schema(&url).await?;
    let url = url_with_schema(&url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;

    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 10_000,
        acquire_timeout_ms: 10_000,
    };
    let store = PostgresStore::connect_without_migrations(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;

    let stream_key = StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
    };
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: Some(3600),
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
            region: Some("eu-west-1".to_string()),
        })
        .await?;
    store
        .patch_stream(
            &stream_key,
            StreamPatchRequest {
                retention: Some(RetentionPolicy {
                    max_age_seconds: Some(7200),
                    max_size_bytes: Some(2048),
                }),
                consistency: Some(ConsistencyLevel::Quorum),
                delivery: Some(DeliveryGuarantee::AtMostOnce),
                durable: Some(true),
            },
        )
        .await?;
    // The region is written with the stream and survives a patch.
    assert_eq!(
        store.get_stream(&stream_key).await?.region.as_deref(),
        Some("eu-west-1")
    );

    let cache_key = CacheKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
    };
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: crate::model::ConsistencyLevel::Leader,
        })
        .await?;
    store
        .patch_cache(
            &cache_key,
            CachePatchRequest {
                display_name: Some("Primary Updated".to_string()),
            },
        )
        .await?;

    let tenant_snapshot = store.tenant_snapshot().await?;
    assert!(!tenant_snapshot.items.is_empty());
    let ns_snapshot = store.namespace_snapshot().await?;
    assert!(!ns_snapshot.items.is_empty());
    let stream_snapshot = store.stream_snapshot().await?;
    assert!(!stream_snapshot.items.is_empty());
    let cache_snapshot = store.cache_snapshot().await?;
    assert!(!cache_snapshot.items.is_empty());

    let tenant_changes = store.tenant_changes(0).await?;
    assert!(!tenant_changes.items.is_empty());
    let ns_changes = store.namespace_changes(0).await?;
    assert!(!ns_changes.items.is_empty());
    let stream_changes = store.stream_changes(0).await?;
    assert!(!stream_changes.items.is_empty());
    let cache_changes = store.cache_changes(0).await?;
    assert!(!cache_changes.items.is_empty());

    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: "https://issuer.example.com".to_string(),
                audiences: vec!["felix-controlplane".to_string()],
                discovery_url: None,
                jwks_url: Some("https://issuer.example.com/jwks".to_string()),
                claim_mappings: Default::default(),
            },
        )
        .await?;
    store
        .add_rbac_policy(
            "t1",
            PolicyRule {
                subject: "role:admin".to_string(),
                object: "tenant:t1".to_string(),
                action: "tenant.manage".to_string(),
            },
        )
        .await?;
    store
        .add_rbac_grouping(
            "t1",
            GroupingRule {
                user: "p:admin".to_string(),
                role: "role:admin".to_string(),
            },
        )
        .await?;
    let keys = generate_signing_keys()?;
    store.set_tenant_signing_keys("t1", keys).await?;
    store.set_tenant_auth_bootstrapped("t1", true).await?;

    store.delete_cache(&cache_key).await?;
    store.delete_stream(&stream_key).await?;
    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
        })
        .await?;
    store.delete_tenant("t1").await?;
    store
        .delete_idp_issuer("t1", "https://issuer.example.com")
        .await?;
    store.health_check().await?;
    assert!(store.is_durable());
    assert_eq!(store.backend_name(), "postgres");

    let _ = PostgresStore::connect(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;

    Ok(())
}

/// A Postgres store in a freshly reset schema, and a pool on that schema for
/// the test's own transactions.
async fn fleet_store(url: &str) -> anyhow::Result<(std::sync::Arc<PostgresStore>, sqlx::PgPool)> {
    let schema = ensure_schema(url).await?;
    let url = url_with_schema(url, &schema);
    run_migrations_once(&url).await?;
    reset_db(&url, &schema).await?;
    let store = PostgresStore::connect_without_migrations(
        &config::PostgresConfig {
            url: url.clone(),
            max_connections: 5,
            connect_timeout_ms: 10_000,
            acquire_timeout_ms: 10_000,
        },
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await?;
    Ok((std::sync::Arc::new(store), pool))
}

/// Hold `node_id`'s row, so a registration of it passes the fleet check and
/// then waits, holding the registration lock, before it can commit.
async fn hold_row(
    pool: &sqlx::PgPool,
    node_id: &str,
) -> anyhow::Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT node_id FROM nodes WHERE node_id = $1 FOR UPDATE")
        .bind(node_id)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// Spawn `step` and give it long enough to reach whatever it will block on.
async fn started<T: Send + 'static>(
    step: impl std::future::Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    let handle = tokio::spawn(step);
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle
}

/// A finalize that ran while a broker without the feature was registering
/// would pass its check (that broker is down) and enable the feature under
/// it. It has to wait for the registration and then see the broker serving.
#[tokio::test]
#[serial]
async fn a_finalize_waits_for_a_registration_in_flight() -> anyhow::Result<()> {
    use crate::model::NodeLifecycle;
    use crate::store::contract::nodes::{node, with_features};
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let (store, pool) = fleet_store(&url).await?;
    store
        .register_node(with_features(node("broker-a", 7001), &["x"]))
        .await?;
    store.register_node(node("broker-old", 7002)).await?;
    store
        .set_node_lifecycle("broker-old", NodeLifecycle::Down)
        .await?;

    let held = hold_row(&pool, "broker-old").await?;
    let old = {
        let store = std::sync::Arc::clone(&store);
        started(async move { store.register_node(node("broker-old", 7002)).await }).await
    };
    let finalize = {
        let store = std::sync::Arc::clone(&store);
        started(async move { store.finalize_fleet_feature("x").await }).await
    };
    held.rollback().await?;
    old.await??;
    let finalized = finalize.await?;
    assert!(
        finalized.is_err(),
        "x was enabled while broker-old, which lacks it, was registering: {finalized:?}"
    );
    assert!(store.enabled_fleet_features().await?.is_empty());
    Ok(())
}

/// The other order: a registration that ran while a finalize was between its
/// check and its commit would see the feature still off and be admitted.
#[tokio::test]
#[serial]
async fn a_registration_waits_for_a_finalize_in_flight() -> anyhow::Result<()> {
    use crate::store::contract::nodes::{node, with_features};
    let Some(url) = pg_url().await else {
        return Ok(());
    };
    let (store, pool) = fleet_store(&url).await?;
    store
        .register_node(with_features(node("broker-a", 7001), &["x"]))
        .await?;

    // An uncommitted row for x makes the finalize's insert wait on it.
    let mut held = pool.begin().await?;
    sqlx::query("INSERT INTO fleet_features (feature) VALUES ('x')")
        .execute(&mut *held)
        .await?;
    let finalize = {
        let store = std::sync::Arc::clone(&store);
        started(async move { store.finalize_fleet_feature("x").await }).await
    };
    let old = {
        let store = std::sync::Arc::clone(&store);
        started(async move { store.register_node(node("broker-old", 7002)).await }).await
    };
    held.rollback().await?;
    finalize.await??;
    let registered = old.await?;
    assert!(
        registered.is_err(),
        "broker-old joined without x while x was being enabled: {registered:?}"
    );
    Ok(())
}

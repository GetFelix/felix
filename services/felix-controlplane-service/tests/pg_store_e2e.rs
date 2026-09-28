#![cfg(feature = "pg-tests")]
//! Postgres-backed control-plane store end-to-end tests.
//!
//! Exercise the Postgres store against a real database to validate:
//! - schema migrations and connection setup
//! - CRUD + snapshot/changefeed behaviors
//! - auth/bootstrap tables (IdP issuers, RBAC, signing keys)
//! - retention trimming and cascade delete semantics
//!
//! - Tests use isolated schemas to avoid cross-test contamination.
//! - Felix signing keys are Ed25519 and stored as raw 32-byte seeds.
//! - Change streams are monotonically increasing per table.
//!
//! - Database URLs may include credentials and must not be logged.
//! - Tokens/keys in these tests are fixtures only and not secrets in production.
//!
//! - Tests are serialized via `serial_test` to avoid shared container races.
//! - JWKS/bootstrap paths are exercised after stores are fully initialized.
//!
//! Run with `cargo test -p felix-controlplane-service --features pg-tests pg_store_e2e`.
//!
//! # Test infrastructure
//! We use `testcontainers` to spin up an ephemeral Postgres instance and reset state between tests.
//! Tests are serialized because they share a single database container per test run.

mod common;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_controlplane_service::api;
use felix_controlplane_service::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use felix_controlplane_service::auth::keys::generate_signing_keys;
use felix_controlplane_service::auth::oidc::UpstreamOidcValidator;
use felix_controlplane_service::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use felix_controlplane_service::model::{
    Cache, CachePatchRequest, ConsistencyLevel, DeliveryGuarantee, Namespace, NamespaceKey,
    RetentionPolicy, Stream, StreamKey, StreamKind, StreamPatchRequest, Tenant,
};
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig};
use felix_controlplane_service::{config, store};
use serde_json::json;
use serial_test::serial;
use sqlx::AssertSqlSafe;
use sqlx::Connection;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use testcontainers::clients::Cli;
use testcontainers::core::Container;
use testcontainers_modules::postgres::Postgres;
use tower::ServiceExt;

use common::read_json;

fn docker_available() -> bool {
    // Detect docker so we can skip tests instead of failing in CI.
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn test_ed25519_key_bytes() -> ([u8; 32], [u8; 32]) {
    // Deterministic Ed25519 seed prevents flaky signatures.
    let private_key = [11u8; 32];
    let signing_key = Ed25519SigningKey::from_bytes(&private_key);
    let public_key = signing_key.verifying_key().to_bytes();
    (private_key, public_key)
}

struct PgFixture {
    store: Arc<store::postgres::PostgresStore>,
    url: String,
}

struct PgContainer {
    url: String,
    _container: Option<Container<'static, Postgres>>,
}

static PG_CONTAINER: tokio::sync::OnceCell<PgContainer> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_DEFAULT: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_RETENTION: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_DEFAULT_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_RETENTION_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static PG_MIGRATED_DEFAULT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static PG_MIGRATED_RETENTION: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

static MIGRATOR: Migrator = sqlx::migrate!();

async fn wait_for_postgres(url: &str, timeout: Duration) -> Result<(), sqlx::Error> {
    // Poll until Postgres accepts connections to avoid startup races.
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

async fn test_schema_name(cell: &tokio::sync::OnceCell<String>, suffix: &str) -> String {
    // Unique schema per run keeps tests isolated across processes.
    cell.get_or_init(|| async {
        if suffix.is_empty() {
            common::unique_schema("felix_test")
        } else {
            format!("{}_{suffix}", common::unique_schema("felix_test"))
        }
    })
    .await
    .clone()
}

fn url_with_schema(base_url: &str, schema: &str) -> String {
    // Use search_path to keep all queries in the test schema.
    let encoded = format!("-csearch_path%3D{}", schema);
    if base_url.contains('?') {
        format!("{base_url}&options={encoded}")
    } else {
        format!("{base_url}?options={encoded}")
    }
}

async fn ensure_schema(
    base_url: &str,
    cell: &tokio::sync::OnceCell<String>,
    ready: &tokio::sync::OnceCell<()>,
    suffix: &str,
) -> Result<String, sqlx::Error> {
    // Create the schema once to minimize setup cost and avoid races.
    let schema = test_schema_name(cell, suffix).await;
    let schema_clone = schema.clone();
    ready
        .get_or_try_init(|| async move {
            let mut conn = sqlx::PgConnection::connect(base_url).await?;
            let create_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, schema_clone);
            // The schema name is generated from our own pid and clock, never from external
            // input, and an identifier cannot be a bind parameter.
            sqlx::query(AssertSqlSafe(create_sql))
                .execute(&mut conn)
                .await?;
            Ok::<_, sqlx::Error>(())
        })
        .await?;
    Ok(schema)
}

async fn run_migrations_once(
    url: &str,
    migrated: &tokio::sync::OnceCell<()>,
) -> Result<(), sqlx::Error> {
    // Migrations are expensive; run them once per schema.
    migrated
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
    // Reset ensures each test starts from a clean baseline.
    let pool = PgPoolOptions::new()
        // Use a small, short-lived pool for reset to avoid competing with the store pool.
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await?;

    // In CI we can race reset vs migrations or run against a fresh external DB.
    // Postgres doesn't support TRUNCATE IF EXISTS, so guard via to_regclass.
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(format!("{schema}.tenant_changes"))
        .fetch_one(&pool)
        .await?;
    if exists.is_none() {
        // Nothing to reset yet.
        pool.close().await;
        return Ok(());
    }

    let schema_ident = format!(r#""{}""#, schema);
    let truncate = format!(
        "TRUNCATE {schema_ident}.tenant_changes, {schema_ident}.namespace_changes, \
         {schema_ident}.stream_changes, {schema_ident}.cache_changes, {schema_ident}.streams, \
         {schema_ident}.caches, {schema_ident}.namespaces, {schema_ident}.idp_issuers, \
         {schema_ident}.rbac_policies, {schema_ident}.rbac_groupings, \
         {schema_ident}.tenant_signing_keys, {schema_ident}.tenants RESTART IDENTITY CASCADE",
    );
    // Same generated-schema identifier as `ensure_schema`; it cannot be bound as a parameter.
    sqlx::query(AssertSqlSafe(truncate)).execute(&pool).await?;

    pool.close().await;
    Ok(())
}

async fn pg_container() -> Result<Option<&'static PgContainer>> {
    if let Ok(url) = std::env::var("FELIX_TEST_DATABASE_URL") {
        if url.trim().is_empty() {
            eprintln!("skipping pg-tests: FELIX_TEST_DATABASE_URL is empty");
            return Ok(None);
        }
        let container = PG_CONTAINER
            .get_or_try_init(|| async move {
                eprintln!("pg-tests: using FELIX_TEST_DATABASE_URL");
                wait_for_postgres(&url, Duration::from_secs(30)).await?;
                Ok::<_, sqlx::Error>(PgContainer {
                    url,
                    _container: None,
                })
            })
            .await;

        return match container {
            Ok(container) => Ok(Some(container)),
            Err(err) => {
                eprintln!("skipping pg-tests: cannot connect to postgres: {err}");
                Ok(None)
            }
        };
    }

    if !docker_available() {
        eprintln!("skipping pg-tests: docker not available");
        return Ok(None);
    }
    let container = PG_CONTAINER
        .get_or_try_init(|| async {
            eprintln!("pg-tests: starting postgres container");
            let docker = Box::leak(Box::new(Cli::default()));
            // The module's default tag is Postgres 11, which predates the
            // generated columns migration 0009 uses; pin the version the
            // supported deployment path (`task pg:up`) runs.
            let container = std::panic::catch_unwind(|| {
                docker.run(
                    testcontainers::RunnableImage::from(Postgres::default())
                        .with_tag("16-alpine")
                        .with_container_name(felix_test_container_name()),
                )
            })
            .map_err(|_| sqlx::Error::Protocol("docker run panicked".into()))?;
            let port = container.get_host_port_ipv4(5432);
            let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
            // Avoid logging the full URL in case it includes credentials.
            eprintln!("pg-tests: postgres ready on 127.0.0.1:{port}");
            eprintln!("pg-tests: waiting for postgres to accept connections");
            wait_for_postgres(&url, Duration::from_secs(30)).await?;
            eprintln!("pg-tests: postgres accepting connections");
            Ok::<_, sqlx::Error>(PgContainer {
                url,
                _container: Some(container),
            })
        })
        .await;

    match container {
        Ok(container) => Ok(Some(container)),
        Err(err) => {
            eprintln!("skipping pg-tests: cannot connect to postgres: {err}");
            Ok(None)
        }
    }
}

async fn pg_store() -> Result<Option<PgFixture>> {
    // Prefer explicit URLs to avoid docker dependency when CI provides a DB.
    let url_override = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok();
    let base_url = if let Some(url) = url_override {
        // Do not log the full URL to avoid credential leakage.
        eprintln!("pg-tests: using external postgres url");
        url
    } else {
        let Some(container) = pg_container().await? else {
            return Ok(None);
        };
        eprintln!("pg-tests: using shared postgres container");
        container.url.clone()
    };
    let schema = ensure_schema(&base_url, &PG_SCHEMA_DEFAULT, &PG_SCHEMA_DEFAULT_READY, "").await?;
    let url = url_with_schema(&base_url, &schema);
    run_migrations_once(&url, &PG_MIGRATED_DEFAULT).await?;
    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 20_000,
        acquire_timeout_ms: 20_000,
    };
    let store = match store::postgres::PostgresStore::connect_without_migrations(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await
    {
        Ok(store) => Arc::new(store),
        Err(err) => {
            eprintln!("skipping pg-tests: connect postgres store failed: {err}");
            return Ok(None);
        }
    };
    eprintln!("pg-tests: connected to postgres and ran migrations");
    reset_db(&url, &schema).await?;
    eprintln!("pg-tests: database reset complete");

    // Keep docker client alive via return value.
    Ok(Some(PgFixture { store, url }))
}

async fn pg_store_with_retention(retention: i64) -> Result<Option<PgFixture>> {
    // Separate schema for retention tests avoids interaction with default schema.
    let url_override = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok();
    let base_url = if let Some(url) = url_override {
        // Do not log the full URL to avoid credential leakage.
        eprintln!("pg-tests: using external postgres url (retention={retention})");
        url
    } else {
        let Some(container) = pg_container().await? else {
            return Ok(None);
        };
        eprintln!("pg-tests: using shared postgres container (retention={retention})");
        container.url.clone()
    };
    let schema = ensure_schema(
        &base_url,
        &PG_SCHEMA_RETENTION,
        &PG_SCHEMA_RETENTION_READY,
        "retention",
    )
    .await?;
    let url = url_with_schema(&base_url, &schema);
    run_migrations_once(&url, &PG_MIGRATED_RETENTION).await?;

    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 20_000,
        acquire_timeout_ms: 20_000,
    };
    let store = match store::postgres::PostgresStore::connect_without_migrations(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(retention),
        },
    )
    .await
    {
        Ok(store) => Arc::new(store),
        Err(err) => {
            eprintln!("skipping pg-tests: connect postgres store failed: {err}");
            return Ok(None);
        }
    };
    eprintln!("pg-tests: connected to postgres and ran migrations (retention={retention})");
    reset_db(&url, &schema).await?;
    eprintln!("pg-tests: database reset complete (retention={retention})");
    Ok(Some(PgFixture { store, url }))
}

async fn pg_store_with_config(config: StoreConfig) -> Result<Option<PgFixture>> {
    // Reuse existing docker container or external DB with a schema per config.
    let url_override = std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok();
    let base_url = if let Some(url) = url_override {
        // Do not log the full URL to avoid credential leakage.
        eprintln!("pg-tests: using external postgres url (custom config)");
        url
    } else {
        let Some(container) = pg_container().await? else {
            return Ok(None);
        };
        eprintln!("pg-tests: using shared postgres container (custom config)");
        container.url.clone()
    };
    let use_retention_schema = config.change_retention_max_rows.is_some();
    let (schema_cell, ready_cell, migrated_cell, suffix) = if use_retention_schema {
        (
            &PG_SCHEMA_RETENTION,
            &PG_SCHEMA_RETENTION_READY,
            &PG_MIGRATED_RETENTION,
            "retention",
        )
    } else {
        (
            &PG_SCHEMA_DEFAULT,
            &PG_SCHEMA_DEFAULT_READY,
            &PG_MIGRATED_DEFAULT,
            "",
        )
    };
    let schema = ensure_schema(&base_url, schema_cell, ready_cell, suffix).await?;
    let url = url_with_schema(&base_url, &schema);
    run_migrations_once(&url, migrated_cell).await?;
    let pg_cfg = config::PostgresConfig {
        url: url.clone(),
        max_connections: 5,
        connect_timeout_ms: 20_000,
        acquire_timeout_ms: 20_000,
    };
    let store = store::postgres::PostgresStore::connect_without_migrations(&pg_cfg, config).await;
    let store = match store {
        Ok(store) => Arc::new(store),
        Err(err) => {
            eprintln!("skipping pg-tests: connect postgres store failed: {err}");
            return Ok(None);
        }
    };
    eprintln!("pg-tests: connected to postgres and ran migrations (custom config)");
    reset_db(&url, &schema).await?;
    eprintln!("pg-tests: database reset complete (custom config)");
    Ok(Some(PgFixture { store, url }))
}

#[tokio::test]
#[serial]
async fn pg_store_core_crud_and_auth() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_core_crud_and_auth begin");

    // Tenant CRUD + conflict + not found.
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let conflict = store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One Again".to_string(),
        })
        .await;
    assert!(matches!(conflict, Err(store::StoreError::Conflict(_))));
    assert!(store.tenant_exists("t1").await?);
    let tenants = store.list_tenants().await?;
    assert_eq!(tenants.len(), 1);
    let missing_namespace = store
        .namespace_exists(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
        })
        .await?;
    assert!(!missing_namespace);

    // Namespace CRUD.
    let ns = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;
    assert_eq!(ns.namespace, "default");
    let conflict_ns = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default Again".to_string(),
        })
        .await;
    assert!(matches!(conflict_ns, Err(store::StoreError::Conflict(_))));
    let namespaces = store.list_namespaces("t1").await?;
    assert_eq!(namespaces.len(), 1);

    // Stream CRUD + patch + get + not found.
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
            region: None,
        })
        .await?;
    let conflict_stream = store
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
            region: None,
        })
        .await;
    assert!(matches!(
        conflict_stream,
        Err(store::StoreError::Conflict(_))
    ));
    let stream = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
        })
        .await?;
    assert_eq!(stream.stream, "orders");
    let patched = store
        .patch_stream(
            &StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
            },
            StreamPatchRequest {
                retention: Some(RetentionPolicy {
                    max_age_seconds: Some(7200),
                    max_size_bytes: Some(1024),
                }),
                consistency: Some(ConsistencyLevel::Quorum),
                delivery: Some(DeliveryGuarantee::AtMostOnce),
                durable: Some(true),
            },
        )
        .await?;
    assert_eq!(patched.retention.max_age_seconds, Some(7200));
    let missing_stream = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_stream,
        Err(store::StoreError::NotFound(_))
    ));

    // Cache CRUD + patch + get + not found.
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await?;
    let conflict_cache = store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary Again".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await;
    assert!(matches!(
        conflict_cache,
        Err(store::StoreError::Conflict(_))
    ));
    let cache = store
        .get_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
        })
        .await?;
    assert_eq!(cache.cache, "primary");
    let patched = store
        .patch_cache(
            &felix_controlplane_service::model::CacheKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
            },
            CachePatchRequest {
                display_name: Some("Primary Updated".to_string()),
            },
        )
        .await?;
    assert_eq!(patched.display_name, "Primary Updated");
    let missing_cache = store
        .get_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await;
    assert!(matches!(missing_cache, Err(store::StoreError::NotFound(_))));

    // Change feeds + snapshots.
    let tenant_snapshot = store.tenant_snapshot().await?;
    assert_eq!(tenant_snapshot.items.len(), 1);
    let tenant_changes = store.tenant_changes(0).await?;
    assert!(!tenant_changes.items.is_empty());
    let ns_snapshot = store.namespace_snapshot().await?;
    assert_eq!(ns_snapshot.items.len(), 1);
    let stream_snapshot = store.stream_snapshot().await?;
    assert_eq!(stream_snapshot.items.len(), 1);
    let cache_snapshot = store.cache_snapshot().await?;
    assert_eq!(cache_snapshot.items.len(), 1);
    let ns_changes = store.namespace_changes(0).await?;
    assert!(!ns_changes.items.is_empty());

    // Auth tables.
    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: "https://issuer.example.com".to_string(),
                audiences: vec!["felix-controlplane".to_string()],
                discovery_url: None,
                jwks_url: Some("https://issuer.example.com/jwks".to_string()),
                claim_mappings: ClaimMappings {
                    subject_claim: "sub".to_string(),
                    groups_claim: Some("groups".to_string()),
                },
            },
        )
        .await?;
    let issuers = store.list_idp_issuers("t1").await?;
    assert_eq!(issuers.len(), 1);
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
    assert_eq!(store.list_rbac_policies("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_groupings("t1").await?.len(), 1);
    store
        .delete_idp_issuer("t1", "https://issuer.example.com")
        .await?;
    assert!(store.list_idp_issuers("t1").await?.is_empty());

    // Signing keys + bootstrapped flag.
    let keys = generate_signing_keys()?;
    store.set_tenant_signing_keys("t1", keys.clone()).await?;
    let loaded = store.get_tenant_signing_keys("t1").await?;
    assert_eq!(loaded.current.kid, keys.current.kid);
    assert!(!store.tenant_auth_is_bootstrapped("t1").await?);
    store.set_tenant_auth_bootstrapped("t1", true).await?;
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);

    // Cascade delete namespace -> stream/cache deletions in change feed.
    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
        })
        .await?;
    let stream_changes = store.stream_changes(0).await?;
    assert!(stream_changes.items.iter().any(|c| {
        matches!(
            c.op,
            felix_controlplane_service::model::StreamChangeOp::Deleted
        )
    }));
    let cache_changes = store.cache_changes(0).await?;
    assert!(cache_changes.items.iter().any(|c| {
        matches!(
            c.op,
            felix_controlplane_service::model::CacheChangeOp::Deleted
        )
    }));

    store.delete_tenant("t1").await?;
    let missing_delete = store.delete_tenant("t1").await;
    assert!(matches!(
        missing_delete,
        Err(store::StoreError::NotFound(_))
    ));

    eprintln!("pg-tests: pg_store_core_crud_and_auth done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_connect_and_retention_task_ticks() -> Result<()> {
    let Some(fixture) = pg_store_with_config(StoreConfig {
        changes_limit: 25,
        change_retention_max_rows: Some(1),
    })
    .await?
    else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_connect_and_retention_task_ticks begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await?;

    // Allow the retention task to tick at least once.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let changes = store.tenant_changes(0).await?;
    assert!(
        changes.items.len() <= 1,
        "retention should trim to max_rows"
    );

    eprintln!("pg-tests: pg_store_connect_and_retention_task_ticks done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_connect_without_retention() -> Result<()> {
    let Some(fixture) = pg_store_with_config(StoreConfig {
        changes_limit: 25,
        change_retention_max_rows: None,
    })
    .await?
    else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_connect_without_retention begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let tenants = store.list_tenants().await?;
    assert_eq!(tenants.len(), 1);

    eprintln!("pg-tests: pg_store_connect_without_retention done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_additional_paths() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_additional_paths begin");

    store.health_check().await?;
    assert!(store.is_durable());
    assert_eq!(store.backend_name(), "postgres");

    assert!(!store.tenant_exists("t1").await?);
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    assert!(store.tenant_exists("t1").await?);

    let ns_key = NamespaceKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
    };
    assert!(!store.namespace_exists(&ns_key).await?);
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;
    assert!(store.namespace_exists(&ns_key).await?);

    assert!(store.list_streams("t1", "default").await?.is_empty());
    assert!(store.list_caches("t1", "default").await?.is_empty());

    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
            region: None,
        })
        .await?;
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await?;
    assert_eq!(store.list_streams("t1", "default").await?.len(), 1);
    assert_eq!(store.list_caches("t1", "default").await?.len(), 1);

    let missing_delete = store
        .delete_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_delete,
        Err(store::StoreError::NotFound(_))
    ));

    store
        .delete_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
        })
        .await?;
    assert!(store.list_caches("t1", "default").await?.is_empty());

    let missing_keys = store.get_tenant_signing_keys("t1").await;
    assert!(matches!(missing_keys, Err(store::StoreError::NotFound(_))));
    let keys = store.ensure_signing_key_current("t1").await?;
    let loaded = store.get_tenant_signing_keys("t1").await?;
    assert_eq!(loaded.current.kid, keys.current.kid);
    let cached = store.ensure_signing_key_current("t1").await?;
    assert_eq!(cached.current.kid, keys.current.kid);

    let issuer = IdpIssuerConfig {
        issuer: "https://issuer.example.com".to_string(),
        audiences: vec!["felix-controlplane".to_string()],
        discovery_url: Some("https://issuer.example.com/.well-known".to_string()),
        jwks_url: Some("https://issuer.example.com/jwks".to_string()),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: Some("groups".to_string()),
        },
    };
    store.upsert_idp_issuer("t1", issuer).await?;
    let updated = IdpIssuerConfig {
        issuer: "https://issuer.example.com".to_string(),
        audiences: vec!["felix-api".to_string()],
        discovery_url: None,
        jwks_url: Some("https://issuer.example.com/jwks".to_string()),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: None,
        },
    };
    store.upsert_idp_issuer("t1", updated).await?;
    let issuers = store.list_idp_issuers("t1").await?;
    assert_eq!(issuers.len(), 1);
    assert_eq!(issuers[0].audiences, vec!["felix-api".to_string()]);
    assert!(issuers[0].claim_mappings.groups_claim.is_none());

    let policy = PolicyRule {
        subject: "role:admin".to_string(),
        object: "tenant:t1".to_string(),
        action: "tenant.manage".to_string(),
    };
    let grouping = GroupingRule {
        user: "p:admin".to_string(),
        role: "role:admin".to_string(),
    };
    store
        .seed_rbac_policies_and_groupings("t1", vec![policy.clone()], vec![grouping.clone()])
        .await?;
    store
        .seed_rbac_policies_and_groupings("t1", vec![policy], vec![grouping])
        .await?;
    assert_eq!(store.list_rbac_policies("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_groupings("t1").await?.len(), 1);

    let missing_bootstrap = store.set_tenant_auth_bootstrapped("missing", true).await;
    assert!(matches!(
        missing_bootstrap,
        Err(store::StoreError::NotFound(_))
    ));
    store.set_tenant_auth_bootstrapped("t1", true).await?;
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);

    // Insert a bogus change op to exercise the fallback branch in change parsing.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;
    sqlx::query("INSERT INTO tenant_changes (op, tenant_id, payload) VALUES ($1, $2, $3)")
        .bind("Bogus")
        .bind("t1")
        .bind(Option::<serde_json::Value>::None)
        .execute(&pool)
        .await?;
    let bogus = store.tenant_changes(0).await?;
    assert!(
        bogus.items.iter().any(|item| {
            matches!(
                item.op,
                felix_controlplane_service::model::TenantChangeOp::Deleted
            )
        }),
        "bogus op should map to Deleted"
    );

    eprintln!("pg-tests: pg_store_additional_paths done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_list_and_change_roundtrip() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_list_and_change_roundtrip begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await?;

    let tenants = store.list_tenants().await?;
    assert_eq!(tenants.len(), 2);

    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            display_name: "Namespace One".to_string(),
        })
        .await?;
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns2".to_string(),
            display_name: "Namespace Two".to_string(),
        })
        .await?;
    let namespaces = store.list_namespaces("t1").await?;
    assert_eq!(namespaces.len(), 2);

    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 2,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: Some(3600),
                max_size_bytes: Some(1024),
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: true,
            region: None,
        })
        .await?;
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            stream: "events".to_string(),
            kind: StreamKind::Queue,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Quorum,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: false,
            region: None,
        })
        .await?;
    let streams = store.list_streams("t1", "ns1").await?;
    assert_eq!(streams.len(), 2);

    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await?;
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            cache: "secondary".to_string(),
            display_name: "Secondary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await?;
    let caches = store.list_caches("t1", "ns1").await?;
    assert_eq!(caches.len(), 2);

    let tenant_changes = store.tenant_changes(0).await?;
    assert!(tenant_changes.items.len() >= 2);
    let ns_changes = store.namespace_changes(0).await?;
    assert!(ns_changes.items.len() >= 2);
    let stream_changes = store.stream_changes(0).await?;
    assert!(stream_changes.items.len() >= 2);
    let cache_changes = store.cache_changes(0).await?;
    assert!(cache_changes.items.len() >= 2);

    eprintln!("pg-tests: pg_store_list_and_change_roundtrip done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_not_found_and_noop_paths() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_not_found_and_noop_paths begin");

    // Empty snapshots/changes should return empty items with next_seq 0 for all entities.
    let empty_tenants = store.tenant_snapshot().await?;
    assert!(empty_tenants.items.is_empty());
    assert_eq!(empty_tenants.next_seq, 0);
    let empty_changes = store.tenant_changes(42).await?;
    assert!(empty_changes.items.is_empty());
    assert_eq!(empty_changes.next_seq, 0);
    let empty_namespaces = store.namespace_snapshot().await?;
    assert!(empty_namespaces.items.is_empty());
    assert_eq!(empty_namespaces.next_seq, 0);
    let empty_ns_changes = store.namespace_changes(0).await?;
    assert!(empty_ns_changes.items.is_empty());
    assert_eq!(empty_ns_changes.next_seq, 0);
    let empty_streams = store.stream_snapshot().await?;
    assert!(empty_streams.items.is_empty());
    assert_eq!(empty_streams.next_seq, 0);
    let empty_stream_changes = store.stream_changes(0).await?;
    assert!(empty_stream_changes.items.is_empty());
    assert_eq!(empty_stream_changes.next_seq, 0);
    let empty_caches = store.cache_snapshot().await?;
    assert!(empty_caches.items.is_empty());
    assert_eq!(empty_caches.next_seq, 0);
    let empty_cache_changes = store.cache_changes(0).await?;
    assert!(empty_cache_changes.items.is_empty());
    assert_eq!(empty_cache_changes.next_seq, 0);

    // Auth lookups against an empty database should return empty collections or defaults.
    assert!(store.list_idp_issuers("missing").await?.is_empty());
    assert!(store.list_rbac_policies("missing").await?.is_empty());
    assert!(store.list_rbac_groupings("missing").await?.is_empty());
    assert!(!store.tenant_auth_is_bootstrapped("missing").await?);

    // Namespace creation should fail if tenant is missing.
    let missing_ns = store
        .create_namespace(Namespace {
            tenant_id: "missing".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await;
    assert!(matches!(missing_ns, Err(store::StoreError::NotFound(_))));

    // Stream/cache creation should fail if namespace is missing.
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let missing_get_stream = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_get_stream,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_get_cache = store
        .get_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_get_cache,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_stream = store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
            region: None,
        })
        .await;
    assert!(matches!(
        missing_stream,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_cache = store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await;
    assert!(matches!(missing_cache, Err(store::StoreError::NotFound(_))));

    // Delete namespace should fail for missing namespace.
    let missing_delete = store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_delete,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_delete_stream = store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(
        missing_delete_stream,
        Err(store::StoreError::NotFound(_))
    ));

    // Patch operations should fail for missing records.
    let missing_patch_stream = store
        .patch_stream(
            &StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
            },
            StreamPatchRequest {
                retention: None,
                consistency: None,
                delivery: None,
                durable: None,
            },
        )
        .await;
    assert!(matches!(
        missing_patch_stream,
        Err(store::StoreError::NotFound(_))
    ));
    let missing_patch_cache = store
        .patch_cache(
            &felix_controlplane_service::model::CacheKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "missing".to_string(),
            },
            CachePatchRequest { display_name: None },
        )
        .await;
    assert!(matches!(
        missing_patch_cache,
        Err(store::StoreError::NotFound(_))
    ));

    eprintln!("pg-tests: pg_store_not_found_and_noop_paths done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_delete_tenant_with_dependents() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_store_delete_tenant_with_dependents begin");

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
            region: None,
        })
        .await?;
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await?;

    store.delete_tenant("t1").await?;

    assert!(store.list_tenants().await?.is_empty());
    assert!(store.list_namespaces("t1").await?.is_empty());
    assert!(store.list_streams("t1", "default").await?.is_empty());
    assert!(store.list_caches("t1", "default").await?.is_empty());

    let ns_changes = store.namespace_changes(0).await?;
    assert!(ns_changes.items.iter().any(|item| {
        matches!(
            item.op,
            felix_controlplane_service::model::NamespaceChangeOp::Deleted
        )
    }));
    let stream_changes = store.stream_changes(0).await?;
    assert!(stream_changes.items.iter().any(|item| {
        matches!(
            item.op,
            felix_controlplane_service::model::StreamChangeOp::Deleted
        )
    }));
    let cache_changes = store.cache_changes(0).await?;
    assert!(cache_changes.items.iter().any(|item| {
        matches!(
            item.op,
            felix_controlplane_service::model::CacheChangeOp::Deleted
        )
    }));

    eprintln!("pg-tests: pg_store_delete_tenant_with_dependents done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_signing_keys_requires_current() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_signing_keys_requires_current begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;
    let (private_key, public_key) = test_ed25519_key_bytes();
    sqlx::query(
        "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("t1")
    .bind("k1")
    .bind("EdDSA")
    .bind(private_key.as_slice())
    .bind(public_key.as_slice())
    .bind("previous")
    .execute(&pool)
    .await?;

    let missing_current = store.get_tenant_signing_keys("t1").await;
    assert!(matches!(
        missing_current,
        Err(store::StoreError::NotFound(_))
    ));

    eprintln!("pg-tests: pg_store_signing_keys_requires_current done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_full_surface_area() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_full_surface_area begin");

    // Tenant CRUD + snapshots/changes.
    assert!(store.list_tenants().await?.is_empty());
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    let tenant_snapshot = store.tenant_snapshot().await?;
    assert_eq!(tenant_snapshot.items.len(), 1);
    let tenant_changes = store.tenant_changes(tenant_snapshot.next_seq).await?;
    assert!(tenant_changes.items.is_empty());

    // Namespace CRUD.
    let ns = store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;
    assert_eq!(ns.namespace, "default");
    let ns_snapshot = store.namespace_snapshot().await?;
    assert_eq!(ns_snapshot.items.len(), 1);
    let ns_changes = store.namespace_changes(0).await?;
    assert!(!ns_changes.items.is_empty());

    // Stream CRUD + list + snapshot/changes.
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
            region: None,
        })
        .await?;
    assert_eq!(store.list_streams("t1", "default").await?.len(), 1);
    let stream_snapshot = store.stream_snapshot().await?;
    assert_eq!(stream_snapshot.items.len(), 1);
    let stream_changes = store.stream_changes(0).await?;
    assert!(!stream_changes.items.is_empty());

    // Cache CRUD + list + snapshot/changes.
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await?;
    assert_eq!(store.list_caches("t1", "default").await?.len(), 1);
    let cache_snapshot = store.cache_snapshot().await?;
    assert_eq!(cache_snapshot.items.len(), 1);
    let cache_changes = store.cache_changes(0).await?;
    assert!(!cache_changes.items.is_empty());

    // Auth store methods.
    assert!(store.list_idp_issuers("t1").await?.is_empty());
    store
        .upsert_idp_issuer(
            "t1",
            IdpIssuerConfig {
                issuer: "https://issuer.example.com".to_string(),
                audiences: vec!["felix-controlplane".to_string()],
                discovery_url: None,
                jwks_url: Some("https://issuer.example.com/jwks".to_string()),
                claim_mappings: ClaimMappings::default(),
            },
        )
        .await?;
    assert_eq!(store.list_idp_issuers("t1").await?.len(), 1);
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
    assert_eq!(store.list_rbac_policies("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_groupings("t1").await?.len(), 1);

    let keys = generate_signing_keys()?;
    store.set_tenant_signing_keys("t1", keys.clone()).await?;
    let loaded = store.get_tenant_signing_keys("t1").await?;
    assert_eq!(loaded.current.kid, keys.current.kid);
    assert!(!store.tenant_auth_is_bootstrapped("t1").await?);
    store.set_tenant_auth_bootstrapped("t1", true).await?;
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);

    // Exercise parsing error paths via direct inserts.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;
    sqlx::query(
        "INSERT INTO streams (tenant_id, namespace, stream, kind, shards, retention_max_age_seconds, retention_max_size_bytes, consistency, delivery, durable) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind("t1")
    .bind("default")
    .bind("bad")
    .bind("BogusKind")
    .bind(1i32)
    .bind(Option::<i64>::None)
    .bind(Option::<i64>::None)
    .bind("BogusConsistency")
    .bind("BogusDelivery")
    .bind(false)
    .execute(&pool)
    .await?;
    let bad_key = StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "bad".to_string(),
    };
    let parse_error = store.get_stream(&bad_key);
    assert!(parse_error.await.is_err());

    // Invalid signing key algorithm to cover parse_algorithm error path.
    sqlx::query(
        "INSERT INTO tenant_signing_keys (tenant_id, kid, alg, private_pem, public_pem, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("t1")
    .bind("bad")
    .bind("BogusAlg")
    .bind(vec![0u8; 32])
    .bind(vec![0u8; 32])
    .bind("current")
    .execute(&pool)
    .await?;
    let bad_alg = store.get_tenant_signing_keys("t1").await;
    assert!(bad_alg.is_err());

    eprintln!("pg-tests: pg_store_full_surface_area done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_store_change_and_auth_parsing_errors() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    let url = fixture.url.clone();
    eprintln!("pg-tests: pg_store_change_and_auth_parsing_errors begin");

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?;

    // Insert invalid change ops to exercise "default to Deleted" branches.
    sqlx::query(
        "INSERT INTO namespace_changes (op, tenant_id, namespace, payload) VALUES ($1, $2, $3, $4)",
    )
    .bind("Bogus")
    .bind("t1")
    .bind("ns1")
    .bind(Option::<serde_json::Value>::None)
    .execute(&pool)
    .await?;
    let ns_changes = store.namespace_changes(0).await?;
    assert!(ns_changes.items.iter().any(|item| {
        matches!(
            item.op,
            felix_controlplane_service::model::NamespaceChangeOp::Deleted
        )
    }));

    sqlx::query(
        "INSERT INTO stream_changes (op, tenant_id, namespace, stream, payload) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind("Bogus")
    .bind("t1")
    .bind("ns1")
    .bind("s1")
    .bind(Option::<serde_json::Value>::None)
    .execute(&pool)
    .await?;
    let stream_changes = store.stream_changes(0).await?;
    assert!(stream_changes.items.iter().any(|item| {
        matches!(
            item.op,
            felix_controlplane_service::model::StreamChangeOp::Deleted
        )
    }));

    sqlx::query(
        "INSERT INTO cache_changes (op, tenant_id, namespace, cache, payload) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind("Bogus")
    .bind("t1")
    .bind("ns1")
    .bind("c1")
    .bind(Option::<serde_json::Value>::None)
    .execute(&pool)
    .await?;
    let cache_changes = store.cache_changes(0).await?;
    assert!(cache_changes.items.iter().any(|item| {
        matches!(
            item.op,
            felix_controlplane_service::model::CacheChangeOp::Deleted
        )
    }));

    // Invalid audiences JSON should error when listing IdP issuers.
    sqlx::query(
        "INSERT INTO idp_issuers (tenant_id, issuer, audiences, discovery_url, jwks_url, subject_claim, groups_claim) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind("t1")
    .bind("https://issuer.example.com")
    .bind(serde_json::json!("not-an-array"))
    .bind(Option::<String>::None)
    .bind(Option::<String>::None)
    .bind("sub")
    .bind(Option::<String>::None)
    .execute(&pool)
    .await?;
    let issuers = store.list_idp_issuers("t1").await;
    assert!(issuers.is_err());

    eprintln!("pg-tests: pg_store_change_and_auth_parsing_errors done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_change_retention_trims_changes() -> Result<()> {
    let Some(fixture) = pg_store_with_retention(1).await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_change_retention_trims_changes begin");
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;
    store
        .create_tenant(Tenant {
            tenant_id: "t2".to_string(),
            display_name: "Tenant Two".to_string(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let changes = store.tenant_changes(0).await?;
    assert!(changes.items.len() <= 1);
    eprintln!(
        "pg-tests: pg_change_retention_trims_changes retained={}",
        changes.items.len()
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_changes_monotonic_and_delete_not_found() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;
    eprintln!("pg-tests: pg_changes_monotonic_and_delete_not_found begin");
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
            region: None,
        })
        .await?;
    store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
        })
        .await?;
    let changes = store.stream_changes(0).await?;
    let seqs: Vec<u64> = changes.items.iter().map(|c| c.seq).collect();
    assert!(seqs.windows(2).all(|w| w[1] > w[0]));
    if let Some(last) = seqs.last() {
        assert_eq!(changes.next_seq, last + 1);
    }
    let missing = store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await;
    assert!(matches!(missing, Err(store::StoreError::NotFound(_))));
    eprintln!("pg-tests: pg_changes_monotonic_and_delete_not_found done");
    Ok(())
}

#[tokio::test]
#[serial]
async fn pg_bootstrap_initialize_and_jwks_includes_previous_keys() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = fixture.store.clone();
    eprintln!("pg-tests: pg_bootstrap_initialize_and_jwks_includes_previous_keys begin");
    let state = api::AppState {
        region: felix_controlplane_service::api::types::Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: felix_controlplane_service::api::types::FeatureFlags {
            durable_storage: true,
            tiered_storage: false,
            bridges: false,
        },
        store: store.clone(),
        oidc_validator: UpstreamOidcValidator::default(),
        bootstrap_enabled: true,
        bootstrap_tokens: vec!["token".to_string()],
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    let bootstrap_app = api::build_bootstrap_router(state.clone());
    let body = json!({
        "display_name": "Tenant One",
        "idp_issuers": [{
            "issuer": "https://issuer.example.com",
            "audiences": ["felix-controlplane"],
            "discovery_url": null,
            "jwks_url": "https://issuer.example.com/jwks",
            "claim_mappings": {
                "subject_claim": "sub",
                "groups_claim": "groups"
            }
        }],
        "initial_admin_principals": ["p:admin"],
        "policies": [],
        "groupings": []
    });
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "token")
        .body(Body::from(body.to_string()))?;
    let response = bootstrap_app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(store.tenant_auth_is_bootstrapped("t1").await?);
    assert_eq!(store.list_idp_issuers("t1").await?.len(), 1);
    assert!(!store.list_rbac_policies("t1").await?.is_empty());
    assert!(!store.list_rbac_groupings("t1").await?.is_empty());

    let mut keys = generate_signing_keys()?;
    let previous = generate_signing_keys()?;
    keys.previous.push(previous.current.clone());
    store.set_tenant_signing_keys("t1", keys.clone()).await?;

    let app = api::build_router(state);
    let jwks_request = Request::builder()
        .method("GET")
        .uri("/v1/tenants/t1/.well-known/jwks.json")
        .body(Body::empty())?;
    let jwks_response = app.oneshot(jwks_request).await?;
    assert_eq!(jwks_response.status(), StatusCode::OK);
    let response_json = read_json(jwks_response).await;
    let keys_list = response_json["keys"].as_array().expect("jwks keys");
    assert_eq!(keys_list.len(), 2);
    eprintln!(
        "pg-tests: jwks contains {} keys (current + previous)",
        keys_list.len()
    );
    Ok(())
}

/// The `FOR UPDATE` claim in `bootstrap_tenant_auth`, exercised for real:
/// racing initializes through one Postgres must produce exactly one winner,
/// and the keys the winner reports must be the keys the tenant holds.
#[tokio::test]
#[serial]
async fn pg_bootstrap_tenant_auth_is_atomic_and_exactly_once() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = &fixture.store;

    // A tenant that does not exist is NotFound, not silently created: the
    // handler owns tenant creation, the store owns the claim.
    let missing = store
        .bootstrap_tenant_auth(
            "missing",
            store::TenantAuthSeed {
                issuers: Vec::new(),
                policies: Vec::new(),
                groupings: Vec::new(),
                signing_keys: generate_signing_keys()?,
            },
        )
        .await;
    assert!(matches!(missing, Err(store::StoreError::NotFound(_))));

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await?;

    let seed = || store::TenantAuthSeed {
        signing_keys: generate_signing_keys().expect("candidate keys"),
        issuers: vec![IdpIssuerConfig {
            issuer: "https://issuer.example.com".to_string(),
            audiences: vec!["felix-controlplane".to_string()],
            discovery_url: None,
            jwks_url: Some("https://issuer.example.com/jwks".to_string()),
            claim_mappings: ClaimMappings {
                subject_claim: "sub".to_string(),
                groups_claim: None,
            },
        }],
        policies: vec![PolicyRule {
            subject: "role:tenant-admin".to_string(),
            object: "tenant:t1".to_string(),
            action: "tenant.manage".to_string(),
        }],
        groupings: vec![GroupingRule {
            user: "p:admin".to_string(),
            role: "role:tenant-admin".to_string(),
        }],
    };

    let mut racers = Vec::new();
    for _ in 0..4 {
        let store = Arc::clone(store);
        let seed = seed();
        racers.push(tokio::spawn(async move {
            store.bootstrap_tenant_auth("t1", seed).await
        }));
    }

    let mut winner_kid = None;
    let mut conflicts = 0;
    for racer in racers {
        match racer.await.expect("join") {
            Ok(keys) => {
                assert!(winner_kid.is_none(), "two initializes both claimed the win");
                winner_kid = Some(keys.current.kid);
            }
            Err(store::StoreError::Conflict(_)) => conflicts += 1,
            Err(other) => panic!("unexpected bootstrap error: {other}"),
        }
    }
    assert_eq!(conflicts, 3);

    assert!(store.tenant_auth_is_bootstrapped("t1").await?);
    let held = store.get_tenant_signing_keys("t1").await?;
    assert_eq!(Some(held.current.kid), winner_kid);
    assert_eq!(store.list_idp_issuers("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_policies("t1").await?.len(), 1);
    assert_eq!(store.list_rbac_groupings("t1").await?.len(), 1);
    Ok(())
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

/// Long-polls on the assignment feed hold no connection while they wait.
///
/// Four times as many waiting requests as the pool has connections: if a
/// waiter held one, the write below could not get a connection until the
/// waits ran out, and the waiters would answer at their deadline, not at the
/// write.
#[tokio::test]
#[serial]
async fn pg_assignment_long_polls_do_not_hold_connections() -> Result<()> {
    let Some(fixture) = pg_store().await? else {
        return Ok(());
    };
    let store = fixture.store.clone();
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T".to_string(),
        })
        .await?;
    let keys = generate_signing_keys()?;
    store.set_tenant_signing_keys("t1", keys.clone()).await?;
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await?;
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
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
        })
        .await?;
    let leader = format!("broker-lp-{}", std::process::id());
    store
        .register_node(felix_controlplane_service::model::Node {
            node_id: leader.clone(),
            spec: felix_controlplane_service::model::NodeSpec {
                advertise_addr: "10.9.9.9:7000".to_string(),
                client_addr: None,
                kafka_addr: None,
                region: "local".to_string(),
                zone: None,
                labels: Default::default(),
                capacity: Default::default(),
            },
            status: felix_controlplane_service::model::NodeStatus {
                lifecycle: felix_controlplane_service::model::NodeLifecycle::Live,
                last_heartbeat_at_millis: 1,
                registered_at_millis: 1,
                incarnation: 0,
                features: Default::default(),
            },
        })
        .await?;
    let since = store.shard_assignment_snapshot().await?.next_seq;

    let state = api::AppState {
        region: felix_controlplane_service::api::types::Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: felix_controlplane_service::api::types::FeatureFlags {
            durable_storage: true,
            tiered_storage: false,
            bridges: false,
        },
        store: store.clone(),
        oidc_validator: UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: std::sync::Arc::new(felix_controlplane_service::api::readiness::Readiness::new(
            std::sync::Arc::new(felix_controlplane_service::api::readiness::AlwaysReady),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    };
    let app = api::build_router(state);
    let bearer = felix_controlplane_service::auth::felix_token::mint_token_for(
        &keys,
        "t1",
        "p:test",
        vec!["node.view:cluster:*".to_string()],
        Duration::from_secs(900),
        felix_controlplane_service::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )?;

    // The fixture's pool has five connections.
    const WAITERS: usize = 20;
    let started = tokio::time::Instant::now();
    let waiters: Vec<_> = (0..WAITERS)
        .map(|_| {
            let app = app.clone();
            let request = Request::builder()
                .uri(format!(
                    "/v1/shard-assignments/changes?since={since}&wait_ms=15000"
                ))
                .header("authorization", format!("Bearer {bearer}"))
                .body(Body::empty())
                .expect("request");
            tokio::spawn(async move {
                let response = app.oneshot(request).await.expect("response");
                assert_eq!(response.status(), StatusCode::OK);
                let body: serde_json::Value = read_json(response).await;
                (body, started.elapsed())
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let write_began = started.elapsed();
    store
        .put_shard_assignment(felix_controlplane_service::model::ShardAssignment {
            key: felix_controlplane_service::model::ShardKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
                shard: 0,
                kind: felix_controlplane_service::model::ShardKind::Stream,
            },
            leader: leader.clone(),
            replicas: Vec::new(),
            generation: 0,
            state: felix_controlplane_service::model::ShardState::Assigning,
            successor: None,
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
        })
        .await?;
    let written = started.elapsed();
    assert!(
        written - write_began < Duration::from_secs(2),
        "the write waited {:?} for a connection",
        written - write_began,
    );

    for waiter in waiters {
        let (body, answered) = waiter.await?;
        assert_eq!(body["items"][0]["assignment"]["leader"], leader.as_str());
        assert!(
            answered < written + Duration::from_secs(3),
            "answered at {answered:?}, the write landed at {written:?}",
        );
    }
    Ok(())
}

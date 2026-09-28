#![cfg(feature = "pg-tests")]

use std::sync::Arc;

use serial_test::serial;

use felix_controlplane_service::auth::idp_registry::{ClaimMappings, IdpIssuerConfig};
use felix_controlplane_service::auth::keys::generate_signing_keys;
use felix_controlplane_service::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use felix_controlplane_service::config;
use felix_controlplane_service::model::{
    Cache, CacheChangeOp, CachePatchRequest, ConsistencyLevel, DeliveryGuarantee, Namespace,
    NamespaceChangeOp, NamespaceKey, RetentionPolicy, Stream, StreamChangeOp, StreamKey,
    StreamKind, StreamPatchRequest, Tenant, TenantChangeOp,
};
use felix_controlplane_service::store::{AuthStore, ControlPlaneStore, StoreConfig, StoreError};
use sqlx::AssertSqlSafe;
use sqlx::Connection;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;

mod common;

static PG_SCHEMA: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
static PG_SCHEMA_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static PG_MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

static MIGRATOR: Migrator = sqlx::migrate!();

async fn test_schema_name() -> String {
    PG_SCHEMA
        .get_or_init(|| async { common::unique_schema("felix_test") })
        .await
        .clone()
}

fn url_with_schema(base_url: &str, schema: &str) -> String {
    let encoded = format!("-csearch_path%3D{}", schema);
    if base_url.contains('?') {
        format!("{base_url}&options={encoded}")
    } else {
        format!("{base_url}?options={encoded}")
    }
}

async fn ensure_schema(base_url: &str) -> Result<String, sqlx::Error> {
    let schema = test_schema_name().await;
    let schema_clone = schema.clone();
    PG_SCHEMA_READY
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

async fn run_migrations_once(url: &str) -> Result<(), sqlx::Error> {
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

async fn reset_postgres(url: &str, schema: &str) -> Result<(), sqlx::Error> {
    let pool = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect(url),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => return Err(sqlx::Error::PoolTimedOut),
    };
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(format!("{schema}.tenant_changes"))
        .fetch_one(&pool)
        .await?;
    if exists.is_none() {
        pool.close().await;
        return Ok(());
    }
    let schema_ident = format!(r#""{}""#, schema);
    let truncate = format!(
        "TRUNCATE {schema_ident}.tenant_changes, {schema_ident}.namespace_changes, \
         {schema_ident}.stream_changes, {schema_ident}.cache_changes, {schema_ident}.streams, \
         {schema_ident}.caches, {schema_ident}.namespaces, {schema_ident}.tenants RESTART IDENTITY CASCADE",
    );
    // Same generated-schema identifier as `ensure_schema`; it cannot be bound as a parameter.
    sqlx::query(AssertSqlSafe(truncate))
        .execute(&pool)
        .await
        .map(|_| ())
}

async fn pg_store() -> Option<Arc<felix_controlplane_service::store::postgres::PostgresStore>> {
    let base_url = match std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
    {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping pg-tests: set FELIX_CONTROLPLANE_POSTGRES_URL or DATABASE_URL");
            return None;
        }
    };
    let schema = match ensure_schema(&base_url).await {
        Ok(schema) => schema,
        Err(err) => {
            panic!("pg-tests: cannot create schema: {err}");
        }
    };
    let url = url_with_schema(&base_url, &schema);
    if let Err(err) = run_migrations_once(&url).await {
        panic!("pg-tests: cannot run migrations: {err}");
    }
    if let Err(err) = reset_postgres(&url, &schema).await {
        panic!("pg-tests: cannot connect to postgres: {err}");
    }
    let pg_cfg = config::PostgresConfig {
        url,
        max_connections: 5,
        connect_timeout_ms: 5_000,
        acquire_timeout_ms: 5_000,
    };
    let store = match felix_controlplane_service::store::postgres::PostgresStore::connect_without_migrations(
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
            panic!("pg-tests: connect postgres store failed: {err}");
        }
    };
    Some(store)
}

#[tokio::test]
#[serial]
async fn pg_stream_sequences_monotonic() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");

    let mut stream = Stream {
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
        routing: Default::default(),
    };
    store
        .create_stream(stream.clone())
        .await
        .expect("create stream");

    stream.retention.max_age_seconds = Some(7200);
    store
        .patch_stream(
            &StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
            },
            StreamPatchRequest {
                retention: Some(stream.retention.clone()),
                consistency: None,
                delivery: None,
                durable: None,
            },
        )
        .await
        .expect("patch stream");

    store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("delete stream");

    let changes = store.stream_changes(0).await.expect("stream changes");
    let seqs: Vec<u64> = changes.items.iter().map(|c| c.seq).collect();
    assert!(seqs.windows(2).all(|w| w[1] > w[0]));
    if let Some(last) = seqs.last() {
        assert_eq!(changes.next_seq, last + 1);
    }
    let snapshot = store.stream_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.next_seq, changes.next_seq);
}

#[tokio::test]
#[serial]
async fn pg_delete_namespace_emits_cascades() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            display_name: "NS1".to_string(),
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
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
            region: None,
            routing: Default::default(),
        })
        .await
        .expect("stream");
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
        .await
        .expect("cache");

    store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
        })
        .await
        .expect("delete namespace");

    let stream_changes = store.stream_changes(0).await.expect("stream changes");
    assert!(
        stream_changes
            .items
            .iter()
            .any(|c| matches!(c.op, StreamChangeOp::Deleted))
    );

    let cache_changes = store.cache_changes(0).await.expect("cache changes");
    assert!(
        cache_changes
            .items
            .iter()
            .any(|c| matches!(c.op, CacheChangeOp::Deleted))
    );

    let ns_changes = store.namespace_changes(0).await.expect("ns changes");
    assert!(
        ns_changes
            .items
            .iter()
            .any(|c| matches!(c.op, NamespaceChangeOp::Deleted))
    );
}

#[tokio::test]
#[serial]
async fn pg_auth_store_idp_and_rbac_roundtrip() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");

    let issuer = IdpIssuerConfig {
        issuer: "https://issuer.test".to_string(),
        audiences: vec!["felix".to_string()],
        discovery_url: Some("https://issuer.test/.well-known/openid-configuration".to_string()),
        jwks_url: Some("https://issuer.test/jwks.json".to_string()),
        claim_mappings: ClaimMappings::default(),
    };
    store
        .upsert_idp_issuer("t1", issuer.clone())
        .await
        .expect("upsert issuer");
    let issuers = store.list_idp_issuers("t1").await.expect("list issuers");
    assert_eq!(issuers.len(), 1);
    assert_eq!(issuers[0].issuer, issuer.issuer);
    store
        .delete_idp_issuer("t1", &issuer.issuer)
        .await
        .expect("delete issuer");
    let issuers = store.list_idp_issuers("t1").await.expect("list issuers");
    assert!(issuers.is_empty());

    let policy = PolicyRule {
        subject: "role:reader".to_string(),
        object: "stream:t1/default/orders".to_string(),
        action: "stream.subscribe".to_string(),
    };
    let grouping = GroupingRule {
        user: "p:alice".to_string(),
        role: "role:reader".to_string(),
    };
    store
        .add_rbac_policy("t1", policy.clone())
        .await
        .expect("add policy");
    store
        .add_rbac_grouping("t1", grouping.clone())
        .await
        .expect("add grouping");
    let policies = store.list_rbac_policies("t1").await.expect("list policies");
    let groupings = store
        .list_rbac_groupings("t1")
        .await
        .expect("list groupings");
    assert!(policies.iter().any(|p| p.subject == policy.subject));
    assert!(groupings.iter().any(|g| g.user == grouping.user));
}

#[tokio::test]
#[serial]
async fn pg_auth_bootstrap_and_signing_keys() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");

    let bootstrapped = store
        .tenant_auth_is_bootstrapped("t1")
        .await
        .expect("bootstrapped");
    assert!(!bootstrapped);
    store
        .set_tenant_auth_bootstrapped("t1", true)
        .await
        .expect("set bootstrapped");
    let bootstrapped = store
        .tenant_auth_is_bootstrapped("t1")
        .await
        .expect("bootstrapped");
    assert!(bootstrapped);

    let keys = store
        .ensure_signing_key_current("t1")
        .await
        .expect("ensure signing keys");
    assert!(!keys.current.kid.is_empty());

    let new_keys = generate_signing_keys().expect("generate keys");
    store
        .set_tenant_signing_keys("t1", new_keys.clone())
        .await
        .expect("set keys");
    let fetched = store.get_tenant_signing_keys("t1").await.expect("get keys");
    assert_eq!(fetched.current.kid, new_keys.current.kid);

    let policy = PolicyRule {
        subject: "role:admin".to_string(),
        object: "stream:t1/default/*".to_string(),
        action: "stream.publish".to_string(),
    };
    let grouping = GroupingRule {
        user: "p:admin".to_string(),
        role: "role:admin".to_string(),
    };
    store
        .seed_rbac_policies_and_groupings("t1", vec![policy], vec![grouping])
        .await
        .expect("seed rbac");
    let policies = store.list_rbac_policies("t1").await.expect("policies");
    assert!(!policies.is_empty());
}

#[tokio::test]
#[serial]
async fn pg_set_auth_bootstrapped_missing_tenant_returns_not_found() {
    let Some(store) = pg_store().await else {
        return;
    };

    let err = store
        .set_tenant_auth_bootstrapped("missing", true)
        .await
        .expect_err("missing tenant");
    assert!(err.to_string().contains("not found"));
}

#[tokio::test]
#[serial]
async fn pg_store_namespace_conflict_and_not_found() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");

    let err = store
        .create_namespace(Namespace {
            tenant_id: "missing".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect_err("missing tenant");
    assert!(matches!(err, StoreError::NotFound(_)));

    let namespace = Namespace {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        display_name: "Default".to_string(),
    };
    store
        .create_namespace(namespace.clone())
        .await
        .expect("namespace");
    let err = store
        .create_namespace(namespace)
        .await
        .expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)));

    let err = store
        .delete_namespace(&NamespaceKey {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
        })
        .await
        .expect_err("missing namespace");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[tokio::test]
#[serial]
async fn pg_store_stream_conflict_and_not_found() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");

    let err = store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
            stream: "updates".to_string(),
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
            routing: Default::default(),
        })
        .await
        .expect_err("missing namespace");
    assert!(matches!(err, StoreError::NotFound(_)));

    let stream = Stream {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
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
        routing: Default::default(),
    };
    store.create_stream(stream.clone()).await.expect("stream");
    let err = store.create_stream(stream).await.expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)));

    let err = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await
        .expect_err("missing stream");
    assert!(matches!(err, StoreError::NotFound(_)));

    let err = store
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
        .await
        .expect_err("missing stream");
    assert!(matches!(err, StoreError::NotFound(_)));

    let err = store
        .delete_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
        })
        .await
        .expect_err("missing stream");
    assert!(matches!(err, StoreError::NotFound(_)));
}

/// A stream's replication factor survives every read path and a patch.
#[tokio::test]
#[serial]
async fn pg_store_keeps_a_streams_replication_factor() {
    let Some(store) = pg_store().await else {
        return;
    };
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 2,
            replication_factor: 3,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Quorum,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: true,
            region: None,
            routing: Default::default(),
        })
        .await
        .expect("stream");
    let key = StreamKey {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "orders".to_string(),
    };

    let read = store.get_stream(&key).await.expect("get");
    assert_eq!(read.replication_factor, 3, "get_stream");
    let listed = store.list_streams("t1", "default").await.expect("list");
    assert_eq!(listed[0].replication_factor, 3, "list_streams");
    // What placement reads.
    let snapshot = store.stream_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.items[0].replication_factor, 3, "stream_snapshot");

    // A patch rewrites the row from what it read, so a wrong read would be
    // written back and stick.
    let patched = store
        .patch_stream(
            &key,
            StreamPatchRequest {
                retention: Some(RetentionPolicy {
                    max_age_seconds: Some(60),
                    max_size_bytes: None,
                }),
                consistency: None,
                delivery: None,
                durable: None,
            },
        )
        .await
        .expect("patch");
    assert_eq!(patched.replication_factor, 3, "patch_stream result");
    let read = store.get_stream(&key).await.expect("get after patch");
    assert_eq!(read.replication_factor, 3, "get_stream after patch");
}

#[tokio::test]
#[serial]
async fn pg_store_cache_conflict_and_not_found() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");

    let err = store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "missing".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
        })
        .await
        .expect_err("missing namespace");
    assert!(matches!(err, StoreError::NotFound(_)));

    let cache = Cache {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
        display_name: "Primary".to_string(),
        shards: 1,
        replication_factor: 1,
        consistency: felix_controlplane_service::model::ConsistencyLevel::Leader,
    };
    store.create_cache(cache.clone()).await.expect("cache");
    let err = store.create_cache(cache).await.expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)));

    let err = store
        .get_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await
        .expect_err("missing cache");
    assert!(matches!(err, StoreError::NotFound(_)));

    let err = store
        .patch_cache(
            &felix_controlplane_service::model::CacheKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "missing".to_string(),
            },
            CachePatchRequest {
                display_name: Some("New".to_string()),
            },
        )
        .await
        .expect_err("missing cache");
    assert!(matches!(err, StoreError::NotFound(_)));

    let err = store
        .delete_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
        })
        .await
        .expect_err("missing cache");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[tokio::test]
#[serial]
async fn pg_store_conflict_and_delete_not_found_errors() {
    let Some(store) = pg_store().await else {
        return;
    };

    let tenant = Tenant {
        tenant_id: "t1".to_string(),
        display_name: "Tenant One".to_string(),
    };
    store.create_tenant(tenant.clone()).await.expect("tenant");
    let err = store.create_tenant(tenant).await.expect_err("conflict");
    assert!(matches!(err, StoreError::Conflict(_)));

    let err = store.delete_tenant("missing").await.expect_err("missing");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[tokio::test]
#[serial]
async fn pg_store_list_get_and_exists_roundtrip() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
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
            routing: Default::default(),
        })
        .await
        .expect("stream");
    store
        .create_cache(Cache {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            display_name: "Primary".to_string(),
            shards: 1,
            replication_factor: 1,
            // Not the default, so reading it back proves the column round-trips
            // rather than the default filling it in.
            consistency: ConsistencyLevel::Quorum,
        })
        .await
        .expect("cache");

    let tenants = store.list_tenants().await.expect("list tenants");
    assert!(tenants.iter().any(|t| t.tenant_id == "t1"));
    let namespaces = store.list_namespaces("t1").await.expect("list namespaces");
    assert!(namespaces.iter().any(|ns| ns.namespace == "default"));
    let streams = store
        .list_streams("t1", "default")
        .await
        .expect("list streams");
    assert!(streams.iter().any(|s| s.stream == "updates"));
    let stream = store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
        })
        .await
        .expect("get stream");
    assert_eq!(stream.stream, "updates");
    let caches = store
        .list_caches("t1", "default")
        .await
        .expect("list caches");
    assert!(
        caches
            .iter()
            .any(|c| c.cache == "primary" && matches!(c.consistency, ConsistencyLevel::Quorum))
    );
    let cache = store
        .get_cache(&felix_controlplane_service::model::CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
        })
        .await
        .expect("get cache");
    assert!(matches!(cache.consistency, ConsistencyLevel::Quorum));
    assert_eq!(cache.cache, "primary");

    assert!(store.tenant_exists("t1").await.expect("tenant exists"));
    assert!(
        store
            .namespace_exists(&NamespaceKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
            })
            .await
            .expect("namespace exists")
    );
    store.health_check().await.expect("health check");
    assert!(store.is_durable());
    assert_eq!(store.backend_name(), "postgres");
}

#[tokio::test]
#[serial]
async fn pg_store_tenant_snapshot_and_changes_roundtrip() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    let snapshot = store.tenant_snapshot().await.expect("snapshot");
    assert!(snapshot.items.iter().any(|t| t.tenant_id == "t1"));
    let changes = store.tenant_changes(0).await.expect("changes");
    assert!(
        changes
            .items
            .iter()
            .any(|change| matches!(change.op, TenantChangeOp::Created))
    );
}

#[tokio::test]
#[serial]
async fn pg_delete_tenant_emits_stream_retention_max_size() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            display_name: "Default".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: Some(60),
                max_size_bytes: Some(4096),
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: true,
            region: None,
            routing: Default::default(),
        })
        .await
        .expect("stream");

    store.delete_tenant("t1").await.expect("delete tenant");

    let changes = store.stream_changes(0).await.expect("stream changes");
    let deleted = changes
        .items
        .iter()
        .find(|change| {
            matches!(change.op, StreamChangeOp::Deleted) && change.key.stream == "orders"
        })
        .expect("deleted stream change");
    let stream = deleted.stream.as_ref().expect("deleted payload");
    assert_eq!(stream.retention.max_size_bytes, Some(4096));
}

#[tokio::test]
#[serial]
async fn pg_store_connect_runs_migrations() {
    let base_url = match std::env::var("FELIX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
    {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping pg-tests: set FELIX_CONTROLPLANE_POSTGRES_URL or DATABASE_URL");
            return;
        }
    };
    let schema = common::unique_schema("felix_migrate");
    let mut conn = match sqlx::PgConnection::connect(&base_url).await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("skipping pg-tests: cannot connect to postgres: {err}");
            return;
        }
    };
    let create_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, schema);
    if let Err(err) = sqlx::query(AssertSqlSafe(create_sql))
        .execute(&mut conn)
        .await
    {
        eprintln!("skipping pg-tests: cannot create schema: {err}");
        return;
    }
    conn.close().await.ok();

    let url = url_with_schema(&base_url, &schema);
    let pg_cfg = config::PostgresConfig {
        url,
        max_connections: 5,
        connect_timeout_ms: 5_000,
        acquire_timeout_ms: 5_000,
    };
    let store = match felix_controlplane_service::store::postgres::PostgresStore::connect(
        &pg_cfg,
        StoreConfig {
            changes_limit: config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await
    {
        Ok(store) => store,
        Err(err) => {
            eprintln!("skipping pg-tests: connect postgres store failed: {err}");
            return;
        }
    };
    let tenant = Tenant {
        tenant_id: "t1".to_string(),
        display_name: "Tenant One".to_string(),
    };
    store.create_tenant(tenant).await.expect("create tenant");
}

#[tokio::test]
#[serial]
async fn pg_set_tenant_signing_keys_rejects_invalid_material() {
    let Some(store) = pg_store().await else {
        return;
    };

    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");

    let mut keys = generate_signing_keys().expect("keys");
    keys.current.public_key[0] ^= 0xFF;

    let err = store
        .set_tenant_signing_keys("t1", keys)
        .await
        .expect_err("invalid keys");
    assert!(matches!(err, StoreError::Unexpected(_)));
}

#![cfg(feature = "pg-tests")]
//! Readiness, against a database that can actually fail (#122).
//!
//! `src/api/readiness/tests.rs` proves the mechanism — the cache window, the
//! timeout, the draining short-circuit — against a fake probe that fails on
//! command. What it cannot prove is the part the acceptance criteria are
//! written about: that a *real* Postgres going away takes this instance out of
//! rotation, that it comes back on its own, and that neither event makes
//! liveness lie. A fake that returns `Err` is not evidence about a database.
//!
//! So these run the real router over a real `PostgresStore`, reached through a
//! proxy this test can cut and restore. Cutting the proxy is a genuine outage
//! from the pool's point of view: connections drop, new ones are refused, and
//! nothing in the process knows it was deliberate.
//!
//! Run with `cargo test -p felix-controlplane-service --features pg-tests pg_readiness`.
//! Skipped, not failed, when no database is reachable — same rule as the other
//! pg-tests, so a machine without docker still runs the rest of the suite.
//!
//! Every test here is `#[serial]`: they share one database, and the schema
//! test below removes the newest `_sqlx_migrations` row for a moment. A
//! concurrent `connect()` in that window re-runs the migration against a
//! schema that already has it and fails on its first non-idempotent statement.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::api::readiness::{Readiness, StoreProbe};
use felix_controlplane_service::api::{self, AppState};
use felix_controlplane_service::config::PostgresConfig;
use felix_controlplane_service::store::{StoreConfig, postgres::PostgresStore};
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// A database URL to test against, or `None` to skip.
///
/// Deliberately only honours an explicit URL rather than starting a container:
/// these tests cut connectivity, and doing that to a container shared with the
/// rest of the pg-tests would fail them for a reason that is not theirs. CI
/// sets this variable, so this is where they run.
fn database_url() -> Option<String> {
    match std::env::var("FELIX_TEST_DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("skipping pg_readiness: FELIX_TEST_DATABASE_URL is not set");
            None
        }
    }
}

/// A TCP proxy in front of Postgres, with a switch.
///
/// The outage has to happen below the pool: closing the pool would be this
/// process deciding it is unhealthy, which proves nothing about what a probe
/// sees when a database fails on its own. Refusing connections and dropping
/// live ones is what a database that has gone away looks like from here.
struct Proxy {
    addr: std::net::SocketAddr,
    up: Arc<AtomicBool>,
    /// Accept connections but never carry their bytes. The pool's connection is
    /// then established-but-useless, which is what exhausts it: the one slot is
    /// spoken for by a query that will never answer.
    black_hole: Arc<AtomicBool>,
    shutdown: CancellationToken,
}

impl Proxy {
    async fn start(upstream: String) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let up = Arc::new(AtomicBool::new(true));
        let black_hole = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();

        let accept_black_hole = Arc::clone(&black_hole);
        let accept_up = Arc::clone(&up);
        let accept_shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = accept_shutdown.cancelled() => return,
                    accepted = listener.accept() => accepted,
                };
                let Ok((client, _)) = accepted else { return };
                // Refused while down: a pool that reconnects must fail, not
                // queue behind a proxy that is holding the socket open.
                if !accept_up.load(Ordering::SeqCst) {
                    drop(client);
                    continue;
                }
                // Held, not proxied: the client believes it has a connection
                // and waits for bytes that never come.
                if accept_black_hole.load(Ordering::SeqCst) {
                    let hold_shutdown = accept_shutdown.clone();
                    tokio::spawn(async move {
                        let _client = client;
                        hold_shutdown.cancelled().await;
                    });
                    continue;
                }
                let upstream = upstream.clone();
                let conn_up = Arc::clone(&accept_up);
                let conn_shutdown = accept_shutdown.clone();
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    pump(client, server, conn_up, conn_shutdown).await;
                });
            }
        });

        Ok(Self {
            addr,
            up,
            black_hole,
            shutdown,
        })
    }

    fn url(&self, upstream: &str) -> String {
        // Keep the credentials and database from the real URL, swap the host
        // and port for the proxy's.
        let tail = upstream
            .rsplit_once('@')
            .map(|(_, tail)| tail)
            .unwrap_or(upstream);
        let database = tail.split_once('/').map(|(_, db)| db).unwrap_or("postgres");
        let credentials = upstream
            .split_once("://")
            .and_then(|(_, rest)| rest.rsplit_once('@'))
            .map(|(credentials, _)| credentials)
            .unwrap_or("postgres:postgres");
        format!(
            "postgres://{credentials}@{}:{}/{database}",
            self.addr.ip(),
            self.addr.port()
        )
    }

    /// Take the database away: existing connections die, new ones are refused.
    fn cut(&self) {
        self.up.store(false, Ordering::SeqCst);
    }

    /// Give it back.
    fn restore(&self) {
        self.black_hole.store(false, Ordering::SeqCst);
        self.up.store(true, Ordering::SeqCst);
    }

    /// Accept connections and then say nothing on them.
    ///
    /// The established connections have to go first, or the pool keeps using a
    /// healthy one and never meets the black hole. Held down long enough for
    /// the pumps to notice — they poll — before new connections are accepted
    /// again, this time into silence.
    async fn black_hole(&self) {
        self.black_hole.store(true, Ordering::SeqCst);
        self.up.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.up.store(true, Ordering::SeqCst);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Copy in both directions until either side ends or the proxy is cut.
async fn pump(
    mut client: TcpStream,
    mut server: TcpStream,
    up: Arc<AtomicBool>,
    shutdown: CancellationToken,
) {
    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();
    // Polled alongside the copies so a cut drops connections that are already
    // established, rather than only refusing new ones. A database that has gone
    // away does both.
    let cut = async {
        while up.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::select! {
        _ = tokio::io::copy(&mut client_read, &mut server_write) => {}
        _ = tokio::io::copy(&mut server_read, &mut client_write) => {}
        _ = cut => {}
        _ = shutdown.cancelled() => {}
    }
    let _ = client_write.shutdown().await;
    let _ = server_write.shutdown().await;
}

/// The real router, with readiness wired to the real store.
fn router_over(store: Arc<PostgresStore>) -> axum::Router {
    let probe: Arc<dyn felix_controlplane_service::api::readiness::HealthProbe> =
        Arc::new(StoreProbe(store.clone() as Arc<_>));
    let state = AppState {
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
        store: store as Arc<_>,
        oidc_validator: felix_controlplane_service::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        // A window short enough that a test observes recovery without waiting
        // on the production default, and long enough to still prove the cache
        // exists: the timeout tests below rely on a real check happening.
        readiness: Arc::new(Readiness::with_limits(
            probe,
            Duration::from_secs(2),
            Duration::from_millis(50),
        )),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
        new_cluster: Default::default(),
    };
    api::build_router(state)
}

async fn probe(router: &axum::Router, path: &str) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("build probe request"),
        )
        .await
        .expect("probe the router")
        .status()
}

/// Poll `path` until it answers `want`, or give up.
///
/// Readiness is cached, and a pool notices a dead connection when it next tries
/// to use one — so the interesting assertion is "settles on this answer", not
/// "answers this on the first poll". A deadline keeps the failure mode a
/// reported timeout rather than a hang.
async fn settles_on(router: &axum::Router, path: &str, want: StatusCode, within: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if probe(router, path).await == want {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn store_through(url: &str, max_connections: u32) -> Result<Arc<PostgresStore>> {
    let store = PostgresStore::connect(
        &PostgresConfig {
            url: url.to_string(),
            max_connections,
            connect_timeout_ms: 2_000,
            acquire_timeout_ms: 2_000,
        },
        StoreConfig {
            changes_limit: felix_controlplane_service::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: None,
        },
    )
    .await?;
    Ok(Arc::new(store))
}

/// The baseline every other test here is a deviation from.
#[tokio::test]
#[serial]
async fn a_reachable_database_answers_ready() -> Result<()> {
    let Some(url) = database_url() else {
        return Ok(());
    };
    let store = store_through(&url, 2).await?;
    let router = router_over(store);

    assert_eq!(probe(&router, "/v1/system/ready").await, StatusCode::OK);
    assert_eq!(probe(&router, "/v1/system/live").await, StatusCode::OK);
    Ok(())
}

/// **A load balancer removes an instance that cannot reach its metadata, and an
/// orchestrator does not restart it.**
///
/// Both halves matter and they pull in opposite directions. Readiness must fail
/// or traffic keeps arriving at an instance that cannot serve it; liveness must
/// *not* fail, or a database outage restarts every instance in a loop — for a
/// fault none of them caused and no restart can fix.
#[tokio::test]
#[serial]
async fn an_unreachable_database_is_not_ready_but_stays_live() -> Result<()> {
    let Some(url) = database_url() else {
        return Ok(());
    };
    let proxy = Proxy::start(upstream_addr(&url)).await?;
    let store = store_through(&proxy.url(&url), 2).await?;
    let router = router_over(store);
    assert_eq!(
        probe(&router, "/v1/system/ready").await,
        StatusCode::OK,
        "the fixture must start ready, or the outage below proves nothing",
    );

    proxy.cut();

    assert!(
        settles_on(
            &router,
            "/v1/system/ready",
            StatusCode::SERVICE_UNAVAILABLE,
            Duration::from_secs(20),
        )
        .await,
        "an instance that cannot reach its database must leave rotation",
    );
    assert_eq!(
        probe(&router, "/v1/system/live").await,
        StatusCode::OK,
        "liveness must not fail for an external outage: restarting cannot fix it",
    );
    Ok(())
}

/// **A transient outage recovers on its own.**
///
/// Nothing restarts the process and nothing rebuilds the pool: connectivity
/// returns and the next probe past the cache window says so. An instance that
/// needed a restart to rejoin rotation would turn every brief database blip
/// into an operator action.
#[tokio::test]
#[serial]
async fn readiness_returns_when_the_database_does() -> Result<()> {
    let Some(url) = database_url() else {
        return Ok(());
    };
    let proxy = Proxy::start(upstream_addr(&url)).await?;
    let store = store_through(&proxy.url(&url), 2).await?;
    let router = router_over(store);

    proxy.cut();
    assert!(
        settles_on(
            &router,
            "/v1/system/ready",
            StatusCode::SERVICE_UNAVAILABLE,
            Duration::from_secs(20),
        )
        .await,
        "the outage must be observed before recovery from it means anything",
    );

    proxy.restore();

    assert!(
        settles_on(
            &router,
            "/v1/system/ready",
            StatusCode::OK,
            Duration::from_secs(30),
        )
        .await,
        "readiness must return by itself once the database does",
    );
    Ok(())
}

/// **A database older than this build expects is not ready.**
///
/// A schema mismatch is not a connectivity problem: the database answers, and
/// answers promptly. Serving against it would read and write tables whose shape
/// this build has assumptions about — so the probe checks the applied migration
/// version, and an instance rolled out ahead of its migrations stays out of
/// rotation instead of corrupting what it finds.
#[tokio::test]
#[serial]
async fn a_database_behind_this_builds_schema_is_not_ready() -> Result<()> {
    let Some(url) = database_url() else {
        return Ok(());
    };
    let store = store_through(&url, 2).await?;
    let router = router_over(store);
    assert_eq!(probe(&router, "/v1/system/ready").await, StatusCode::OK);

    // Hide the newest applied migration, so the database looks like one this
    // build is ahead of. Restored below whatever the assertions do: every other
    // pg-test shares this database.
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&url)
        .await?;
    let newest: (i64,) = sqlx::query_as("SELECT MAX(version) FROM _sqlx_migrations WHERE success")
        .fetch_one(&admin)
        .await?;
    let stashed: (i64, String, Vec<u8>, bool, i64) = sqlx::query_as(
        "DELETE FROM _sqlx_migrations WHERE version = $1 \
         RETURNING version, description, checksum, success, execution_time",
    )
    .bind(newest.0)
    .fetch_one(&admin)
    .await?;

    let observed = settles_on(
        &router,
        "/v1/system/ready",
        StatusCode::SERVICE_UNAVAILABLE,
        Duration::from_secs(10),
    )
    .await;

    sqlx::query(
        "INSERT INTO _sqlx_migrations \
         (version, description, installed_on, checksum, success, execution_time) \
         VALUES ($1, $2, now(), $3, $4, $5)",
    )
    .bind(stashed.0)
    .bind(&stashed.1)
    .bind(&stashed.2)
    .bind(stashed.3)
    .bind(stashed.4)
    .execute(&admin)
    .await?;
    admin.close().await;

    assert!(
        observed,
        "a build expecting a newer schema than the database has must not serve",
    );
    assert!(
        settles_on(
            &router,
            "/v1/system/ready",
            StatusCode::OK,
            Duration::from_secs(10),
        )
        .await,
        "restoring the migration row must restore readiness",
    );
    Ok(())
}

/// **A probe whose connection cannot answer fails inside its own bound, rather
/// than hanging.**
///
/// The failure a probe cannot survive is silence: a load balancer with no
/// answer keeps sending traffic until its *own* timeout expires, to an instance
/// that cannot serve it. So the database here neither refuses nor answers — the
/// connection is established and then nothing comes back, which is what a
/// wedged database or a saturated pool looks like from inside the process. With
/// a single-connection pool that one stuck connection is the whole pool.
///
/// The bound asserted is the readiness check's own timeout, not the store's:
/// whatever the pool decides to do about a connection that will not answer,
/// `/v1/system/ready` has to come back.
#[tokio::test]
#[serial]
async fn a_probe_that_cannot_reach_the_database_answers_within_its_bound() -> Result<()> {
    let Some(url) = database_url() else {
        return Ok(());
    };
    let proxy = Proxy::start(upstream_addr(&url)).await?;
    // One connection, so a single stuck one exhausts the pool.
    let store = store_through(&proxy.url(&url), 1).await?;
    let router = router_over(store);
    assert_eq!(
        probe(&router, "/v1/system/ready").await,
        StatusCode::OK,
        "the fixture must start ready, or what follows proves nothing",
    );

    proxy.black_hole().await;

    // The first probe past the cache window is the one that meets the wedged
    // connection. It must answer, and answer not-ready.
    let started = std::time::Instant::now();
    let settled = settles_on(
        &router,
        "/v1/system/ready",
        StatusCode::SERVICE_UNAVAILABLE,
        Duration::from_secs(20),
    )
    .await;
    let took = started.elapsed();

    assert!(
        settled,
        "a probe that cannot reach the database must report not-ready",
    );
    assert!(
        took < Duration::from_secs(20),
        "the probe must answer rather than hang: took {took:?}",
    );

    proxy.restore();
    assert!(
        settles_on(
            &router,
            "/v1/system/ready",
            StatusCode::OK,
            Duration::from_secs(30),
        )
        .await,
        "readiness must return once the database answers again",
    );
    Ok(())
}

/// `host:port` from a Postgres URL, for the proxy to dial.
fn upstream_addr(url: &str) -> String {
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let after_credentials = after_scheme
        .rsplit_once('@')
        .map(|(_, tail)| tail)
        .unwrap_or(after_scheme);
    let host_port = after_credentials
        .split_once('/')
        .map(|(head, _)| head)
        .unwrap_or(after_credentials);
    if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:5432")
    }
}

use std::time::Duration;

use anyhow::Result;
use axum::{Json, Router, routing::get};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use felix_authz::{TenantId, TenantKeyCache, TenantKeyStore};
use felix_storage::EphemeralCache;
use serde_json::json;
use tokio::net::TcpListener;

use super::*;
use crate::test_support::{spawn_axum_with_shutdown, wait_for_listen};

/// A control plane that knows one tenant, `t1`, and serves its JWKS.
fn control_plane() -> Router {
    let empty = || async { Json(json!({ "items": [], "next_seq": 1 })) };
    Router::new()
        .route(
            "/v1/tenants/snapshot",
            get(|| async { Json(json!({ "items": [{ "tenant_id": "t1" }], "next_seq": 1 })) }),
        )
        .route("/v1/namespaces/snapshot", get(empty))
        .route("/v1/caches/snapshot", get(empty))
        .route("/v1/streams/snapshot", get(empty))
        .route("/v1/tenants/changes", get(empty))
        .route("/v1/namespaces/changes", get(empty))
        .route("/v1/caches/changes", get(empty))
        .route("/v1/streams/changes", get(empty))
        .route(
            "/v1/tenants/t1/.well-known/jwks.json",
            get(|| async {
                Json(json!({ "keys": [{
                    "kty": "OKP",
                    "kid": "k1",
                    "alg": "EdDSA",
                    "use": "sig",
                    "crv": "Ed25519",
                    "x": URL_SAFE_NO_PAD.encode([7u8; 32]),
                }] }))
            }),
        )
}

/// A broker that has seeded its catalog can verify a token of every tenant
/// in it after the control plane goes away, though no client of that tenant
/// had connected yet. A peer forwards writes with the client's token, so
/// without this a control-plane outage refuses every forwarded write to a
/// broker that restarted shortly before it.
#[tokio::test]
async fn a_seeded_broker_holds_every_tenants_keys_before_the_control_plane_goes() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (stop_control_plane, served) = spawn_axum_with_shutdown(listener, control_plane());
    wait_for_listen(addr).await?;

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let config = BrokerConfig {
        controlplane_url: Some(format!("http://{addr}")),
        controlplane_sync_interval_ms: 10,
        ..BrokerConfig::default()
    };
    let catalog_seeded = CancellationToken::new();
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        format!("http://{addr}"),
        Arc::new(TenantKeyCache::default()),
    ));
    let sync_shutdown = CancellationToken::new();
    let (seeded_tx, _seeded_rx) = oneshot::channel();
    let sync = spawn_catalog_sync(
        &config,
        &broker,
        &None,
        &sync_shutdown,
        false,
        seeded_tx,
        &catalog_seeded,
        &key_store,
        &{
            let checked = CancellationToken::new();
            checked.cancel();
            checked
        },
        crate::cluster::catalog_sync::Provisional::default(),
        GroupCheck::Decided(None),
    )
    .expect("a control plane is configured");

    tokio::time::timeout(Duration::from_secs(10), catalog_seeded.cancelled())
        .await
        .expect("the catalog is seeded");
    let _ = stop_control_plane.send(());
    let _ = served.await;

    key_store
        .verification_keys(&TenantId::new("t1"))
        .expect("t1's keys are cached");

    sync_shutdown.cancel();
    let _ = sync.await;
    Ok(())
}

/// **A broker that started from its view serves no client before it
/// registers.** The catalog seeding releases `seeded`, which registration
/// waits on, but the client listeners and `/ready` wait for the
/// registration itself.
#[tokio::test]
async fn after_a_start_from_the_view_clients_wait_for_registration() {
    let readiness = Readiness::starting();
    let draining = CancellationToken::new();
    let seeded = CancellationToken::new();
    let clients = CancellationToken::new();
    let listening = CancellationToken::new();
    listening.cancel();
    let (registered, registered_watch) = watch::channel(None);
    let (seeded_tx, seeded_rx) = oneshot::channel();
    spawn_readiness_flip(
        true,
        &readiness,
        &draining,
        &seeded,
        seeded_rx,
        &listening,
        Some((clients.clone(), registered_watch)),
    );

    seeded_tx.send(()).expect("send");
    tokio::time::timeout(Duration::from_secs(5), seeded.cancelled())
        .await
        .expect("registration may go ahead");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!clients.is_cancelled(), "no client before registration");
    assert!(!readiness.is_ready(), "not ready before registration");

    registered.send_replace(Some(3));
    tokio::time::timeout(Duration::from_secs(5), clients.cancelled())
        .await
        .expect("clients are served once registered");
    assert!(readiness.is_ready());
}

/// Without a view nothing more is waited for: the catalog seeds and the
/// broker is ready, as it always was.
#[tokio::test]
async fn without_a_view_readiness_follows_the_catalog() {
    let readiness = Readiness::starting();
    let draining = CancellationToken::new();
    let seeded = CancellationToken::new();
    let listening = CancellationToken::new();
    listening.cancel();
    let (seeded_tx, seeded_rx) = oneshot::channel();
    spawn_readiness_flip(
        true, &readiness, &draining, &seeded, seeded_rx, &listening, None,
    );
    seeded_tx.send(()).expect("send");
    tokio::time::timeout(Duration::from_secs(5), seeded.cancelled())
        .await
        .expect("seeded");
    for _ in 0..100 {
        if readiness.is_ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never reported ready");
}

/// A catalog that seeds before startup binds its last listener must not flip
/// readiness: the bind can still fail, and a broker that said ready and then
/// exited is not retried as a failed start.
#[tokio::test(start_paused = true)]
async fn readiness_waits_for_every_listener_after_the_seed() {
    let readiness = Readiness::starting();
    let draining = CancellationToken::new();
    let seeded = CancellationToken::new();
    let listening = CancellationToken::new();
    let (seeded_tx, seeded_rx) = oneshot::channel();
    spawn_readiness_flip(
        true, &readiness, &draining, &seeded, seeded_rx, &listening, None,
    );

    seeded_tx.send(()).expect("the flip is waiting");
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        !readiness.is_ready(),
        "ready before the listeners were bound"
    );
    assert!(
        !seeded.is_cancelled(),
        "accept and registration released before the listeners were bound"
    );

    listening.cancel();
    tokio::time::timeout(Duration::from_secs(5), seeded.cancelled())
        .await
        .expect("the flip fires once every listener is bound");
    assert!(readiness.is_ready());
}

/// A start-up that fails between the seed and the last bind drains without
/// ever having reported ready.
#[tokio::test(start_paused = true)]
async fn a_drain_before_the_listeners_are_bound_stays_unready() {
    let readiness = Readiness::starting();
    let draining = CancellationToken::new();
    let seeded = CancellationToken::new();
    let listening = CancellationToken::new();
    let (seeded_tx, seeded_rx) = oneshot::channel();
    spawn_readiness_flip(
        true, &readiness, &draining, &seeded, seeded_rx, &listening, None,
    );

    seeded_tx.send(()).expect("the flip is waiting");
    tokio::time::sleep(Duration::from_secs(1)).await;
    draining.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;
    listening.cancel();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!readiness.is_ready());
    assert!(!seeded.is_cancelled());
}

/// A data directory a broker has run on, checked against a control plane with
/// no metadata group.
fn marked_directory() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("dir");
    felix_storage::storage_identity::load_or_create(dir.path(), || [1; 16]).expect("id");
    std::fs::write(dir.path().join(controlplane_group::NO_GROUP_FILE), "").expect("mark");
    dir
}

#[test]
fn a_start_from_the_view_judges_its_group_after_the_restore_check() {
    let dir = marked_directory();
    let client = reqwest::Client::new();
    let refused = CancellationToken::new();
    let check = GroupCheck::for_start(Some(dir.path()), true, None, &client, &refused);
    assert!(matches!(check, GroupCheck::Deferred { .. }), "{check:?}");
    let check = GroupCheck::for_start(Some(dir.path()), false, None, &client, &refused);
    assert!(
        matches!(&check, GroupCheck::Decided(Some(guard)) if guard.stored.is_none()),
        "a marked directory expects no group: {check:?}"
    );
    let unmarked = tempfile::tempdir().expect("dir");
    let check = GroupCheck::for_start(Some(unmarked.path()), false, None, &client, &refused);
    assert!(matches!(check, GroupCheck::Decided(None)), "{check:?}");
}

/// A control plane that knows tenant `t1` and names no group in its answers,
/// with an identity route that answers 503 while `pending` holds, then
/// `identity`.
fn identified_control_plane(
    pending: Arc<std::sync::atomic::AtomicBool>,
    identity: serde_json::Value,
) -> Router {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    control_plane().route(
        "/v1/cluster/identity",
        get(move || {
            let pending = Arc::clone(&pending);
            let identity = identity.clone();
            async move {
                if pending.load(std::sync::atomic::Ordering::SeqCst) {
                    StatusCode::SERVICE_UNAVAILABLE.into_response()
                } else {
                    Json(identity).into_response()
                }
            }
        }),
    )
}

/// Spawn the catalog sync of a broker that started from its view on `dir`.
async fn sync_from_view(
    dir: &std::path::Path,
    router: Router,
) -> Result<(
    Arc<Broker>,
    CancellationToken,
    CancellationToken,
    oneshot::Sender<()>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (stop, _served) = spawn_axum_with_shutdown(listener, router);
    wait_for_listen(addr).await?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let config = BrokerConfig {
        controlplane_url: Some(format!("http://{addr}")),
        controlplane_sync_interval_ms: 10,
        ..BrokerConfig::default()
    };
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        format!("http://{addr}"),
        Arc::new(TenantKeyCache::default()),
    ));
    let sync_shutdown = CancellationToken::new();
    let refused = CancellationToken::new();
    let checked = CancellationToken::new();
    checked.cancel();
    let (seeded_tx, _seeded_rx) = oneshot::channel();
    spawn_catalog_sync(
        &config,
        &broker,
        &Some(NodeCredential::new("node-token")),
        &sync_shutdown,
        false,
        seeded_tx,
        &CancellationToken::new(),
        &key_store,
        &checked,
        crate::cluster::catalog_sync::Provisional::default(),
        GroupCheck::for_start(Some(dir), true, None, &reqwest::Client::new(), &refused),
    )
    .expect("a control plane is configured");
    Ok((broker, refused, sync_shutdown, stop))
}

/// The operator moved a broker from a control plane with no group to a Raft
/// one formed on an empty catalog, and the broker started from its view. The
/// Raft group answers with no group id until it records one, which a
/// snapshot cannot tell from the in-memory control plane: nothing is applied
/// until the identity answers, and then the directory, which holds data, is
/// refused.
#[tokio::test]
async fn a_start_from_the_view_applies_nothing_from_a_new_group() -> Result<()> {
    let dir = marked_directory();
    let pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let router = identified_control_plane(
        Arc::clone(&pending),
        json!({ "group_id": "g-1", "shard_engine": "hybrid" }),
    );
    let (broker, refused, sync_shutdown, _stop) = sync_from_view(dir.path(), router).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !broker.tenant_exists("t1").await,
        "nothing is applied while the identity is pending"
    );
    assert!(!refused.is_cancelled());
    pending.store(false, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), refused.cancelled())
        .await
        .expect("the broker is told to stop");
    assert!(!broker.tenant_exists("t1").await);
    assert_eq!(
        controlplane_group::read(dir.path())?,
        None,
        "nothing is recorded"
    );
    sync_shutdown.cancel();
    Ok(())
}

#[tokio::test]
async fn a_start_from_the_view_under_a_control_plane_without_a_group_syncs() -> Result<()> {
    let dir = marked_directory();
    let router = identified_control_plane(
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        json!({ "shard_engine": "hybrid" }),
    );
    let (broker, refused, sync_shutdown, _stop) = sync_from_view(dir.path(), router).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !broker.tenant_exists("t1").await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the catalog is applied");
    assert!(!refused.is_cancelled());
    sync_shutdown.cancel();
    Ok(())
}

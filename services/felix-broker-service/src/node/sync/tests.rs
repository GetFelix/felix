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

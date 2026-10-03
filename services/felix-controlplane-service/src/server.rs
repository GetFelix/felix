//! Running the control plane: build the state, start the listeners and the
//! background loops, then drain them in order when asked to stop.
//!
//! The drain order is the part to be careful with; `drain.rs` explains it.
mod drain;
mod observability;
mod state;
pub mod tls;

use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::time::Duration;

use felix_common::lifecycle::Readiness;
use tokio_util::sync::CancellationToken;

use crate::api::{build_bootstrap_router, build_bootstrap_router_with_dev_tokens, build_router};
use crate::cluster::{membership, placement};
use crate::config::ControlPlaneConfig;
use crate::raft::LeadershipGate;
use crate::store;
use drain::Running;
use state::build_state;

/// Serve until `shutdown` resolves, then drain and return.
///
/// Returns early, with nothing to drain, if the API listener fails on its own.
pub async fn run<F>(config: ControlPlaneConfig, shutdown: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let metrics_handle = observability::init_observability("felix-controlplane");
    // A `FELIX_*` name nothing reads is a typo, and a typo is a default quietly
    // taking effect. Reported after logging is up so the warning is actually
    // seen, and as a warning rather than a refusal: an orchestrator may inject
    // variables meant for something else, and refusing to start over one is
    // worse than the mistake it guards against.
    for warning in felix_common::env_registry::unrecognised_warnings() {
        tracing::warn!("{warning}");
    }

    // One flag for the whole process. The metrics endpoint's `/ready` and the
    // API's `/v1/system/ready` both read it, so a drain is visible on both at
    // once rather than one of them insisting the instance is fine.
    //
    // It is created before the state so the state can hold it. The *tokens*
    // below stay separate because the metrics endpoint has to outlive the API
    // drain — that is how an operator watches the drain happen.
    let readiness = Readiness::ready();
    let api_shutdown = CancellationToken::new();
    let (state, raft_handle) =
        build_state(config.clone(), readiness.clone(), &api_shutdown).await?;
    let _backend_name = state.store.backend_name();
    // Under Raft, singleton background work runs only on the (freshly
    // confirmed) leader; the other backends keep every instance sweeping,
    // which their stores make safe.
    let leadership = match &raft_handle {
        Some(handle) => LeadershipGate::Leader(handle.clone()),
        None => LeadershipGate::Always,
    };
    let metrics_shutdown = CancellationToken::new();

    let metrics_task = {
        let metrics_shutdown = metrics_shutdown.clone();
        tokio::spawn(observability::serve_metrics(
            metrics_handle,
            config.metrics_bind,
            readiness.clone(),
            async move { metrics_shutdown.cancelled().await },
        ))
    };

    // Liveness expiry runs on the API token: it stops admitting work at the same
    // point the listener does, before anything drains.
    // Consensus position as gauges, on the API token like the other
    // background work.
    let raft_metrics_task = raft_handle
        .as_ref()
        .map(|handle| handle.spawn_metrics(api_shutdown.clone()));

    let expiry_task = membership::spawn_expiry_sweep(
        Arc::clone(&state.store) as Arc<dyn store::ControlPlaneStore + Send + Sync>,
        state.node_liveness.clone(),
        leadership.clone(),
        api_shutdown.clone(),
    );

    // Shard placement runs on the API token like expiry: it stops admitting work
    // at the same point the listener does. The holder id is per process, so a
    // restarted instance is a new holder and advances the placement token.
    let reconcile_task = placement::spawn_reconciler(
        Arc::clone(&state.store) as Arc<dyn store::ControlPlaneStore + Send + Sync>,
        state.node_liveness.clone(),
        config.shard_moves.clone(),
        Duration::from_millis(state.node_liveness.shard_reconcile_interval_ms),
        uuid::Uuid::new_v4().to_string(),
        leadership,
        Arc::clone(&state.placement_wakes),
        api_shutdown.clone(),
    );

    let app = build_router(state.clone());

    // The Raft RPCs get a listener of their own, authenticated as peers:
    // `propose` can replace the whole store, so it must never share the
    // public API port. Bound before the API so a bad address fails startup.
    // It stops after the Raft node does, on its own token, so a draining
    // member keeps answering its peers until it has left.
    let peer_shutdown = CancellationToken::new();
    let raft_peer_task = match (&raft_handle, &config.raft) {
        (Some(handle), Some(raft_cfg)) => {
            let listener = tokio::net::TcpListener::bind(raft_cfg.peer_bind_addr).await?;
            tracing::info!(
                addr = %listener.local_addr().unwrap_or(raft_cfg.peer_bind_addr),
                mtls = raft_cfg.security.tls.is_some(),
                authenticated = raft_cfg.security.token.is_some(),
                "raft peer listener listening"
            );
            if raft_cfg.security.token.is_none() {
                tracing::warn!(
                    "FELIX_RAFT_INSECURE_PEERS is set: anyone who reaches the raft peer \
                     listener can replace the metadata store"
                );
            }
            let handle = handle.clone();
            let shutdown = peer_shutdown.clone();
            Some(tokio::spawn(async move {
                if let Err(err) = handle.serve_peers(listener, shutdown).await {
                    tracing::error!(error = %err, "raft peer listener failed");
                }
            }))
        }
        _ => None,
    };

    let bootstrap_task = if config.bootstrap.enabled {
        let bootstrap_addr = config.bootstrap.bind_addr;
        let bootstrap_app = if config.bootstrap.dev_tokens {
            tracing::warn!(
                addr = %bootstrap_addr,
                "bootstrap dev tokens are ENABLED: the bootstrap token mints a token for any \
                 principal; for local development only",
            );
            build_bootstrap_router_with_dev_tokens(state.clone())
        } else {
            build_bootstrap_router(state.clone())
        };
        let api_shutdown = api_shutdown.clone();
        // Loaded before the task spawns: unreadable key material is a
        // misconfiguration that must fail startup, not a log line inside a
        // task nothing checks.
        let bootstrap_tls = config
            .bootstrap
            .tls
            .as_ref()
            .map(tls::load_server_config)
            .transpose()?;
        Some(tokio::spawn(async move {
            match tokio::net::TcpListener::bind(bootstrap_addr).await {
                Ok(listener) => {
                    // Logged after the bind, with the address the socket got, so
                    // a port-0 bind reports its real port and a failed bind never
                    // claims to be listening.
                    tracing::info!(
                        addr = %listener.local_addr().unwrap_or(bootstrap_addr),
                        mtls = bootstrap_tls.is_some(),
                        "bootstrap control plane listening"
                    );
                    match bootstrap_tls {
                        Some(tls) => {
                            tls::serve_tls("bootstrap", listener, bootstrap_app, tls, api_shutdown)
                                .await;
                        }
                        None => {
                            let _ = axum::serve(listener, bootstrap_app.into_make_service())
                                .with_graceful_shutdown(
                                    async move { api_shutdown.cancelled().await },
                                )
                                .await;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to bind bootstrap listener");
                }
            }
        }))
    } else {
        None
    };

    // Loaded before the bind, so unreadable key material fails startup.
    let api_tls = match &config.api_tls {
        Some(paths) => {
            let identity = tls::load_api_identity(paths)?;
            let server_config = tls::api_server_config(Arc::clone(&identity))?;
            let reload_shutdown = api_shutdown.clone();
            drop(tokio::spawn(async move {
                tokio::select! {
                    _ = reload_shutdown.cancelled() => {}
                    _ = identity.reload_periodically("API") => {}
                }
            }));
            Some(server_config)
        }
        None => None,
    };

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let addr = listener.local_addr().unwrap_or(config.bind_addr);
    tracing::info!(%addr, tls = api_tls.is_some(), "control plane listening");
    if api_tls.is_none() {
        tracing::warn!(
            "the control-plane API is plain HTTP: node credentials, token exchange and tenant \
             JWKS cross the network unencrypted. Set FELIX_CONTROLPLANE_TLS_CERT and \
             FELIX_CONTROLPLANE_TLS_KEY, or terminate TLS in front of it"
        );
    }

    // The API server drains itself: `with_graceful_shutdown` stops accepting new
    // connections and lets in-flight requests finish. Racing the server against
    // the shutdown future instead would drop it mid-request.
    let mut api_task = {
        let api_shutdown = api_shutdown.clone();
        match api_tls {
            Some(server_config) => tokio::spawn(async move {
                tls::serve_tls("api", listener, app, server_config, api_shutdown).await;
                Ok(())
            }),
            None => tokio::spawn(
                axum::serve(listener, app.into_make_service())
                    .with_graceful_shutdown(async move { api_shutdown.cancelled().await })
                    .into_future(),
            ),
        }
    };

    tokio::pin!(shutdown);
    tokio::select! {
        result = &mut api_task => {
            // The listener failed on its own; there is nothing left to drain.
            result??;
            return Ok(());
        }
        _ = &mut shutdown => {
            // Step 1: stop advertising readiness so load balancers remove this
            // instance while it can still serve. Must precede the listener stopping.
            readiness.begin_draining();
            tracing::info!("readiness set to draining");
        }
    }

    Running {
        config,
        api_shutdown,
        metrics_shutdown,
        api_task,
        reconcile_task,
        expiry_task,
        bootstrap_task,
        raft_metrics_task,
        raft_handle,
        raft_peer_task,
        peer_shutdown,
        metrics_task,
    }
    .drain()
    .await
}

#[cfg(test)]
mod tests;

//! Turning a [`ControlPlaneConfig`] into the [`AppState`] handlers share.
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use felix_common::lifecycle::Readiness;
use tokio_util::sync::CancellationToken;

use crate::api::AppState;
use crate::api::readiness::StoreProbe;
use crate::api::types::{FeatureFlags, Region};
use crate::auth::oidc::UpstreamOidcValidator;
use crate::cluster::placement::PlacementWakes;
use crate::config::{ControlPlaneConfig, RaftBackendConfig, StorageBackend};
use crate::raft::{AppStateMachine, RaftHandle, RaftMember, RaftSettings};
use crate::store::memory::InMemoryStore;
use crate::store::raft::RaftStore;
use crate::store::raft::state_machine::MetadataStateMachine;
use crate::store::{ControlPlaneAuthStore, StoreConfig};

/// This instance's member of the Raft group, and the listener its peers
/// reach it on.
pub(super) struct RaftParts {
    pub(super) handle: RaftHandle,
    pub(super) peer_task: tokio::task::JoinHandle<()>,
    /// Stops the peer listener. Its own token: a draining member keeps
    /// answering its peers until its node has left.
    pub(super) peer_shutdown: CancellationToken,
}

/// Build the handlers' state, and the Raft member when the backend is Raft.
pub(super) async fn build_state(
    config: ControlPlaneConfig,
    lifecycle_readiness: Readiness,
    api_shutdown: &CancellationToken,
) -> anyhow::Result<(AppState, Option<RaftParts>)> {
    let (store, raft) = open_store(&config, api_shutdown).await?;

    let readiness = Arc::new(crate::api::readiness::Readiness::with_lifecycle(
        // The same flag the metrics endpoint reads, so a drain is visible on
        // both ports at once.
        lifecycle_readiness.clone(),
        // The store, seen through the one method readiness needs.
        Arc::new(StoreProbe(Arc::clone(&store))),
        Duration::from_millis(config.readiness_timeout_ms),
        Duration::from_millis(config.readiness_cache_ttl_ms),
    ));

    Ok((
        AppState {
            region: Region {
                region_id: config.region_id,
                display_name: "Local Region".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: store.is_durable(),
                tiered_storage: false,
                bridges: false,
            },
            store,
            readiness,
            in_flight: Default::default(),
            placement_wakes: Arc::new(PlacementWakes::new(
                api_shutdown.child_token(),
                config.shard_moves.fence_max_lag_records,
            )),
            oidc_validator: UpstreamOidcValidator::new_with_allowed_algorithms(
                Duration::from_secs(3600),
                Duration::from_secs(3600),
                60,
                config.oidc_allowed_algorithms,
            ),
            bootstrap_enabled: config.bootstrap.enabled,
            bootstrap_tokens: config.bootstrap.accepted_tokens(),
            node_liveness: config.node_liveness,
            move_policy: config.shard_moves,
            new_cluster: Arc::new(crate::cluster::fleet::NewClusterFinalize::new(
                api_shutdown.child_token(),
            )),
        },
        raft,
    ))
}

/// Open the configured backend. Under Raft this also starts this instance's
/// member of the group, which is why its parts come back alongside.
async fn open_store(
    config: &ControlPlaneConfig,
    api_shutdown: &CancellationToken,
) -> anyhow::Result<(
    Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    Option<RaftParts>,
)> {
    let store_config = StoreConfig {
        changes_limit: config.changes_limit,
        change_retention_max_rows: config.change_retention_max_rows,
    };
    match config.storage {
        StorageBackend::Memory => Ok((Arc::new(InMemoryStore::new(store_config)), None)),
        StorageBackend::Raft => {
            let raft_cfg = config.raft.as_ref().context("raft configuration missing")?;
            let inner = Arc::new(InMemoryStore::new(store_config));
            let machine = Arc::new(MetadataStateMachine::new(inner));
            let member = RaftMember::open(
                raft_settings(raft_cfg),
                Arc::clone(&machine) as Arc<dyn AppStateMachine>,
            )
            .await?;
            let (peer_task, peer_shutdown) = match serve_peers(&member, raft_cfg).await {
                Ok(serving) => serving,
                Err(err) => {
                    if let Err(close) = member.close().await {
                        tracing::warn!(error = %close, "close the store after a failed start");
                    }
                    return Err(err);
                }
            };
            let handle = match start_node(&member, raft_cfg, api_shutdown).await {
                Ok(handle) => handle,
                Err(err) => {
                    peer_shutdown.cancel();
                    return Err(err);
                }
            };
            let store = Arc::new(RaftStore::new(handle.clone(), machine));
            let parts = RaftParts {
                handle,
                peer_task,
                peer_shutdown,
            };
            Ok((store, Some(parts)))
        }
    }
}

/// Bind the peer listener and serve the member on it.
///
/// The Raft routes get a listener of their own, authenticated as peers:
/// `propose` can replace the whole store, so it must never share the public
/// API port. It serves before the member has a Raft node, so the member
/// answers `standing` from the start and nothing else until the node is
/// attached.
async fn serve_peers(
    member: &RaftMember,
    raft_cfg: &RaftBackendConfig,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, CancellationToken)> {
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
    let shutdown = CancellationToken::new();
    let (member, token) = (member.clone(), shutdown.clone());
    let task = tokio::spawn(async move {
        if let Err(err) = member.serve_peers(listener, token).await {
            tracing::error!(error = %err, "raft peer listener failed");
        }
    });
    Ok((task, shutdown))
}

/// Start the member's Raft node and open its routes.
async fn start_node(
    member: &RaftMember,
    raft_cfg: &RaftBackendConfig,
    api_shutdown: &CancellationToken,
) -> anyhow::Result<RaftHandle> {
    let handle = member.start_node().await?;
    // Before the node's routes serve: an empty member must not answer a
    // vote until it knows whether it is forming the group or rejoining one
    // it forgot.
    let entered = handle
        .enter_group(
            raft_cfg.peers.clone(),
            raft_cfg.initial_cluster_state,
            api_shutdown.child_token(),
        )
        .await;
    if let Err(err) = entered {
        if let Err(stop) = handle.shutdown().await {
            tracing::warn!(error = %stop, "stop the raft node after a failed start");
        }
        return Err(err);
    }
    member.attach(&handle);
    Ok(handle)
}

/// The seam's defaults, overridden by whatever the operator set.
fn raft_settings(raft_cfg: &RaftBackendConfig) -> RaftSettings {
    let mut settings = RaftSettings::new(
        raft_cfg.node_id,
        raft_cfg.data_dir.clone(),
        raft_cfg.security.clone(),
    );
    settings.peer_addrs = raft_cfg.peers.clone();
    if let Some(ms) = raft_cfg.heartbeat_ms {
        settings.heartbeat_interval = Duration::from_millis(ms);
    }
    if let Some(ms) = raft_cfg.election_timeout_min_ms {
        settings.election_timeout.0 = Duration::from_millis(ms);
    }
    if let Some(ms) = raft_cfg.election_timeout_max_ms {
        settings.election_timeout.1 = Duration::from_millis(ms);
    }
    if let Some(logs) = raft_cfg.snapshot_logs_since_last {
        settings.snapshot_logs_since_last = logs;
    }
    if let Some(logs) = raft_cfg.logs_kept_behind_snapshot {
        settings.logs_kept_behind_snapshot = logs;
    }
    if let Some(ms) = raft_cfg.write_timeout_ms {
        settings.write_timeout = Duration::from_millis(ms);
    }
    settings
}

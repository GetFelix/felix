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
use crate::raft::{AppStateMachine, RaftHandle, RaftSettings};
use crate::store::memory::InMemoryStore;
use crate::store::postgres::PostgresStore;
use crate::store::raft::RaftStore;
use crate::store::raft::state_machine::MetadataStateMachine;
use crate::store::{ControlPlaneAuthStore, StoreConfig};

/// Build the handlers' state, and the Raft member when the backend is Raft.
pub(super) async fn build_state(
    config: ControlPlaneConfig,
    lifecycle_readiness: Readiness,
    api_shutdown: &CancellationToken,
) -> anyhow::Result<(AppState, Option<RaftHandle>)> {
    let (store, raft_handle) = open_store(&config, api_shutdown).await?;

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
        },
        raft_handle,
    ))
}

/// Open the configured backend. Under Raft this also starts this instance's
/// member of the group, which is why the handle comes back alongside.
async fn open_store(
    config: &ControlPlaneConfig,
    api_shutdown: &CancellationToken,
) -> anyhow::Result<(
    Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    Option<RaftHandle>,
)> {
    let store_config = StoreConfig {
        changes_limit: config.changes_limit,
        change_retention_max_rows: config.change_retention_max_rows,
    };
    let mut raft_handle = None;
    let store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = match config.storage {
        StorageBackend::Memory => Arc::new(InMemoryStore::new(store_config)),
        StorageBackend::Postgres => {
            let pg = config
                .postgres
                .as_ref()
                .context("postgres configuration missing")?;
            Arc::new(PostgresStore::connect(pg, store_config).await?)
        }
        StorageBackend::Raft => {
            let raft_cfg = config.raft.as_ref().context("raft configuration missing")?;
            let inner = Arc::new(InMemoryStore::new(store_config));
            let machine = Arc::new(MetadataStateMachine::new(inner));
            let settings = raft_settings(raft_cfg);
            let handle =
                RaftHandle::start(settings, Arc::clone(&machine) as Arc<dyn AppStateMachine>)
                    .await?;
            // Before the routes serve: an empty member must not answer a
            // vote until it knows whether it is forming the group or
            // rejoining one it forgot.
            handle.enter_group(
                raft_cfg.peers.clone(),
                raft_cfg.initial_cluster_state,
                api_shutdown.child_token(),
            )?;
            raft_handle = Some(handle.clone());
            Arc::new(RaftStore::new(handle, machine))
        }
    };
    Ok((store, raft_handle))
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

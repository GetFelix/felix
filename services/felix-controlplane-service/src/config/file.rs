//! The YAML config file, and how it overrides the environment.
use std::str::FromStr;

use anyhow::{Context, Result};
use serde::Deserialize;

use super::{
    ApiTlsConfig, BootstrapTlsConfig, ControlPlaneConfig, StorageBackend,
    parse_oidc_allowed_algorithms,
};

/// The settings a config file may override.
///
/// `deny_unknown_fields`, here and on every nested override, because a key
/// nobody reads is a lie: an operator who writes `bind_adrr` gets the default,
/// no error, and an instance listening somewhere they did not ask for. A typo
/// inside `postgres:` is just as silent, which is why the nested ones carry it
/// too.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ControlPlaneConfigOverride {
    bind_addr: Option<String>,
    tls: Option<ApiTlsConfig>,
    metrics_bind: Option<String>,
    region_id: Option<String>,
    storage: Option<StorageOverride>,
    postgres: Option<PostgresOverride>,
    changes_limit: Option<u64>,
    change_retention_max_rows: Option<i64>,
    oidc_allowed_algorithms: Option<Vec<String>>,
    bootstrap: Option<BootstrapOverride>,
    node_liveness: Option<NodeLivenessOverride>,
    max_concurrent_shard_moves: Option<usize>,
    max_shard_moves_per_node: Option<usize>,
    shard_move_fence_max_lag_records: Option<u64>,
    shard_move_timeout_ms: Option<u64>,
    shutdown_drain_timeout_ms: Option<u64>,
    shutdown_predrain_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeLivenessOverride {
    heartbeat_interval_ms: Option<u64>,
    expiry_timeout_ms: Option<u64>,
    sweep_interval_ms: Option<u64>,
    shard_reconcile_interval_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageOverride {
    backend: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresOverride {
    url: Option<String>,
    max_connections: Option<u32>,
    connect_timeout_ms: Option<u64>,
    acquire_timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapOverride {
    enabled: Option<bool>,
    bind_addr: Option<String>,
    token: Option<String>,
    previous_token: Option<String>,
    tls: Option<BootstrapTlsConfig>,
}

impl ControlPlaneConfig {
    /// Fold a parsed config file over the values already taken from the
    /// environment.
    ///
    /// Separate from the read so the precedence rules — including a postgres
    /// block selecting the backend — are testable without a file on disk.
    pub(super) fn apply(&mut self, override_cfg: ControlPlaneConfigOverride) -> Result<()> {
        let config = self;

        if let Some(value) = override_cfg.bind_addr {
            config.bind_addr = value.parse().with_context(|| "parse bind_addr")?;
        }
        if let Some(value) = override_cfg.metrics_bind {
            config.metrics_bind = value.parse().with_context(|| "parse metrics_bind")?;
        }
        if let Some(value) = override_cfg.region_id {
            config.region_id = value;
        }
        if let Some(value) = override_cfg.changes_limit {
            config.changes_limit = value;
        }
        if let Some(value) = override_cfg.change_retention_max_rows {
            config.change_retention_max_rows = Some(value);
        }
        if let Some(liveness) = override_cfg.node_liveness {
            if let Some(value) = liveness.heartbeat_interval_ms {
                config.node_liveness.heartbeat_interval_ms = value;
            }
            if let Some(value) = liveness.expiry_timeout_ms {
                config.node_liveness.expiry_timeout_ms = value;
            }
            if let Some(value) = liveness.sweep_interval_ms {
                config.node_liveness.sweep_interval_ms = value;
            }
            if let Some(value) = liveness.shard_reconcile_interval_ms {
                config.node_liveness.shard_reconcile_interval_ms = value;
            }
        }
        if let Some(value) = override_cfg.max_concurrent_shard_moves {
            config.shard_moves.max_concurrent = value;
        }
        if let Some(value) = override_cfg.max_shard_moves_per_node {
            config.shard_moves.max_per_node = (value > 0).then_some(value);
        }
        if let Some(value) = override_cfg.shard_move_fence_max_lag_records {
            config.shard_moves.fence_max_lag_records = value;
        }
        if let Some(value) = override_cfg.shard_move_timeout_ms {
            config.shard_moves.timeout_millis = (value > 0).then_some(value);
        }
        if let Some(value) = override_cfg.shutdown_drain_timeout_ms
            && value > 0
        {
            config.shutdown_drain_timeout_ms = value;
        }
        if let Some(value) = override_cfg.shutdown_predrain_ms {
            config.shutdown_predrain_ms = value;
        }
        if let Some(values) = override_cfg.oidc_allowed_algorithms {
            config.oidc_allowed_algorithms = parse_oidc_allowed_algorithms(values)?;
        }
        if let Some(storage_override) = override_cfg.storage
            && let Some(backend) = storage_override.backend
        {
            config.storage = StorageBackend::from_str(&backend)?;
        }
        if let Some(pg_override) = override_cfg.postgres {
            let mut pg_cfg = config.postgres.take().unwrap_or_default();
            if let Some(url) = pg_override.url {
                pg_cfg.url = url;
            }
            if let Some(max) = pg_override.max_connections {
                pg_cfg.max_connections = max;
            }
            if let Some(timeout) = pg_override.connect_timeout_ms {
                pg_cfg.connect_timeout_ms = timeout;
            }
            if let Some(timeout) = pg_override.acquire_timeout_ms {
                pg_cfg.acquire_timeout_ms = timeout;
            }
            config.postgres = Some(pg_cfg);
            if matches!(config.storage, StorageBackend::Memory) {
                config.storage = StorageBackend::Postgres;
            }
        }
        if let Some(tls) = override_cfg.tls {
            config.api_tls = Some(tls);
        }
        if let Some(bootstrap_override) = override_cfg.bootstrap {
            if let Some(enabled) = bootstrap_override.enabled {
                config.bootstrap.enabled = enabled;
            }
            if let Some(value) = bootstrap_override.bind_addr {
                config.bootstrap.bind_addr = value.parse().with_context(|| "parse bind_addr")?;
            }
            if let Some(token) = bootstrap_override.token {
                config.bootstrap.token = Some(token);
            }
            if let Some(token) = bootstrap_override.previous_token {
                config.bootstrap.previous_token = Some(token);
            }
            if let Some(tls) = bootstrap_override.tls {
                config.bootstrap.tls = Some(tls);
            }
        }
        Ok(())
    }
}

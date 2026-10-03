//! Reading the configuration from `FELIX_*` environment variables.
use std::str::FromStr;

use anyhow::{Context, Result};

use super::api_tls::api_tls_from_env;
use super::bootstrap::bootstrap_tls_from_env;
use super::storage::raft_from_env;
use super::{
    BootstrapConfig, ControlPlaneConfig, DEFAULT_BOOTSTRAP_BIND_ADDR,
    DEFAULT_CHANGE_RETENTION_MAX_ROWS, DEFAULT_CHANGES_LIMIT,
    DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS, DEFAULT_NODE_EXPIRY_TIMEOUT_MS,
    DEFAULT_NODE_HEARTBEAT_INTERVAL_MS, DEFAULT_PG_ACQUIRE_TIMEOUT_MS,
    DEFAULT_PG_CONNECT_TIMEOUT_MS, DEFAULT_PG_MAX_CONNECTIONS, DEFAULT_READINESS_CACHE_TTL_MS,
    DEFAULT_READINESS_TIMEOUT_MS, DEFAULT_SHARD_RECONCILE_INTERVAL_MS,
    DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS, DEFAULT_SHUTDOWN_PREDRAIN_MS, NodeLivenessConfig,
    PostgresConfig, StorageBackend, parse_oidc_allowed_algorithms_csv, parse_positive_env,
};

impl ControlPlaneConfig {
    /// The configuration the environment describes, validated.
    pub fn from_env() -> Result<Self> {
        let metrics_bind = std::env::var("FELIX_CONTROLPLANE_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_CONTROLPLANE_METRICS_BIND")?;
        let bind_addr = std::env::var("FELIX_CONTROLPLANE_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
            .parse()
            .with_context(|| "parse FELIX_CONTROLPLANE_BIND")?;
        let region_id = std::env::var("FELIX_REGION_ID").unwrap_or_else(|_| "local".to_string());

        let mut storage = std::env::var("FELIX_CONTROLPLANE_STORAGE_BACKEND")
            .ok()
            .and_then(|v| StorageBackend::from_str(&v).ok())
            .unwrap_or(StorageBackend::Memory);

        let changes_limit = std::env::var("FELIX_CONTROLPLANE_CHANGES_LIMIT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_CHANGES_LIMIT);
        let node_liveness = NodeLivenessConfig {
            heartbeat_interval_ms: parse_positive_env("FELIX_NODE_HEARTBEAT_INTERVAL_MS")
                .unwrap_or(DEFAULT_NODE_HEARTBEAT_INTERVAL_MS),
            expiry_timeout_ms: parse_positive_env("FELIX_NODE_EXPIRY_TIMEOUT_MS")
                .unwrap_or(DEFAULT_NODE_EXPIRY_TIMEOUT_MS),
            // Zero is refused by validation, not treated as unset.
            regrant_margin_ms: std::env::var("FELIX_NODE_REGRANT_MARGIN_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok()),
            sweep_interval_ms: parse_positive_env("FELIX_NODE_EXPIRY_SWEEP_INTERVAL_MS")
                .unwrap_or(DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS),
            shard_reconcile_interval_ms: parse_positive_env("FELIX_SHARD_RECONCILE_INTERVAL_MS")
                .unwrap_or(DEFAULT_SHARD_RECONCILE_INTERVAL_MS),
        };
        let mut shard_moves = shard_moves_from_env();
        shard_moves.regions = std::sync::Arc::new(region_bridges_from_env()?);
        let readiness_timeout_ms = parse_positive_env("FELIX_READINESS_TIMEOUT_MS")
            .unwrap_or(DEFAULT_READINESS_TIMEOUT_MS);
        let readiness_cache_ttl_ms = parse_positive_env("FELIX_READINESS_CACHE_TTL_MS")
            .unwrap_or(DEFAULT_READINESS_CACHE_TTL_MS);
        let shutdown_drain_timeout_ms = std::env::var("FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS);
        // Unlike the drain timeout, zero is a meaningful setting here rather
        // than an unset one, so it is not filtered out.
        let shutdown_predrain_ms = std::env::var("FELIX_SHUTDOWN_PREDRAIN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHUTDOWN_PREDRAIN_MS);
        let change_retention_max_rows =
            std::env::var("FELIX_CONTROLPLANE_CHANGE_RETENTION_MAX_ROWS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .or(Some(DEFAULT_CHANGE_RETENTION_MAX_ROWS));

        let pg_url = std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .ok();
        let mut postgres = None;
        if let Some(url) = pg_url {
            postgres = Some(PostgresConfig {
                url,
                max_connections: std::env::var("FELIX_CONTROLPLANE_POSTGRES_MAX_CONNECTIONS")
                    .ok()
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(DEFAULT_PG_MAX_CONNECTIONS),
                connect_timeout_ms: std::env::var("FELIX_CONTROLPLANE_POSTGRES_CONNECT_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_PG_CONNECT_TIMEOUT_MS),
                acquire_timeout_ms: std::env::var("FELIX_CONTROLPLANE_POSTGRES_ACQUIRE_TIMEOUT_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_PG_ACQUIRE_TIMEOUT_MS),
            });
            if matches!(storage, StorageBackend::Memory) {
                storage = StorageBackend::Postgres;
            }
        }

        let raft = raft_from_env()?;
        if raft.is_some() && matches!(storage, StorageBackend::Memory) {
            storage = StorageBackend::Raft;
        }

        let config = Self {
            bind_addr,
            api_tls: api_tls_from_env()?,
            metrics_bind,
            region_id,
            storage,
            postgres,
            raft,
            changes_limit,
            change_retention_max_rows,
            oidc_allowed_algorithms: std::env::var("FELIX_CONTROLPLANE_OIDC_ALLOWED_ALGORITHMS")
                .ok()
                .map(|value| parse_oidc_allowed_algorithms_csv(&value))
                .transpose()?
                .unwrap_or_else(|| crate::auth::oidc::DEFAULT_ALLOWED_ALGORITHMS.to_vec()),
            bootstrap: BootstrapConfig {
                enabled: std::env::var("FELIX_BOOTSTRAP_ENABLED")
                    .ok()
                    .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
                bind_addr: std::env::var("FELIX_BOOTSTRAP_BIND_ADDR")
                    .unwrap_or_else(|_| DEFAULT_BOOTSTRAP_BIND_ADDR.to_string())
                    .parse()
                    .with_context(|| "parse FELIX_BOOTSTRAP_BIND_ADDR")?,
                token: std::env::var("FELIX_BOOTSTRAP_TOKEN").ok(),
                previous_token: std::env::var("FELIX_BOOTSTRAP_TOKEN_PREVIOUS").ok(),
                tls: bootstrap_tls_from_env()?,
            },
            node_liveness,
            shard_moves,
            shutdown_drain_timeout_ms,
            shutdown_predrain_ms,
            readiness_timeout_ms,
            readiness_cache_ttl_ms,
        };
        config.validate()?;
        Ok(config)
    }
}

/// The region allowlist placement holds a stream with a home region to.
/// Unset is no bridges: such a stream stays in its own region. The router's
/// local region goes unused here; placement asks about a stream's region, not
/// the control plane's.
fn region_bridges_from_env() -> Result<felix_router::RegionRouter<String>> {
    let bridges = match std::env::var("FELIX_REGION_BRIDGES") {
        Ok(spec) => felix_router::parse_bridges(&spec).context("parse FELIX_REGION_BRIDGES")?,
        Err(_) => Vec::new(),
    };
    Ok(felix_router::RegionRouter::with_bridges(
        String::new(),
        bridges,
    ))
}

/// How shard moves are paced, from the environment.
fn shard_moves_from_env() -> crate::cluster::placement::MovePolicy {
    let parse = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
    };
    let defaults = crate::cluster::placement::MovePolicy::default();
    crate::cluster::placement::MovePolicy {
        // Zero is meaningful: it holds every move.
        max_concurrent: parse("FELIX_SHARD_MOVES_MAX_CONCURRENT")
            .map_or(defaults.max_concurrent, |value| value as usize),
        // Unset or zero is no per-node limit; a limit of zero would only
        // repeat what the cluster-wide one already says.
        max_per_node: parse("FELIX_SHARD_MOVES_MAX_PER_NODE")
            .filter(|value| *value > 0)
            .map(|value| value as usize),
        fence_max_lag_records: parse("FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS")
            .unwrap_or(defaults.fence_max_lag_records),
        // Zero never gives up.
        timeout_millis: match parse("FELIX_SHARD_MOVE_TIMEOUT_MS") {
            Some(0) => None,
            Some(millis) => Some(millis),
            None => defaults.timeout_millis,
        },
        // Zero never replaces a lost follower.
        restore_after_millis: match parse("FELIX_SHARD_RESTORE_AFTER_MS") {
            Some(0) => None,
            Some(millis) => Some(millis),
            None => defaults.restore_after_millis,
        },
        // Read from the store each pass, not configured.
        paused: false,
        regions: defaults.regions,
    }
}

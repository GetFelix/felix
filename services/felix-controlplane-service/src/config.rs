//! Control-plane configuration: the environment first, then an optional YAML
//! file named by `FELIX_CONTROLPLANE_CONFIG` folded over it, then validation.
//!
//! Defaults keep a dev setup simple while still bounding resource use.
mod api_tls;
mod bootstrap;
mod env;
mod file;
mod liveness;
mod storage;

pub use api_tls::ApiTlsConfig;
pub use bootstrap::{BootstrapConfig, BootstrapTlsConfig};
pub use liveness::NodeLivenessConfig;
pub use storage::{PostgresConfig, RaftBackendConfig, StorageBackend};
pub(crate) use storage::{insecure_peers_from_env, peer_security_from_env};

use std::fs;
use std::net::SocketAddr;

use anyhow::{Context, Result, anyhow};
use jsonwebtoken::Algorithm;

use file::ControlPlaneConfigOverride;

pub const DEFAULT_CHANGES_LIMIT: u64 = 1000;
/// Total budget for draining in-flight HTTP requests after a termination signal.
/// Kubernetes defaults `terminationGracePeriodSeconds` to 30 and sends SIGKILL once
/// it expires, so this leaves headroom to finish the drain and exit before then.
pub const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 25_000;

/// How long the instance keeps serving after it starts reporting unready.
///
/// Readiness-first shutdown only helps if something has time to act on it. A
/// load balancer learns this instance is draining by polling, so closing the
/// listener the moment readiness flips means requests are still being routed
/// here when the socket goes away. This is that gap, and it should exceed the
/// prober's interval times its failure threshold.
pub const DEFAULT_SHUTDOWN_PREDRAIN_MS: u64 = 5_000;

/// Two seconds, matching `readiness::DEFAULT_CHECK_TIMEOUT`.
pub const DEFAULT_READINESS_TIMEOUT_MS: u64 = 2_000;
/// One second, matching `readiness::DEFAULT_CACHE_TTL`.
pub const DEFAULT_READINESS_CACHE_TTL_MS: u64 = 1_000;
pub const DEFAULT_CHANGE_RETENTION_MAX_ROWS: i64 = 10_000;
/// How often a healthy broker is expected to report health.
pub const DEFAULT_NODE_HEARTBEAT_INTERVAL_MS: u64 = 5_000;
/// How long a node may go unheard before the cluster calls it down.
///
/// Three intervals: one lost heartbeat is a hiccup, three is a pattern.
pub const DEFAULT_NODE_EXPIRY_TIMEOUT_MS: u64 = 15_000;
/// How often shards are placed onto live brokers.
///
/// Placement is idempotent, so a pass over a settled cluster writes nothing;
/// this only bounds how long a new stream waits for an owner, or a failed
/// broker's shards wait to move.
pub const DEFAULT_SHARD_RECONCILE_INTERVAL_MS: u64 = 5_000;
/// How often the expiry sweep runs. Finer than the timeout so a node is marked
/// down close to when it actually expires rather than a whole timeout later.
pub const DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS: u64 = 2_000;
const DEFAULT_PG_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_PG_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PG_ACQUIRE_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_BOOTSTRAP_BIND_ADDR: &str = "127.0.0.1:9095";

/// Everything the control plane is configured with.
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub bind_addr: SocketAddr,
    /// When set, the API listener serves TLS with this certificate.
    pub api_tls: Option<ApiTlsConfig>,
    pub metrics_bind: SocketAddr,
    pub region_id: String,
    pub storage: StorageBackend,
    pub postgres: Option<PostgresConfig>,
    pub raft: Option<RaftBackendConfig>,
    pub changes_limit: u64,
    pub change_retention_max_rows: Option<i64>,
    pub oidc_allowed_algorithms: Vec<Algorithm>,
    pub bootstrap: BootstrapConfig,
    pub node_liveness: NodeLivenessConfig,
    /// How shard moves are paced. See `placement::MovePolicy`.
    pub shard_moves: crate::cluster::placement::MovePolicy,
    /// Total budget for draining in-flight requests after SIGTERM/SIGINT before
    /// remaining tasks are force-cancelled.
    pub shutdown_drain_timeout_ms: u64,
    /// How long to keep serving after readiness flips to draining, giving load
    /// balancers time to remove this instance before the listener closes.
    ///
    /// Zero skips the wait, which is right for a single instance nothing routes
    /// to and wrong behind a load balancer. A second termination signal cuts it
    /// short.
    pub shutdown_predrain_ms: u64,
    /// Longest a readiness check may take before it is treated as a failure.
    ///
    /// Set below the prober's own timeout, so the answer is this service's
    /// rather than the network giving up first.
    pub readiness_timeout_ms: u64,
    /// How long a readiness answer is reused before the store is asked again.
    ///
    /// Bounds probe cost at one query per window however many probers there
    /// are, and bounds how long recovery takes to become visible.
    pub readiness_cache_ttl_ms: u64,
}

impl ControlPlaneConfig {
    /// Read the environment, fold the YAML file over it when one is named,
    /// and refuse a configuration that cannot work.
    pub fn from_env_or_yaml() -> Result<Self> {
        let mut config = Self::from_env()?;
        if let Ok(path) = std::env::var("FELIX_CONTROLPLANE_CONFIG") {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("read FELIX_CONTROLPLANE_CONFIG: {path}"))?;
            let override_cfg: ControlPlaneConfigOverride = serde_yaml_ng::from_str(&contents)
                .with_context(|| "parse control plane config yaml")?;
            config.apply(override_cfg)?;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if matches!(self.storage, StorageBackend::Postgres) && self.postgres.is_none() {
            return Err(anyhow!(
                "postgres backend requested but FELIX_CONTROLPLANE_POSTGRES_URL / postgres.url is not set"
            ));
        }
        if matches!(self.storage, StorageBackend::Raft) {
            let raft = self.raft.as_ref().ok_or_else(|| {
                anyhow!(
                    "raft backend requested but FELIX_RAFT_NODE_ID / FELIX_RAFT_DATA_DIR / FELIX_RAFT_PEERS are not set"
                )
            })?;
            if !raft.peers.contains_key(&raft.node_id) {
                return Err(anyhow!(
                    "FELIX_RAFT_PEERS must include this instance's own FELIX_RAFT_NODE_ID ({})",
                    raft.node_id
                ));
            }
            raft.validate()?;
        }
        if self.bootstrap.enabled && self.bootstrap.token.is_none() {
            return Err(anyhow!(
                "bootstrap enabled but FELIX_BOOTSTRAP_TOKEN / bootstrap.token is not set"
            ));
        }
        // A previous token with no current one means the rotation removed the
        // wrong half; refusing beats quietly running on the token being retired.
        if self.bootstrap.previous_token.is_some() && self.bootstrap.token.is_none() {
            return Err(anyhow!(
                "bootstrap.previous_token is set without bootstrap.token; \
                 the rotation should replace token and demote the old one"
            ));
        }
        if self.oidc_allowed_algorithms.is_empty() {
            return Err(anyhow!(
                "oidc_allowed_algorithms cannot be empty; include at least ES256"
            ));
        }
        self.node_liveness.validate()?;
        Ok(())
    }
}

/// Read a positive integer from the environment, ignoring absent, unparsable,
/// and zero values so a typo falls back to the default rather than disabling a
/// timer.
fn parse_positive_env(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn parse_oidc_allowed_algorithms_csv(value: &str) -> Result<Vec<Algorithm>> {
    parse_oidc_allowed_algorithms(
        value
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(ToString::to_string)
            .collect(),
    )
}

fn parse_oidc_allowed_algorithms(values: Vec<String>) -> Result<Vec<Algorithm>> {
    let mut algorithms = Vec::new();
    for raw in values {
        let alg = parse_oidc_algorithm(&raw).ok_or_else(|| {
            anyhow!(
                "invalid OIDC algorithm '{}'; supported: ES256, RS256, RS384, RS512, PS256, PS384, PS512",
                raw
            )
        })?;
        if !algorithms.contains(&alg) {
            algorithms.push(alg);
        }
    }
    Ok(algorithms)
}

fn parse_oidc_algorithm(value: &str) -> Option<Algorithm> {
    match value.trim().to_ascii_uppercase().as_str() {
        "ES256" => Some(Algorithm::ES256),
        "RS256" => Some(Algorithm::RS256),
        "RS384" => Some(Algorithm::RS384),
        "RS512" => Some(Algorithm::RS512),
        "PS256" => Some(Algorithm::PS256),
        "PS384" => Some(Algorithm::PS384),
        "PS512" => Some(Algorithm::PS512),
        _ => None,
    }
}

#[cfg(test)]
mod tests;

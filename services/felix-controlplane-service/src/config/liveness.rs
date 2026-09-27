//! How quickly a silent broker is declared down, and how often the
//! timer-driven loops run.
use anyhow::{Result, anyhow};

use super::{
    DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS, DEFAULT_NODE_EXPIRY_TIMEOUT_MS,
    DEFAULT_NODE_HEARTBEAT_INTERVAL_MS, DEFAULT_SHARD_RECONCILE_INTERVAL_MS,
};

/// Timings for broker liveness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLivenessConfig {
    /// Advertised to brokers in every heartbeat response.
    pub heartbeat_interval_ms: u64,
    /// Silence beyond this marks a node down.
    pub expiry_timeout_ms: u64,
    /// How long past `expiry_timeout_ms` a silent broker is still left live,
    /// so its shards are not handed on while it may still believe it leads.
    /// `None` is a quarter of the timeout; see [`Self::regrant_margin_ms`].
    pub regrant_margin_ms: Option<u64>,
    /// How often the sweep looks for expired nodes.
    pub sweep_interval_ms: u64,
    /// How often unplaced shards are assigned to live brokers.
    pub shard_reconcile_interval_ms: u64,
}

impl Default for NodeLivenessConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: DEFAULT_NODE_HEARTBEAT_INTERVAL_MS,
            expiry_timeout_ms: DEFAULT_NODE_EXPIRY_TIMEOUT_MS,
            regrant_margin_ms: None,
            sweep_interval_ms: DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS,
            shard_reconcile_interval_ms: DEFAULT_SHARD_RECONCILE_INTERVAL_MS,
        }
    }
}

impl NodeLivenessConfig {
    /// The regrant margin in effect.
    ///
    /// A broker stops serving a quarter of the timeout before its own lease
    /// ends, and the control plane waits this long past the timeout before a
    /// node is down and its shards move. Between them they have to outlast how
    /// far the two ends' clocks can disagree over one lease; the TLA+ model
    /// (`docs/formal/FelixShardRealMargins.cfg`) checks a quarter on each side
    /// against a drift of a quarter, and finds two leaders with none here.
    pub fn regrant_margin_ms(&self) -> u64 {
        self.regrant_margin_ms
            .unwrap_or(self.expiry_timeout_ms / MIN_MARGIN_FRACTION)
    }

    /// How long a broker has to be silent before it is marked down.
    pub fn silence_before_down_ms(&self) -> u64 {
        self.expiry_timeout_ms
            .saturating_add(self.regrant_margin_ms())
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.heartbeat_interval_ms == 0 {
            return Err(anyhow!(
                "node heartbeat_interval_ms must be greater than zero"
            ));
        }
        if self.sweep_interval_ms == 0 {
            return Err(anyhow!("node sweep_interval_ms must be greater than zero"));
        }
        if self.shard_reconcile_interval_ms == 0 {
            return Err(anyhow!(
                "shard_reconcile_interval_ms must be greater than zero"
            ));
        }
        // A timeout at or below the interval expires brokers that are heartbeating
        // exactly as told to, which takes down a healthy cluster.
        if self.expiry_timeout_ms <= self.heartbeat_interval_ms {
            return Err(anyhow!(
                "node expiry_timeout_ms ({}) must exceed heartbeat_interval_ms ({})",
                self.expiry_timeout_ms,
                self.heartbeat_interval_ms
            ));
        }
        // Below this the model finds two brokers serving one shard.
        if self.regrant_margin_ms() < self.expiry_timeout_ms / MIN_MARGIN_FRACTION {
            return Err(anyhow!(
                "node regrant_margin_ms ({}) must be at least a quarter of \
                 expiry_timeout_ms ({})",
                self.regrant_margin_ms(),
                self.expiry_timeout_ms
            ));
        }
        Ok(())
    }
}

/// The smallest regrant margin, as a fraction of the expiry timeout.
const MIN_MARGIN_FRACTION: u64 = 4;

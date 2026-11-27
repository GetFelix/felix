//! Which shard engine this control plane's group is formed for.
use std::str::FromStr;

use anyhow::{Result, anyhow};

/// The engine a cluster's shards replicate with, recorded once when the
/// metadata group forms. Raft is the only one: the hybrid is gone, and
/// `FELIX_SHARD_ENGINE` is retired (`docs/hybrid-retirement.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShardEngine {
    #[default]
    Raft,
}

impl ShardEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raft => "raft",
        }
    }
}

/// The name a metadata group formed for the hybrid recorded.
const HYBRID: &str = "hybrid";

impl FromStr for ShardEngine {
    type Err = anyhow::Error;

    /// `raft` is accepted, so an old manifest still starts. `hybrid` is
    /// refused, so nobody believes they still run it.
    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "raft" => Ok(Self::Raft),
            HYBRID => Err(anyhow!(
                "FELIX_SHARD_ENGINE=hybrid is refused: the hybrid shard engine was removed and \
                 shards run only on Raft; leave FELIX_SHARD_ENGINE unset"
            )),
            other => Err(anyhow!(
                "invalid FELIX_SHARD_ENGINE '{other}'; it is retired, and only 'raft' is accepted"
            )),
        }
    }
}

/// Report a metadata group that recorded the hybrid at formation. No broker
/// of this build joins it, since each compares its engine with the record.
pub(crate) fn warn_if_hybrid(engine: &str) {
    if engine == HYBRID {
        tracing::error!(
            "this metadata group was formed for the hybrid shard engine, which was removed; no \
             broker of this build will join it. Start every member again on an empty data \
             directory"
        );
    }
}

/// `FELIX_SHARD_ENGINE`, or Raft when unset.
pub(super) fn shard_engine_from_env() -> Result<ShardEngine> {
    match std::env::var("FELIX_SHARD_ENGINE") {
        Ok(value) => value.parse(),
        Err(_) => Ok(ShardEngine::default()),
    }
}

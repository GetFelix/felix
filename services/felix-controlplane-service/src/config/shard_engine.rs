//! Which shard engine this control plane's group is formed for.
use std::str::FromStr;

use anyhow::{Result, anyhow};

/// The engine a cluster's shards replicate with. Recorded once, when the
/// metadata group forms; after that the record wins over this setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShardEngine {
    #[default]
    Hybrid,
    Raft,
}

impl ShardEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hybrid => "hybrid",
            Self::Raft => "raft",
        }
    }
}

impl FromStr for ShardEngine {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "hybrid" => Ok(Self::Hybrid),
            "raft" => Ok(Self::Raft),
            other => Err(anyhow!(
                "invalid FELIX_SHARD_ENGINE '{other}'; use 'hybrid' or 'raft'"
            )),
        }
    }
}

/// `FELIX_SHARD_ENGINE`, or the default when unset.
pub(super) fn shard_engine_from_env() -> Result<ShardEngine> {
    match std::env::var("FELIX_SHARD_ENGINE") {
        Ok(value) => value.parse(),
        Err(_) => Ok(ShardEngine::default()),
    }
}

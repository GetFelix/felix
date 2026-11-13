//! Recording the group's identity once it has a leader.
//!
//! Any member may propose it: the first command applied wins and the rest
//! are answered with it, so two proposers at once are harmless. Only the
//! leader tries, because that is the cheap way to keep a three-member group
//! from proposing three times.
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{MetaCommand, MetaResponse, RaftStore};
use crate::store::identity::{ClusterIdentity, GroupIdentity};

const RETRY_FIRST: Duration = Duration::from_millis(250);
const RETRY_MAX: Duration = Duration::from_secs(5);

impl RaftStore {
    /// Propose a fresh identity for `shard_engine` until the group has one,
    /// then stop. Proposals are refused until every member reports the level
    /// that knows the command, and before a leader exists, so this retries
    /// with a backoff. Stops when `shutdown` is cancelled.
    pub fn spawn_identity_recorder(
        self: &Arc<Self>,
        shard_engine: String,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let mut delay = RETRY_FIRST;
            loop {
                if let Some(identity) = store.local().recorded_identity().await {
                    warn_on_engine_mismatch(&identity, &shard_engine);
                    return;
                }
                if store.handle.confirm_leadership().await {
                    let fresh = ClusterIdentity::generate(&shard_engine);
                    let proposed = MetaCommand::RecordClusterIdentity {
                        group_id: fresh.group_id,
                        shard_engine: fresh.shard_engine,
                    };
                    match store.propose(proposed).await {
                        Ok(MetaResponse::ClusterIdentity { identity }) => {
                            tracing::info!(group_id = %identity.group_id, shard_engine = %identity.shard_engine, "cluster identity recorded");
                            warn_on_engine_mismatch(&identity, &shard_engine);
                            return;
                        }
                        Ok(_) => tracing::warn!("cluster identity: unexpected answer"),
                        Err(err) => {
                            tracing::debug!(error = %err, "cluster identity not recorded yet");
                        }
                    }
                }
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(RETRY_MAX);
            }
        })
    }

    pub(super) async fn identity(&self) -> GroupIdentity {
        match self.local().recorded_identity().await {
            Some(identity) => GroupIdentity::Recorded(identity),
            None => GroupIdentity::Pending,
        }
    }
}

/// The record wins: a mismatch is reported and nothing else changes.
fn warn_on_engine_mismatch(recorded: &ClusterIdentity, configured: &str) {
    if recorded.shard_engine != configured {
        tracing::warn!(
            recorded = %recorded.shard_engine,
            configured,
            "FELIX_SHARD_ENGINE differs from the engine this metadata group recorded at \
             formation; the recorded engine wins"
        );
    }
}

//! Store liveness beside the hybrid, in shadow mode (`docs/per-shard-raft.md`,
//! "Store liveness").
//!
//! Compiled only with the `raft` feature and started only when the peer
//! config asks for it. Without the feature this type is empty and every
//! method does nothing, so the callers need no feature checks of their own.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use felix_replication::peer::PeerRequester;
use felix_router::ShardRouter;
use felix_wire::internal::InternalMessage;
use tokio_util::sync::CancellationToken;

use crate::config::BrokerConfig;

#[cfg(feature = "raft")]
use felix_replication::liveness::{Liveness, LivenessConfig, ProbeDisk, VolumeProbe};

/// This broker's store liveness, or nothing.
#[derive(Clone, Default)]
pub(crate) struct StoreLiveness {
    #[cfg(feature = "raft")]
    running: Option<(Arc<Liveness>, Arc<dyn ProbeDisk>)>,
}

impl StoreLiveness {
    /// Raise the store epoch and open the probe file under `root`, when the
    /// peer config turns store liveness on.
    pub(super) async fn open(config: &BrokerConfig, root: Option<&Path>) -> Result<Self> {
        #[cfg(feature = "raft")]
        return Self::open_with(config, root, |probe| Arc::new(probe)).await;
        #[cfg(not(feature = "raft"))]
        {
            let _ = (config, root);
            Ok(Self::default())
        }
    }

    /// [`Self::open`], with `slow` wrapped around the probe: for a simulator
    /// that stalls the disk.
    #[cfg(feature = "raft")]
    pub(super) async fn open_with(
        config: &BrokerConfig,
        root: Option<&Path>,
        slow: impl FnOnce(VolumeProbe) -> Arc<dyn ProbeDisk>,
    ) -> Result<Self> {
        use anyhow::Context;

        let on = config
            .peer_transport
            .as_ref()
            .is_some_and(|peer| peer.store_liveness);
        let (true, Some(root)) = (on, root) else {
            return Ok(Self::default());
        };
        let root = root.to_path_buf();
        let (epoch, probe) = felix_common::blocking::spawn(move || {
            let epoch = felix_storage::store_liveness::next_epoch(&root)
                .context("raise the store epoch")?;
            let probe = VolumeProbe::open(&root).context("open the store liveness probe")?;
            anyhow::Ok((epoch, probe))
        })
        .await
        .context("open store liveness")??;
        Ok(Self {
            running: Some((Liveness::new(epoch, LivenessConfig::default()), slow(probe))),
        })
    }

    /// Start probing the disk and exchanging heartbeats over `peers`.
    pub(super) fn start<R: PeerRequester + Send + Sync + 'static>(
        &self,
        peers: &Option<Arc<R>>,
        router: Option<&Arc<ShardRouter>>,
        shutdown: &CancellationToken,
    ) {
        #[cfg(feature = "raft")]
        if let (Some((liveness, disk)), Some(peers), Some(router)) = (&self.running, peers, router)
        {
            liveness.start(
                Arc::clone(peers),
                Arc::clone(router),
                Arc::clone(disk),
                shutdown.clone(),
            );
        }
        #[cfg(not(feature = "raft"))]
        let _ = (peers, router, shutdown);
    }

    /// Take a peer's heartbeat. `None` when this broker does not run store
    /// liveness or the peer did not say who it is.
    pub(crate) fn heard(
        &self,
        peer: Option<&str>,
        beat: felix_wire::internal::StoreLiveness,
    ) -> Option<InternalMessage> {
        #[cfg(feature = "raft")]
        if let (Some((liveness, _)), Some(peer)) = (&self.running, peer) {
            liveness.heard(peer, beat.epoch);
            return Some(InternalMessage::Pong(felix_wire::internal::Pong {
                correlation_id: beat.correlation_id,
            }));
        }
        let _ = (peer, beat);
        None
    }

    /// The running instance, for a simulator's checks.
    #[cfg(all(feature = "raft", feature = "sim"))]
    pub(super) fn running(&self) -> Option<Arc<Liveness>> {
        self.running
            .as_ref()
            .map(|(liveness, _)| Arc::clone(liveness))
    }
}

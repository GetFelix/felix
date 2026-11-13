//! A member from the moment its store is open, with or without a Raft node.
//!
//! The peer listener serves from this, so a member answers `standing`
//! before its node exists. Every other route answers 503 until the node is
//! attached, which every sender reads as an unreachable member.
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, Result};

use super::join::Standing;
use super::version::Versions;
use super::{AppStateMachine, NodeId, PeerSecurity, RaftHandle, RaftSettings, http, store};

/// This instance's member of the metadata Raft group, as its peers reach it.
///
/// Cheap to clone; all clones are the same member.
#[derive(Clone)]
pub struct RaftMember {
    /// The configured id, which is also the node's.
    pub(super) id: NodeId,
    pub(super) security: Arc<PeerSecurity>,
    pub(super) app: Arc<dyn AppStateMachine>,
    pub(super) state: store::MemberState,
    /// Shared with the node, so what a peer said before the node existed
    /// is not forgotten.
    pub(super) versions: Arc<Mutex<Versions>>,
    /// What the node is built from, until [`RaftMember::start_node`] takes
    /// it.
    pub(super) opened: Arc<Mutex<Option<(RaftSettings, store::Opened)>>>,
    pub(super) node: Arc<OnceLock<RaftHandle>>,
}

impl RaftMember {
    /// Open (or create) this instance's Raft state under
    /// `settings.data_dir`. No node runs yet: the member votes for nothing
    /// and takes no entries.
    pub async fn open(settings: RaftSettings, app: Arc<dyn AppStateMachine>) -> Result<Self> {
        // The store is opened, and on the segment store marked as started,
        // before any node exists: a node campaigns on its own timer.
        let opened = store::open_member(&settings, Arc::clone(&app)).await?;
        Ok(Self {
            id: settings.node_id,
            security: Arc::new(settings.security.clone()),
            app,
            state: opened.member.clone(),
            versions: Arc::default(),
            opened: Arc::new(Mutex::new(Some((settings, opened)))),
            node: Arc::default(),
        })
    }

    /// Start this member's Raft node. Its routes stay closed until
    /// [`RaftMember::attach`], so the caller can settle whether the node
    /// may vote first ([`RaftHandle::enter_group`]).
    ///
    /// A failed start closes the store.
    pub async fn start_node(&self) -> Result<RaftHandle> {
        let taken = self.opened.lock().expect("opened lock").take();
        let (settings, opened) = taken.context("this member's raft node was already started")?;
        let versions = Arc::clone(&self.versions);
        match RaftHandle::start_on(settings, Arc::clone(&self.app), opened, versions).await {
            Ok(handle) => Ok(handle),
            Err(err) => {
                // Closed here, or the store stays locked and a retry in this
                // process is refused as busy.
                if let Err(close) = self.state.close().await {
                    tracing::warn!(error = %close, "close the store after a failed start");
                }
                Err(err)
            }
        }
    }

    /// Serve `node` on this member's routes from here on.
    pub fn attach(&self, node: &RaftHandle) {
        let _ = self.node.set(node.clone());
    }

    pub(super) fn node(&self) -> Option<&RaftHandle> {
        self.node.get()
    }

    /// Close the store of a member whose node never started. One with a
    /// node is stopped through [`RaftHandle::shutdown`].
    pub async fn close(&self) -> Result<()> {
        drop(self.opened.lock().expect("opened lock").take());
        self.state.close().await
    }

    /// Router serving this member's peer routes, peer authentication
    /// included. Serve it on the peer listener only:
    /// [`RaftMember::serve_peers`] does, with mTLS when configured.
    pub fn rpc_router(&self) -> axum::Router {
        http::router(self.clone())
    }

    /// Serve the peer routes on `listener` until `shutdown` fires, over mTLS
    /// when the peer security says so.
    pub async fn serve_peers(
        &self,
        listener: tokio::net::TcpListener,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        let router = self.rpc_router();
        match self.security.server_tls()? {
            Some(tls) => {
                crate::server::tls::serve_tls("raft", listener, router, tls, shutdown).await;
            }
            None => {
                axum::serve(listener, router.into_make_service())
                    .with_graceful_shutdown(async move { shutdown.cancelled().await })
                    .await
                    .context("serve raft peer listener")?;
            }
        }
        Ok(())
    }

    /// What this member reports to one deciding how to enter the group, or
    /// asking what it can apply.
    ///
    /// Fails when the store cannot be read: an answer without the record
    /// would read as "nobody set out to form". Fails too for a member that
    /// holds Raft state and has no node yet. Its log is not in the answer,
    /// and without it the member would read as an empty one.
    pub(super) async fn standing(&self) -> Result<Standing> {
        let formed = self.state.formed().await?;
        let node = self.node();
        let last_log_index = match node {
            // The node's own state, not its metrics: those are empty until
            // the node first publishes them, and a log would be missing
            // from the answer.
            Some(node) => node
                .raft
                .with_raft_state(|state| state.log_ids.last().map(|id| id.index))
                .await
                .map_err(|err| anyhow::anyhow!("raft node: {err}"))?,
            None => {
                anyhow::ensure!(
                    !self.state.holds_state()?,
                    "this member holds raft state and its node has not started"
                );
                None
            }
        };
        Ok(Standing {
            last_log_index,
            version: self.app.version(),
            formed: formed.into_iter().collect(),
            raft: node.is_some(),
            node_id: node.map(|node| node.id),
            member: Some(self.id),
        })
    }
}

#[cfg(test)]
mod tests;

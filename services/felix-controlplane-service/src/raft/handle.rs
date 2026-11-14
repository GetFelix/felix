//! One running member of the group: starting it, changing the membership,
//! and stopping it.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context, Result};
use openraft::async_runtime::WatchReceiver;

use super::types::Raft;
use super::{
    AppStateMachine, NodeId, PeerSecurity, RaftMember, RaftSettings, RaftStatus, network, store,
};

/// One running member of the metadata Raft group.
///
/// Cheap to clone; all clones drive the same underlying node.
#[derive(Clone)]
pub struct RaftHandle {
    pub(super) raft: Raft,
    pub(super) id: NodeId,
    #[cfg(test)]
    pub(super) faults: super::Faults,
    pub(super) write_timeout: Duration,
    /// For forwarding proposals to the leader; pooled per host underneath.
    pub(super) forward: reqwest::Client,
    pub(super) security: Arc<PeerSecurity>,
    /// Configured peer addresses, preferred over the ones in the membership.
    pub(super) peer_addrs: Arc<BTreeMap<NodeId, String>>,
    /// Kept past construction only for [`AppStateMachine::restamp`], which
    /// runs on the proposal path rather than the apply loop.
    pub(super) app: Arc<dyn AppStateMachine>,
    /// The store's own record of this member: whether it holds Raft state
    /// and whether its vote is withheld. It does not keep the store open
    /// past a stop, so a restart in the same process can reopen it while
    /// clones of the handle live on.
    pub(super) member: store::MemberState,
    /// Where the store lives, and the `form-without` waiver beside it.
    pub(super) data_dir: Arc<std::path::PathBuf>,
    /// The member ids [`RaftHandle::enter_group`] was given: the group this
    /// member would form, and the only one it records a peer as forming.
    pub(super) configured: Arc<std::sync::OnceLock<std::collections::BTreeSet<NodeId>>>,
    /// False while this member withholds its vote (`super::join`): vote
    /// requests are refused and it does not stand for election.
    pub(super) may_vote: Arc<AtomicBool>,
    /// Answers `ask_leader` on this member; set once by the application.
    pub(super) leader_service: Arc<std::sync::OnceLock<Arc<dyn super::LeaderService>>>,
    /// What each member last reported it can apply ([`super::version`]).
    pub(super) versions: Arc<std::sync::Mutex<super::version::Versions>>,
    /// Holds replication to members below the join floor ([`super::hold`]).
    pub(super) hold: Arc<super::hold::Hold>,
    /// Set by [`RaftHandle::shutdown`]. openraft's metrics keep the last
    /// leader after a stop, so leadership checks read this first.
    pub(super) stopped: Arc<AtomicBool>,
    /// The one snapshot install this member runs at a time, ranked by the
    /// sender's vote.
    pub(super) install_slot:
        Arc<felix_storage::raft_log::transfer::InstallSlot<super::types::Vote>>,
    /// The largest snapshot this member accepts.
    pub(super) snapshot_max_bytes: u64,
    /// Holds reads and readiness after a restart until the node has applied
    /// its whole log tail ([`RaftHandle::caught_up`]).
    pub(super) replay_gate: Arc<super::health::ReplayGate>,
}

/// How long a failed start waits for the node to stop. Short under test so
/// the stuck-stop case does not take five seconds.
const STOP_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(5)
};

/// Stop a node whose start failed. Bounded: the stop waits for an apply in
/// flight, and a stuck one must not turn a failed start into a hang.
///
/// A stop that times out leaves the old node running, and it may still
/// finish that apply. An in-process retry with the same `app` would then
/// replay beside it, so after a failed start the process should exit
/// instead of calling [`RaftHandle::start`] again with the same state
/// machine.
async fn stop_failed_start(raft: &Raft, #[cfg(test)] faults: super::Faults) {
    #[cfg(test)]
    if let Some(probe) = faults.stop_probe {
        probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    let stop = tokio::time::timeout(STOP_TIMEOUT, async {
        #[cfg(test)]
        if faults.stall_failed_start_stop {
            return std::future::pending().await;
        }
        raft.shutdown().await
    })
    .await;
    #[cfg(test)]
    if faults.stall_failed_start_stop {
        let _ = raft.shutdown().await;
    }
    if !matches!(stop, Ok(Ok(()))) {
        tracing::warn!("the raft node did not stop cleanly after a failed start");
    }
}

impl RaftHandle {
    /// Open (or create) this instance's Raft state under
    /// `settings.data_dir` and start the node.
    ///
    /// Starting is not joining: a fresh node idles until either
    /// [`RaftHandle::initialize`] forms a new group, an existing leader
    /// adds it as a learner, or [`RaftHandle::enter_group`] decides which.
    /// A node with prior state on disk resumes its membership without
    /// ceremony — that is the point of the data dir — unless it was still
    /// catching up after starting empty, in which case it keeps withholding
    /// its vote.
    ///
    /// [`RaftMember::open`] and [`RaftMember::start_node`] are the two
    /// halves, for a caller that serves the peer listener in between.
    pub async fn start(settings: RaftSettings, app: Arc<dyn AppStateMachine>) -> Result<Self> {
        RaftMember::open(settings, app).await?.start_node().await
    }

    pub(super) async fn start_on(
        settings: RaftSettings,
        app: Arc<dyn AppStateMachine>,
        opened: store::Opened,
        versions: Arc<std::sync::Mutex<super::version::Versions>>,
    ) -> Result<Self> {
        let store::Opened {
            log_store,
            state_machine,
            may_vote,
            committed_floor,
            log_tail,
            member,
        } = opened;
        let config = openraft::Config {
            cluster_name: "felix-metadata".to_string(),
            heartbeat_interval: settings.heartbeat_interval.as_millis() as u64,
            election_timeout_min: settings.election_timeout.0.as_millis() as u64,
            election_timeout_max: settings.election_timeout.1.as_millis() as u64,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                settings.snapshot_logs_since_last,
            ),
            max_in_snapshot_log_to_keep: settings.logs_kept_behind_snapshot,
            // Set at construction, not after: a member resuming a half-filled
            // log already knows a membership that lists it as a voter, and
            // its election timer starts with the node.
            enable_elect: may_vote,
            // A restarted leader campaigns at once, whatever `enable_elect`
            // says. A member withholding its vote must not: its log may be
            // empty, and the vote it stored is from before it was wiped.
            enable_leader_restore: Some(may_vote),
            // A member that returns with a wiped volume reports a log that
            // went backwards, which this design supports. Without this the
            // leader refuses to replicate to it.
            allow_log_reversion: Some(true),
            // Off until it is tested with members that withhold their vote.
            enable_pre_vote: Some(false),
            ..Default::default()
        };
        let config = Arc::new(config.validate().context("raft config")?);

        let hold = Arc::new(super::hold::Hold::new(
            Arc::clone(&app),
            Arc::clone(&versions),
        ));
        let raft = Raft::new(
            settings.node_id,
            config,
            network::HttpNetworkFactory::new(
                &settings.security,
                settings.peer_addrs.clone(),
                settings.heartbeat_interval,
                Arc::clone(&hold),
            )?,
            log_store,
            state_machine,
        )
        .await
        .context("start raft node")?;

        // Replays locally up to the committed index this node last saved.
        // That index is saved at most once a second, so the node may have
        // applied more before it stopped; the replay gate holds reads and
        // readiness until a leader has committed the rest of the log tail.
        let replayed = Self::replayed(&raft, committed_floor).await;
        #[cfg(test)]
        let replayed = match settings.faults.fail_after_node_start {
            true => Err(anyhow::anyhow!("injected: the start fails")),
            false => replayed,
        };
        if let Err(err) = replayed {
            stop_failed_start(
                &raft,
                #[cfg(test)]
                settings.faults,
            )
            .await;
            return Err(err);
        }

        let forward = settings.security.client(settings.write_timeout);
        #[cfg(test)]
        let forward = match settings.faults.fail_forward_client {
            true => Err(anyhow::anyhow!("injected: the client build fails")),
            false => forward,
        };
        let forward = match forward {
            Ok(forward) => forward,
            Err(err) => {
                stop_failed_start(
                    &raft,
                    #[cfg(test)]
                    settings.faults,
                )
                .await;
                return Err(err).context("build forwarding client");
            }
        };
        let handle = Self {
            raft,
            id: settings.node_id,
            #[cfg(test)]
            faults: settings.faults,
            write_timeout: settings.write_timeout,
            forward,
            security: Arc::new(settings.security),
            peer_addrs: Arc::new(settings.peer_addrs),
            app,
            member,
            data_dir: Arc::new(settings.data_dir),
            configured: Arc::default(),
            may_vote: Arc::new(AtomicBool::new(may_vote)),
            leader_service: Arc::new(std::sync::OnceLock::new()),
            versions,
            hold,
            stopped: Arc::default(),
            install_slot: Arc::default(),
            snapshot_max_bytes: settings.snapshot_max_bytes,
            replay_gate: Arc::new(super::health::ReplayGate::new(committed_floor, log_tail)),
        };
        tokio::spawn(handle.clone().watch_versions());
        Ok(handle)
    }

    /// Wait until the state machine is back to `committed_floor`, the
    /// committed index this node last saved.
    async fn replayed(raft: &Raft, committed_floor: Option<u64>) -> Result<()> {
        if let Some(floor) = committed_floor {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let applied = raft
                    .metrics()
                    .borrow_watched()
                    .last_applied
                    .map(|log_id| log_id.index)
                    .unwrap_or(0);
                if applied >= floor {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "state machine replay stalled: applied {applied} of committed {floor}"
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(())
    }

    pub fn status(&self) -> RaftStatus {
        let metrics = self.raft.metrics().borrow_watched().clone();
        RaftStatus {
            id: self.id,
            leader: metrics.current_leader.filter(|_| !self.is_stopped()),
            term: super::health::shown_term(metrics.current_term),
            last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
            voters: metrics.membership_config.membership().voter_ids().collect(),
        }
    }

    /// Form a brand-new group from `members` (`node_id -> reachable addr`,
    /// this node included). Exactly once per cluster, ever; a restart
    /// resumes from disk instead. Initializing two disjoint member sets is
    /// how split brain is manufactured, so this fails on a node that
    /// already has state.
    pub async fn initialize(&self, members: BTreeMap<NodeId, String>) -> Result<()> {
        let nodes: BTreeMap<NodeId, openraft::impls::BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, openraft::impls::BasicNode { addr }))
            .collect();
        self.raft
            .initialize(nodes)
            .await
            .context("initialize raft group")?;
        Ok(())
    }

    /// This node as the member its peers reach.
    pub(super) fn peer(&self) -> RaftMember {
        RaftMember {
            id: self.id,
            security: Arc::clone(&self.security),
            app: Arc::clone(&self.app),
            state: self.member.clone(),
            versions: Arc::clone(&self.versions),
            opened: Arc::default(),
            node: Arc::new(self.clone().into()),
        }
    }

    /// Router serving this node's Raft RPCs, peer authentication included.
    /// Serve it on the peer listener only: [`RaftHandle::serve_peers`] does,
    /// with mTLS when configured.
    pub fn rpc_router(&self) -> axum::Router {
        self.peer().rpc_router()
    }

    /// Serve the Raft RPCs on `listener` until `shutdown` fires, over mTLS
    /// when the peer security says so.
    pub async fn serve_peers(
        &self,
        listener: tokio::net::TcpListener,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        self.peer().serve_peers(listener, shutdown).await
    }

    /// The base URL of a member's peer listener: the configured address when
    /// there is one, otherwise `recorded` (the membership's).
    pub(super) fn peer_url(&self, id: Option<NodeId>, recorded: Option<&str>) -> Option<String> {
        let addr = id
            .and_then(|id| self.peer_addrs.get(&id).map(String::as_str))
            .or(recorded)?;
        Some(format!("{}://{addr}", self.security.scheme()))
    }

    /// Add a node as a non-voting learner and wait until it has caught up.
    /// Learner-first is what keeps a join from costing quorum: the group's
    /// vote arithmetic only changes at [`RaftHandle::change_membership`],
    /// after the newcomer already holds the data.
    ///
    /// Refused, before the membership changes, when the node runs a build
    /// below the level the group already depends on.
    pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<()> {
        self.admit(id, &addr).await?;
        self.raft
            .add_learner(id, openraft::impls::BasicNode { addr }, true)
            .await
            .context("add learner")?;
        Ok(())
    }

    /// A learner that is never waited for, for tests whose "member" is a
    /// stub answering only some routes.
    #[cfg(test)]
    pub(crate) async fn add_learner_without_waiting(&self, id: NodeId, addr: String) -> Result<()> {
        self.admit(id, &addr).await?;
        self.raft
            .add_learner(id, openraft::impls::BasicNode { addr }, false)
            .await
            .context("add learner")?;
        Ok(())
    }

    /// Replace the set of voting members. Nodes being added must already be
    /// learners; removed nodes are retained as learners rather than
    /// abandoned, so a scale-down mistake is reversible.
    pub async fn change_membership(&self, voters: impl IntoIterator<Item = NodeId>) -> Result<()> {
        let ids: std::collections::BTreeSet<NodeId> = voters.into_iter().collect();
        self.raft
            .change_membership(ids, true)
            .await
            .context("change membership")?;
        Ok(())
    }

    /// Linearizable leadership check: true only when this instance is the
    /// leader *and* its applied state is current — openraft's read-index,
    /// so a deposed leader that has not heard the news fails it.
    pub async fn confirm_leadership(&self) -> bool {
        self.raft
            .ensure_linearizable(openraft::raft::ReadPolicy::ReadIndex)
            .await
            .is_ok()
    }

    /// Ask for a snapshot now rather than at the log-size policy point.
    pub async fn trigger_snapshot(&self) -> Result<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .context("trigger snapshot")?;
        Ok(())
    }

    /// Stop participating. In-flight proposals fail; disk state remains, so
    /// the next [`RaftHandle::start`] with the same data dir resumes.
    ///
    /// From the call on, this member no longer reports leadership or answers
    /// leader-only requests.
    pub async fn shutdown(&self) -> Result<()> {
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // The store is closed even when the node's shutdown fails, so its
        // lock does not outlive the member.
        let stopped = self
            .raft
            .shutdown()
            .await
            .map_err(|err| anyhow::anyhow!("raft shutdown: {err}"));
        #[cfg(test)]
        let stopped = match self.faults.fail_node_stop {
            true => Err(anyhow::anyhow!("injected: the stop fails")),
            false => stopped,
        };
        let closed = self.member.close().await;
        stopped.and(closed)
    }
}

#[cfg(test)]
mod tests;

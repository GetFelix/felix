//! Requests only the leader answers, from state it keeps outside the log.
//!
//! Some state is only meaningful on the leader and is cheaper to hold there
//! than to replicate: broker liveness is the case that motivated it, where a
//! log entry per heartbeat is an fsync on a majority for a fact that is stale
//! seconds later. The application registers a [`LeaderService`]; a request
//! reaching a follower is forwarded to the leader over the peer listener, the
//! same way a proposal is. Requests stay opaque bytes here, like commands.
use openraft::async_runtime::WatchReceiver;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;

use super::{NodeId, RaftHandle};

/// Answers leader-only requests. Registered with
/// [`RaftHandle::set_leader_service`].
#[async_trait::async_trait]
pub trait LeaderService: Send + Sync + 'static {
    /// Handle `request` on the member that believes it leads. [`NotLeader`]
    /// when it turns out not to, so the caller asks the new leader.
    async fn handle(&self, request: &[u8]) -> Result<Vec<u8>, NotLeader>;
}

/// This member does not lead (any more).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotLeader;

/// Why [`RaftHandle::ask_leader`] got no answer.
#[derive(Debug, thiserror::Error)]
pub enum AskLeaderError {
    /// The leader runs a build without the leader route. Only seen mid-way
    /// through a rolling upgrade; the caller falls back to the log.
    #[error("the raft leader does not serve leader-only requests")]
    Unsupported,
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

/// Name of the route a follower forwards leader-only requests to.
pub(super) const LEADER_ROUTE: &str = "leader";

impl RaftHandle {
    /// Register the service answering [`RaftHandle::ask_leader`]. Once per
    /// member; a second registration is ignored.
    pub fn set_leader_service(&self, service: std::sync::Arc<dyn LeaderService>) {
        let _ = self.leader_service.set(service);
    }

    /// Have the leader answer `request`, wherever this member stands.
    ///
    /// Bounded by the write budget, like a proposal: through an election it
    /// retries, and past the budget "no leader" is an error.
    pub async fn ask_leader(&self, request: Vec<u8>) -> Result<Vec<u8>, AskLeaderError> {
        const RETRY_DELAY: Duration = Duration::from_millis(100);
        const ATTEMPT_CAP: Duration = Duration::from_secs(2);
        let deadline = tokio::time::Instant::now() + self.write_timeout;
        let mut last = anyhow::anyhow!("no raft leader is known");
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            // A stopped member's metrics still name the leader they last saw,
            // often itself. Refuse at once so the caller tries another member.
            if self.is_stopped() {
                last = anyhow::anyhow!("this raft member has stopped");
            }
            if remaining.is_zero() || self.is_stopped() {
                return Err(AskLeaderError::Failed(anyhow::Error::new(
                    super::NoQuorum::new(self.write_timeout, &last),
                )));
            }
            let (leader, recorded) = self.known_leader();
            match leader {
                Some(leader) if leader == self.id => {
                    let service = self
                        .leader_service
                        .get()
                        .context("no leader service is registered")?;
                    match service.handle(&request).await {
                        Ok(bytes) => return Ok(bytes),
                        Err(NotLeader) => last = anyhow::anyhow!("this member stopped leading"),
                    }
                }
                Some(leader) => {
                    let Some(base) = self.peer_url(Some(leader), recorded.as_deref()) else {
                        last = anyhow::anyhow!("no address for raft leader {leader}");
                        tokio::time::sleep(RETRY_DELAY.min(remaining)).await;
                        continue;
                    };
                    let sent = self
                        .forward
                        .post(format!("{base}{}", super::peer_route(LEADER_ROUTE)))
                        .timeout(remaining.min(ATTEMPT_CAP))
                        .body(request.clone())
                        .send()
                        .await;
                    match sent {
                        Ok(response) if response.status().is_success() => {
                            return Ok(response
                                .bytes()
                                .await
                                .context("read the leader's answer")?
                                .to_vec());
                        }
                        Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                            return Err(AskLeaderError::Unsupported);
                        }
                        Ok(response) => {
                            last = anyhow::anyhow!("raft leader answered {}", response.status());
                        }
                        Err(err) => last = anyhow::Error::new(err).context("reach the raft leader"),
                    }
                }
                None => {}
            }
            tokio::time::sleep(RETRY_DELAY.min(remaining)).await;
        }
    }

    /// Whether this member leads, confirmed by a quorum within `within`.
    ///
    /// For answers that promise something past this instant: a leader cut off
    /// from the group keeps believing it leads until it hears otherwise, and
    /// an answer it gave in that time would be one the real leader never saw.
    pub async fn confirm_leadership_within(&self, within: Duration) -> bool {
        tokio::time::timeout(within, self.confirm_leadership())
            .await
            .unwrap_or(false)
    }

    /// Whether this member leads in `term` and has applied the blank entry
    /// that opened it, so its applied state, the join floor included, holds
    /// everything earlier terms committed. Only the first call of a term
    /// pays for the read-index; see `docs/metadata-raft-design.md`
    /// ("Liveness is leader soft state").
    pub(crate) async fn await_own_blank(&self, term: u64, within: Duration) -> bool {
        if self.blank_applied.load(Ordering::Acquire) == term {
            return true;
        }
        // A read-index from a later term proves nothing about this one.
        if !self.confirm_leadership_within(within).await || self.current_term() != term {
            return false;
        }
        self.blank_applied.fetch_max(term, Ordering::AcqRel);
        true
    }

    /// The level proposals are settled at: the group's level with members
    /// that have not answered counted at the join floor. On the leader the
    /// floor is read behind a fresh read-index, so a write is not settled
    /// below any level the log committed, this term's included. These
    /// writes are rare, so each pays one. [`super::NoQuorum`], which callers
    /// retry, if it cannot confirm that `within`.
    pub(crate) async fn proposal_level(&self, within: Duration) -> Result<u16, super::NoQuorum> {
        if self.is_leader() {
            return Ok(self.leader_level(within).await?.1);
        }
        Ok(self
            .cluster_version_with_unknown_at(self.app.join_floor())
            .await)
    }

    /// [`Self::proposal_level`] as this member leads, with the term a fresh
    /// read-index confirmed it in, or [`super::NoQuorum`] if it cannot
    /// confirm that `within`.
    pub(crate) async fn leader_level(
        &self,
        within: Duration,
    ) -> Result<(u64, u16), super::NoQuorum> {
        let term = self.current_term();
        // A read-index from a later term proves nothing about this one.
        if !self.confirm_leadership_within(within).await || self.current_term() != term {
            return Err(super::NoQuorum::new(
                within,
                &anyhow::anyhow!("no quorum confirmed this term's leader"),
            ));
        }
        let level = self
            .cluster_version_with_unknown_at(self.app.join_floor())
            .await;
        Ok((term, level))
    }

    /// The level off the leader, when this member's own join floor does not
    /// decide it. `None` when a member that never answered is counted at
    /// that floor and it is below what the others answered: the floor may
    /// lag the log, so only the leader's confirmed level can settle it.
    pub(crate) async fn level_without_own_floor(&self) -> Option<u16> {
        self.level_unless_unknown_decides(self.app.join_floor())
            .await
    }

    /// The leader this member knows of, with the address its membership
    /// records for it.
    pub(crate) fn known_leader(&self) -> (Option<NodeId>, Option<String>) {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let leader = metrics.current_leader;
        let recorded = leader.and_then(|id| {
            metrics
                .membership_config
                .membership()
                .get_node(&id)
                .map(|node| node.addr.clone())
        });
        (leader, recorded)
    }

    /// The current term, as this member knows it.
    pub fn current_term(&self) -> u64 {
        self.raft.metrics().borrow_watched().current_term
    }

    /// Whether this member believes it leads. Local and unconfirmed; false
    /// once [`RaftHandle::shutdown`] has been called.
    pub fn is_leader(&self) -> bool {
        !self.is_stopped() && self.raft.metrics().borrow_watched().current_leader == Some(self.id)
    }

    /// Step down by granting `to` the next term's vote, as this leader's own
    /// transfer would authorize. A lone member then wins the term after.
    /// False if the vote was refused.
    #[cfg(test)]
    pub(crate) async fn give_up_term(&self, to: NodeId) -> bool {
        let metrics = self.raft.metrics().borrow_watched().clone();
        let last = metrics
            .last_log_index
            .map(|index| super::types::test_log_id(metrics.current_term, index));
        let next = super::types::Vote {
            leader_id: super::types::LeaderId {
                term: metrics.current_term + 1,
                voted_for: to,
            },
            committed: false,
        };
        let mut rpc = openraft::raft::VoteRequest::new(next, last);
        // Without it a leader whose quorum still acks refuses any vote.
        rpc.leadership_transfer_from = Some(metrics.vote);
        // Refused when the metrics lag a term this member just won.
        self.raft.vote(rpc).await.expect("vote").vote_granted
    }

    /// Campaign now, as an election timeout would.
    #[cfg(test)]
    pub(crate) async fn campaign(&self) {
        self.raft
            .trigger()
            .elect(false)
            .await
            .expect("trigger an election");
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The peer route's half: answer only while leading.
    pub(super) async fn serve_leader_request(&self, request: &[u8]) -> Option<Vec<u8>> {
        if !self.is_leader() {
            return None;
        }
        self.leader_service.get()?.handle(request).await.ok()
    }
}

//! Requests only the leader answers, from state it keeps outside the log.
//!
//! Some state is only meaningful on the leader and is cheaper to hold there
//! than to replicate: broker liveness is the case that motivated it, where a
//! log entry per heartbeat is an fsync on a majority for a fact that is stale
//! seconds later. The application registers a [`LeaderService`]; a request
//! reaching a follower is forwarded to the leader over the peer listener, the
//! same way a proposal is. Requests stay opaque bytes here, like commands.
use std::time::Duration;

use anyhow::Context;

use super::RaftHandle;

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

/// Path of the route a follower forwards leader-only requests to.
pub(super) const LEADER_ROUTE: &str = "/internal/raft/leader";

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
            if remaining.is_zero() {
                return Err(AskLeaderError::Failed(anyhow::Error::new(
                    super::NoQuorum::new(self.write_timeout, &last),
                )));
            }
            let (leader, recorded) = {
                let metrics = self.raft.metrics();
                let metrics = metrics.borrow();
                let leader = metrics.current_leader;
                let recorded = leader.and_then(|id| {
                    metrics
                        .membership_config
                        .membership()
                        .get_node(&id)
                        .map(|node| node.addr.clone())
                });
                (leader, recorded)
            };
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
                        .post(format!("{base}{LEADER_ROUTE}"))
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

    /// The current term, as this member knows it.
    pub fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// Whether this member believes it leads. Local and unconfirmed.
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.id)
    }

    /// The peer route's half: answer only while leading.
    pub(super) async fn serve_leader_request(&self, request: &[u8]) -> Option<Vec<u8>> {
        if !self.is_leader() {
            return None;
        }
        self.leader_service.get()?.handle(request).await.ok()
    }
}

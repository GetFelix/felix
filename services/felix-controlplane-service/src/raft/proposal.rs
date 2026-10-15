//! Getting a command committed from any member: bounded retries, forwarding
//! to the leader, and the leader's clock in place of the proposer's.
use std::time::Duration;

use anyhow::{Context, Result};

use super::RaftHandle;

/// A proposal ran out its write budget: no leader, or no quorum, answered in
/// time. The outcome is unknown rather than failed — it may still commit — so
/// callers report it as retryable, and a retry can meet its own earlier write.
#[derive(Debug, thiserror::Error)]
#[error("raft write: not committed within {budget:?} (no leader, or quorum lost): {last}")]
pub struct NoQuorum {
    budget: Duration,
    last: String,
}

impl NoQuorum {
    pub(super) fn new(budget: Duration, last: &anyhow::Error) -> Self {
        Self {
            budget,
            last: format!("{last:#}"),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(Duration::from_secs(1), &anyhow::anyhow!("no leader"))
    }
}

impl RaftHandle {
    /// Propose one command and wait until it is committed and applied;
    /// returns the state machine's response.
    ///
    /// Works from any member: a follower forwards the bytes to the leader it
    /// knows, and an instance caught mid-election retries briefly before
    /// giving up. The bound matters — a caller must get "no leader" as an
    /// error it can surface, not an indefinite hang inside the seam.
    pub async fn write(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        const RETRY_DELAY: Duration = Duration::from_millis(250);
        // Each attempt is capped well below the whole budget: a single hung
        // hop — a forward to a leader that is frozen, not dead, so its
        // socket accepts and then stalls — must not consume every retry the
        // budget was meant to fund. The chaos suite's freeze fault found
        // exactly that.
        const ATTEMPT_CAP: Duration = Duration::from_secs(2);
        let deadline = tokio::time::Instant::now() + self.write_timeout;

        // One id for this write, stamped before the first attempt and carried
        // by every retry. A timed-out attempt does not mean the proposal
        // failed -- it means no answer arrived in time -- so a retry may be
        // re-proposing a command that committed. The id is what lets the state
        // machine answer the retry with the original response instead of the
        // conflict its post-commit state would otherwise produce (#529).
        //
        // Stamped here rather than per attempt, and never restamped: a fresh
        // id on each retry is indistinguishable from a fresh command.
        let command = crate::store::raft::command::stamp_request_id(
            &command,
            &uuid::Uuid::new_v4().to_string(),
        )
        .unwrap_or(command);

        let mut last_refusal = None;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // The counter to alert on: a proposal ran out its whole
                // budget, which means no leader or no quorum.
                metrics::counter!("felix_meta_raft_write_timeouts_total").increment(1);
                let last = last_refusal
                    .unwrap_or_else(|| anyhow::anyhow!("no quorum committed the proposal"));
                return Err(anyhow::Error::new(NoQuorum::new(self.write_timeout, &last)));
            }
            let attempt = remaining.min(ATTEMPT_CAP);
            // Bounded per attempt too: a leader that lost quorum queues
            // proposals forever, and that must become this instance's error,
            // not its hang.
            let proposal = self.proposal_bytes(&command);
            match tokio::time::timeout(attempt, self.raft.client_write(proposal)).await {
                Err(_) => {
                    last_refusal = Some(anyhow::anyhow!("proposal not committed in time"));
                    continue;
                }
                Ok(Ok(response)) => return Ok(response.data),
                Ok(Err(err)) => match self.forward_target(&err) {
                    Some(base) => match self.forward_to(&base, &command, attempt).await {
                        Ok(bytes) => {
                            // The counter that says the load balancer keeps
                            // handing writes to followers — informational,
                            // since forwarding is correct, just one hop more.
                            metrics::counter!("felix_meta_raft_forwarded_proposals_total")
                                .increment(1);
                            return Ok(bytes);
                        }
                        // The leader we were told about may itself have just
                        // lost leadership; loop and re-ask.
                        Err(fwd_err) => last_refusal = Some(fwd_err),
                    },
                    None => last_refusal = Some(anyhow::anyhow!(err.to_string())),
                },
            }
            tokio::time::sleep(RETRY_DELAY.min(remaining)).await;
        }
    }

    /// What to append, with the leader's clock in place of the proposer's.
    ///
    /// A clock inside a command has to be the leader's when it will later be
    /// compared against a reading taken on the leader, and this is the only
    /// place that knows which instance that is. `client_write` succeeds on
    /// the leader alone, so stamping right before it is stamping at the
    /// moment of acceptance.
    ///
    /// `command` itself is left alone. If this instance turns out to be
    /// deposed between the check and the write, what gets forwarded carries
    /// no reading of ours and the real leader stamps its own.
    fn proposal_bytes(&self, command: &[u8]) -> Vec<u8> {
        if self.is_leader()
            && let Some(stamped) = self.app.restamp(command, crate::clock::now_millis())
        {
            return stamped;
        }
        command.to_vec()
    }

    async fn forward_to(&self, base: &str, command: &[u8], budget: Duration) -> Result<Vec<u8>> {
        let response = self
            .forward
            .post(format!("{base}/internal/raft/propose"))
            .timeout(budget)
            .body(command.to_vec())
            .send()
            .await
            .context("forward proposal")?;
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            anyhow::bail!("leader refused forwarded proposal: {status} {detail}");
        }
        Ok(response
            .bytes()
            .await
            .context("read forwarded response")?
            .to_vec())
    }

    /// Where a refused proposal should go instead, when the refusal says.
    fn forward_target(
        &self,
        err: &openraft::error::RaftError<
            u64,
            openraft::error::ClientWriteError<u64, openraft::BasicNode>,
        >,
    ) -> Option<String> {
        if let openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ForwardToLeader(forward),
        ) = err
        {
            self.peer_url(
                forward.leader_id,
                forward.leader_node.as_ref().map(|node| node.addr.as_str()),
            )
        } else {
            None
        }
    }
}

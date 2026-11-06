//! Handing this broker's shards to the others before it stops.
//!
//! A broker that just exits leaves each shard it leads to fail over: clients
//! are refused until the control plane notices and promotes a follower. Asking
//! the control plane to drain it first turns each of those into a planned
//! move, which clients follow without a refusal. This runs after readiness
//! goes false and before the listener closes, because a move needs the old
//! leader serving and forwarding until it cuts over.
//!
//! Best effort by design: every way it can fail (nobody to hand to, the
//! control plane unreachable, the timeout, a second signal) falls through to
//! the ordinary shutdown, and whatever is still led then fails over as it
//! always did.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::cluster::credential::NodeCredential;
use crate::cluster::{membership, node_catalog};
use crate::shards::watch::ShardOwnership;

/// Shards handed off during shutdown.
pub(crate) const SHARDS_TOTAL: &str = "felix_broker_shutdown_handoff_shards_total";
/// Handoffs by `outcome`: `completed`, `timed_out`, `interrupted`, `skipped`.
pub(crate) const HANDOFFS_TOTAL: &str = "felix_broker_shutdown_handoffs_total";
/// How long the last handoff took, whatever its outcome.
pub(crate) const DURATION_MS: &str = "felix_broker_shutdown_handoff_duration_ms";

/// Bound on each control-plane call. One that does not answer in this long
/// is not going to run a drain either, and every second spent here is taken
/// from the pod's grace period.
const CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// How often to look at what this broker still leads. The watch applies
/// changes as they arrive, so this only sets how quickly the loop notices.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// What the handoff needs: who this broker is, how to reach the control
/// plane, and the assignments the shard watch keeps current.
pub(crate) struct Handoff {
    pub(crate) client: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) node_id: String,
    pub(crate) credential: Option<NodeCredential>,
    pub(crate) ownership: Arc<RwLock<ShardOwnership>>,
    pub(crate) timeout: Duration,
}

/// How a handoff ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Nothing was asked of the control plane.
    Skipped(&'static str),
    /// Every shard this broker led is led elsewhere.
    Completed { handed_off: usize },
    /// The timeout ran out with `remaining` shards still led here.
    TimedOut { handed_off: usize, remaining: usize },
    /// A second termination signal asked to stop waiting.
    Interrupted { handed_off: usize, remaining: usize },
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Self::Skipped(_) => "skipped",
            Self::Completed { .. } => "completed",
            Self::TimedOut { .. } => "timed_out",
            Self::Interrupted { .. } => "interrupted",
        }
    }

    /// Whether the operator asked to skip the rest of the waiting.
    pub(crate) fn interrupted(&self) -> bool {
        matches!(self, Self::Interrupted { .. })
    }
}

impl Handoff {
    /// Drain this broker and wait, up to the timeout, until it leads nothing.
    /// `interrupt` resolving (a second signal) ends the wait early.
    pub(crate) async fn run(self, interrupt: impl Future<Output = ()>) -> Outcome {
        let started = Instant::now();
        let outcome = self.hand_off(interrupt).await;
        metrics::counter!(HANDOFFS_TOTAL, "outcome" => outcome.label()).increment(1);
        metrics::gauge!(DURATION_MS).set(started.elapsed().as_millis() as f64);
        let elapsed_ms = started.elapsed().as_millis() as u64;
        match &outcome {
            Outcome::Skipped(reason) => {
                tracing::info!(reason, "not handing shards off before shutdown");
            }
            Outcome::Completed { handed_off } => {
                metrics::counter!(SHARDS_TOTAL).increment(*handed_off as u64);
                tracing::info!(handed_off, elapsed_ms, "handed every shard off");
            }
            Outcome::TimedOut {
                handed_off,
                remaining,
            } => {
                metrics::counter!(SHARDS_TOTAL).increment(*handed_off as u64);
                tracing::warn!(
                    handed_off,
                    remaining,
                    elapsed_ms,
                    "shutdown handoff timed out; the shards still led here will fail over",
                );
            }
            Outcome::Interrupted {
                handed_off,
                remaining,
            } => {
                metrics::counter!(SHARDS_TOTAL).increment(*handed_off as u64);
                tracing::warn!(
                    handed_off,
                    remaining,
                    elapsed_ms,
                    "second termination signal; stopping without finishing the handoff",
                );
            }
        }
        outcome
    }

    async fn hand_off(&self, interrupt: impl Future<Output = ()>) -> Outcome {
        let led = self.leading().await;
        if led == 0 {
            return Outcome::Skipped("leads no shards");
        }
        let token = self
            .credential
            .as_ref()
            .map(|credential| credential.bearer().to_string());

        let link = felix_replication::controlplane_link::HttpControlPlaneLink::new(
            self.client.clone(),
            &self.base_url,
        );

        // Draining the only broker would hold every shard waiting for a
        // destination until the timeout, then fail over to nobody.
        let catalog =
            tokio::time::timeout(CALL_TIMEOUT, node_catalog::fetch(&link, token.as_deref())).await;
        match catalog {
            Ok(Ok(catalog)) => {
                if !catalog
                    .nodes
                    .values()
                    .any(|node| node.live && node.node_id != self.node_id)
                {
                    return Outcome::Skipped("no other broker can take its shards");
                }
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "could not read the node catalog");
                return Outcome::Skipped("control plane unreachable");
            }
            Err(_) => return Outcome::Skipped("control plane unreachable"),
        }

        let drain = tokio::time::timeout(
            CALL_TIMEOUT,
            membership::drain(&link, &self.node_id, token.as_deref().unwrap_or_default()),
        )
        .await;
        match drain {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "the control plane did not accept the drain");
                return Outcome::Skipped("drain refused");
            }
            Err(_) => return Outcome::Skipped("control plane unreachable"),
        }
        tracing::info!(
            shards = led,
            timeout_ms = self.timeout.as_millis() as u64,
            "handing shards off before shutdown",
        );

        let deadline = tokio::time::Instant::now() + self.timeout;
        tokio::pin!(interrupt);
        loop {
            let remaining = self.leading().await;
            // Counted against what it led at the start. A shard placed here
            // since would make this undercount, but a draining node is not
            // given new shards.
            let handed_off = led.saturating_sub(remaining);
            if remaining == 0 {
                return Outcome::Completed { handed_off };
            }
            if tokio::time::Instant::now() >= deadline {
                return Outcome::TimedOut {
                    handed_off,
                    remaining,
                };
            }
            tokio::select! {
                _ = &mut interrupt => {
                    return Outcome::Interrupted { handed_off, remaining };
                }
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    async fn leading(&self) -> usize {
        self.ownership
            .read()
            .await
            .assignments()
            .values()
            .filter(|assignment| assignment.leader == self.node_id)
            .count()
    }
}

#[cfg(test)]
mod tests;

//! Broker liveness expiry.
//!
//! A broker reports health on an interval. Nothing reports that a broker has
//! *stopped*, so silence is the only signal, and something has to notice it.
//! This is that something: a periodic sweep that marks nodes down once their
//! last heartbeat is older than the configured timeout.
//!
//! Running several control-plane instances is fine. The store moves each node
//! exactly once and only the instance that moved it publishes the change, so
//! duplicate sweeps cost a query and produce no duplicate events.
pub mod metrics;

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::NodeLivenessConfig;
use crate::store::ControlPlaneStore;

/// Run one expiry pass against `now_millis`.
///
/// Separate from the loop so tests can drive it at an exact time instead of
/// waiting for a timer.
pub async fn expire_once(
    store: &dyn ControlPlaneStore,
    liveness: &NodeLivenessConfig,
    now_millis: u64,
) -> usize {
    // Saturating: before the timeout has elapsed since the epoch nothing can be
    // stale, and wrapping would expire the whole cluster.
    let expiry_before = now_millis.saturating_sub(liveness.expiry_timeout_ms);

    match store.expire_stale_nodes(expiry_before).await {
        Ok(expired) => {
            for node in &expired {
                tracing::warn!(
                    node_id = %node.node_id,
                    last_heartbeat_at_millis = node.status.last_heartbeat_at_millis,
                    "broker missed its heartbeat window and was marked down",
                );
            }
            ::metrics::counter!(crate::cluster::membership::metrics::NODE_EXPIRY_TOTAL)
                .increment(expired.len() as u64);
            // Published from the store, not from the delta above, so the gauge
            // is a statement about current state that cannot drift from what
            // the node listing returns.
            match store.list_nodes().await {
                Ok(nodes) => crate::cluster::membership::metrics::publish_census(&nodes),
                Err(err) => {
                    tracing::warn!(error = %err, "could not refresh the membership census")
                }
            }
            expired.len()
        }
        Err(err) => {
            // Logged and retried on the next tick. A transient database error
            // must not leave liveness frozen for the rest of the process's life.
            tracing::error!(error = %err, "node expiry sweep failed");
            ::metrics::counter!(crate::cluster::membership::metrics::NODE_EXPIRY_FAILURES_TOTAL)
                .increment(1);
            0
        }
    }
}

/// Sweep for expired nodes until `shutdown` fires.
///
/// `gate` decides whether this instance sweeps at all this tick. For the
/// memory/Postgres backends it is `Always` — duplicate sweeps are safe by
/// store contract. Under Raft it holds only on the leader, freshly
/// confirmed, replacing cross-instance claim coordination with something
/// strictly simpler: one sweep because there is one leader.
///
/// Nothing is expired until this instance has watched for a full expiry
/// window; see [`SweepGrace`].
pub fn spawn_expiry_sweep(
    store: Arc<dyn ControlPlaneStore + Send + Sync>,
    liveness: NodeLivenessConfig,
    gate: crate::raft::LeadershipGate,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(liveness.sweep_interval_ms));
        // A sweep that overruns its interval must not then run back-to-back
        // trying to catch up; the next tick is soon enough.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut grace = SweepGrace::new(Duration::from_millis(liveness.expiry_timeout_ms));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if !gate.holds().await {
                        grace.lost("this instance is not the leader");
                        continue;
                    }
                    // The store's clock, which is the one heartbeats were
                    // stamped with. Reading this instance's own would make
                    // expiry depend on two processes' wall clocks agreeing.
                    match store.now_millis().await {
                        Ok(now) => {
                            if grace.may_sweep(tokio::time::Instant::now()) {
                                expire_once(store.as_ref(), &liveness, now).await;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "skipping the expiry sweep: could not read the store clock",
                            );
                            grace.lost("the store is unreachable");
                        }
                    }
                }
            }
        }
    })
}

/// Holds the sweep off for one expiry window after this instance starts
/// watching: at startup, on becoming the Raft leader, and once the store is
/// reachable again after it was not.
///
/// While nobody was watching, brokers could not heartbeat either, so every
/// last-heartbeat stamp is as old as the outage. Sweeping straight away would
/// mark the whole fleet down the moment the control plane came back. Waiting
/// one window gives every live broker a chance to report; one that stays
/// silent through it is expired as usual.
#[derive(Debug)]
pub struct SweepGrace {
    window: Duration,
    watching_since: Option<tokio::time::Instant>,
}

impl SweepGrace {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            watching_since: None,
        }
    }

    /// Whether a sweep may run at `now`. The first call after a gap starts
    /// the window.
    pub fn may_sweep(&mut self, now: tokio::time::Instant) -> bool {
        let since = *self.watching_since.get_or_insert_with(|| {
            tracing::info!(
                grace_ms = self.window.as_millis() as u64,
                "holding node expiry for one window so brokers can report in",
            );
            now
        });
        now.saturating_duration_since(since) >= self.window
    }

    /// This instance stopped watching, so the next sweep waits a full window.
    pub fn lost(&mut self, why: &str) {
        if self.watching_since.take().is_some() {
            tracing::info!(reason = why, "node expiry paused");
        }
    }
}

#[cfg(test)]
mod tests;

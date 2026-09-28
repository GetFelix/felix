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
use crate::model::{Node, NodeLifecycle};
use crate::store::ControlPlaneStore;

/// Run one expiry pass against `now_millis`, the store's clock.
///
/// A node is down once its last heartbeat is older than the expiry timeout
/// plus the regrant margin: its shards move only then, and the margin is what
/// keeps the broker's own lease, which it gives up a quarter early, from still
/// running when they do.
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
    let expiry_before = now_millis.saturating_sub(liveness.silence_before_down_ms());
    expire_before(store, expiry_before).await
}

/// [`expire_once`], sparing any node `watch` has not itself seen silent for
/// the whole window on this process's monotonic clock.
///
/// The store's clock alone is a wall clock, and after a Raft election or a
/// database failover a different machine's: a step forward would expire a
/// broker that is still inside its lease. The watch cannot be stepped, so a
/// node goes down only when both agree.
pub async fn expire_observed(
    store: &dyn ControlPlaneStore,
    liveness: &NodeLivenessConfig,
    now_millis: u64,
    watch: &mut SilenceWatch,
    at: tokio::time::Instant,
) -> usize {
    let nodes = match list_clamped(store, now_millis).await {
        Ok(nodes) => nodes,
        Err(err) => {
            tracing::warn!(error = %err, "skipping the expiry sweep: could not list nodes");
            return 0;
        }
    };
    let window = std::time::Duration::from_millis(liveness.silence_before_down_ms());
    let by_clock = now_millis.saturating_sub(liveness.silence_before_down_ms());
    // The store expires by threshold alone, so a node to spare caps it at its
    // own stamp. One that could have gone this pass waits for the next.
    let expiry_before = watch
        .spared(&nodes, window, at)
        .fold(by_clock, |before, stamp| before.min(stamp));
    expire_before(store, expiry_before).await
}

/// Whether a node that has left may still be serving on a lease it was
/// granted before it left.
///
/// Deregistering stops a broker's lease from being renewed, not the lease it
/// already holds, and nothing tells the control plane when the broker stopped.
/// So a node that left is treated as a silent one: its last heartbeat is when
/// its last lease was granted (a heartbeat to a node that has left grants and
/// records nothing), and it may still serve until the same window
/// [`expire_once`] waits out has passed on the store's clock.
pub fn left_within_lease(node: &Node, liveness: &NodeLivenessConfig, now_millis: u64) -> bool {
    node.status.lifecycle == NodeLifecycle::Left
        && node.status.last_heartbeat_at_millis
            >= now_millis.saturating_sub(liveness.silence_before_down_ms())
}

/// Every node, with any heartbeat stamp ahead of `now_millis` first pulled
/// back to it.
///
/// Such a stamp means the store's clock stepped back after it was taken, and
/// a threshold on that clock would not reach it until real time caught up:
/// a broker dying just after a step back of an hour would stay live for the
/// hour. At now, it is a stamp the watch sees change, so the node gets one
/// full window of silence from here like any other.
async fn list_clamped(
    store: &dyn ControlPlaneStore,
    now_millis: u64,
) -> crate::store::StoreResult<Vec<Node>> {
    let nodes = store.list_nodes().await?;
    if nodes
        .iter()
        .all(|node| node.status.last_heartbeat_at_millis <= now_millis)
    {
        return Ok(nodes);
    }
    let clamped = store.clamp_future_heartbeats(now_millis).await?;
    if clamped > 0 {
        tracing::warn!(
            clamped,
            now_millis,
            "heartbeat stamps were ahead of the store clock, which must have stepped back",
        );
    }
    store.list_nodes().await
}

async fn expire_before(store: &dyn ControlPlaneStore, expiry_before: u64) -> usize {
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
        let mut watch = SilenceWatch::default();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if !gate.holds().await {
                        grace.lost("this instance is not the leader");
                        watch.clear();
                        continue;
                    }
                    // The store's clock, which is the one heartbeats were
                    // stamped with. Reading this instance's own would make
                    // expiry depend on two processes' wall clocks agreeing.
                    match store.now_millis().await {
                        Ok(now) => {
                            let at = tokio::time::Instant::now();
                            // Watched from the first tick, grace or not, so
                            // the window has been running when grace ends.
                            let swept = grace.may_sweep(at);
                            if swept {
                                expire_observed(store.as_ref(), &liveness, now, &mut watch, at)
                                    .await;
                            } else if let Ok(nodes) = list_clamped(store.as_ref(), now).await {
                                watch.observe(&nodes, at);
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "skipping the expiry sweep: could not read the store clock",
                            );
                            grace.lost("the store is unreachable");
                            watch.clear();
                        }
                    }
                }
            }
        }
    })
}

/// When this process last saw each node's heartbeat stamp change, on its own
/// monotonic clock.
///
/// A node this instance has only just started watching counts as heard from
/// now, so a fresh instance, a new Raft leader or one that lost the store
/// waits a full window before expiring anything.
#[derive(Debug, Default)]
pub struct SilenceWatch {
    seen: std::collections::HashMap<String, (u64, u64, tokio::time::Instant)>,
}

impl SilenceWatch {
    /// Note each node's stamp as of `at`.
    pub fn observe(&mut self, nodes: &[crate::model::Node], at: tokio::time::Instant) {
        self.seen
            .retain(|node_id, _| nodes.iter().any(|node| &node.node_id == node_id));
        for node in nodes {
            let stamp = (
                node.status.incarnation,
                node.status.last_heartbeat_at_millis,
            );
            let entry = self
                .seen
                .entry(node.node_id.clone())
                .or_insert((stamp.0, stamp.1, at));
            if (entry.0, entry.1) != stamp {
                *entry = (stamp.0, stamp.1, at);
            }
        }
    }

    /// Observe `nodes`, then the heartbeat stamps of those not yet silent for
    /// `window` by this watch.
    pub fn spared<'a>(
        &'a mut self,
        nodes: &'a [crate::model::Node],
        window: Duration,
        at: tokio::time::Instant,
    ) -> impl Iterator<Item = u64> + 'a {
        self.observe(nodes, at);
        nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.status.lifecycle,
                    crate::model::NodeLifecycle::Live | crate::model::NodeLifecycle::Draining
                )
            })
            .filter_map(move |node| {
                let (_, stamp, since) = self.seen.get(&node.node_id)?;
                (at.saturating_duration_since(*since) < window).then_some(*stamp)
            })
    }

    /// Forget everything: whatever this instance saw before a gap in watching
    /// is not evidence of silence through it.
    pub fn clear(&mut self) {
        self.seen.clear();
    }
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

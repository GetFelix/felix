//! Store liveness, in shadow mode: brokers that share a shard tell each
//! other, peer to peer, that their disks still take synced writes.
//!
//! Each broker syncs a small probe file once per interval and sends its peers
//! a heartbeat only while a probe finished recently, so a broker whose disk
//! has stalled goes silent though its process and network are fine. A
//! heartbeat carries the sender's store epoch, which rises at each start.
//! The control plane is not involved. See "Store liveness" in
//! `docs/per-shard-raft.md`.
//!
//! Nothing acts on it yet. What it would decide (support withdrawn, a peer
//! restarted, the hold on restarted members) is counted, logged and kept in
//! [`Liveness::shadow`]. The hybrid's own signal is `suspicion`, which this
//! neither reads nor writes.

mod disk;
mod state;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use felix_common::random::HashSet;
use felix_router::ShardRouter;
use felix_wire::internal::{InternalMessage, PeerCapabilities, StoreLiveness};
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::metrics;
use crate::peer::PeerRequester;

pub use disk::{ProbeDisk, VolumeProbe};
use state::State;
pub use state::{Decision, LivenessConfig};

/// How many decisions [`Liveness::shadow`] keeps. Older ones are counted in
/// the metrics and dropped here.
const SHADOW_KEPT: usize = 256;

/// One broker's store liveness.
#[derive(Debug)]
pub struct Liveness {
    config: LivenessConfig,
    epoch: u64,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: State,
    decisions: VecDeque<Decision>,
    /// Peers that answered that they run store liveness, where no handshake
    /// is on record to say so.
    running: HashSet<String>,
    /// Peers being asked right now, so each is asked by one task at a time.
    asking: HashSet<String>,
}

/// What store liveness would have done so far, and how it sees its peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shadow {
    /// This broker's store epoch.
    pub epoch: u64,
    /// Whether this broker is sending heartbeats: a probe finished within
    /// the stall limit.
    pub sending: bool,
    /// The latest decisions, oldest first.
    pub decisions: Vec<Decision>,
}

impl Liveness {
    /// `epoch` is this start's store epoch (`felix_storage::store_liveness`).
    pub fn new(epoch: u64, config: LivenessConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            epoch,
            inner: Mutex::new(Inner {
                state: State::new(config, Instant::now()),
                decisions: VecDeque::new(),
                running: HashSet::default(),
                asking: HashSet::default(),
            }),
        })
    }

    pub fn config(&self) -> LivenessConfig {
        self.config
    }

    /// A heartbeat from `peer` arrived.
    pub fn heard(&self, peer: &str, epoch: u64) {
        let mut inner = self.inner.lock();
        let decisions = inner.state.heard(peer, epoch, Instant::now());
        inner.record(decisions);
    }

    pub fn shadow(&self) -> Shadow {
        let inner = self.inner.lock();
        Shadow {
            epoch: self.epoch,
            sending: inner.state.may_send(Instant::now()),
            decisions: inner.decisions.iter().cloned().collect(),
        }
    }

    /// Whether `peer` has this broker's support; `None` for a peer it does
    /// not watch.
    pub fn supports(&self, peer: &str) -> Option<bool> {
        self.inner.lock().state.supports(peer)
    }

    /// The store epoch `peer` last showed.
    pub fn epoch_of(&self, peer: &str) -> Option<u64> {
        self.inner.lock().state.epoch_of(peer)
    }

    /// Whether a member whose group `leader` leads would still be held after
    /// this broker's start.
    pub fn holds(&self, leader: &str) -> bool {
        self.inner.lock().state.holds(leader, Instant::now())
    }

    /// Probe `disk` and exchange heartbeats with the brokers `router` says
    /// this one shares a shard with, until `shutdown`.
    pub fn start<R: PeerRequester + Send + Sync + 'static>(
        self: &Arc<Self>,
        requester: Arc<R>,
        router: Arc<ShardRouter>,
        disk: Arc<dyn ProbeDisk>,
        shutdown: CancellationToken,
    ) {
        tracing::info!(
            epoch = self.epoch,
            interval = ?self.config.interval,
            "store liveness started in shadow mode: it records what it would decide and acts on nothing",
        );
        tokio::spawn(Arc::clone(self).probe(disk, shutdown.clone()));
        tokio::spawn(Arc::clone(self).exchange(requester, router, shutdown));
    }

    /// One probe at a time, an interval apart. A probe that overruns the
    /// interval is waited for, never doubled: a second sync behind a stalled
    /// one would only queue on the same device.
    async fn probe(self: Arc<Self>, disk: Arc<dyn ProbeDisk>, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failing = false;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let began = Instant::now();
            let probed = tokio::select! {
                _ = shutdown.cancelled() => return,
                probed = disk.probe() => probed,
            };
            let now = Instant::now();
            ::metrics::histogram!(metrics::LIVENESS_PROBE_SECONDS)
                .record(now.saturating_duration_since(began).as_secs_f64());
            match probed {
                Ok(()) => {
                    self.inner.lock().state.probe_finished(now);
                    if std::mem::take(&mut failing) {
                        tracing::info!("the store liveness probe succeeds again");
                    }
                }
                Err(err) => {
                    ::metrics::counter!(metrics::LIVENESS_PROBE_FAILURES_TOTAL).increment(1);
                    if !std::mem::replace(&mut failing, true) {
                        tracing::warn!(error = %err, "the store liveness probe failed");
                    }
                }
            }
        }
    }

    /// Never waits on a peer: heartbeats and capability questions run as
    /// their own tasks. One unreachable broker must not stretch the gap
    /// between the heartbeats the others get, or they would withdraw support
    /// for a healthy broker.
    async fn exchange<R: PeerRequester + Send + Sync + 'static>(
        self: Arc<Self>,
        requester: Arc<R>,
        router: Arc<ShardRouter>,
        shutdown: CancellationToken,
    ) {
        let every = self.config.interval;
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let peers = sharing(&router);
            let running: Vec<&SharedPeer> = peers
                .iter()
                .filter(|peer| self.runs_it(&requester, peer, &shutdown))
                .collect();
            if self.inner.lock().state.may_send(Instant::now()) {
                for peer in &running {
                    let (requester, peer, epoch) =
                        (Arc::clone(&requester), (*peer).clone(), self.epoch);
                    let shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = shutdown.cancelled() => {}
                            _ = beat(requester.as_ref(), &peer, epoch, every) => {}
                        }
                    });
                }
            }
            let watched: HashSet<String> =
                running.iter().map(|peer| peer.node_id.clone()).collect();
            let leaders: HashSet<String> = running
                .iter()
                .filter(|peer| peer.leads)
                .map(|peer| peer.node_id.clone())
                .collect();
            let mut inner = self.inner.lock();
            let now = Instant::now();
            let decisions = inner.state.tick(&watched, &leaders, now);
            inner.record(decisions);
            ::metrics::gauge!(metrics::LIVENESS_SENDING).set(if inner.state.may_send(now) {
                1.0
            } else {
                0.0
            });
            ::metrics::gauge!(metrics::LIVENESS_PEERS).set(inner.state.watched() as f64);
            ::metrics::gauge!(metrics::LIVENESS_PEERS_WITHDRAWN)
                .set(inner.state.withdrawn() as f64);
            ::metrics::gauge!(metrics::LIVENESS_HELD_LEADERS).set(
                leaders
                    .iter()
                    .filter(|leader| inner.state.holds(leader, now))
                    .count() as f64,
            );
        }
    }

    /// Whether `peer` runs store liveness. Its latest handshake decides. With
    /// none on record it is asked in the background and stays what it was
    /// last known to be: a peer that was watched and has died must stay
    /// watched to be missed.
    fn runs_it<R: PeerRequester + Send + Sync + 'static>(
        self: &Arc<Self>,
        requester: &Arc<R>,
        peer: &SharedPeer,
        shutdown: &CancellationToken,
    ) -> bool {
        if let Some(offered) = requester.recorded_capabilities(&peer.node_id) {
            return offered.contains(PeerCapabilities::STORE_LIVENESS);
        }
        let mut inner = self.inner.lock();
        if inner.asking.insert(peer.node_id.clone()) {
            let (this, requester, peer) = (Arc::clone(self), Arc::clone(requester), peer.clone());
            let (within, shutdown) = (self.config.interval, shutdown.clone());
            tokio::spawn(async move {
                let asked = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    asked = tokio::time::timeout(
                        within,
                        requester.capabilities(&peer.node_id, peer.addr),
                    ) => asked,
                };
                let mut inner = this.inner.lock();
                inner.asking.remove(&peer.node_id);
                match asked {
                    Ok(Ok(offered)) if offered.contains(PeerCapabilities::STORE_LIVENESS) => {
                        inner.running.insert(peer.node_id);
                    }
                    Ok(Ok(_)) => {
                        inner.running.remove(&peer.node_id);
                    }
                    _ => {}
                }
            });
        }
        inner.running.contains(&peer.node_id)
    }
}

impl Inner {
    /// Shadow mode: count and keep each decision. Nothing else reads them.
    fn record(&mut self, decisions: Vec<Decision>) {
        for decision in decisions {
            let kind = match &decision {
                Decision::Withdraw { peer } => {
                    tracing::info!(%peer, "store liveness (shadow) would withdraw support");
                    "withdraw"
                }
                Decision::Restore { peer } => {
                    tracing::info!(%peer, "store liveness (shadow) would restore support");
                    "restore"
                }
                Decision::Restarted { peer, epoch } => {
                    tracing::info!(%peer, epoch, "store liveness (shadow) saw a new store epoch");
                    "restarted"
                }
                Decision::Silenced => {
                    tracing::warn!(
                        "store liveness (shadow): no disk probe finished within the stall limit; heartbeats stop",
                    );
                    "silenced"
                }
                Decision::Resumed => "resumed",
                Decision::HoldReleased { .. } => "hold_released",
                Decision::HoldExpired { unheard } => {
                    tracing::info!(
                        ?unheard,
                        "store liveness (shadow): the restart hold ran out"
                    );
                    "hold_expired"
                }
            };
            ::metrics::counter!(metrics::LIVENESS_DECISIONS_TOTAL, "decision" => kind).increment(1);
            if self.decisions.len() == SHADOW_KEPT {
                self.decisions.pop_front();
            }
            self.decisions.push_back(decision);
        }
    }
}

/// A broker this one shares a shard with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SharedPeer {
    node_id: String,
    addr: SocketAddr,
    /// Whether it leads a shard this broker follows.
    leads: bool,
}

/// Every broker in a replica set this broker is also in, except those the
/// node catalog has no address for (port 0), which cannot be sent anything.
fn sharing(router: &ShardRouter) -> Vec<SharedPeer> {
    let local = router.local_node_id();
    let mut peers: Vec<SharedPeer> = Vec::new();
    for (_, route) in router.snapshot().iter() {
        let follows = route.replicas.iter().any(|node| node.node_id == local);
        if !follows && route.leader.node_id != local {
            continue;
        }
        for node in route.replicas.iter().chain(std::iter::once(&route.leader)) {
            if node.node_id == local || node.advertise_addr.port() == 0 {
                continue;
            }
            let leads = follows && node.node_id == route.leader.node_id;
            match peers.iter_mut().find(|peer| peer.node_id == node.node_id) {
                Some(peer) => peer.leads |= leads,
                None => peers.push(SharedPeer {
                    node_id: node.node_id.clone(),
                    addr: node.advertise_addr,
                    leads,
                }),
            }
        }
    }
    peers
}

/// Send `peer` one heartbeat, giving up after `within`. The answer only
/// frees the request: what this broker thinks of `peer` comes from the
/// heartbeats `peer` sends, never from its answers.
async fn beat<R: PeerRequester>(requester: &R, peer: &SharedPeer, epoch: u64, within: Duration) {
    let sent = tokio::time::timeout(
        within,
        requester.request(
            &peer.node_id,
            peer.addr,
            InternalMessage::StoreLiveness(StoreLiveness {
                correlation_id: 0,
                epoch,
            }),
        ),
    )
    .await;
    let outcome = match sent {
        Ok(Ok(InternalMessage::Pong(_))) => metrics::OUTCOME_OK,
        Ok(_) => metrics::OUTCOME_REFUSED,
        Err(_) => metrics::OUTCOME_TIMEOUT,
    };
    ::metrics::counter!(metrics::LIVENESS_HEARTBEATS_TOTAL, "outcome" => outcome).increment(1);
}

#[cfg(test)]
mod tests;

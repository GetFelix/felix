//! Which leaders this broker, as a follower, cannot reach.
//!
//! Once the fleet acknowledges `Quorum` writes by their followers, a deposed
//! leader is kept out by the fence rather than by its lease running out. So
//! the control plane may promote as soon as a majority of a shard's set says
//! it cannot reach the leader, instead of waiting for the leader to be marked
//! down. This is the followers' half: ping every broker that leads a shard
//! this one follows, and name the ones that have not answered for
//! `suspect_after`. The heartbeat carries the names. See
//! `docs/replication-design.md` ("Failover on the followers' word").
//!
//! Being wrong costs availability, never safety: a leader suspected by
//! mistake is replaced by a leader that fenced a majority first.
use felix_common::random::{HashMap, HashSet};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use felix_router::ShardRouter;
use felix_wire::internal::{InternalMessage, PeerCapabilities, Ping};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::peer::PeerRequester;

/// How long a leader may go without answering before this broker names it.
pub(crate) const DEFAULT_SUSPECT_AFTER: Duration = Duration::from_secs(5);
/// How long an elected leader that has not started its generation holds the
/// shard. Generous: a fence or catch-up under load can take many seconds,
/// and giving up on one costs a generation.
pub(crate) const DEFAULT_DEFER_FOR: Duration = Duration::from_secs(60);
const MIN_DEFER_FOR: Duration = Duration::from_secs(1);
/// How long a copy may go without hearing its leader, with that leader's
/// generation not started here, before it may stand for the shard though the
/// leader's broker answers pings. Above the control plane's stuck-leader
/// backstop (70 s by default), so with the control plane up its rename comes
/// first and restarts the clock.
pub(crate) const DEFAULT_SHARD_SUSPECT_AFTER: Duration = Duration::from_secs(120);
const MIN_SHARD_SUSPECT_AFTER: Duration = Duration::from_secs(1);

/// The shortest gap between pings, whatever `suspect_after` is.
const MIN_PROBE_EVERY: Duration = Duration::from_millis(50);

/// When this broker suspects a leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SuspicionConfig {
    /// Silence past this names the leader. Zero turns the watch off.
    pub suspect_after: Duration,
    /// How long a copy defers to an elected leader that has not started its
    /// generation. See `Elections::with_defer_for`.
    pub defer_for: Duration,
    /// How long a shard's leader may make no progress here before this copy
    /// may stand for that shard alone. Zero turns per-shard suspicion off.
    /// See "Suspecting a shard" in `docs/replication-design.md`.
    pub shard_suspect_after: Duration,
}

impl Default for SuspicionConfig {
    fn default() -> Self {
        Self {
            suspect_after: DEFAULT_SUSPECT_AFTER,
            defer_for: DEFAULT_DEFER_FOR,
            shard_suspect_after: DEFAULT_SHARD_SUSPECT_AFTER,
        }
    }
}

impl SuspicionConfig {
    /// `FELIX_LEADER_SUSPECT_AFTER_MS`, `FELIX_ELECTION_DEFER_FOR_MS` and
    /// `FELIX_SHARD_SUSPECT_AFTER_MS`, or the defaults.
    pub(crate) fn from_env() -> Self {
        let millis = |name: &str, default: Duration| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .map_or(default, Duration::from_millis)
        };
        Self {
            suspect_after: millis("FELIX_LEADER_SUSPECT_AFTER_MS", DEFAULT_SUSPECT_AFTER),
            // Zero would drop the deferral altogether and let a follower
            // stand over every standing still fencing.
            defer_for: millis("FELIX_ELECTION_DEFER_FOR_MS", DEFAULT_DEFER_FOR).max(MIN_DEFER_FOR),
            shard_suspect_after: shard_bound(millis(
                "FELIX_SHARD_SUSPECT_AFTER_MS",
                DEFAULT_SHARD_SUSPECT_AFTER,
            )),
        }
    }

    /// Whether a copy may stand for a shard whose leader's broker answers.
    pub(crate) fn suspects_shards(&self) -> bool {
        !self.shard_suspect_after.is_zero()
    }

    /// Four pings per window, so one lost ping is not a suspicion.
    pub(crate) fn probe_every(&self) -> Duration {
        (self.suspect_after / 4).max(MIN_PROBE_EVERY)
    }
}

/// Zero stays off; anything else is at least a second, so a typo in
/// milliseconds does not make every quiet moment a suspicion.
fn shard_bound(bound: Duration) -> Duration {
    if bound.is_zero() {
        bound
    } else {
        bound.max(MIN_SHARD_SUSPECT_AFTER)
    }
}

/// The leaders this broker currently suspects, by node id.
#[derive(Debug)]
pub struct Suspects {
    names: watch::Sender<BTreeSet<String>>,
}

impl Default for Suspects {
    fn default() -> Self {
        Self {
            names: watch::Sender::new(BTreeSet::new()),
        }
    }
}

impl Suspects {
    pub(crate) fn current(&self) -> BTreeSet<String> {
        self.names.borrow().clone()
    }

    /// Changes whenever the set does, so a heartbeat can carry a new
    /// suspicion at once instead of an interval later.
    pub fn subscribe(&self) -> watch::Receiver<BTreeSet<String>> {
        self.names.subscribe()
    }

    /// Replace the set. The watch's to call; a test may stand in for it.
    pub fn publish(&self, names: BTreeSet<String>) {
        self.names.send_if_modified(|current| {
            if *current == names {
                return false;
            }
            for added in names.difference(current) {
                tracing::warn!(leader = %added, "a leader this broker follows has stopped answering");
            }
            for cleared in current.difference(&names) {
                tracing::info!(leader = %cleared, "a leader this broker suspected answers again");
            }
            *current = names;
            true
        });
    }
}

/// When each watched leader last answered.
#[derive(Debug)]
pub(crate) struct Silence {
    suspect_after: Duration,
    heard: HashMap<String, Instant>,
}

impl Silence {
    pub(crate) fn new(suspect_after: Duration) -> Self {
        Self {
            suspect_after,
            heard: HashMap::default(),
        }
    }

    /// Fold one round of pings into the record and say who is suspected.
    ///
    /// `watched` are the leaders pinged this round, `answered` those that
    /// answered. A leader first watched now counts as heard now, so it gets a
    /// whole window like any other; one no longer watched is forgotten.
    pub(crate) fn round(
        &mut self,
        watched: &HashSet<String>,
        answered: &HashSet<String>,
        now: Instant,
    ) -> BTreeSet<String> {
        self.heard.retain(|node, _| watched.contains(node));
        for node in watched {
            let heard = self.heard.entry(node.clone()).or_insert(now);
            if answered.contains(node) {
                *heard = now;
            }
        }
        self.heard
            .iter()
            .filter(|(_, heard)| now.saturating_duration_since(**heard) >= self.suspect_after)
            .map(|(node, _)| node.clone())
            .collect()
    }
}

/// What one ping found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Answer {
    Pong,
    /// The peer did not offer [`PeerCapabilities::PING`]. It is not watched:
    /// silence from a broker that never answers pings says nothing.
    Unsupported,
    Silent,
}

/// Ping `node_id` once, giving up after `within`.
pub(crate) async fn ping<R: PeerRequester>(
    requester: &R,
    node_id: &str,
    addr: SocketAddr,
    within: Duration,
) -> Answer {
    let asked = async {
        match requester.capabilities(node_id, addr).await {
            Ok(offered) if !offered.contains(PeerCapabilities::PING) => Answer::Unsupported,
            Ok(_) => match requester
                .request(
                    node_id,
                    addr,
                    InternalMessage::Ping(Ping { correlation_id: 0 }),
                )
                .await
            {
                Ok(InternalMessage::Pong(_)) => Answer::Pong,
                _ => Answer::Silent,
            },
            Err(_) => Answer::Silent,
        }
    };
    tokio::time::timeout(within, asked)
        .await
        .unwrap_or(Answer::Silent)
}

/// Every broker that leads a shard this one follows, except leaders the node
/// catalog has no address for (port 0): failing to reach one of those says
/// nothing about whether it is up, so it must not make it a suspect.
pub(crate) fn leaders_followed(router: &ShardRouter) -> Vec<(String, SocketAddr)> {
    let local = router.local_node_id();
    let mut seen = HashSet::default();
    let mut leaders = Vec::new();
    for (_, route) in router.snapshot().iter() {
        if route.leader.node_id == local
            || route.leader.advertise_addr.port() == 0
            || !route
                .replicas
                .iter()
                .any(|replica| replica.node_id == local)
        {
            continue;
        }
        if seen.insert(route.leader.node_id.clone()) {
            leaders.push((route.leader.node_id.clone(), route.leader.advertise_addr));
        }
    }
    leaders
}

/// Ping the leaders of every shard this broker follows until `shutdown`, and
/// keep `suspects` current. `None` when the config turns the watch off.
pub(crate) fn spawn<R: PeerRequester + Send + Sync + 'static>(
    requester: Arc<R>,
    router: Arc<ShardRouter>,
    suspects: Arc<Suspects>,
    config: SuspicionConfig,
    shutdown: CancellationToken,
) -> Option<tokio::task::JoinHandle<()>> {
    if config.suspect_after.is_zero() {
        return None;
    }
    let every = config.probe_every();
    Some(tokio::spawn(async move {
        let mut silence = Silence::new(config.suspect_after);
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let leaders = leaders_followed(&router);
            let answers = futures::future::join_all(leaders.iter().map(|(node, addr)| {
                let requester = &requester;
                async move { (node, ping(requester.as_ref(), node, *addr, every).await) }
            }))
            .await;
            let watched: HashSet<String> = answers
                .iter()
                .filter(|(_, answer)| *answer != Answer::Unsupported)
                .map(|(node, _)| (*node).clone())
                .collect();
            let answered: HashSet<String> = answers
                .iter()
                .filter(|(_, answer)| *answer == Answer::Pong)
                .map(|(node, _)| (*node).clone())
                .collect();
            suspects.publish(silence.round(&watched, &answered, Instant::now()));
        }
    }))
}

#[cfg(test)]
mod tests;

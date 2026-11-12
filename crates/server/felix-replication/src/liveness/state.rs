//! What store liveness decides, as a pure function of what was heard and
//! when. No I/O and no clock of its own, so every rule is tested on its own.

use std::time::Duration;

use felix_common::random::{HashMap, HashSet};
use tokio::time::Instant;

/// The timers. The defaults are the decided starting values
/// (`docs/per-shard-raft.md`, Questions, 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessConfig {
    /// How often the disk is probed and a heartbeat is sent to each peer.
    pub interval: Duration,
    /// Silence from a peer past this withdraws support for it.
    pub withdraw_after: Duration,
    /// This broker stops sending once no probe has finished for this long.
    /// Longer than `withdraw_after` on purpose: one slow flush that still
    /// finishes inside it silences nobody.
    pub stall_limit: Duration,
    /// The longest election timeout. A restarted member is held for
    /// `withdraw_after` plus this, unless its leader is heard first.
    pub election_timeout: Duration,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            withdraw_after: Duration::from_millis(1500),
            stall_limit: Duration::from_secs(5),
            election_timeout: Duration::from_secs(2),
        }
    }
}

impl LivenessConfig {
    /// How long after its start a member is held when no leader is heard.
    pub(crate) fn hold_for(&self) -> Duration {
        self.withdraw_after + self.election_timeout
    }
}

/// One thing store liveness would have done. In shadow mode it is recorded
/// and nothing else happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Support for `peer` is withdrawn: it was silent for `withdraw_after`.
    /// The groups shared with it would wake.
    Withdraw { peer: String },
    /// `peer` is heard again after a withdrawal.
    Restore { peer: String },
    /// `peer` showed a new store epoch: it restarted. The groups shared
    /// with it would wake.
    Restarted { peer: String, epoch: u64 },
    /// No disk probe has finished within the stall limit, so this broker
    /// stops sending heartbeats.
    Silenced,
    /// A probe finished again, so this broker sends again.
    Resumed,
    /// The hold on this broker's restarted members ended for the groups
    /// `leader` leads, because it was heard.
    HoldReleased { leader: String },
    /// The hold ended on its time limit with these leaders never heard.
    HoldExpired { unheard: Vec<String> },
}

#[derive(Debug)]
struct Peer {
    /// The epoch it last showed.
    epoch: Option<u64>,
    /// When it was last heard, or first watched if never.
    heard: Instant,
    supported: bool,
}

/// What this broker knows about its own disk and its peers' stores.
#[derive(Debug)]
pub(crate) struct State {
    config: LivenessConfig,
    /// When the last disk probe finished.
    probed: Option<Instant>,
    sending: bool,
    peers: HashMap<String, Peer>,
    hold: RestartHold,
}

impl State {
    pub(crate) fn new(config: LivenessConfig, started: Instant) -> Self {
        Self {
            config,
            probed: None,
            sending: false,
            peers: HashMap::default(),
            hold: RestartHold::new(started + config.hold_for()),
        }
    }

    /// A disk probe finished at `now`.
    pub(crate) fn probe_finished(&mut self, now: Instant) {
        self.probed = Some(now);
    }

    /// Whether this broker may send heartbeats at `now`: only while a probe
    /// finished within the stall limit. Nothing is sent before the first.
    pub(crate) fn may_send(&self, now: Instant) -> bool {
        self.probed
            .is_some_and(|at| now.saturating_duration_since(at) < self.config.stall_limit)
    }

    /// A heartbeat from `peer` carrying `epoch` arrived at `now`.
    pub(crate) fn heard(&mut self, peer: &str, epoch: u64, now: Instant) -> Vec<Decision> {
        let mut decisions = Vec::new();
        let known = self.peers.entry(peer.to_string()).or_insert(Peer {
            epoch: None,
            heard: now,
            supported: true,
        });
        match known.epoch {
            // Sent before the restart this broker already knows of. It says
            // nothing about the store that is running now.
            Some(seen) if epoch < seen && known.supported => return decisions,
            // A lower epoch from a peer that had gone silent is a store that
            // lost its epoch file (a replaced disk), not an old message:
            // those stop arriving with the process that sent them.
            Some(seen) if epoch != seen => decisions.push(Decision::Restarted {
                peer: peer.to_string(),
                epoch,
            }),
            _ => {}
        }
        known.epoch = Some(epoch);
        known.heard = now;
        if !known.supported {
            known.supported = true;
            decisions.push(Decision::Restore {
                peer: peer.to_string(),
            });
        }
        if self.hold.contact(peer, now) {
            decisions.push(Decision::HoldReleased {
                leader: peer.to_string(),
            });
        }
        decisions
    }

    /// Fold the passing of time into the record. `watched` are the peers this
    /// broker shares a shard with and that run store liveness; `leaders` are
    /// those of them leading a shard this broker follows. A peer first
    /// watched now gets a whole window; one no longer watched is forgotten.
    pub(crate) fn tick(
        &mut self,
        watched: &HashSet<String>,
        leaders: &HashSet<String>,
        now: Instant,
    ) -> Vec<Decision> {
        let mut decisions = Vec::new();
        let sending = self.may_send(now);
        if sending != self.sending {
            // Before the first probe there is nothing to call a stall.
            if sending {
                decisions.push(Decision::Resumed);
            } else if self.probed.is_some() {
                decisions.push(Decision::Silenced);
            }
            self.sending = sending;
        }
        self.peers.retain(|node, _| watched.contains(node));
        let mut withdrawn = Vec::new();
        for node in watched {
            let peer = self.peers.entry(node.clone()).or_insert(Peer {
                epoch: None,
                heard: now,
                supported: true,
            });
            if peer.supported
                && now.saturating_duration_since(peer.heard) >= self.config.withdraw_after
            {
                peer.supported = false;
                withdrawn.push(node.clone());
            }
        }
        // The map's order is not stable; the record should be.
        withdrawn.sort();
        decisions.extend(
            withdrawn
                .into_iter()
                .map(|peer| Decision::Withdraw { peer }),
        );
        if let Some(unheard) = self.hold.expire(leaders, now) {
            decisions.push(Decision::HoldExpired { unheard });
        }
        decisions
    }

    /// Whether `peer` has this broker's support. A peer it does not watch
    /// has none to withdraw, so the answer is `None`.
    pub(crate) fn supports(&self, peer: &str) -> Option<bool> {
        self.peers.get(peer).map(|peer| peer.supported)
    }

    /// The epoch `peer` last showed.
    pub(crate) fn epoch_of(&self, peer: &str) -> Option<u64> {
        self.peers.get(peer).and_then(|peer| peer.epoch)
    }

    /// Whether a member whose group `leader` leads would be given no ticks
    /// at `now`.
    pub(crate) fn holds(&self, leader: &str, now: Instant) -> bool {
        self.hold.holds(leader, now)
    }

    pub(crate) fn withdrawn(&self) -> usize {
        self.peers.values().filter(|peer| !peer.supported).count()
    }

    pub(crate) fn watched(&self) -> usize {
        self.peers.len()
    }
}

/// The hold on members that start with this broker: no ticks until their
/// leader is heard, or until the hold's time limit. Without it a restarted
/// member would campaign before a sleeping leader has noticed its new epoch.
#[derive(Debug)]
struct RestartHold {
    until: Instant,
    heard: HashSet<String>,
    expired: bool,
}

impl RestartHold {
    fn new(until: Instant) -> Self {
        Self {
            until,
            heard: HashSet::default(),
            expired: false,
        }
    }

    fn holds(&self, leader: &str, now: Instant) -> bool {
        now < self.until && !self.heard.contains(leader)
    }

    /// `leader` was heard. True the first time, while the hold is running.
    fn contact(&mut self, leader: &str, now: Instant) -> bool {
        now < self.until && self.heard.insert(leader.to_string())
    }

    /// Once, at the time limit: the leaders never heard, if any.
    fn expire(&mut self, leaders: &HashSet<String>, now: Instant) -> Option<Vec<String>> {
        if self.expired || now < self.until {
            return None;
        }
        self.expired = true;
        let mut unheard: Vec<String> = leaders
            .iter()
            .filter(|leader| !self.heard.contains(*leader))
            .cloned()
            .collect();
        unheard.sort();
        (!unheard.is_empty()).then_some(unheard)
    }
}

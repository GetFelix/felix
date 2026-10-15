//! The command level every member of the group can apply, in the manner of
//! KRaft's `metadata.version`.
//!
//! A member that cannot decode a committed command records `Unsupported`
//! while the leader applies it, and their states part ways. A StatefulSet
//! rollout cannot promise to upgrade the leader last, so instead of relying
//! on the order, a command newer than the baseline is proposed only once
//! every member of the current membership, learners included, has said it
//! can apply it. Members say so on the `standing` route they already serve
//! for joins.
//!
//! A member that cannot be reached counts at what it last reported, or 0 if
//! it never has: an unknown member holds a newer command back, it never lets
//! one through. The member asking says its own level on the probe, so every
//! member has heard the leader's. A leader that dies is then not an unknown
//! to the one elected after it, which would otherwise hold back every newer
//! command, the leader's soft state included, for as long as it stays down.
use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::Instant;

use super::join::Standing;
use super::{NodeId, RaftHandle};

/// How long a computed level is served without asking again. The level
/// rises within about this long of the last member upgrading.
const FRESH_FOR: Duration = Duration::from_secs(2);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
/// How often a leader asks every member its level again, for the
/// replication hold ([`super::hold`]).
const REFRESH_EVERY: Duration = Duration::from_secs(10);
/// How often a member that is held, or lost its log, is asked again.
const HELD_RETRY: Duration = Duration::from_millis(500);

/// Who is asking for a member's standing, and the level it can apply.
/// Absent from a member that predates it.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct Asker {
    pub(super) from: Option<NodeId>,
    pub(super) version: Option<u16>,
}

#[derive(Default)]
pub(super) struct Versions {
    reported: BTreeMap<NodeId, u16>,
    checked: Option<(Instant, Level)>,
    refreshing: bool,
    /// Members that lost their log since they last reported, each with the
    /// [`Self::epoch`] it was marked at.
    stale: BTreeMap<NodeId, u64>,
    epoch: u64,
}

impl Versions {
    pub(super) fn reported(&self, id: NodeId) -> Option<u16> {
        self.reported.get(&id).copied()
    }

    /// Remember what `id` answered, and work the level out again next time.
    pub(super) fn record(&mut self, id: NodeId, version: u16, asked_at: u64) {
        self.heard(id, version, asked_at);
        self.checked = None;
    }

    /// `id` answered `version` to a probe sent at `asked_at`. An answer sent
    /// before the member lost its log does not clear the mark.
    fn heard(&mut self, id: NodeId, version: u16, asked_at: u64) {
        self.reported.insert(id, version);
        if self
            .stale
            .get(&id)
            .is_some_and(|marked| *marked <= asked_at)
        {
            self.stale.remove(&id);
        }
    }

    /// For [`Self::heard`]: taken before a probe is sent.
    pub(super) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// `id` lost its log: its level is unknown until it answers again.
    pub(super) fn mark_stale(&mut self, id: NodeId) {
        self.epoch += 1;
        self.stale.insert(id, self.epoch);
    }

    pub(super) fn is_stale(&self, id: NodeId) -> bool {
        self.stale.contains_key(&id)
    }
}

/// Ask the member at `base` for the level it can apply. `None` when it does
/// not answer.
pub(super) async fn probe(client: &reqwest::Client, base: &str, asker: &Asker) -> Option<u16> {
    let response = client
        .get(format!("{base}/internal/raft/standing"))
        .query(asker)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    Some(response.json::<Standing>().await.ok()?.version)
}

/// The levels the members reported, as of one refresh.
#[derive(Debug, Clone, Copy)]
struct Level {
    /// The lowest level any member that has answered can apply, this one
    /// included.
    answered: u16,
    /// Some member has never answered since this process started.
    unknown: bool,
}

impl Level {
    /// The level, counting a member that never answered at `unknown`.
    fn counting_unknown_at(self, unknown: u16) -> u16 {
        if self.unknown {
            self.answered.min(unknown)
        } else {
            self.answered
        }
    }
}

impl RaftHandle {
    /// The lowest command level any member of the current membership can
    /// apply, this one included. A caller proposes a command only if its
    /// level is at most this.
    ///
    /// Only the first call waits for the members to answer. After that a
    /// stale level is served while a refresh runs in the background: the
    /// leader asks on every broker heartbeat, and one member being down must
    /// not add a probe timeout to each of them.
    pub async fn cluster_version(&self) -> u16 {
        self.level().await.counting_unknown_at(0)
    }

    /// [`Self::cluster_version`], counting a member that has never answered
    /// since this process started at `unknown` rather than at 0. For a
    /// caller that knows a floor every member reached, such as a finalized
    /// feature that needed it. A member that answered counts at what it
    /// said, however low.
    pub async fn cluster_version_with_unknown_at(&self, unknown: u16) -> u16 {
        self.level().await.counting_unknown_at(unknown)
    }

    async fn level(&self) -> Level {
        let cached = {
            let mut versions = self.versions.lock().expect("versions lock");
            match versions.checked {
                Some((at, level)) if at.elapsed() < FRESH_FOR => return level,
                Some((_, level)) => {
                    if !versions.refreshing {
                        versions.refreshing = true;
                        let handle = self.clone();
                        tokio::spawn(async move { handle.refresh_versions().await });
                    }
                    Some(level)
                }
                None => None,
            }
        };
        match cached {
            Some(level) => level,
            None => self.refresh_versions().await,
        }
    }

    async fn refresh_versions(&self) -> Level {
        let members = self.probe_members().await;
        let mut versions = self.versions.lock().expect("versions lock");
        let level = Level {
            answered: members
                .iter()
                .filter_map(|id| versions.reported.get(id).copied())
                .fold(self.app.version(), u16::min),
            unknown: members.iter().any(|id| !versions.reported.contains_key(id)),
        };
        versions.checked = Some((Instant::now(), level));
        versions.refreshing = false;
        level
    }

    /// Ask every other member of the current membership its level, and
    /// remember the answers. Returns who was asked.
    async fn probe_members(&self) -> Vec<NodeId> {
        let members: Vec<(NodeId, String)> = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow();
            metrics
                .membership_config
                .membership()
                .nodes()
                .filter(|(id, _)| **id != self.id)
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect()
        };
        let asked_at = self.versions.lock().expect("versions lock").epoch();
        let mut probes = tokio::task::JoinSet::new();
        for (id, addr) in &members {
            let Some(base) = self.peer_url(Some(*id), Some(addr)) else {
                continue;
            };
            let (client, asker, id) = (self.forward.clone(), self.asker(), *id);
            probes.spawn(async move { Some((id, probe(&client, &base, &asker).await?)) });
        }
        let mut answers = Vec::new();
        while let Some(joined) = probes.join_next().await {
            if let Ok(Some(answer)) = joined {
                answers.push(answer);
            }
        }
        let mut versions = self.versions.lock().expect("versions lock");
        for (id, version) in answers {
            versions.heard(id, version, asked_at);
        }
        members.into_iter().map(|(id, _)| id).collect()
    }

    /// Ask one member at `base` for the level it can apply, and remember the
    /// answer. `None` when it does not answer.
    pub(super) async fn probe_version(&self, id: NodeId, base: &str) -> Option<u16> {
        let asked_at = self.versions.lock().expect("versions lock").epoch();
        let version = probe(&self.forward, base, &self.asker()).await?;
        self.versions
            .lock()
            .expect("versions lock")
            .record(id, version, asked_at);
        Some(version)
    }

    /// While this member leads, keep every member's reported level fresh
    /// for the replication hold: on taking the lead, every
    /// [`REFRESH_EVERY`] ([`HELD_RETRY`] while a member is held), and at
    /// once when a member lost its log. Returns when the node stops.
    pub(super) async fn watch_versions(self) {
        let mut metrics = self.raft.metrics();
        let mut next: Option<Instant> = None;
        loop {
            if self.is_stopped() {
                return;
            }
            let leading = metrics.borrow_and_update().current_leader == Some(self.id);
            if !leading {
                next = None;
            } else if next.is_none_or(|at| at <= Instant::now()) {
                self.probe_members().await;
                // A held member is asked often, so it is fed again soon
                // after it reports the floor.
                let settled = self
                    .versions
                    .lock()
                    .expect("versions lock")
                    .stale
                    .is_empty()
                    && !self.hold.any_held();
                let wait = if settled { REFRESH_EVERY } else { HELD_RETRY };
                next = Some(Instant::now() + wait);
            }
            let sleep = tokio::time::sleep_until(
                next.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600)),
            );
            tokio::select! {
                changed = metrics.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                () = self.hold.wake.notified() => {
                    if leading {
                        next = Some(Instant::now());
                    }
                }
                () = sleep => {}
            }
        }
    }

    fn asker(&self) -> Asker {
        Asker {
            from: Some(self.id),
            version: Some(self.app.version()),
        }
    }

    /// Forget every level the members reported, as a restart does.
    #[cfg(test)]
    pub(crate) fn forget_reported_versions(&self) {
        let mut versions = self.versions.lock().expect("versions lock");
        versions.reported.clear();
        versions.stale.clear();
        versions.checked = None;
    }

    /// Remember the level a probing member said it is at.
    pub(super) fn note_asker(&self, asker: Asker) {
        let (Some(from), Some(version)) = (asker.from, asker.version) else {
            return;
        };
        if from == self.id {
            return;
        }
        let mut versions = self.versions.lock().expect("versions lock");
        let now = versions.epoch();
        versions.heard(from, version, now);
    }
}

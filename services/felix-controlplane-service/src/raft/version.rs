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
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tokio::time::Instant;

use super::join::Standing;
use super::{NodeId, RaftHandle};

/// How long a computed level is served without asking again. The level
/// rises within about this long of the last member upgrading.
const FRESH_FOR: Duration = Duration::from_secs(2);
pub(super) const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
/// Bound on the first probe that settles one member's fresh mark
/// ([`super::hold`]). Well under the election timeout, so a member at the
/// floor that answers promptly is fed again before it could time out.
pub(super) const ASK_TIMEOUT: Duration = Duration::from_millis(200);
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
    /// Members whose reported level is in doubt until they answer again,
    /// each with the [`Self::epoch`] it was marked at.
    stale: BTreeMap<NodeId, (u64, Doubt)>,
    epoch: u64,
}

/// Why a member's reported level cannot be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Doubt {
    /// It conflicted below an entry it acknowledged. Held until it answers.
    LostLog,
    /// It conflicted before acknowledging anything to a replication client,
    /// so it may have lost its log or just be behind. Held until one probe
    /// sent after the mark settles it; a probe that fails releases it, since
    /// silence must never hold a healthy member.
    Unconfirmed,
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
            .is_some_and(|(marked, _)| *marked <= asked_at)
        {
            self.stale.remove(&id);
        }
    }

    /// A probe of `id` sent at `asked_at` came back with `answer`, `None`
    /// when it got none. Silence settles only a [`Doubt::Unconfirmed`] mark
    /// made before the probe, so it never holds a member.
    pub(super) fn settle(&mut self, id: NodeId, answer: Option<u16>, asked_at: u64) {
        match answer {
            Some(version) => self.heard(id, version, asked_at),
            None => {
                if self.stale.get(&id).is_some_and(|(marked, doubt)| {
                    *doubt == Doubt::Unconfirmed && *marked <= asked_at
                }) {
                    self.stale.remove(&id);
                }
            }
        }
    }

    /// For [`Self::heard`]: taken before a probe is sent.
    pub(super) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// `id` lost its log: its level is unknown until it answers again.
    pub(super) fn mark_stale(&mut self, id: NodeId) {
        self.epoch += 1;
        self.stale.insert(id, (self.epoch, Doubt::LostLog));
    }

    /// `id` may have lost its log. False when it is already marked; never
    /// weakens a [`Doubt::LostLog`] mark.
    pub(super) fn mark_unconfirmed(&mut self, id: NodeId) -> bool {
        if self.stale.contains_key(&id) {
            return false;
        }
        self.epoch += 1;
        self.stale.insert(id, (self.epoch, Doubt::Unconfirmed));
        true
    }

    pub(super) fn doubt(&self, id: NodeId) -> Option<Doubt> {
        self.stale.get(&id).map(|(_, doubt)| *doubt)
    }

    #[cfg(test)]
    pub(super) fn is_stale(&self, id: NodeId) -> bool {
        self.stale.contains_key(&id)
    }

    /// Forget the marks of members `keep` rejects.
    pub(super) fn retain_marks(&mut self, keep: impl Fn(NodeId) -> bool) {
        self.stale.retain(|id, _| keep(*id));
    }
}

/// Ask the member at `base` for the level it can apply. `None` when it does
/// not answer within `timeout`.
pub(super) async fn probe(
    client: &reqwest::Client,
    base: &str,
    asker: &Asker,
    timeout: Duration,
) -> Option<u16> {
    let response = client
        .get(format!("{base}/internal/raft/standing"))
        .query(asker)
        .timeout(timeout)
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
            probes.spawn(async move { (id, probe(&client, &base, &asker, PROBE_TIMEOUT).await) });
        }
        let mut answers = Vec::new();
        while let Some(joined) = probes.join_next().await {
            if let Ok(answer) = joined {
                answers.push(answer);
            }
        }
        let mut versions = self.versions.lock().expect("versions lock");
        for (id, version) in answers {
            versions.settle(id, version, asked_at);
        }
        members.into_iter().map(|(id, _)| id).collect()
    }

    /// Ask one member at `base` for the level it can apply, and remember the
    /// answer. `None` when it does not answer.
    pub(super) async fn probe_version(&self, id: NodeId, base: &str) -> Option<u16> {
        let asked_at = self.versions.lock().expect("versions lock").epoch();
        let version = probe(&self.forward, base, &self.asker(), PROBE_TIMEOUT).await;
        let mut versions = self.versions.lock().expect("versions lock");
        match version {
            Some(version) => versions.record(id, version, asked_at),
            None => versions.settle(id, None, asked_at),
        }
        version
    }

    /// While this member leads, keep every member's reported level fresh
    /// for the replication hold: on taking the lead, every
    /// [`REFRESH_EVERY`] ([`HELD_RETRY`] while a member is held), and at
    /// once when a member is marked or held. While not leading, clears
    /// whatever a late send left. Returns when the node stops.
    pub(super) async fn watch_versions(self) {
        let mut metrics = self.raft.metrics();
        let mut next: Option<Instant> = None;
        loop {
            if self.is_stopped() {
                return;
            }
            let (leading, members) = {
                let metrics = metrics.borrow_and_update();
                let members: BTreeSet<NodeId> = metrics
                    .membership_config
                    .membership()
                    .nodes()
                    .map(|(id, _)| *id)
                    .collect();
                (metrics.current_leader == Some(self.id), members)
            };
            if !leading {
                // The marks and holds belong to this leader's replication.
                self.hold.clear();
                next = None;
            } else {
                self.hold.retain(&members);
            }
            if leading && next.is_none_or(|at| at <= Instant::now()) {
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

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

/// Who is asking for a member's standing, and the level it can apply.
/// Absent from a member that predates it.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct Asker {
    from: Option<NodeId>,
    version: Option<u16>,
}

#[derive(Default)]
pub(super) struct Versions {
    reported: BTreeMap<NodeId, u16>,
    checked: Option<(Instant, u16)>,
    refreshing: bool,
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

    async fn refresh_versions(&self) -> u16 {
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
        let mut probes = tokio::task::JoinSet::new();
        for (id, addr) in &members {
            let Some(base) = self.peer_url(Some(*id), Some(addr)) else {
                continue;
            };
            let request = self
                .forward
                .get(format!("{base}/internal/raft/standing"))
                .query(&Asker {
                    from: Some(self.id),
                    version: Some(self.app.version()),
                })
                .timeout(PROBE_TIMEOUT);
            let id = *id;
            probes.spawn(async move {
                let response = request.send().await.ok()?;
                if !response.status().is_success() {
                    return None;
                }
                let standing = response.json::<Standing>().await.ok()?;
                Some((id, standing.version))
            });
        }
        let mut answers = Vec::new();
        while let Some(joined) = probes.join_next().await {
            if let Ok(Some(answer)) = joined {
                answers.push(answer);
            }
        }
        let mut versions = self.versions.lock().expect("versions lock");
        versions.reported.extend(answers);
        let level = members
            .iter()
            .map(|(id, _)| versions.reported.get(id).copied().unwrap_or(0))
            .fold(self.app.version(), u16::min);
        versions.checked = Some((Instant::now(), level));
        versions.refreshing = false;
        level
    }

    /// Remember the level a probing member said it is at.
    pub(super) fn note_asker(&self, asker: Asker) {
        let (Some(from), Some(version)) = (asker.from, asker.version) else {
            return;
        };
        if from == self.id {
            return;
        }
        self.versions
            .lock()
            .expect("versions lock")
            .reported
            .insert(from, version);
    }
}

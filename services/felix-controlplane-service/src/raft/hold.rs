//! Holding back replication to a member below the group's join floor.
//!
//! [`super::RaftHandle::add_learner`] refuses a newcomer below
//! [`super::AppStateMachine::join_floor`], but a member already in the
//! membership never asks again: one whose volume was wiped and that restarts
//! on an older build under the same id would be fed the log and a snapshot it
//! cannot apply, and part ways with the others. So while there is evidence a
//! member is below the floor, an append carrying entries or a snapshot to it
//! fails as `Unreachable`. Commits still need only a majority.
//!
//! A held member that is behind gets nothing at all, heartbeats included:
//! openraft retries the held append or snapshot with backoff and never sends
//! a heartbeat in its place. It may time out and call elections, which the
//! others refuse while they hear from the leader, and once released its
//! first append can come back with a higher vote and depose the leader: one
//! extra election per long hold. A held member gets heartbeats only while it
//! is caught up and nothing new has been written.
//!
//! The evidence is the level the member last reported, which the leader's
//! version watch ([`super::version`]) keeps fresh in the background; a send
//! never waits on a probe. A member that has not answered is not held, since
//! holding a healthy voter on a slow probe would cost the group its quorum.
//! A member that answers an append with a conflict below an entry it already
//! acknowledged has lost its log, possibly to a restart on another build, so
//! it alone is held until it reports its level again. A conflict before a
//! replication client has seen any acknowledgement cannot tell a lost log
//! from a member that is behind, so it holds that member only until one
//! probe sent after it settles: a level at the floor or no answer releases
//! it. Each mark is settled by a probe of that member alone, not by the
//! watch's pass over every member, with one probe in flight per member. The
//! probe waits [`super::version::ASK_TIMEOUT`] and, on silence, asks once
//! more for up to [`super::version::PROBE_TIMEOUT`], so a slow member is not
//! released by silence. Only the held member waits on that.
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use super::version::{ASK_TIMEOUT, Asker, Doubt, PROBE_TIMEOUT, Versions, probe};
use super::{AppStateMachine, NodeId};

pub(super) struct Hold {
    app: Arc<dyn AppStateMachine>,
    versions: Arc<Mutex<Versions>>,
    /// Held now; logged and counted once per hold.
    held: Mutex<BTreeSet<NodeId>>,
    /// Members with an [`Self::ask`] in flight.
    asking: Mutex<BTreeSet<NodeId>>,
    /// Wakes the leader's version watch: to retry a marked member, and to
    /// clear a hold made by a late send after this member stopped leading.
    pub(super) wake: tokio::sync::Notify,
}

/// Why a member is not being fed.
#[derive(Debug)]
pub(super) struct BelowFloor {
    why: Why,
    floor: u16,
}

#[derive(Debug, Clone, Copy)]
enum Why {
    Reported(u16),
    Doubted(Doubt),
}

impl std::fmt::Display for BelowFloor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let floor = self.floor;
        match self.why {
            Why::Reported(version) => write!(
                f,
                "held: member supports metadata version {version}, below the group's {floor}"
            ),
            Why::Doubted(Doubt::LostLog) => write!(
                f,
                "held: member lost its log and has not reported a metadata version since; the \
                 group is at {floor}"
            ),
            Why::Doubted(Doubt::Unconfirmed) => write!(
                f,
                "held: member conflicted before acknowledging an append; asking its metadata \
                 version against the group's {floor}"
            ),
        }
    }
}

impl std::error::Error for BelowFloor {}

impl Hold {
    pub(super) fn new(app: Arc<dyn AppStateMachine>, versions: Arc<Mutex<Versions>>) -> Self {
        Self {
            app,
            versions,
            held: Mutex::default(),
            asking: Mutex::default(),
            wake: tokio::sync::Notify::new(),
        }
    }

    /// `Err` while member `target` must not be sent entries or a snapshot.
    /// Reads only cached state.
    pub(super) fn check(&self, target: NodeId) -> Result<(), BelowFloor> {
        let floor = self.app.join_floor();
        if floor == 0 {
            self.release(target);
            return Ok(());
        }
        let (reported, doubt) = {
            let versions = self.versions.lock().expect("versions lock");
            (versions.reported(target), versions.doubt(target))
        };
        if let Some(doubt) = doubt {
            return Err(self.hold(target, Why::Doubted(doubt), floor));
        }
        match reported {
            Some(version) if version < floor => {
                Err(self.hold(target, Why::Reported(version), floor))
            }
            _ => {
                self.release(target);
                Ok(())
            }
        }
    }

    /// Member `target` answered a conflict below an entry it had
    /// acknowledged: hold it until it reports its level again. True when
    /// the caller should [`Self::ask`] it now; false while an ask of it is
    /// still in flight.
    pub(super) fn lost_log(&self, target: NodeId) -> bool {
        if self.app.join_floor() == 0 {
            return false;
        }
        self.versions
            .lock()
            .expect("versions lock")
            .mark_stale(target);
        self.wake.notify_one();
        self.start_ask(target)
    }

    /// Member `target` conflicted before acknowledging anything to this
    /// replication client: hold it until a probe sent from now on answers
    /// or fails. True when the caller should [`Self::ask`] it now; false
    /// while an earlier mark is still unsettled or an ask is in flight.
    pub(super) fn unconfirmed(&self, target: NodeId) -> bool {
        if self.app.join_floor() == 0 {
            return false;
        }
        let marked = self
            .versions
            .lock()
            .expect("versions lock")
            .mark_unconfirmed(target);
        if marked {
            self.wake.notify_one();
        }
        marked && self.start_ask(target)
    }

    /// Probe member `target` at `base` for its level and settle its mark
    /// with the answer, or with silence. Only for a caller that
    /// [`Self::lost_log`] or [`Self::unconfirmed`] told to ask.
    pub(super) async fn ask(&self, target: NodeId, client: &reqwest::Client, base: &str) {
        let asked_at = self.versions.lock().expect("versions lock").epoch();
        // Silence releases a first-conflict mark, so a member that is only
        // slow gets the watch's longer bound before it counts as silent.
        let answer = match probe(client, base, &Asker::default(), ASK_TIMEOUT).await {
            Some(version) => Some(version),
            None => probe(client, base, &Asker::default(), PROBE_TIMEOUT).await,
        };
        self.versions
            .lock()
            .expect("versions lock")
            .settle(target, answer, asked_at);
        self.asking.lock().expect("asking lock").remove(&target);
    }

    fn start_ask(&self, target: NodeId) -> bool {
        self.asking.lock().expect("asking lock").insert(target)
    }

    /// Forget the marks and holds of members no longer in `members`.
    pub(super) fn retain(&self, members: &BTreeSet<NodeId>) {
        self.versions
            .lock()
            .expect("versions lock")
            .retain_marks(|id| members.contains(&id));
        let gone: Vec<NodeId> = self
            .held
            .lock()
            .expect("hold lock")
            .iter()
            .filter(|id| !members.contains(id))
            .copied()
            .collect();
        for id in gone {
            self.forget(id);
        }
    }

    /// Forget every mark and hold, as this member stops leading.
    pub(super) fn clear(&self) {
        self.versions
            .lock()
            .expect("versions lock")
            .retain_marks(|_| false);
        let held = std::mem::take(&mut *self.held.lock().expect("hold lock"));
        for id in held {
            set_gauge(id, false);
        }
    }

    pub(super) fn any_held(&self) -> bool {
        !self.held.lock().expect("hold lock").is_empty()
    }

    fn hold(&self, target: NodeId, why: Why, floor: u16) -> BelowFloor {
        if self.held.lock().expect("hold lock").insert(target) {
            set_gauge(target, true);
            self.wake.notify_one();
            match why {
                Why::Reported(version) => tracing::warn!(
                    member = target,
                    reported = version,
                    floor,
                    "not replicating to a raft member below the group's metadata version; it \
                     would refuse entries the others apply. Upgrade it to a release at that \
                     version or later"
                ),
                Why::Doubted(Doubt::LostLog) => tracing::warn!(
                    member = target,
                    floor,
                    "raft member lost its log; not replicating to it until it reports a \
                     metadata version at the group's"
                ),
                Why::Doubted(Doubt::Unconfirmed) => tracing::info!(
                    member = target,
                    floor,
                    "raft member conflicted before acknowledging an append; asking its metadata \
                     version before replicating to it"
                ),
            }
        }
        BelowFloor { why, floor }
    }

    fn release(&self, target: NodeId) {
        if self.held.lock().expect("hold lock").remove(&target) {
            set_gauge(target, false);
            tracing::info!(
                member = target,
                "raft member reached the group's metadata version; replicating to it again"
            );
        }
    }

    fn forget(&self, target: NodeId) {
        if self.held.lock().expect("hold lock").remove(&target) {
            set_gauge(target, false);
        }
    }
}

fn set_gauge(member: NodeId, held: bool) {
    metrics::gauge!("felix_meta_raft_member_below_floor", "member" => member.to_string())
        .set(if held { 1.0 } else { 0.0 });
}

#[cfg(test)]
pub(super) mod tests;

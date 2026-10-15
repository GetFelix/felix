//! Holding back replication to a member below the group's join floor.
//!
//! [`super::RaftHandle::add_learner`] refuses a newcomer below
//! [`super::AppStateMachine::join_floor`], but a member already in the
//! membership never asks again: one whose volume was wiped and that restarts
//! on an older build under the same id would be fed the log and a snapshot it
//! cannot apply, and part ways with the others. So an append or snapshot to a
//! member fails as `Unreachable` while there is evidence it is below the
//! floor. To openraft it is then a follower that is down: commits still need
//! only a majority.
//!
//! The evidence is the level the member last reported, which the leader's
//! version watch ([`super::version`]) keeps fresh in the background; a send
//! never waits on a probe. A member that has not answered is not held, since
//! holding a healthy voter on a slow probe would cost the group its quorum.
//! A member that answers an append with a conflict below an entry it already
//! acknowledged has lost its log, possibly to a restart on another build, so
//! it alone is held until it reports its level again.
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use super::version::Versions;
use super::{AppStateMachine, NodeId};

pub(super) struct Hold {
    app: Arc<dyn AppStateMachine>,
    versions: Arc<Mutex<Versions>>,
    /// Held now; logged and counted once per hold.
    held: Mutex<BTreeSet<NodeId>>,
    /// Asks the leader's version watch to probe the members that lost
    /// their log.
    pub(super) wake: tokio::sync::Notify,
}

/// Why a member is not being fed.
#[derive(Debug)]
pub(super) struct BelowFloor {
    /// `None` when the member lost its log and has not reported since.
    reported: Option<u16>,
    floor: u16,
}

impl std::fmt::Display for BelowFloor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reported {
            Some(version) => write!(
                f,
                "held: member supports metadata version {version}, below the group's {}",
                self.floor
            ),
            None => write!(
                f,
                "held: member lost its log and has not reported a metadata version since; the \
                 group is at {}",
                self.floor
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
        let (reported, lost_log) = {
            let versions = self.versions.lock().expect("versions lock");
            (versions.reported(target), versions.is_stale(target))
        };
        if lost_log {
            return Err(self.hold(target, None, floor));
        }
        match reported {
            Some(version) if version < floor => Err(self.hold(target, Some(version), floor)),
            _ => {
                self.release(target);
                Ok(())
            }
        }
    }

    /// Member `target` answered a conflict below an entry it had
    /// acknowledged: hold it until it reports its level again, and have
    /// that asked now.
    pub(super) fn lost_log(&self, target: NodeId) {
        if self.app.join_floor() == 0 {
            return;
        }
        self.versions
            .lock()
            .expect("versions lock")
            .mark_stale(target);
        self.wake.notify_one();
    }

    pub(super) fn any_held(&self) -> bool {
        !self.held.lock().expect("hold lock").is_empty()
    }

    fn hold(&self, target: NodeId, reported: Option<u16>, floor: u16) -> BelowFloor {
        if self.held.lock().expect("hold lock").insert(target) {
            metrics::gauge!("felix_meta_raft_member_below_floor", "member" => target.to_string())
                .set(1.0);
            match reported {
                Some(version) => tracing::warn!(
                    member = target,
                    reported = version,
                    floor,
                    "not replicating to a raft member below the group's metadata version; it \
                     would refuse entries the others apply. Upgrade it to a release at that \
                     version or later"
                ),
                None => tracing::warn!(
                    member = target,
                    floor,
                    "raft member lost its log; not replicating to it until it reports a \
                     metadata version at the group's"
                ),
            }
        }
        BelowFloor { reported, floor }
    }

    fn release(&self, target: NodeId) {
        if self.held.lock().expect("hold lock").remove(&target) {
            metrics::gauge!("felix_meta_raft_member_below_floor", "member" => target.to_string())
                .set(0.0);
            tracing::info!(
                member = target,
                "raft member reached the group's metadata version; replicating to it again"
            );
        }
    }
}

#[cfg(test)]
pub(super) mod tests;

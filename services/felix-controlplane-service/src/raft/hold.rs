//! Holding back replication to a member below the group's join floor.
//!
//! [`super::RaftHandle::add_learner`] refuses a newcomer below
//! [`super::AppStateMachine::join_floor`], but a member already in the
//! membership never asks again: one whose volume was wiped and that restarts
//! on an older build under the same id would be fed the log and a snapshot it
//! cannot apply, and part ways with the others. So the leader checks the
//! member's reported level before each append or snapshot it sends, and while
//! the member is below the floor the send fails as `Unreachable`. To openraft
//! it is then a follower that is down: commits still need only a majority.
//!
//! A level a member reported is trusted until it answers an append with a
//! conflict, which is what a member that lost its log does; it is asked
//! again before the next send. A held member is asked on every retry, so it
//! is fed again within a backoff of reporting the floor.
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use super::version::{Asker, Versions, probe};
use super::{AppStateMachine, NodeId};

pub(super) struct Hold {
    id: NodeId,
    app: Arc<dyn AppStateMachine>,
    versions: Arc<Mutex<Versions>>,
    state: Mutex<Members>,
}

#[derive(Default)]
struct Members {
    /// Held now; logged and counted once per hold.
    held: BTreeSet<NodeId>,
    /// Answered a conflict since it last reported a level.
    recheck: BTreeSet<NodeId>,
}

/// Why a member is not being fed.
#[derive(Debug)]
pub(super) struct BelowFloor {
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
                "held: member has not reported a metadata version, and the group is at {}",
                self.floor
            ),
        }
    }
}

impl std::error::Error for BelowFloor {}

impl Hold {
    pub(super) fn new(
        id: NodeId,
        app: Arc<dyn AppStateMachine>,
        versions: Arc<Mutex<Versions>>,
    ) -> Self {
        Self {
            id,
            app,
            versions,
            state: Mutex::default(),
        }
    }

    /// `Err` while member `target` at `base` must not be sent entries or a
    /// snapshot.
    pub(super) async fn check(
        &self,
        target: NodeId,
        client: &reqwest::Client,
        base: &str,
    ) -> Result<(), BelowFloor> {
        let floor = self.app.join_floor().await;
        if floor == 0 {
            self.release(target);
            return Ok(());
        }
        let known = self
            .versions
            .lock()
            .expect("versions lock")
            .reported(target);
        let recheck = self
            .state
            .lock()
            .expect("hold lock")
            .recheck
            .contains(&target);
        let reported = match known {
            Some(version) if version >= floor && !recheck => version,
            _ => {
                let asker = Asker {
                    from: Some(self.id),
                    version: Some(self.app.version()),
                };
                match probe(client, base, &asker).await {
                    Some(version) => {
                        self.versions
                            .lock()
                            .expect("versions lock")
                            .record(target, version);
                        self.state
                            .lock()
                            .expect("hold lock")
                            .recheck
                            .remove(&target);
                        version
                    }
                    // After a conflict the old answer may no longer hold.
                    None => match known.filter(|_| !recheck) {
                        Some(version) => version,
                        None => return Err(self.hold(target, None, floor)),
                    },
                }
            }
        };
        if reported < floor {
            return Err(self.hold(target, Some(reported), floor));
        }
        self.release(target);
        Ok(())
    }

    /// Member `target` answered an append with a conflict: ask its level
    /// again before feeding it.
    pub(super) fn conflicted(&self, target: NodeId) {
        self.state.lock().expect("hold lock").recheck.insert(target);
    }

    fn hold(&self, target: NodeId, reported: Option<u16>, floor: u16) -> BelowFloor {
        if self.state.lock().expect("hold lock").held.insert(target) {
            metrics::gauge!("felix_meta_raft_member_below_floor", "member" => target.to_string())
                .set(1.0);
            tracing::warn!(
                member = target,
                reported = ?reported,
                floor,
                "not replicating to a raft member below the group's metadata version; it would \
                 refuse entries the others apply. Upgrade it to a release at that version or \
                 later"
            );
        }
        BelowFloor { reported, floor }
    }

    fn release(&self, target: NodeId) {
        if self.state.lock().expect("hold lock").held.remove(&target) {
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

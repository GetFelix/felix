//! Which members have written a record before their fsync, as they told
//! this leader, and the wait of a `Quorum` write on a stream that does not
//! fsync on commit.
//!
//! The count here releases a publisher's acknowledgement and nothing else.
//! The commit index, votes and the read bound rest on fsynced positions. See
//! `docs/per-shard-raft.md`, "Two signals from one append", and
//! `docs/formal/FelixRaftWritten.tla`.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::raft_log::LogPoint;
use openraft::{Raft, ServerState};
use tokio::sync::watch;

use super::machine::Machine;
use super::types::{MemberId, ShardTypes};

/// Most members a leader keeps a report from: what one voter set may name
/// on the wire. It bounds what peers can make this broker hold.
const KEPT: usize = 64;

/// The written positions this member was told, one per sender.
pub(crate) struct Reports {
    /// Set by the first wait. A leader asks its followers for reports only
    /// once a publisher has waited on one, so a shard whose writes never
    /// wait this way costs its followers no message.
    wanted: AtomicBool,
    heard: watch::Sender<Vec<(MemberId, LogPoint)>>,
}

impl Reports {
    pub(crate) fn new() -> Self {
        Self {
            wanted: AtomicBool::new(false),
            heard: watch::Sender::default(),
        }
    }

    /// Whether a leader's append should ask the follower for its report.
    pub(crate) fn wanted(&self) -> bool {
        self.wanted.load(Ordering::Relaxed)
    }

    /// `from` says the last entry it has written is `at`. The furthest
    /// position each sender named is kept, by term and then index, so a
    /// report that arrives late does not undo a newer one.
    pub(crate) fn heard(&self, from: MemberId, at: LogPoint) {
        let key = |point: &LogPoint| (point.term, point.index);
        self.heard.send_if_modified(|heard| {
            if let Some((_, had)) = heard.iter_mut().find(|(member, _)| *member == from) {
                let further = key(&at) > key(had);
                if further {
                    *had = at;
                }
                return further;
            }
            if heard.len() == KEPT {
                // Full of senders: the one furthest behind makes room.
                let Some(behind) = (0..heard.len()).min_by_key(|i| key(&heard[*i].1)) else {
                    return false;
                };
                if key(&heard[behind].1) >= key(&at) {
                    return false;
                }
                heard.swap_remove(behind);
            }
            heard.push((from, at));
            true
        });
    }

    /// Returns once a majority of the voters has written the entry at `at`,
    /// as far as this leader was told. It never returns otherwise: the
    /// caller races it with the commit, which also says when this member
    /// stops leading.
    pub(crate) async fn majority(&self, raft: &Raft<ShardTypes, Machine>, me: &MemberId, at: LogPoint) {
        use openraft::async_runtime::watch::WatchReceiver;
        self.wanted.store(true, Ordering::Relaxed);
        let mut heard = self.heard.subscribe();
        let mut metrics = raft.metrics();
        loop {
            // The voters as of a log that holds the entry: a membership
            // read before the entry was appended could be two changes
            // behind, and count a set that no longer elects.
            let membership = {
                let metrics = metrics.borrow_watched();
                let leads = metrics.state == ServerState::Leader && metrics.current_term == at.term;
                let holds = metrics.last_log_index.is_some_and(|last| last >= at.index);
                (leads && holds).then(|| metrics.membership_config.clone())
            };
            let moved = match membership {
                Some(membership) => {
                    let voters = membership.membership().get_joint_config();
                    if majority_wrote(voters, me, &heard.borrow_and_update(), at) {
                        return;
                    }
                    // Reports only: openraft's metrics change with every
                    // heartbeat answer and would wake every waiter.
                    heard.changed().await.is_ok()
                }
                None => metrics.changed().await.is_ok(),
            };
            if !moved {
                return std::future::pending().await;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn from(&self, member: &MemberId) -> Option<LogPoint> {
        let heard = self.heard.borrow();
        let found = heard.iter().find(|(from, _)| from == member);
        found.map(|(_, at)| *at)
    }
}

/// Whether a majority of every voter set has written the entry at `at`,
/// which the leader `me` wrote in `at.term`.
///
/// A report counts for its sender only, and only when its log id carries
/// the leader's term and is at or past the entry: an entry of that term
/// comes from this leader alone, so the sender's log matches up to there.
/// A log id of another term says nothing about what sits below it. Only
/// voters count, the leader included, and a joint membership needs both
/// sets. A member that has said nothing has written nothing.
pub(crate) fn majority_wrote(
    voters: &[BTreeSet<MemberId>],
    me: &MemberId,
    heard: &[(MemberId, LogPoint)],
    at: LogPoint,
) -> bool {
    let wrote = |member: &MemberId| {
        member == me
            || heard.iter().any(|(from, point)| {
                from == member && point.term == at.term && point.index >= at.index
            })
    };
    !voters.is_empty()
        && voters
            .iter()
            .all(|set| 2 * set.iter().filter(|member| wrote(member)).count() > set.len())
}

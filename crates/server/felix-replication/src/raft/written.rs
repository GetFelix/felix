//! Which members have written a record before their fsync, as they told
//! this leader, and the wait of a `Quorum` write on a stream that does not
//! fsync on commit.
//!
//! The count here releases a publisher's acknowledgement and nothing else.
//! The commit index and the read bound rest on fsynced positions. Votes
//! compare written logs, which is what keeps such a record from being cut
//! by a candidate without it. See
//! `docs/per-shard-raft.md`, "Two signals from one append", and
//! `docs/formal/FelixRaftWritten.tla`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::raft_log::LogPoint;
use felix_storage::waiters::Queue;
use openraft::{Raft, ServerState};
use parking_lot::Mutex;
use tokio::sync::watch;

use super::machine::Machine;
use super::network::Ask;
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
    /// This member's log counts its written entries toward the commit.
    written_commit: bool,
    heard: watch::Sender<Vec<(MemberId, LogPoint)>>,
    /// `ranks[n - 1]` wakes the waits that need `n` senders at or past
    /// their entry, keyed by the `n`th furthest position heard. A write
    /// cannot have a majority before that position reaches it, so a report
    /// wakes only the writes it could have completed.
    ranks: Mutex<Vec<Arc<Queue<Key>>>>,
}

/// A position as reports are ordered: by term, then index.
type Key = (u64, u64);

fn key(point: &LogPoint) -> Key {
    (point.term, point.index)
}

/// What one look at the reports found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Look {
    /// This member does not lead the entry's term with the entry in its
    /// log, so the voters are not known.
    Unknown,
    Wrote,
    /// Not yet, and it takes at least this many members other than the
    /// leader saying they wrote it.
    Short(usize),
}

impl Reports {
    /// `written_commit`: this member's log counts written entries toward
    /// the commit ([`RaftLog::written_commit`](felix_storage::raft_log::RaftLog::written_commit)).
    pub(crate) fn new(written_commit: bool) -> Self {
        Self {
            wanted: AtomicBool::new(false),
            written_commit,
            heard: watch::Sender::default(),
            ranks: Mutex::new(Vec::new()),
        }
    }

    /// What a leader's append that carries entries asks the follower for.
    pub(crate) fn ask(&self) -> Ask {
        match (self.wanted.load(Ordering::Relaxed), self.written_commit) {
            (false, _) => Ask::Nothing,
            (true, false) => Ask::Written,
            (true, true) => Ask::WrittenIfStrict,
        }
    }

    /// `from` says the last entry it has written is `at`. The furthest
    /// position each sender named is kept, by term and then index, so a
    /// report that arrives late does not undo a newer one.
    pub(crate) fn heard(&self, from: MemberId, at: LogPoint) {
        let moved = self.heard.send_if_modified(|heard| {
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
        if moved {
            self.advance();
        }
    }

    /// Moves every rank to the reports now held. Each rank advances on
    /// every report, even when its position is unchanged: a wait whose
    /// position was reached but whose voters had not all said so (a
    /// learner counted, another term) is woken by any report.
    fn advance(&self) {
        let mut furthest: Vec<Key> = self.heard.borrow().iter().map(|(_, at)| key(at)).collect();
        furthest.sort_unstable_by(|a, b| b.cmp(a));
        let ranks = self.ranks.lock().clone();
        // Outside the lock: a wake can drop a task that holds a queue. Two
        // reports racing here may advance a rank out of order; a lower
        // position only queues later waits by key, and every wait looks at
        // the watch itself.
        for (rank, queue) in ranks.iter().enumerate() {
            queue.advance(furthest.get(rank).copied());
        }
    }

    fn rank(&self, needed: usize) -> Arc<Queue<Key>> {
        let mut ranks = self.ranks.lock();
        while ranks.len() < needed {
            ranks.push(Arc::new(Queue::new()));
        }
        Arc::clone(&ranks[needed - 1])
    }

    /// Returns once a majority of the voters has written the entry at `at`,
    /// as far as this leader was told. It never returns otherwise: the
    /// caller races it with the commit, which also says when this member
    /// stops leading.
    pub(crate) async fn majority(
        &self,
        raft: &Raft<ShardTypes, Machine>,
        me: &MemberId,
        at: LogPoint,
    ) {
        use openraft::async_runtime::watch::WatchReceiver;
        self.wanted.store(true, Ordering::Relaxed);
        let mut metrics = raft.metrics();
        loop {
            let wrote = self
                .written(at, |heard| {
                    // The voters as of a log that holds the entry: a
                    // membership read before the entry was appended could
                    // be two changes behind, and count a set that no
                    // longer elects. Read on every look, so a change is
                    // seen at the next wake.
                    let metrics = metrics.borrow_watched();
                    let leads =
                        metrics.state == ServerState::Leader && metrics.current_term == at.term;
                    let holds = metrics.last_log_index.is_some_and(|last| last >= at.index);
                    if !(leads && holds) {
                        return Look::Unknown;
                    }
                    let voters = metrics.membership_config.membership().get_joint_config();
                    look(voters, me, heard, at)
                })
                .await;
            if wrote {
                return;
            }
            // Not on reports: openraft's metrics change with every
            // heartbeat answer and would wake every waiter.
            if metrics.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }

    /// Waits on the reports until `look` finds the entry at `at` written
    /// (true) or the voters unknown (false). A wait sleeps on the rank its
    /// last look named, so it wakes only when that many senders could
    /// have reached it.
    pub(crate) async fn written(
        &self,
        at: LogPoint,
        mut look: impl FnMut(&[(MemberId, LogPoint)]) -> Look,
    ) -> bool {
        let heard = self.heard.subscribe();
        let mut found = look(&heard.borrow());
        loop {
            let needed = match found {
                Look::Unknown => return false,
                Look::Wrote => return true,
                Look::Short(needed) => needed.clamp(1, KEPT),
            };
            let waiters = self.rank(needed).waiters();
            let waited = waiters.wait_for(&heard, key(&at), |heard| {
                let now = look(heard);
                // Still short by as many: nothing to do until the rank moves.
                let same = matches!(now, Look::Short(n) if n.clamp(1, KEPT) == needed);
                (!same).then_some(now)
            });
            match waited.await {
                Ok(now) => found = now,
                // The sender lives as long as `self`.
                Err(_) => return std::future::pending().await,
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

/// One look at `heard` for the entry at `at` under `voters`.
pub(crate) fn look(
    voters: &[BTreeSet<MemberId>],
    me: &MemberId,
    heard: &[(MemberId, LogPoint)],
    at: LogPoint,
) -> Look {
    if majority_wrote(voters, me, heard, at) {
        return Look::Wrote;
    }
    // Every set needs its own majority, the leader counting itself in the
    // sets it belongs to. The largest shortfall is a lower bound on the
    // senders that must still reach the entry.
    let needed = voters.iter().map(|set| {
        let majority = set.len() / 2 + 1;
        majority.saturating_sub(usize::from(set.contains(me)))
    });
    Look::Short(needed.max().unwrap_or(1))
}

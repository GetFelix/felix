//! Waking the placement loop and assignment long-polls on this instance
//! instead of leaving them to their timers.
use std::collections::{BTreeMap, HashMap};

use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

use crate::model::{RaftFloorReport, ShardKey};

/// How many shards' reported voters are remembered. Past it the memory
/// starts over, which costs one more pass, not a missed one.
const MAX_VOTERS_SEEN: usize = 1 << 16;

/// Wakes shared by this instance's API handlers and its placement loop.
///
/// Both are hints, never the only path: a long-poll still re-checks the store,
/// which is how it sees another instance's writes, and the reconciler's timer
/// still runs. A missed wake costs latency, never a change.
#[derive(Debug)]
pub struct PlacementWakes {
    written: watch::Sender<u64>,
    pass: Notify,
    closing: CancellationToken,
    fence_max_lag_records: u64,
    voters_seen: parking_lot::Mutex<HashMap<ShardKey, VotersSeen>>,
}

/// The voters a Raft shard's leader last reported here.
#[derive(Debug, PartialEq, Eq)]
struct VotersSeen {
    group: (u64, u64),
    index: u64,
    voters: BTreeMap<String, u64>,
}

impl Default for PlacementWakes {
    fn default() -> Self {
        Self::new(
            CancellationToken::new(),
            super::DEFAULT_FENCE_MAX_LAG_RECORDS,
        )
    }
}

impl PlacementWakes {
    /// Long-polls waiting on these wakes return once `closing` fires, so a
    /// drain is not held open by requests that are only waiting.
    ///
    /// `fence_max_lag_records` is the placement policy's, so a report puts a
    /// destination close enough to fence exactly when placement will.
    pub(crate) fn new(closing: CancellationToken, fence_max_lag_records: u64) -> Self {
        Self {
            written: watch::Sender::new(0),
            pass: Notify::new(),
            closing,
            fence_max_lag_records,
            voters_seen: Default::default(),
        }
    }

    /// How far behind a move's destination may be to be fenced.
    pub(crate) fn fence_max_lag_records(&self) -> u64 {
        self.fence_max_lag_records
    }

    /// This instance wrote a shard assignment.
    pub(crate) fn assignment_written(&self) {
        self.written
            .send_modify(|count| *count = count.wrapping_add(1));
    }

    /// Resolves on each [`Self::assignment_written`] after this call.
    pub(crate) fn watch_assignments(&self) -> watch::Receiver<u64> {
        self.written.subscribe()
    }

    /// Ask the placement loop for a pass now.
    ///
    /// Requests coalesce: any number made while a pass is pending or running
    /// produce one more pass, not one each.
    pub(crate) fn request_pass(&self) {
        self.pass.notify_one();
    }

    /// Resolves when a pass has been asked for since the last one was taken.
    pub async fn pass_requested(&self) {
        self.pass.notified().await;
    }

    /// Fires when this instance stops serving.
    pub(crate) fn closing(&self) -> &CancellationToken {
        &self.closing
    }

    /// Note the voters in a floor report from `node_id`, and answer whether
    /// any shard's changed. Raft set steps (a one-member move's shrink, a
    /// top-up, a replacement) wait on exactly that, and leaders report every
    /// second, so a repeat must not wake placement. A report from an older
    /// group or index than one seen, or that does not name its own sender,
    /// is no leader's news and changes nothing.
    pub(crate) fn raft_voters_reported(&self, node_id: &str, reports: &[RaftFloorReport]) -> bool {
        let mut seen = self.voters_seen.lock();
        let mut changed = false;
        for report in reports {
            let Some(voters) = &report.voters else {
                continue;
            };
            if !voters.contains_key(node_id) {
                continue;
            }
            let next = VotersSeen {
                group: (report.incarnation, report.epoch),
                index: report.index,
                voters: voters.clone(),
            };
            match seen.get_mut(&report.key) {
                Some(held)
                    if held.group > next.group
                        || (held.group == next.group && held.index > next.index) => {}
                Some(held) if held.group == next.group && held.voters == next.voters => {
                    held.index = next.index;
                }
                held => {
                    if held.is_none() && seen.len() >= MAX_VOTERS_SEEN {
                        seen.clear();
                    }
                    seen.insert(report.key.clone(), next);
                    changed = true;
                }
            }
        }
        changed
    }

    /// Forget `key`'s voters, so its next floor report wakes placement: a
    /// copy id that changed may have stored a target, which that report
    /// then marks formed.
    pub(crate) fn forget_raft_voters(&self, key: &ShardKey) {
        self.voters_seen.lock().remove(key);
    }
}

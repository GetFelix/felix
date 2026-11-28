//! Waking the placement loop on this instance instead of leaving it to its
//! timer, and ending assignment long-polls on a drain.
use std::collections::{BTreeMap, HashMap};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::model::{RaftFloorReport, ShardKey};

/// How many shards' reported voters are remembered. Past it the memory
/// starts over, which costs one more pass, not a missed one.
const MAX_VOTERS_SEEN: usize = 1 << 16;

/// Wakes shared by this instance's API handlers and its placement loop.
///
/// A pass request is a hint, never the only path: the reconciler's timer
/// still runs, so a missed wake costs latency, never a change. Long-polls are
/// woken by the store itself (`watch_shard_assignment_changes`).
#[derive(Debug)]
pub struct PlacementWakes {
    pass: Notify,
    closing: CancellationToken,
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
        Self::new(CancellationToken::new())
    }
}

impl PlacementWakes {
    /// Long-polls waiting on these wakes return once `closing` fires, so a
    /// drain is not held open by requests that are only waiting.
    pub(crate) fn new(closing: CancellationToken) -> Self {
        Self {
            pass: Notify::new(),
            closing,
            voters_seen: Default::default(),
        }
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

//! Waking the placement loop and assignment long-polls on this instance
//! instead of leaving them to their timers.
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

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
}

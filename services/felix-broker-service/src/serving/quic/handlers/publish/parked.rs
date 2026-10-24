//! Publishes a control stream has parked in the publish queue while it goes on
//! reading.
//!
//! A publish allowed to wait for room (`Wait`, `Backpressure`) is parked in the
//! scheduler at once, which fixes its place in line, and the wait for a slot
//! runs here instead of in the stream's read loop. The loop then keeps serving
//! the stream's other frames, including publishes to shards whose queues have
//! room. The parked count per stream is capped; past the cap the loop waits
//! inline, which stops its reads and leaves the backpressure to QUIC stream
//! flow control.
//!
//! Unacked publishes get a much smaller cap than acked ones. An acked
//! publisher is held back by its publish window, but nothing holds back a
//! fire-and-forget one, so a large cap lets a busy broker's streams park
//! until the shared byte budget runs out. Every publish then waits on that
//! budget instead of the queue, which cost small batches about an eighth of
//! their throughput.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::task::TaskTracker;

/// One control stream's parked publishes.
pub(crate) struct ParkedPublishes {
    acked: Arc<Semaphore>,
    unacked: Arc<Semaphore>,
    tasks: TaskTracker,
}

impl ParkedPublishes {
    /// Room for `acked` parked acked publishes and `unacked` unacked ones at
    /// once.
    pub(crate) fn new(acked: usize, unacked: usize) -> Arc<Self> {
        Arc::new(Self {
            acked: Arc::new(Semaphore::new(acked)),
            unacked: Arc::new(Semaphore::new(unacked)),
            tasks: TaskTracker::new(),
        })
    }

    /// A slot for one more parked publish, or `None` at its kind's cap.
    pub(crate) fn try_slot(&self, acked: bool) -> Option<OwnedSemaphorePermit> {
        let slots = if acked { &self.acked } else { &self.unacked };
        Arc::clone(slots).try_acquire_owned().ok()
    }

    /// Run a parked publish's wait.
    pub(crate) fn spawn<F>(&self, wait: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks.spawn(wait);
    }

    /// Wait for every parked publish to be admitted or given up.
    ///
    /// A stream that ends cleanly calls this before tearing down, so a parked
    /// publish is not cancelled just because the client finished the stream.
    pub(crate) async fn drain(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    /// Parked publishes not yet settled.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tasks.len()
    }
}

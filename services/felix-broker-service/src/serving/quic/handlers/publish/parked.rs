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

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::task::TaskTracker;

/// One control stream's parked publishes.
pub(crate) struct ParkedPublishes {
    slots: Arc<Semaphore>,
    tasks: TaskTracker,
}

impl ParkedPublishes {
    /// Room for `limit` parked publishes at once.
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(limit)),
            tasks: TaskTracker::new(),
        })
    }

    /// A slot for one more parked publish, or `None` at the cap.
    pub(crate) fn try_slot(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.slots).try_acquire_owned().ok()
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

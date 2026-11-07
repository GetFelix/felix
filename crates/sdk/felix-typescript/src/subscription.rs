//! A subscription to one stream, read with `nextEvent`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use felix_client::ClusterSubscription;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, watch};

use crate::errors::classify;
use crate::types::Event;

/// A live subscription. Read it with `nextEvent`, and `close` it when done.
///
/// A read in flight holds the handle, so `close` cannot wait for it: a consumer
/// shutting down is almost always parked on `nextEvent`, and waiting for the
/// read it is cancelling would hang exactly the path that needs to make
/// progress. `close` therefore cancels the read instead, and the reader drops
/// the subscription on its way out.
#[napi]
pub struct SubscriptionHandle {
    start_offset: Option<u64>,
    live_offset: Option<u64>,
    queue_capacity: Option<u32>,
    /// Copied from the subscription after each read. The subscription is held
    /// by the read in flight, so a getter cannot ask it directly.
    progress: Progress,
    inner: Arc<Mutex<Option<ClusterSubscription>>>,
    closed: watch::Sender<bool>,
}

/// What a subscription has done so far, readable while a read is in flight.
#[derive(Default)]
struct Progress {
    moves: AtomicU64,
    reconnects: AtomicU64,
    stream_deleted: AtomicBool,
}

impl Progress {
    fn record(&self, subscription: &ClusterSubscription) {
        self.moves.store(subscription.moves(), Ordering::Relaxed);
        self.reconnects
            .store(subscription.reconnects(), Ordering::Relaxed);
        self.stream_deleted
            .store(subscription.stream_deleted(), Ordering::Relaxed);
    }
}

impl SubscriptionHandle {
    pub(crate) fn new(subscription: ClusterSubscription) -> Self {
        Self {
            start_offset: subscription.start_offset(),
            live_offset: subscription.live_offset(),
            queue_capacity: subscription.queue_capacity(),
            progress: Progress::default(),
            inner: Arc::new(Mutex::new(Some(subscription))),
            closed: watch::Sender::new(false),
        }
    }
}

#[napi]
impl SubscriptionHandle {
    /// The offset the subscription began at when it opened, or `null` for an
    /// in-memory stream. After a shard move the subscription resumes where it
    /// left off, and this does not change.
    #[napi(getter)]
    pub fn start_offset(&self) -> Option<BigInt> {
        self.start_offset.map(BigInt::from)
    }

    /// The offset live delivery began at when the subscription opened, or
    /// `null` for an in-memory stream.
    #[napi(getter)]
    pub fn live_offset(&self) -> Option<BigInt> {
        self.live_offset.map(BigInt::from)
    }

    /// The broker-side queue capacity the broker granted, in published
    /// batches, when `brokerSubQueueCapacity` asked for one. `null` means the
    /// stream's default.
    #[napi(getter)]
    pub fn queue_capacity(&self) -> Option<u32> {
        self.queue_capacity
    }

    /// How many times the subscription followed its shard to another broker.
    /// Updated as reads return.
    #[napi(getter)]
    pub fn moves(&self) -> BigInt {
        BigInt::from(self.progress.moves.load(Ordering::Relaxed))
    }

    /// How many times the subscription resubscribed after losing its
    /// connection. Updated as reads return.
    #[napi(getter)]
    pub fn reconnects(&self) -> BigInt {
        BigInt::from(self.progress.reconnects.load(Ordering::Relaxed))
    }

    /// Whether the subscription ended because its stream was deleted. Check it
    /// once `nextEvent` returns `null`; there is nothing to resubscribe to.
    #[napi(getter)]
    pub fn stream_deleted(&self) -> bool {
        self.progress.stream_deleted.load(Ordering::Relaxed)
    }

    /// The next record, or `null` once the subscription has ended.
    ///
    /// Resolves when a record arrives; there is no timeout argument because a
    /// caller that wants one can race this promise against a timer. The losing
    /// read stays in flight and resolves with the next record, so keep the
    /// promise rather than calling again.
    #[napi]
    pub async fn next_event(&self) -> Result<Option<Event>> {
        // A `watch` receiver and not a flag plus a notify: `wait_for` returns
        // at once when the value is already set, so a close that lands between
        // the check and the wait cannot be missed.
        let mut closed = self.closed.subscribe();
        if *closed.borrow_and_update() {
            return Ok(None);
        }
        let inner = Arc::clone(&self.inner);
        let mut guard = inner.lock().await;
        let Some(subscription) = guard.as_mut() else {
            return Ok(None);
        };
        let outcome = tokio::select! {
            result = subscription.next_event() => Some(result),
            _ = closed.wait_for(|closed| *closed) => None,
        };
        // Recorded on errors and the end too: that is where a delete shows.
        if outcome.is_some()
            && let Some(subscription) = guard.as_ref()
        {
            self.progress.record(subscription);
        }
        match outcome {
            // Closed underneath this read, so this reader is the one holding
            // the subscription and the one that has to let it go.
            None => {
                guard.take();
                Ok(None)
            }
            Some(Ok(Some(event))) => Ok(Some(event.into())),
            Some(Ok(None)) => Ok(None),
            Some(Err(err)) => Err(classify(err)),
        }
    }

    /// Release the subscription. Idempotent, and never waits on a read.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        // `send_replace` and not `send`: with nothing reading there is no
        // receiver, and `send` reports that as an error and leaves the value
        // alone — so a close with no read in flight would not take.
        self.closed.send_replace(true);
        // Only when nothing is reading. When something is, it owns the lock and
        // drops the subscription as it unwinds — waiting for that here is the
        // deadlock this whole arrangement exists to avoid.
        if let Ok(mut guard) = self.inner.try_lock() {
            guard.take();
        }
        Ok(())
    }

    #[napi(getter)]
    pub fn closed(&self) -> bool {
        *self.closed.borrow()
    }
}

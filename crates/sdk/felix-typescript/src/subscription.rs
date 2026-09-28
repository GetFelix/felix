//! A subscription to one stream, read with `nextEvent`.

use std::sync::Arc;

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
    inner: Arc<Mutex<Option<ClusterSubscription>>>,
    closed: watch::Sender<bool>,
}

impl SubscriptionHandle {
    pub(crate) fn new(subscription: ClusterSubscription) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(subscription))),
            closed: watch::Sender::new(false),
        }
    }
}

#[napi]
impl SubscriptionHandle {
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
        match outcome {
            // Closed underneath this read, so this reader is the one holding
            // the subscription and the one that has to let it go.
            None => {
                guard.take();
                Ok(None)
            }
            Some(Ok(Some(event))) => Ok(Some(Event {
                tenant_id: event.tenant_id.to_string(),
                namespace: event.namespace.to_string(),
                stream: event.stream.to_string(),
                payload: event.payload.to_vec().into(),
                offset: event.offset.map(BigInt::from),
                skipped_before: BigInt::from(event.skipped_before),
            })),
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

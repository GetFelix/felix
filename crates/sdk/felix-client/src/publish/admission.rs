//! The in-flight byte budget every publish is admitted against.
//!
//! Shared by all of a client's publishers. A publish takes its bytes before
//! it is queued and gives them back once it is written, or once the broker
//! answers if it asked for an ack. So a slow broker makes callers wait for
//! room rather than letting the client buffer without limit. A publish larger
//! than the whole budget waits for all of it and is sent alone.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct PublishAdmission {
    semaphore: Arc<Semaphore>,
    limit: usize,
}

impl PublishAdmission {
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.clamp(1, u32::MAX as usize);
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    pub(super) async fn acquire(&self, wire_bytes: usize) -> Result<OwnedSemaphorePermit> {
        // A publish bigger than the whole budget takes all of it: it waits
        // until nothing else is in flight and then goes out alone. Refusing it
        // would make frames between the budget and the frame cap unpublishable.
        let wire_bytes = wire_bytes.clamp(1, self.limit);
        self.semaphore
            .clone()
            .acquire_many_owned(wire_bytes as u32)
            .await
            .map_err(|_| anyhow::anyhow!("publish admission closed"))
    }
}

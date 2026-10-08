//! Tests for publishing, grouped by what they pin: the admission budget,
//! stream routing, per-shard streams, the public API, which encoding goes on
//! the wire, the writer task, how it fails, and the broker's acks.

mod ack;
mod admission;
mod api;
mod encoding;
mod routing;
mod shard_streams;
mod stub_broker;
mod widths;
mod writer;
mod writer_failures;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use super::Acked;
use super::routing::PublishSharding;
use super::writer::{PublishRequest, PublishWorker};
use super::{Publisher, PublisherInner};

pub(super) fn test_publish_permit() -> OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("test publish permit")
}

/// A worker whose "writer" acknowledges everything at once, until `Finish`.
pub(super) fn fake_worker() -> PublishWorker {
    let (tx, mut rx) = mpsc::channel::<PublishRequest>(8);
    let handle = tokio::spawn(async move {
        while let Some(request) = rx.recv().await {
            match request {
                PublishRequest::Message { response, .. } => {
                    let _ = response.send(Ok(Acked::default()));
                }
                PublishRequest::BinaryBytes { response, .. } => {
                    let _ = response.send(Ok(Acked::default()));
                }
                PublishRequest::Finish { response } => {
                    let _ = response.send(Ok(Acked::default()));
                    break;
                }
            }
        }
        Ok(())
    });
    PublishWorker {
        tx,
        handle: tokio::sync::Mutex::new(Some(handle)),
        request_counter: AtomicU64::new(1),
        server_flags: felix_wire::KNOWN_FLAGS,
        publish_window: 0,
    }
}

pub(super) fn make_publisher(sharding: PublishSharding, workers: usize) -> Publisher {
    let publish_workers = (0..workers).map(|_| fake_worker()).collect();
    Publisher {
        inner: Arc::new(PublisherInner::new(Arc::new(publish_workers), sharding)),
    }
}

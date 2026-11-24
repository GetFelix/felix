use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use parking_lot::Mutex;

use crate::stream::{ReadBound, ReadBounds};
use crate::{Broker, ConsistencyLevel, DurableStorage, StreamMetadata};

#[derive(Debug)]
struct Bound(Mutex<ReadBound>);

impl ReadBounds for Bound {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        *self.0.lock()
    }
}

async fn broker(dir: &std::path::Path) -> (Broker, Arc<Bound>) {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir, config).expect("storage"));
    let bound = Arc::new(Bound(Mutex::new(ReadBound::Unbounded)));
    broker.set_read_bounds(Arc::clone(&bound) as Arc<dyn ReadBounds>);
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "s",
            StreamMetadata {
                durable: true,
                consistency: ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, bound)
}

/// A publish acknowledged at a majority's write can be ahead of the read
/// bound for a moment. A read after that acknowledgement waits for the bound
/// instead of answering with nothing.
#[tokio::test]
async fn a_read_waits_for_acknowledged_records_to_pass_the_bound() {
    let dir = tempfile::tempdir().expect("dir");
    let (broker, bound) = broker(dir.path()).await;
    let handle = broker
        .resolve_stream_handle("t1", "ns", "s", 0)
        .await
        .expect("handle");
    for n in 0..3 {
        broker
            .publish_batch_to_handle(&handle, &[Bytes::from(format!("r{n}"))])
            .await
            .expect("publish");
    }
    *bound.0.lock() = ReadBound::Committed(0);
    let lift = Arc::clone(&bound);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        *lift.0.lock() = ReadBound::Committed(3);
    });
    let page = broker
        .read_range("t1", "ns", "s", 0, 0, Some(3), 100, 1 << 20)
        .await
        .expect("read");
    assert_eq!(page.records.len(), 3);
}

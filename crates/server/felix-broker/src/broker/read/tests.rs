use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::disk_log::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig, RecordMark};
use parking_lot::Mutex;

use crate::durable::StreamLog;
use crate::stream::{ReadBound, ReadBounds};
use crate::{Broker, BrokerError, FollowerReads, NotReadable, StreamMetadata};

#[derive(Debug)]
struct Bound(Mutex<ReadBound>);

impl ReadBounds for Bound {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        *self.0.lock()
    }
}

/// An engine's read barrier: it moves the bound to `applies` before it lets
/// a read through, as a member applying the read index does, or refuses the
/// read when `applies` is `None`.
#[derive(Debug)]
struct Barrier {
    bound: Arc<Bound>,
    applies: Option<ReadBound>,
}

impl FollowerReads for Barrier {
    fn before_read<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
        _: u32,
        _: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, NotReadable>> + Send + 'a>> {
        Box::pin(async move {
            let applies = self.applies.ok_or(NotReadable::Settling)?;
            *self.bound.0.lock() = applies;
            Ok(None)
        })
    }

    fn after_tail_join<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
        _: u32,
        _: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotReadable>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// A stream on an engine's log holding three records, none of them under
/// the read bound until the barrier runs.
async fn engine_log_stream(dir: &std::path::Path, applies: Option<ReadBound>) -> Broker {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let log = StreamLog::from_log(DiskLog::open(dir, "s", config).expect("log"));
    let payloads = vec![Bytes::from_static(b"r"); 3];
    let order = Arc::new(felix_storage::CommitSequencer::new(0));
    let (pending, _turn) = log
        .begin_append_marked(&payloads, &[RecordMark::None; 3], &[], None, 1, &order)
        .await
        .expect("append");
    log.commit(&pending).await.expect("commit");

    let bound = Arc::new(Bound(Mutex::new(ReadBound::Committed(0))));
    let broker = Broker::new(EphemeralCache::new().into());
    broker.leave_durable_streams_to_engines();
    broker.set_read_bounds(Arc::clone(&bound) as Arc<dyn ReadBounds>);
    broker.set_follower_reads(Arc::new(Barrier { bound, applies }));
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "s",
            StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    broker
        .open_stream_on_engine_log("t1", "ns", "s", 0, log)
        .await
        .expect("open");
    broker
}

/// A publish is acknowledged before this member applies it. The read goes
/// through the engine's barrier first, so it sees what was acknowledged.
#[tokio::test]
async fn reads_pass_the_barrier_before_they_take_the_bound() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = engine_log_stream(dir.path(), Some(ReadBound::Committed(3))).await;
    let page = broker
        .read_range("t1", "ns", "s", 0, 0, Some(3), 100, 1 << 20)
        .await
        .expect("read");
    assert_eq!(page.records.len(), 3);
    let found = broker
        .offset_for_time("t1", "ns", "s", 0, 0)
        .await
        .expect("lookup");
    assert_eq!(found, Some(0));
}

/// A lookup that cannot pass the barrier fails retryably. Answering from
/// the stale bound would say nothing is readable, and a client that was
/// just acknowledged would then wait past its own record.
#[tokio::test]
async fn a_lookup_refused_by_the_barrier_fails_instead_of_answering_nothing() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = engine_log_stream(dir.path(), None).await;
    let err = broker
        .offset_for_time("t1", "ns", "s", 0, 0)
        .await
        .expect_err("refused");
    assert!(
        matches!(
            err,
            BrokerError::NotReadable {
                reason: NotReadable::Settling,
                ..
            }
        ),
        "{err:?}"
    );
    let err = broker
        .read_range("t1", "ns", "s", 0, 0, Some(3), 100, 1 << 20)
        .await
        .expect_err("refused");
    assert!(matches!(err, BrokerError::NotReadable { .. }), "{err:?}");
}

/// A shard whose engine member is closing, or not open yet, can report no
/// commit at all (`Unbounded`). The read is refused: on a follower the raw
/// tail can hold entries a later leader cuts.
#[tokio::test]
async fn an_engine_log_without_a_commit_bound_refuses_the_read() {
    let dir = tempfile::tempdir().expect("dir");
    let broker = engine_log_stream(dir.path(), Some(ReadBound::Unbounded)).await;
    let err = broker
        .read_range("t1", "ns", "s", 0, 0, Some(3), 100, 1 << 20)
        .await
        .expect_err("refused");
    assert!(
        matches!(
            err,
            BrokerError::NotReadable {
                reason: NotReadable::Refused,
                ..
            }
        ),
        "{err:?}"
    );
}

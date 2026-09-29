//! A commit's event and state reach readers together, and only together.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig, LogRecord, RecordMark};
use parking_lot::Mutex;

use super::{CommitRecord, StateOp, client_record};
use crate::stream::{ReadBound, ReadBounds};
use crate::{Broker, BrokerError, ConsistencyLevel, DurableStorage, StreamMetadata, Subscription};

#[derive(Debug)]
struct Bound(Mutex<ReadBound>);

impl ReadBounds for Bound {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        *self.0.lock()
    }
}

async fn broker(dir: &std::path::Path, consistency: ConsistencyLevel) -> (Broker, Arc<Bound>) {
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
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                consistency,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    (broker, bound)
}

async fn commit(broker: &Broker, event: &'static [u8], ops: Vec<StateOp>) -> u64 {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let outcome = broker
        .commit_to_handle(&handle, Bytes::from_static(event), ops)
        .await
        .expect("commit");
    outcome.offsets.expect("offsets").0
}

async fn get(broker: &Broker, key: &str) -> crate::StateRead {
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    broker.state_get(&handle, key).await.expect("state")
}

fn put(key: &str, value: &'static [u8]) -> StateOp {
    StateOp::Put {
        key: key.to_owned(),
        value: Bytes::from_static(value),
    }
}

async fn next(sub: &mut Subscription) -> Bytes {
    tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("delivered in time")
        .expect("subscription open")
}

#[test]
fn a_record_round_trips() {
    let record = CommitRecord {
        event: Bytes::from_static(b"order placed"),
        ops: vec![
            put("order/1", b"placed"),
            StateOp::Delete {
                key: "cart/1".to_owned(),
            },
        ],
    };
    assert_eq!(
        CommitRecord::decode(&record.encode()).expect("decode"),
        record
    );
}

#[test]
fn a_damaged_record_is_refused_rather_than_misread() {
    let encoded = CommitRecord {
        event: Bytes::from_static(b"e"),
        ops: vec![put("k", b"v")],
    }
    .encode();
    for bad in [
        encoded.slice(..encoded.len() - 1),
        Bytes::from([encoded.as_ref(), b"x"].concat()),
        Bytes::from_static(&[2, 0, 0, 0, 0, 0, 0, 0, 0]),
        // One byte of event, then a count of four billion operations.
        Bytes::from_static(&[1, 0, 0, 0, 1, b'e', 0xff, 0xff, 0xff, 0xff]),
    ] {
        assert!(
            matches!(
                CommitRecord::decode(&bad),
                Err(BrokerError::MalformedCommit(_))
            ),
            "decoded {bad:?}"
        );
    }
}

#[test]
fn a_reader_sees_a_commit_record_as_its_event() {
    let stored = CommitRecord {
        event: Bytes::from_static(b"event"),
        ops: vec![put("k", b"v")],
    }
    .encode();
    let record = |mark| LogRecord {
        offset: 7,
        timestamp_micros: 0,
        checksum: 0,
        payload: stored.clone(),
        mark,
    };
    assert_eq!(client_record(record(RecordMark::Commit)).payload, "event");
    assert_eq!(client_record(record(RecordMark::None)).payload, stored);
}

/// **A commit reaches a subscriber, a history read and a state read, each as
/// what it is.** The subscriber and the history get the event, never the
/// stored record, and the state carries the commit's offset as its version.
#[tokio::test]
async fn a_commit_is_one_offset_seen_by_every_reader() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    let mut live = broker
        .subscribe("t1", "ns", "orders", 0)
        .await
        .expect("subscribe");

    let first = commit(&broker, b"placed", vec![put("order/1", b"placed")]).await;
    let second = commit(&broker, b"shipped", vec![put("order/1", b"shipped")]).await;
    assert_eq!((first, second), (0, 1));

    assert_eq!(next(&mut live).await, "placed");
    assert_eq!(next(&mut live).await, "shipped");
    let read = get(&broker, "order/1").await;
    assert_eq!(read.value.as_deref(), Some(&b"shipped"[..]));
    assert_eq!(read.version, Some(1));
    assert_eq!(read.as_of, Some(1));

    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let history = handle
        .log()
        .expect("durable")
        .read_from(0, 1 << 20)
        .await
        .expect("history");
    let events: Vec<_> = history
        .iter()
        .map(|record| record.payload.clone())
        .collect();
    assert_eq!(events, ["placed", "shipped"]);
}

/// **On a `Quorum` stream the state waits for the mark with the event.** A
/// commit past the committed mark is held from the ring; its state updates
/// are held with it and applied when it is released, never before and never
/// dropped. Applied at the local commit instead, a reader would see state a
/// failover can take back, and the event missing beside it.
#[tokio::test]
async fn a_held_commit_releases_its_state_with_its_event() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, bound) = broker(dir.path(), ConsistencyLevel::Quorum).await;
    *bound.0.lock() = ReadBound::Committed(0);
    let mut live = broker
        .subscribe("t1", "ns", "orders", 0)
        .await
        .expect("subscribe");
    // Built before the commit, so the commit must reach it by the release.
    assert_eq!(get(&broker, "order/1").await.as_of, None);

    assert_eq!(
        commit(&broker, b"placed", vec![put("order/1", b"placed")]).await,
        0
    );
    let read = get(&broker, "order/1").await;
    assert_eq!((read.value, read.as_of), (None, None));

    *bound.0.lock() = ReadBound::Committed(1);
    broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle")
        .release_committed();
    assert_eq!(next(&mut live).await, "placed");
    let read = get(&broker, "order/1").await;
    assert_eq!(read.value.as_deref(), Some(&b"placed"[..]));
    assert_eq!(read.version, Some(0));
}

/// **The state is rebuilt from the log.** Nothing about it is stored beside
/// the log, so a broker that restarts answers from the records alone.
#[tokio::test]
async fn state_survives_a_restart_by_replaying_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
        commit(&broker, b"placed", vec![put("order/1", b"placed")]).await;
        commit(
            &broker,
            b"cancelled",
            vec![StateOp::Delete {
                key: "order/1".to_owned(),
            }],
        )
        .await;
        commit(&broker, b"placed", vec![put("order/2", b"placed")]).await;
    }
    let (broker, _) = broker(dir.path(), ConsistencyLevel::Leader).await;
    assert_eq!(get(&broker, "order/1").await.value, None);
    let read = get(&broker, "order/2").await;
    assert_eq!(read.value.as_deref(), Some(&b"placed"[..]));
    assert_eq!((read.version, read.as_of), (Some(2), Some(2)));
}

/// **A rebuilt view holds only what is committed.** A broker that takes over
/// a log (a restart here, a promotion in a cluster) holds records past the
/// committed mark that a failover may still replace. Until the mark covers
/// them the state is not readable, rather than read from them.
#[tokio::test]
async fn a_rebuild_waits_for_the_mark_to_cover_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let (broker, _) = broker(dir.path(), ConsistencyLevel::Quorum).await;
        commit(&broker, b"placed", vec![put("order/1", b"placed")]).await;
    }
    let (broker, bound) = broker(dir.path(), ConsistencyLevel::Quorum).await;
    *bound.0.lock() = ReadBound::Committed(0);
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    assert!(matches!(
        broker.state_get(&handle, "order/1").await,
        Err(BrokerError::StateNotReadable(crate::NotReadable::Settling))
    ));
    *bound.0.lock() = ReadBound::Committed(1);
    let read = get(&broker, "order/1").await;
    assert_eq!(read.value.as_deref(), Some(&b"placed"[..]));
}

#[tokio::test]
async fn an_ephemeral_stream_refuses_a_commit() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "ns", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let refused = broker
        .commit_to_handle(&handle, Bytes::from_static(b"e"), Vec::new())
        .await;
    assert!(matches!(
        refused,
        Err(BrokerError::CommitNeedsDurableStream)
    ));
}

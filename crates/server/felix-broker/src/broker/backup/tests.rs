//! A backup point's offsets are the committed ones, not the tail.

use std::sync::Arc;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::stream::{ReadBound, ReadBounds};
use crate::{
    Broker, BrokerError, ConsistencyLevel, DurableStorage, LogKind, NotReadable, StreamMetadata,
};

#[derive(Debug)]
struct Fixed(ReadBound);

impl ReadBounds for Fixed {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> ReadBound {
        self.0
    }
}

async fn broker_with(dir: &std::path::Path, bound: ReadBound) -> Broker {
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(DurableStorage::open(dir, config).expect("storage"));
    broker.set_read_bounds(Arc::new(Fixed(bound)));
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    for (name, consistency) in [
        ("quorum", ConsistencyLevel::Quorum),
        ("leader", ConsistencyLevel::Leader),
    ] {
        broker
            .register_stream(
                "t1",
                "ns",
                name,
                StreamMetadata {
                    durable: true,
                    consistency,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
        for payload in [b"a", b"b", b"c"] {
            let handle = broker
                .resolve_stream_handle("t1", "ns", name, 0)
                .await
                .expect("handle");
            broker
                .publish_batch_with_outcome(&handle, &[Bytes::from_static(payload)], None)
                .await
                .expect("publish");
        }
    }
    broker
}

#[tokio::test]
async fn a_quorum_stream_is_cut_at_its_mark_and_a_leader_stream_at_its_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with(dir.path(), ReadBound::Committed(1)).await;

    let quorum = broker
        .committed_offsets(LogKind::Stream, "t1", "ns", "quorum", 0, |_| {
            ReadBound::Unbounded
        })
        .await
        .expect("offsets")
        .expect("durable");
    assert_eq!(quorum.records, 1, "three written, one past the mark");
    assert_eq!(quorum.group_cursors, None);

    let leader = broker
        .committed_offsets(LogKind::Stream, "t1", "ns", "leader", 0, |_| {
            ReadBound::Unbounded
        })
        .await
        .expect("offsets")
        .expect("durable");
    assert_eq!(leader.records, 3);
}

#[tokio::test]
async fn a_shard_with_no_committed_answer_yet_is_not_given_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_with(dir.path(), ReadBound::Settling).await;

    let err = broker
        .committed_offsets(LogKind::Stream, "t1", "ns", "quorum", 0, |_| {
            ReadBound::Unbounded
        })
        .await
        .expect_err("settling");
    assert!(matches!(
        err,
        BrokerError::NotReadable {
            reason: NotReadable::Settling,
            ..
        }
    ));
}

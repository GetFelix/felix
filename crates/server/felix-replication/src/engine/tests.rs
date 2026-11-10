use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::ConsistencyLevel;

use super::{Engine, Hybrid};
use crate::ShardKey;
use crate::quorum::{QuorumError, QuorumMarks, ShardServing, Unreadable};

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: crate::ShardKind::Stream,
    }
}

/// Serves the shard at `generation`, or not at all.
struct ServedAt {
    generation: Option<u64>,
    replicated: bool,
}

impl ShardServing for ServedAt {
    fn replicated(&self, _key: &ShardKey) -> bool {
        self.replicated
    }

    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        self.generation
    }

    fn lease_valid(&self) -> bool {
        true
    }

    fn record_ack_refusal(&self) {}
}

fn hybrid(marks: &Arc<QuorumMarks>, serving: ServedAt) -> Hybrid<ServedAt> {
    Hybrid::new(Some(Arc::clone(marks)), Some(Arc::new(serving)))
}

#[test]
fn the_hybrid_bounds_a_quorum_read_at_the_mark_for_the_served_generation() {
    let quorum = Some(ConsistencyLevel::Quorum);
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&key(), 3, 5);
    let engine = hybrid(
        &marks,
        ServedAt {
            generation: Some(4),
            replicated: true,
        },
    );

    assert_eq!(engine.read_bound(quorum, &key()), Err(Unreadable::Settling));
    marks.publish(&key(), 4, 7);
    assert_eq!(engine.read_bound(quorum, &key()), Ok(Some(7)));
}

#[test]
fn the_hybrid_leaves_reads_unbounded_where_nothing_is_replicated() {
    let marks = Arc::new(QuorumMarks::new());
    let quorum = Some(ConsistencyLevel::Quorum);

    let unreplicated = hybrid(
        &marks,
        ServedAt {
            generation: Some(1),
            replicated: false,
        },
    );
    assert_eq!(unreplicated.read_bound(quorum, &key()), Ok(None));

    let leader_only = hybrid(
        &marks,
        ServedAt {
            generation: Some(1),
            replicated: true,
        },
    );
    assert_eq!(
        leader_only.read_bound(Some(ConsistencyLevel::Leader), &key()),
        Ok(None)
    );

    let single_node: Hybrid<ServedAt> = Hybrid::new(None, None);
    assert_eq!(single_node.read_bound(quorum, &key()), Ok(None));
}

#[test]
fn the_hybrid_refuses_a_quorum_read_of_a_shard_it_does_not_serve() {
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&key(), 3, 5);
    let engine = hybrid(
        &marks,
        ServedAt {
            generation: None,
            replicated: true,
        },
    );

    assert_eq!(
        engine.read_bound(Some(ConsistencyLevel::Quorum), &key()),
        Err(Unreadable::NotServed)
    );
}

/// A broker holding one durable stream at `consistency` with one batch
/// published to it, and that batch's outcome.
async fn written(
    consistency: ConsistencyLevel,
) -> (
    Arc<felix_broker::Broker>,
    felix_broker::StreamHandle,
    felix_broker::PublishOutcome,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = felix_storage::log::LogConfig {
        fsync_mode: felix_storage::log::FsyncMode::None,
        preallocate_segments: false,
        ..felix_storage::log::LogConfig::default()
    };
    let storage = felix_broker::DurableStorage::open(dir.path().join("streams"), config.clone())
        .expect("storage");
    let broker = Arc::new(
        felix_broker::Broker::new(Box::new(
            felix_storage::LogCache::open(dir.path().join("caches"), config).expect("cache"),
        ))
        .with_durable_storage(storage),
    );
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
            felix_broker::StreamMetadata {
                durable: true,
                consistency,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let outcome = broker
        .publish_batch_with_outcome(&handle, &[Bytes::from_static(b"a")], None, None)
        .await
        .expect("publish");
    assert_eq!(outcome.offsets, Some((0, 0)));
    (broker, handle, outcome, dir)
}

fn served_at_4() -> ServedAt {
    ServedAt {
        generation: Some(4),
        replicated: true,
    }
}

#[tokio::test]
async fn the_hybrid_commits_a_leader_write_at_once() {
    let (_broker, handle, outcome, _dir) = written(ConsistencyLevel::Leader).await;
    let marks = Arc::new(QuorumMarks::new());
    let engine = hybrid(&marks, served_at_4());

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    engine
        .await_commit(&handle, Some(&key()), &outcome, 4, Duration::from_secs(60))
        .await
        .expect("a Leader write needs no quorum");
    assert_eq!(started.elapsed(), Duration::ZERO);
}

/// The wait is held to the generation the batch was written at: a mark at
/// that generation commits it, and the engine must not substitute another.
#[tokio::test]
async fn the_hybrid_commits_a_quorum_write_once_the_mark_at_its_generation_passes_it() {
    let (_broker, handle, outcome, _dir) = written(ConsistencyLevel::Quorum).await;
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&key(), 4, 1);
    let engine = hybrid(&marks, served_at_4());

    engine
        .await_commit(&handle, Some(&key()), &outcome, 4, Duration::from_secs(60))
        .await
        .expect("committed at its own generation");
}

/// The wait ends at the timeout it is given, not before and not at some
/// other bound.
#[tokio::test]
async fn the_hybrid_times_out_a_quorum_write_at_the_given_timeout() {
    let (_broker, handle, outcome, _dir) = written(ConsistencyLevel::Quorum).await;
    let marks = Arc::new(QuorumMarks::new());
    let engine = hybrid(&marks, served_at_4());
    let timeout = Duration::from_millis(300);

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let err = engine
        .await_commit(&handle, Some(&key()), &outcome, 4, timeout)
        .await
        .expect_err("no mark, so no commit");
    let waited = started.elapsed();

    assert!(
        matches!(
            err.downcast_ref::<QuorumError>(),
            Some(QuorumError::TimedOut { timeout: t, .. }) if *t == timeout
        ),
        "{err:?}"
    );
    assert!(waited >= timeout, "ended early after {waited:?}");
    assert!(waited < timeout * 2, "ended late after {waited:?}");
}

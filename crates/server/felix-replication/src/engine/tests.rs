use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::ConsistencyLevel;

use super::{Engine, Hybrid};
use crate::ShardKey;
use crate::quorum::{Access, CacheWrite, QuorumError, QuorumMarks, ShardServing, Unreadable};

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

fn cache_key() -> ShardKey {
    ShardKey {
        stream: "sessions".to_string(),
        kind: crate::ShardKind::Cache,
        ..key()
    }
}

/// A broker with a `Quorum` cache `sessions` holding one value and a counter
/// log with one add.
async fn cache_broker() -> (felix_broker::Broker, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = felix_storage::log::LogConfig {
        fsync_mode: felix_storage::log::FsyncMode::None,
        preallocate_segments: false,
        ..felix_storage::log::LogConfig::default()
    };
    let broker = felix_broker::Broker::new(Box::new(
        felix_storage::LogCache::open(dir.path().join("caches"), config.clone()).expect("cache"),
    ))
    .with_counters(Arc::new(
        felix_storage::CounterStore::open(dir.path().join("counters"), config).expect("counters"),
    ));
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "ns")
        .await
        .expect("namespace");
    broker
        .register_cache(
            "t1",
            "ns",
            "sessions",
            felix_broker::CacheMetadata {
                consistency: ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("cache");
    broker
        .cache()
        .put(
            "t1",
            "ns",
            "sessions",
            0,
            "k",
            Bytes::from_static(b"v"),
            None,
        )
        .await
        .expect("put");
    broker
        .counters()
        .expect("counters")
        .add("t1", "ns", "sessions", 0, "hits", 1)
        .await
        .expect("add");
    (broker, dir)
}

/// Serves the shard at generation 3 without the lease.
struct Unleased;

impl ShardServing for Unleased {
    fn replicated(&self, _key: &ShardKey) -> bool {
        true
    }

    fn generation(&self, _key: &ShardKey) -> Option<u64> {
        Some(3)
    }

    fn lease_valid(&self) -> bool {
        false
    }

    fn record_ack_refusal(&self) {}
}

fn served_at_3() -> ServedAt {
    ServedAt {
        generation: Some(3),
        replicated: true,
    }
}

fn timed_out_at(err: &anyhow::Error, timeout: Duration) -> bool {
    matches!(
        err.downcast_ref::<QuorumError>(),
        Some(QuorumError::TimedOut { timeout: t, .. }) if *t == timeout
    )
}

/// The write starts at the generation it is given and the tail the log has,
/// and only where the shard is replicated at `Quorum`.
#[tokio::test]
async fn the_hybrid_marks_where_a_cache_write_starts_at_the_given_generation() {
    let (broker, _dir) = cache_broker().await;
    let marks = Arc::new(QuorumMarks::new());
    let engine = hybrid(&marks, served_at_3());

    let at = engine
        .cache_write_at(&broker, &cache_key(), 3)
        .await
        .expect("a quorum write");
    assert_eq!(at.generation, 3);
    assert!(at.from > 0, "the tail is read from the cache log");

    let bare: Hybrid<ServedAt> = Hybrid::new(None, None);
    assert_eq!(bare.cache_write_at(&broker, &cache_key(), 3).await, None);
}

/// A cache read waits the timeout it is given for the mark, and is released
/// by it at once; an engine without marks would answer both at once.
#[tokio::test]
async fn the_hybrid_waits_a_cache_read_its_timeout_then_the_mark_releases_it() {
    let (broker, _dir) = cache_broker().await;
    let marks = Arc::new(QuorumMarks::new());
    let engine = Hybrid::new(Some(Arc::clone(&marks)), Some(Arc::new(served_at_3())));
    let timeout = Duration::from_millis(300);

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let err = engine
        .await_cache_quorum(&broker, &cache_key(), timeout, Access::Read, None)
        .await
        .expect_err("no mark yet");
    assert!(timed_out_at(&err, timeout), "{err:?}");
    assert!(started.elapsed() >= timeout);
    assert!(started.elapsed() < timeout * 2);

    marks.publish(&cache_key(), 3, 1 << 20);
    let started = tokio::time::Instant::now();
    engine
        .await_cache_quorum(&broker, &cache_key(), timeout, Access::Read, None)
        .await
        .expect("the mark passed the tail");
    assert_eq!(started.elapsed(), Duration::ZERO);
}

/// A write is held to the generation it was taken at and to the mark the
/// followers decided, so it is not acknowledged on a report alone.
#[tokio::test]
async fn the_hybrid_acknowledges_a_cache_write_on_the_followers_mark_at_its_generation() {
    let (broker, _dir) = cache_broker().await;
    let written = Some(CacheWrite {
        generation: 3,
        from: 0,
    });
    let timeout = Duration::from_millis(300);
    let wait = |marks: &Arc<QuorumMarks>, written| {
        let engine = Hybrid::new(Some(Arc::clone(marks)), Some(Arc::new(Unleased)));
        let broker = &broker;
        async move {
            engine
                .await_cache_quorum(broker, &cache_key(), timeout, Access::Write, written)
                .await
        }
    };

    let decided = Arc::new(QuorumMarks::new());
    decided.publish_by_followers(&cache_key(), 3, 1);
    wait(&decided, written).await.expect("followers decided it");
    wait(&decided, None)
        .await
        .expect_err("a write that does not say where it started");

    let reported = Arc::new(QuorumMarks::new());
    reported.publish(&cache_key(), 3, 1);
    wait(&reported, written)
        .await
        .expect_err("a report needs the lease");
}

/// A counter add waits the timeout it is given at the generation it is given.
#[tokio::test]
async fn the_hybrid_waits_a_counter_add_at_its_generation_for_its_timeout() {
    let (broker, _dir) = cache_broker().await;
    let marks = Arc::new(QuorumMarks::new());
    marks.counters().publish_by_followers(&cache_key(), 3, 1);
    let engine = Hybrid::new(Some(Arc::clone(&marks)), Some(Arc::new(Unleased)));
    let timeout = Duration::from_millis(300);

    engine
        .await_counter_quorum(&broker, &cache_key(), timeout, Some(1), 3, "counter add")
        .await
        .expect("followers decided it at generation 3");

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let err = engine
        .await_counter_quorum(&broker, &cache_key(), timeout, Some(2), 3, "counter add")
        .await
        .expect_err("the mark is short of the add");
    assert!(timed_out_at(&err, timeout), "{err:?}");
    assert!(started.elapsed() >= timeout);
    assert!(started.elapsed() < timeout * 2);

    let err = engine
        .await_counter_quorum(&broker, &cache_key(), timeout, Some(1), 2, "counter add")
        .await
        .expect_err("the mark is at another generation");
    assert!(matches!(
        err.downcast_ref::<QuorumError>(),
        Some(QuorumError::LeadershipLost { .. } | QuorumError::TimedOut { .. })
    ));
}

/// A read skips the lease only for a `Quorum` cache the fleet reads by round.
#[tokio::test]
async fn the_hybrid_skips_the_lease_for_a_quorum_cache_read_by_round() {
    use felix_common::fleet::{FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};
    struct Confirms;
    #[async_trait::async_trait]
    impl crate::quorum::LeadershipCheck for Confirms {
        async fn confirm(&self, _key: &ShardKey, _generation: u64) -> Result<(), QuorumError> {
            Ok(())
        }
    }

    let (broker, _dir) = cache_broker().await;
    let fleet = Arc::new(FleetGate::new([
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
    ]));
    let marks = Arc::new(QuorumMarks::with_fleet(Arc::clone(&fleet)));
    marks.set_read_check(Arc::new(Confirms), false);
    fleet.observe([
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
    ]);
    let engine = hybrid(&marks, served_at_3());
    assert!(engine.read_skips_lease(&broker, &cache_key()).await);

    let bare: Hybrid<ServedAt> = Hybrid::new(None, None);
    assert!(!bare.read_skips_lease(&broker, &cache_key()).await);
    let unreplicated = hybrid(
        &marks,
        ServedAt {
            generation: Some(3),
            replicated: false,
        },
    );
    assert!(!unreplicated.read_skips_lease(&broker, &cache_key()).await);
}

/// A watcher is held to the mark for the served generation and to the timeout
/// it is given.
#[tokio::test]
async fn the_hybrid_holds_a_cache_watcher_to_the_mark_for_its_timeout() {
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&cache_key(), 4, 7);
    let engine = hybrid(&marks, served_at_4());
    let quorum = Some(ConsistencyLevel::Quorum);
    let timeout = Duration::from_millis(300);

    engine
        .await_readable(quorum, &cache_key(), 7, timeout)
        .await
        .expect("the mark covers it");
    engine
        .await_readable(Some(ConsistencyLevel::Leader), &cache_key(), 9, timeout)
        .await
        .expect("a Leader cache waits for nothing");

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let err = engine
        .await_readable(quorum, &cache_key(), 9, timeout)
        .await
        .expect_err("the mark is short of it");
    assert_eq!(
        err,
        QuorumError::TimedOut {
            what: "read",
            timeout
        }
    );
    assert!(started.elapsed() >= timeout);
    assert!(started.elapsed() < timeout * 2);

    let bare: Hybrid<ServedAt> = Hybrid::new(None, None);
    bare.await_readable(quorum, &cache_key(), 9, timeout)
        .await
        .expect("no marks, nothing to wait on");
}

/// The broker core's readers stop at the mark for the served generation and
/// are refused once the lease no longer lets this broker serve, through
/// [`super::CommittedReads`] as well.
#[test]
fn the_hybrid_bounds_the_broker_cores_readers_at_the_mark_and_the_lease() {
    use felix_broker::{ReadBound, ReadBounds};
    let marks = Arc::new(QuorumMarks::new());
    marks.publish(&key(), 3, 5);
    let engine = hybrid(&marks, served_at_4());
    assert_eq!(engine.committed_bound(&key()), ReadBound::Settling);
    marks.publish(&key(), 4, 7);
    assert_eq!(engine.committed_bound(&key()), ReadBound::Committed(7));

    let reads = super::CommittedReads::new(hybrid(&marks, served_at_4()));
    assert_eq!(
        reads.stream_bound("t1", "ns", "orders", 0),
        ReadBound::Committed(7)
    );
    assert_eq!(
        reads.stream_bound("t1", "ns", "orders", 1),
        ReadBound::Settling,
        "another shard has no mark"
    );

    let unleased = Hybrid::new(Some(Arc::clone(&marks)), Some(Arc::new(Unleased)));
    assert_eq!(unleased.committed_bound(&key()), ReadBound::Refused);
    let not_served = hybrid(
        &marks,
        ServedAt {
            generation: None,
            replicated: true,
        },
    );
    assert_eq!(not_served.committed_bound(&key()), ReadBound::Refused);
    let unreplicated = hybrid(
        &marks,
        ServedAt {
            generation: Some(4),
            replicated: false,
        },
    );
    assert_eq!(unreplicated.committed_bound(&key()), ReadBound::Unbounded);
    let bare: Hybrid<ServedAt> = Hybrid::default();
    assert_eq!(bare.committed_bound(&key()), ReadBound::Unbounded);
}

/// A round that refuses every read, remembering the generations it was asked at.
#[derive(Default)]
struct Refuses(parking_lot::Mutex<Vec<u64>>);

#[async_trait::async_trait]
impl crate::quorum::LeadershipCheck for Refuses {
    async fn confirm(&self, _key: &ShardKey, generation: u64) -> Result<(), QuorumError> {
        self.0.lock().push(generation);
        Err(QuorumError::LeadershipLost {
            what: "read",
            detail: "a newer leader fenced the majority",
        })
    }
}

/// A state read on a broker that stands for election but has not heard the
/// fleet enable `lease_free_reads` needs the lease, then the round at the
/// served generation (GetFelix/felix#2161). An unreplicated shard asks only
/// the lease.
#[tokio::test]
async fn the_hybrid_confirms_a_state_read_by_the_lease_then_the_round() {
    use felix_common::fleet::{
        FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK, REPLICA_ELECTION,
    };
    let fleet = Arc::new(FleetGate::new([
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
        REPLICA_ELECTION.name(),
    ]));
    fleet.restart(std::iter::empty());
    let marks = Arc::new(QuorumMarks::with_fleet(fleet));
    let round = Arc::new(Refuses::default());
    marks.set_read_check(Arc::clone(&round) as _, false);

    let unleased = Hybrid::new(Some(Arc::clone(&marks)), Some(Arc::new(Unleased)));
    let err = unleased
        .confirm_read(&key())
        .await
        .expect_err("a lapsed lease");
    assert!(format!("{err:#}").contains("lease"), "{err:#}");
    assert!(round.0.lock().is_empty(), "the lease is checked first");

    hybrid(&marks, served_at_3())
        .confirm_read(&key())
        .await
        .expect_err("the round refuses");
    assert_eq!(
        *round.0.lock(),
        vec![3],
        "one round, at the served generation"
    );

    hybrid(
        &marks,
        ServedAt {
            generation: Some(3),
            replicated: false,
        },
    )
    .confirm_read(&key())
    .await
    .expect("no replicas to ask: the lease decides");
    assert_eq!(round.0.lock().len(), 1);

    let bare: Hybrid<ServedAt> = Hybrid::default();
    bare.confirm_read(&key())
        .await
        .expect("no marks, nothing to confirm");
}

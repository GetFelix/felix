//! Retention on a Raft shard: its leader hides what the stream's bounds drop.

use bytes::Bytes;
use felix_broker::{BrokerError, StartPosition, StreamHandle};
use felix_storage::log::{AppendOnlyLog, Retention};

use super::super::retention::Pass;
use super::*;

/// Long enough that only a test's own calls run a pass.
const NO_TIMER: Duration = Duration::from_secs(3600);

/// A single member leading `orders`, whose own retention keeps about 2 KiB
/// of 512-byte segments, with the retention timer at `interval`.
struct Bounded {
    _root: tempfile::TempDir,
    broker: Arc<Broker>,
    shards: Arc<RaftShards>,
    engine: Arc<crate::shards::routing::StreamEngine>,
    handle: StreamHandle,
    log: DiskLog,
    term: u64,
}

impl Bounded {
    async fn start(interval: Duration, trims_allowed: bool) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        let config = LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            segment_size_bytes: 512,
            retention_check_interval: interval,
            ..LogConfig::default()
        };
        let broker = broker_with_config(
            root.path(),
            1,
            INCARNATION,
            ConsistencyLevel::Leader,
            config,
        )
        .await;
        if trims_allowed {
            broker.allow_raft_head_trims_when(|| true);
        }
        // The bound comes from the stream's registration, as the control
        // plane sends it, not from the broker.
        broker
            .register_stream(
                "t1",
                "ns",
                "orders",
                StreamMetadata {
                    durable: true,
                    incarnation: INCARNATION,
                    replication_factor: Some(1),
                    consistency: ConsistencyLevel::Leader,
                    retention: Retention {
                        bytes: Some(2048),
                        age: None,
                    },
                    ..StreamMetadata::default()
                },
            )
            .await
            .expect("the stream's bounds");
        let shards = shards(&broker);
        lead_orders(&shards).await;
        let orders = key("orders", 0);
        let term = shards.leading(&orders).expect("term");
        let engine = Arc::new(engine(&shards));
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .expect("handle");
        let log = shards.log_of(&orders).await.expect("log");
        Self {
            _root: root,
            broker,
            shards,
            engine,
            handle,
            log,
            term,
        }
    }

    async fn publish(&self, count: usize) {
        for _ in 0..count {
            publish_one(&self.broker, &self.engine, &self.handle, self.term).await;
        }
    }

    async fn pass(&self) -> Pass {
        self.shards
            .enforce_retention(&key("orders", 0), &self.log, &self.handle)
            .await
    }

    async fn tail(&self) -> u64 {
        self.log.tail_offset().await.expect("tail")
    }
}

async fn publish_one(
    broker: &Arc<Broker>,
    engine: &crate::shards::routing::StreamEngine,
    handle: &StreamHandle,
    term: u64,
) {
    crate::serving::direct_publish::publish_direct(
        broker,
        None,
        engine,
        target(handle.clone(), &key("orders", 0), term),
        &[Bytes::from(vec![7u8; 64])],
        None,
        Duration::from_secs(10),
    )
    .await
    .expect("published");
}

/// **The leader hides what the stream's own retention bounds drop, on the
/// retention timer, and deletes nothing.** A Raft log refuses bounds of its
/// own, so without the leader's trim they would never be enforced.
#[tokio::test]
async fn the_leader_trims_what_the_streams_retention_drops() {
    let shard = Bounded::start(Duration::from_millis(20), true).await;
    let first = shard.tail().await;
    shard.publish(64).await;

    until("retention to raise the floor", || {
        shard.log.head_trim().is_some_and(|trim| trim.floor > first)
    })
    .await;

    assert!(
        shard.log.base_offset() <= first,
        "a Raft log's entries go only by a purge"
    );
    assert!(
        matches!(
            shard
                .broker
                .subscribe_from("t1", "ns", "orders", 0, StartPosition::Offset(first))
                .await,
            Err(BrokerError::CursorTooOld { .. })
        ),
        "a resume below the floor was served"
    );
    shard.shards.runtime.shutdown().await;
}

/// **Retention keeps up on a busy shard.** A trim conditional on the tail
/// it read is refused by any publish landing first, which on a busy shard
/// is nearly every time, so the floor would stay behind for as long as
/// publishes keep coming. The pass is held after it planned its trim, a
/// publish lands, and the trim must still go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_trims_a_shard_when_a_publish_lands_mid_pass() {
    let shard = Arc::new(Bounded::start(NO_TIMER, true).await);
    shard.publish(64).await;
    shard.shards.retention_gate.arm();
    let pass = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move { shard.pass().await })
    };
    shard.shards.retention_gate.reached.notified().await;
    let planned = shard.tail().await;
    shard.publish(1).await;
    assert!(shard.tail().await > planned, "the publish did not land");
    shard.shards.retention_gate.release.notify_one();

    let pass = pass.await.expect("pass");
    assert!(
        matches!(pass, Pass::Trimmed(_)),
        "a publish landing mid-pass refused the trim: {pass:?}"
    );
    shard.shards.runtime.shutdown().await;
}

/// **A member that does not lead the shard proposes nothing**, though its
/// own log is past the bounds: followers never evaluate them, and a
/// deposed leader's term is gone.
#[tokio::test]
async fn a_member_not_leading_proposes_no_trim() {
    let shard = Bounded::start(NO_TIMER, true).await;
    shard.publish(64).await;
    let tail = shard.tail().await;
    shard.shards.leading.forget(&key("orders", 0));

    assert_eq!(shard.pass().await, Pass::Nothing);
    assert_eq!(shard.tail().await, tail, "a trim was written");
    assert!(shard.log.head_trim().is_none());
    shard.shards.runtime.shutdown().await;
}

/// **A member deposed between planning its trim and proposing it writes
/// nothing and says nothing**: the next leader runs its own pass, and a
/// warning here would be noise on every failover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_deposed_mid_pass_proposes_nothing_and_does_not_warn() {
    let shard = Arc::new(Bounded::start(NO_TIMER, true).await);
    shard.publish(64).await;
    let tail = shard.tail().await;
    shard.shards.retention_gate.arm();
    let pass = {
        let shard = Arc::clone(&shard);
        tokio::spawn(async move { shard.pass().await })
    };
    shard.shards.retention_gate.reached.notified().await;
    // The group going away is what makes the proposal fail; the member
    // no longer leading is what makes that a deposition.
    shard.shards.leading.forget(&key("orders", 0));
    shard.shards.runtime.shutdown().await;
    shard.shards.retention_gate.release.notify_one();

    // A refusal, not Nothing, is what the timer logs as a warning.
    assert_eq!(pass.await.expect("pass"), Pass::Nothing);
    assert_eq!(shard.tail().await, tail, "a trim was written");
}

/// **Until the fleet finalizes `raft_head_trim`, retention writes nothing**
/// and says why: a member that predates trim entries would stop
/// replicating at one.
#[tokio::test]
async fn retention_writes_nothing_until_raft_head_trim_is_finalized() {
    let shard = Bounded::start(NO_TIMER, false).await;
    shard.publish(64).await;
    let tail = shard.tail().await;

    let pass = shard.pass().await;

    assert!(
        matches!(&pass, Pass::Refused(why) if why.contains("raft_head_trim")),
        "{pass:?}"
    );
    assert_eq!(shard.tail().await, tail, "a trim was written");
    shard.shards.runtime.shutdown().await;
}

/// **A zero retention interval starts no retention timer**, which could not
/// run on it. It is valid while no stream sets retention, and one that does
/// is refused at registration.
#[tokio::test]
async fn a_zero_retention_interval_starts_no_timer() {
    let root = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        retention_check_interval: Duration::ZERO,
        ..LogConfig::default()
    };
    let broker = broker_with_config(
        root.path(),
        1,
        INCARNATION,
        ConsistencyLevel::Leader,
        config,
    )
    .await;
    let shards = shards(&broker);
    lead_orders(&shards).await;
    let running = shards
        .open
        .lock()
        .await
        .get(&key("orders", 0))
        .map(|member| member.retention.is_some());
    assert_eq!(running, Some(false));
    shards.runtime.shutdown().await;
}

/// **A Raft member's log does not offload**, even on a broker that offloads
/// its hybrid logs. Only a purge may drop a Raft log's entries, and an
/// offload pass reads a stream log's layout.
#[tokio::test]
async fn a_raft_member_does_not_offload() {
    let root = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        offload: Some(felix_storage::log::OffloadTarget::LocalDir {
            dir: root.path().join("archive"),
            node: "broker-a".to_string(),
        }),
        ..LogConfig::default()
    };
    let broker = broker_with_config(
        root.path(),
        1,
        INCARNATION,
        ConsistencyLevel::Leader,
        config,
    )
    .await;
    let shards = shards(&broker);
    lead_orders(&shards).await;
    let log = shards.log_of(&key("orders", 0)).await.expect("log");
    assert_eq!(log.config().offload, None);
    shards.runtime.shutdown().await;
}

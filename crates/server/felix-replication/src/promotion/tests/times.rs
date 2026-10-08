//! Record times through replication and promotion: a follower stores the
//! leader's, so a promoted replica reports the times readers saw before the
//! failover, and a peer on either side that predates them still replicates.

use felix_broker::LogKind;
use felix_wire::internal::GenerationStart;

use super::*;

/// Times far from any clock this test could read, so a record stamped by a
/// follower's own clock cannot pass for one of them.
const TIMES: [u64; 3] = [1_000_001, 1_000_002, 1_000_003];

/// Delivers a leader's batches to one replica's handler through the codec,
/// as the pool would, offering `capabilities` and remembering what was sent.
struct Follower<'a> {
    handler: &'a ReplicaHandler,
    capabilities: PeerCapabilities,
    sent: Mutex<Vec<Kind>>,
}

impl<'a> Follower<'a> {
    fn new(handler: &'a ReplicaHandler, capabilities: PeerCapabilities) -> Self {
        Self {
            handler,
            capabilities,
            sent: Mutex::new(Vec::new()),
        }
    }
}

impl PeerRequester for Follower<'_> {
    async fn request(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        self.sent.lock().expect("lock").push(message.kind());
        let message = InternalMessage::decode(message.encode().expect("encode")).expect("decode");
        match message {
            InternalMessage::ReplicateRecords(batch)
            | InternalMessage::ReplicateMarkedRecords(batch) => {
                Ok(self.handler.apply(batch, LogKind::Stream).await)
            }
            other => panic!("the leader sent {:?}", other.kind()),
        }
    }

    fn recorded_capabilities(&self, _node_id: &str) -> Option<PeerCapabilities> {
        Some(self.capabilities)
    }
}

/// A leader at `generation` holding `values` stamped with `TIMES`.
async fn timed_leader(generation: u64, values: &[&str]) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker_on(dir.path());
    let log = stream_log(&broker).await;
    log.record_generation(generation, 0).expect("generation");
    let payloads: Vec<Bytes> = values
        .iter()
        .map(|v| Bytes::copy_from_slice(v.as_bytes()))
        .collect();
    let pending = log
        .begin_append_marked_at(0, &payloads, &[], &[], &TIMES[..values.len()])
        .await
        .expect("append")
        .expect("at the tail");
    log.commit(&pending).await.expect("commit");
    (broker, dir)
}

async fn stream_log(broker: &Broker) -> felix_broker::StreamLog {
    broker
        .shard_log(LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
}

async fn times(broker: &Broker) -> Vec<u64> {
    stream_log(broker)
        .await
        .read_log_from(0, 1 << 20)
        .await
        .expect("read")
        .iter()
        .map(|record| record.timestamp_micros)
        .collect()
}

/// Ship everything `leader` holds to `follower`, at `generation`.
async fn ship_all<R: PeerRequester>(follower: &R, leader: &Broker, generation: u64) {
    let mut cursor =
        crate::follower::FollowerCursor::new(LEADER, "10.0.0.1:7001".parse().expect("addr"), 0);
    let progress = crate::ship::ship_once(
        follower,
        &stream_log(leader).await,
        &batch(generation, 0, &[]).shard,
        LogKind::Stream,
        &mut cursor,
        1 << 20,
        &crate::rebuild::Rebuilds::disabled(),
    )
    .await;
    assert!(
        matches!(progress, crate::ship::Progress::Stored { .. }),
        "{progress:?}"
    );
}

/// **A promoted replica reports the leader's times for the records it
/// inherited**, and answers a time lookup against them, rather than the
/// times its own clock read as the records arrived.
#[tokio::test]
async fn a_promoted_replica_reports_the_leaders_times() {
    let (leader, _leader_dir) = timed_leader(1, &["a", "b", "c"]).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let replica = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&replica), router_for("broker-b", 1));
    let follower = Follower::new(
        &handler,
        REQUIRED
            .union(PeerCapabilities::GENERATION_LABELS)
            .union(PeerCapabilities::RECORD_TIMES),
    );

    ship_all(&follower, &leader, 1).await;

    assert_eq!(
        *follower.sent.lock().expect("lock"),
        vec![Kind::ReplicateTimedRecords]
    );
    assert_eq!(times(&replica).await, TIMES);
    let log = stream_log(&replica).await;
    assert_eq!(
        log.offset_for_time(TIMES[1], 3).await.expect("lookup"),
        Some((1, TIMES[1]))
    );
}

/// **A follower that predates shipped times is sent what it reads**: the
/// labelled batch, without them. It stamps the records with its own clock.
#[tokio::test]
async fn a_follower_without_the_capability_is_sent_no_times() {
    let (leader, _leader_dir) = timed_leader(1, &["a", "b"]).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let replica = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&replica), router_for("broker-b", 1));
    let follower = Follower::new(
        &handler,
        REQUIRED.union(PeerCapabilities::GENERATION_LABELS),
    );

    ship_all(&follower, &leader, 1).await;

    assert_eq!(
        *follower.sent.lock().expect("lock"),
        vec![Kind::ReplicateLabelledRecords]
    );
    assert!(
        times(&replica)
            .await
            .iter()
            .all(|time| !TIMES.contains(time)),
        "an old follower cannot have stored the leader's times"
    );
}

/// **A leader that predates shipped times sends none, and the follower uses
/// its own clock**, so a rolling upgrade keeps replicating in both
/// directions.
#[tokio::test]
async fn a_batch_from_a_leader_without_times_takes_the_followers_clock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let replica = broker_on(dir.path());
    let handler = ReplicaHandler::new(Arc::clone(&replica), router_for("broker-b", 1));
    let mut labelled = batch(1, 0, &["a", "b"]);
    labelled.generations = Some(vec![GenerationStart {
        generation: 1,
        start_offset: 0,
    }]);
    let before = felix_broker::append_time_now();

    let answer = handler.apply(labelled, LogKind::Stream).await;

    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    let stored = times(&replica).await;
    assert_eq!(stored.len(), 2);
    assert!(stored.iter().all(|time| *time >= before), "{stored:?}");
}

/// **A promoted leader that takes a replica's tail takes its times too**, so
/// records it never held before the failover keep the time they were
/// written at.
#[tokio::test]
async fn a_tail_taken_from_a_replica_keeps_its_times() {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get_mut(id).capabilities = REQUIRED
            .union(PeerCapabilities::GENERATION_LABELS)
            .union(PeerCapabilities::RECORD_TIMES);
    }
    // broker-b took all three from the old leader, with its times.
    let mut shipped = batch(4, 0, &["a", "b", "c"]);
    shipped.generations = Some(vec![GenerationStart {
        generation: 4,
        start_offset: 0,
    }]);
    shipped.times = Some(TIMES.to_vec());
    let handler = ReplicaHandler::new(
        Arc::clone(&replicas.get("broker-b").broker),
        router_for("broker-b", 4),
    );
    let answer = handler.apply(shipped, LogKind::Stream).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = timed_leader(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(held(&leader).await, vec!["a", "b", "c"]);
    assert_eq!(times(&leader).await, TIMES);
    assert!(!replicas.sent(Kind::ReplicateTimedFetch).is_empty());
}

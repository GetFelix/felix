//! Publishes to a Raft shard through the control-stream handlers and the
//! real scheduler, with a subscriber on the leader. Every record must reach
//! it whether or not the publisher asked for an answer.

use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::shards::raft::tests as fixture;
use crate::shards::routing::{EngineShards, StreamEngine};

use super::*;

struct Leader {
    broker: Arc<Broker>,
    shards: Arc<crate::shards::raft::RaftShards>,
    ctx: PublishContext,
    out_tx: mpsc::Sender<Outgoing>,
    out_rx: mpsc::Receiver<Outgoing>,
    cache: crate::serving::quic::handlers::publish::stream_cache::StreamHandleCache,
    _root: tempfile::TempDir,
}

/// A broker leading `orders` shard 0 on Raft, wired as a cluster member is:
/// routes that ask the Raft shards first, and executors behind the queue.
async fn leader(ack_durable_on_commit: bool) -> Leader {
    leader_of(
        ack_durable_on_commit,
        felix_broker::ConsistencyLevel::Leader,
    )
    .await
}

/// [`leader`], with `orders` at `consistency`.
async fn leader_of(
    ack_durable_on_commit: bool,
    consistency: felix_broker::ConsistencyLevel,
) -> Leader {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker_at(root.path(), 1, fixture::INCARNATION, consistency).await;
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let engine = Arc::new(StreamEngine::new(None, Some(fixture::ingress(&shards))));
    engine.set_raft(Arc::clone(&shards));
    let config = BrokerConfig {
        pub_workers_per_conn: 1,
        pub_queue_depth: 64,
        ..BrokerConfig::default()
    };
    let cluster = ClusterContext {
        engine,
        ..ClusterContext::default()
    };
    let mut ctx = build_publish_context(Arc::clone(&broker), &config, cluster);
    ctx.ack_durable_on_commit = ack_durable_on_commit;
    let (out_tx, out_rx) = mpsc::channel(8);
    Leader {
        broker,
        shards,
        ctx,
        out_tx,
        out_rx,
        cache: HashMap::default(),
        _root: root,
    }
}

impl Leader {
    async fn publish_batch(&mut self, payloads: Vec<Bytes>, request_id: Option<u64>) {
        let ack = match request_id {
            Some(_) => felix_wire::AckMode::PerBatch,
            None => felix_wire::AckMode::None,
        };
        let commit_acks = CommitAcks::for_test(&self.out_tx, Arc::new(Semaphore::new(8)));
        handle_publish_batch_message(
            0,
            &self.broker,
            &self.ctx,
            None,
            &mut self.cache,
            &mut String::new(),
            false,
            false,
            AckEncoding::Json,
            &self.out_tx,
            &Arc::new(AtomicUsize::new(0)),
            &watch::channel(false).0,
            &Arc::new(parking_lot::Mutex::new(
                AckTimeoutState::new(Instant::now()),
            )),
            &watch::channel(false).0,
            &commit_acks,
            "t1".to_string(),
            "ns".to_string(),
            "orders".to_string(),
            payloads,
            None,
            None,
            None,
            request_id,
            Some(ack),
            false,
            String::new(),
            None,
        )
        .await
        .expect("batch handled");
    }

    async fn publish(&mut self, payload: &[u8], request_id: Option<u64>) {
        let ack = match request_id {
            Some(_) => felix_wire::AckMode::PerMessage,
            None => felix_wire::AckMode::None,
        };
        let commit_acks = CommitAcks::for_test(&self.out_tx, Arc::new(Semaphore::new(8)));
        handle_publish_message(
            &self.broker,
            &self.ctx,
            &mut self.cache,
            &mut String::new(),
            false,
            false,
            &self.out_tx,
            &Arc::new(AtomicUsize::new(0)),
            &watch::channel(false).0,
            &Arc::new(parking_lot::Mutex::new(
                AckTimeoutState::new(Instant::now()),
            )),
            &watch::channel(false).0,
            &commit_acks,
            "t1".to_string(),
            "ns".to_string(),
            "orders".to_string(),
            payload.to_vec(),
            None,
            None,
            request_id,
            Some(ack),
            false,
            String::new(),
        )
        .await
        .expect("publish handled");
    }

    /// Where `shard`'s committed records end.
    fn committed(&self, shard: &crate::shards::ShardKey) -> u64 {
        use felix_replication::engine::Engine;
        self.ctx
            .engine
            .read_bound(None, shard)
            .ok()
            .flatten()
            .expect("a commit bound")
    }

    async fn acknowledged(&mut self, request_id: u64) {
        let answer = tokio::time::timeout(Duration::from_secs(10), self.out_rx.recv()).await;
        match answer.expect("answered in time").expect("an answer") {
            Outgoing::Message(Message::PublishOk { request_id: id, .. }) if id == request_id => {}
            other => panic!("publish {request_id} was not acknowledged: {other:?}"),
        }
    }
}

fn records(prefix: &str, count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|i| Bytes::from(format!("{prefix}-{i}")))
        .collect()
}

/// The next `count` records the subscriber gets, with their offsets.
async fn delivered(
    receiver: &mut felix_broker::SubscriptionReceiver,
    count: usize,
) -> Vec<(u64, Bytes)> {
    let mut got = Vec::new();
    while got.len() < count {
        let next = tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await;
        let envelope = next
            .unwrap_or_else(|_| panic!("{} of {count} records delivered", got.len()))
            .expect("subscription open");
        let base = envelope.base_offset().expect("an offset");
        got.extend((base..).zip(envelope.payloads().iter().cloned()));
    }
    got
}

/// **A publish nobody waits on reaches a Raft shard's subscribers like an
/// acknowledged one.** It has no place in the hybrid's write fence, which
/// never opens for the shard, so entering it at the queue refused the batch
/// with nobody to tell.
#[tokio::test]
async fn an_unacknowledged_publish_to_a_raft_shard_is_delivered() {
    let mut leader = leader(BrokerConfig::default().ack_durable_on_commit).await;
    let (mut receiver, _guard) = leader
        .broker
        .subscribe("t1", "ns", "orders", 0)
        .await
        .expect("subscribe")
        .into_parts();

    let unacked = records("unacked", 16);
    leader.publish_batch(unacked.clone(), None).await;
    let acked = records("acked", 16);
    leader.publish_batch(acked.clone(), Some(7)).await;
    leader.acknowledged(7).await;
    leader.publish(b"one-unacked", None).await;
    leader.publish(b"one-acked", Some(8)).await;
    leader.acknowledged(8).await;

    let sent: Vec<Bytes> = unacked
        .into_iter()
        .chain(acked)
        .chain([
            Bytes::from_static(b"one-unacked"),
            Bytes::from_static(b"one-acked"),
        ])
        .collect();
    let got = delivered(&mut receiver, sent.len()).await;
    let first = got[0].0;
    let expected: Vec<(u64, Bytes)> = (first..).zip(sent).collect();
    assert_eq!(got, expected, "a record was dropped or reordered");
    EngineShards::shutdown(&*leader.shards).await;
}

/// **A `Quorum` stream on Raft answers at the commit even with
/// `FELIX_ACK_ON_COMMIT=0`.** An answer on enqueue would promise a write the
/// term may not live to make. A `Leader` stream may be answered on the
/// lease, which is after the write but before the commit.
#[tokio::test]
async fn a_quorum_stream_on_raft_answers_at_the_commit_with_ack_on_commit_off() {
    let mut leader = leader_of(false, felix_broker::ConsistencyLevel::Quorum).await;
    let orders = fixture::key("orders", 0);
    let before = leader.committed(&orders);

    let batch = records("queued", 4);
    leader.publish_batch(batch.clone(), Some(3)).await;
    assert!(
        leader.out_rx.try_recv().is_err(),
        "answered when it was queued"
    );
    leader.acknowledged(3).await;
    let committed = leader.committed(&orders);
    assert!(
        committed >= before + batch.len() as u64,
        "answered at {committed}, before the batch committed"
    );

    leader.publish(b"one", Some(4)).await;
    assert!(
        leader.out_rx.try_recv().is_err(),
        "answered when it was queued"
    );
    leader.acknowledged(4).await;
    let after = leader.committed(&orders);
    assert!(after > committed, "answered before the record committed");
    EngineShards::shutdown(&*leader.shards).await;
}

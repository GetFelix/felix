//! An idempotent Kafka producer through a leader failover and a move.
//!
//! The producer here is a few lines of Kafka protocol rather than kcat, so it
//! can do what a real producer only does by bad luck: re-send a batch whose
//! answer it never got, to whichever broker leads the shard now. No Docker
//! needed.
//!
//! Run with `cargo test -p felix-cluster --test failures kafka_produce::`.
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec, wait};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    FetchRequest, InitProducerIdRequest, ListOffsetsRequest, ProduceRequest, RequestHeader,
    SaslAuthenticateRequest, SaslHandshakeRequest, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, Record, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};
use serial_test::serial;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const STREAM: &str = "orders";
/// Kafka's codes for "go and ask Metadata again".
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const RETRIABLE: [i16; 3] = [3, 5, NOT_LEADER_OR_FOLLOWER];

/// **An idempotent Kafka producer's re-send lands once, whoever leads.** The
/// producer writes five records to a `Quorum` stream with acks=all. The leader
/// is killed; the producer re-sends its last batch, as it would had the answer
/// been lost, to the promoted leader, which answers with the original offset
/// and writes nothing. The producer carries on, the shard moves, and the same
/// happens at the destination. A reader then sees every record once, in
/// order, with no gaps.
///
/// It holds because the sequences are in the log: a leader keeping them in
/// memory, or the listener doing so, would know nothing of the producer after
/// either change.
#[serial]
#[tokio::test]
async fn an_idempotent_kafka_producer_lands_each_record_once_through_failover_and_a_move() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        kafka: true,
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let topic = format!("{}.{STREAM}", cluster.namespace);

    let first = cluster
        .serving_leader(STREAM, Duration::from_secs(30))
        .await
        .expect("serving leader");
    let mut producer = Producer::connect(&cluster, &first, &topic).await;
    let mut offsets = Vec::new();
    for sequence in 0..5 {
        offsets.push(producer.send_ok(sequence).await);
    }
    // Every replica level with the leader, so whichever is promoted holds
    // the batches.
    wait_until_replicated(&cluster, &first).await;

    cluster.stop_node(&first).await.expect("kill the leader");
    let second = new_owner(&cluster, &[first.as_str()]).await;
    let mut producer = Producer::connect(&cluster, &second, &topic)
        .await
        .with_id(producer.id);
    let (error, offset) = producer.send_until_served(4).await;
    assert_eq!(
        (error, offset),
        (0, offsets[4]),
        "the promoted leader did not answer the re-send with its original offset"
    );
    for sequence in 5..10 {
        offsets.push(producer.send_ok(sequence).await);
    }

    // A Raft shard's leader moves only within its voting set, and placement
    // does not manage that yet (#2235), so the move half runs on hybrid only.
    if felix_cluster::suite_runs_on_raft() {
        let read = read_back(&cluster, &second).await;
        let expected: Vec<(u64, String)> = offsets
            .iter()
            .enumerate()
            .map(|(i, offset)| (*offset as u64, format!("r{i}")))
            .collect();
        assert_eq!(read, expected, "every record once, in order");
        cluster.shutdown().await;
        return;
    }
    let third = cluster
        .node_ids()
        .into_iter()
        .find(|node| node != &first && node != &second)
        .expect("a third broker");
    move_shard(&cluster, &third).await;
    let (error, _) = producer.send(9).await;
    assert_eq!(error, NOT_LEADER_OR_FOLLOWER, "the old leader took a write");
    let mut producer = Producer::connect(&cluster, &third, &topic)
        .await
        .with_id(producer.id);
    let (error, offset) = producer.send_until_served(9).await;
    assert_eq!(
        (error, offset),
        (0, offsets[9]),
        "the move's destination did not answer the re-send with its original offset"
    );
    offsets.push(producer.send_ok(10).await);

    let read = read_back(&cluster, &third).await;
    let expected: Vec<(u64, String)> = offsets
        .iter()
        .enumerate()
        .map(|(i, offset)| (*offset as u64, format!("r{i}")))
        .collect();
    assert_eq!(read, expected, "every record once, in order");
    assert!(
        offsets.windows(2).all(|pair| pair[1] > pair[0]),
        "{offsets:?}"
    );
    cluster.shutdown().await;
}

/// **On a Raft shard, an idempotent Kafka producer's re-send lands once,
/// whoever leads.** As above, with the shard run by a Raft group. A batch of
/// five records is in flight when the leader is killed, so the group may hold
/// any prefix of it; its re-send to the next leader writes only the rest, an
/// older re-send is answered with its original offset, and a reader sees
/// every record once, in order.
#[serial]
#[tokio::test]
async fn an_idempotent_kafka_producer_on_a_raft_shard_lands_each_record_once_through_failover() {
    if !super::raft::opted_in() {
        return;
    }
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        kafka: true,
        broker_env: vec![super::raft::engine()],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let topic = format!("{}.{STREAM}", cluster.namespace);
    let nodes = cluster.node_ids();

    let first = super::raft::leader_among(&cluster, &nodes).await;
    let mut producer = Producer::connect(&cluster, &first, &topic).await;
    let mut offsets = vec![producer.send_until_served(0).await.1];
    for sequence in 1..5 {
        offsets.push(producer.send_ok(sequence).await);
    }
    let (error, offset) = producer.send(4).await;
    assert_eq!(
        (error, offset),
        (0, offsets[4]),
        "the leader wrote a re-send"
    );
    let id = producer.id;

    // A batch of several records goes out as the leader is killed, so the
    // group may hold any prefix of it.
    let cut = tokio::spawn(async move { producer.send_batch(5, 5).await });
    cluster.stop_node(&first).await.expect("kill the leader");
    let _ = cut.await;
    let live: Vec<String> = nodes.into_iter().filter(|node| node != &first).collect();
    let second = super::raft::leader_among(&cluster, &live).await;
    let mut producer = Producer::connect(&cluster, &second, &topic)
        .await
        .with_id(id);
    let (error, resent) = producer.send_batch_until_served(5, 5).await;
    assert_eq!(error, 0, "the next leader refused the cut batch's re-send");
    let (error, again) = producer.send(4).await;
    assert_eq!(
        (error, again),
        (0, offsets[4]),
        "the next leader did not answer an old re-send with its original offset"
    );
    let next = producer.send_ok(10).await;

    let read = read_back(&cluster, &second).await;
    let payloads: Vec<String> = read.iter().map(|(_, payload)| payload.clone()).collect();
    let expected: Vec<String> = (0..11).map(|i| format!("r{i}")).collect();
    assert_eq!(payloads, expected, "every record once, in order");
    let at = |payload: &str| {
        read.iter()
            .find(|(_, held)| held == payload)
            .map(|(offset, _)| *offset as i64)
    };
    assert_eq!(at("r0"), Some(offsets[0]));
    assert_eq!(
        at("r5"),
        Some(resent),
        "the re-send answered with another offset"
    );
    assert_eq!(at("r10"), Some(next));
    cluster.shutdown().await;
}

/// **On a Raft shard, a bumped epoch is written at sequence 0 and the old
/// epoch is fenced, whoever leads.** The epoch is in each record's producer
/// mark, so the next leader derives it from the log.
#[serial]
#[tokio::test]
async fn a_kafka_epoch_bump_on_a_raft_shard_survives_failover() {
    if !super::raft::opted_in() {
        return;
    }
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        kafka: true,
        broker_env: vec![super::raft::engine()],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let topic = format!("{}.{STREAM}", cluster.namespace);
    let nodes = cluster.node_ids();

    let first = super::raft::leader_among(&cluster, &nodes).await;
    let mut producer = Producer::connect(&cluster, &first, &topic).await;
    let (error, _) = producer.send_until_served(0).await;
    assert_eq!(error, 0, "the first send was never served");
    producer.send_ok(1).await;
    let mut producer = producer.with_epoch(1);
    let bumped = producer.send_ok(0).await;
    // Acknowledged on a majority (`acks=-1`), so the next leader holds it.
    let id = producer.id;
    cluster.stop_node(&first).await.expect("kill the leader");
    let live: Vec<String> = nodes.into_iter().filter(|node| node != &first).collect();
    let second = super::raft::leader_among(&cluster, &live).await;
    let mut stale = Producer::connect(&cluster, &second, &topic)
        .await
        .with_id(id);
    let (error, _) = stale.send_until_served(2).await;
    assert_eq!(error, 47, "the next leader took the fenced epoch");
    let mut producer = stale.with_epoch(1);
    let (error, again) = producer.send(0).await;
    assert_eq!((error, again), (0, bumped), "the bump's re-send");
    producer.send_ok(1).await;

    let read = read_back(&cluster, &second).await;
    let payloads: Vec<&str> = read.iter().map(|(_, payload)| payload.as_str()).collect();
    assert_eq!(payloads, ["r0", "r1", "r0", "r1"]);
    cluster.shutdown().await;
}

/// The shard engine a Kafka test runs on.
#[derive(Clone, Copy, PartialEq)]
enum Engine {
    Hybrid,
    Raft,
}

impl Engine {
    fn broker_env(self) -> Vec<(String, String)> {
        match self {
            Engine::Hybrid => Vec::new(),
            Engine::Raft => vec![super::raft::engine()],
        }
    }

    /// Whether the brokers run Raft groups: a hybrid test runs on Raft when
    /// the suite does.
    fn raft(self) -> bool {
        self == Engine::Raft || felix_cluster::suite_runs_on_raft()
    }

    /// The broker leading the shard among `among`.
    async fn leader(self, cluster: &Cluster, among: &[String]) -> String {
        match self.raft() {
            false => cluster
                .serving_leader(STREAM, Duration::from_secs(30))
                .await
                .expect("serving leader"),
            true => super::raft::leader_among(cluster, among).await,
        }
    }

    /// The leader once none of `gone` is.
    async fn next_leader(self, cluster: &Cluster, gone: &str, live: &[String]) -> String {
        match self.raft() {
            false => new_owner(cluster, &[gone]).await,
            true => super::raft::leader_among(cluster, live).await,
        }
    }
}

/// **A Kafka consumer reads what was produced, at the offsets the producer
/// was told, before and after the leader changes.** ListOffsets and Fetch are
/// bounded by what the shard has committed, so a new leader that answered
/// from a stale or settling bound would show a gap, a reused offset or a
/// record that moved.
#[serial]
#[tokio::test]
async fn kafka_fetch_and_list_offsets_hold_through_a_failover() {
    reads_through_a_failover(Engine::Hybrid).await;
}

#[serial]
#[tokio::test]
async fn kafka_fetch_and_list_offsets_hold_through_a_failover_on_a_raft_shard() {
    reads_through_a_failover(Engine::Raft).await;
}

async fn reads_through_a_failover(engine: Engine) {
    if engine == Engine::Raft && !super::raft::opted_in() {
        return;
    }
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        kafka: true,
        broker_env: engine.broker_env(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let topic = format!("{}.{STREAM}", cluster.namespace);
    let nodes = cluster.node_ids();

    let first = engine.leader(&cluster, &nodes).await;
    let mut producer = Producer::connect(&cluster, &first, &topic).await;
    let mut offsets = Vec::new();
    for sequence in 0..5 {
        offsets.push(producer.send_ok(sequence).await);
    }
    if engine == Engine::Hybrid {
        wait_until_replicated(&cluster, &first).await;
    }
    let id = producer.id;

    let latest = producer.latest_until_served().await;
    assert!(
        latest > offsets[4],
        "latest {latest} is not past {offsets:?}"
    );
    assert!(producer.earliest().await <= offsets[0]);
    assert_eq!(
        producer.fetch_until_served(offsets[2]).await,
        [
            (offsets[2], "r2".to_string()),
            (offsets[3], "r3".to_string()),
            (offsets[4], "r4".to_string()),
        ],
        "a fetch from the middle on the first leader"
    );
    assert!(producer.fetch_until_served(latest).await.is_empty());

    cluster.stop_node(&first).await.expect("kill the leader");
    let live: Vec<String> = nodes.into_iter().filter(|node| *node != first).collect();
    let second = engine.next_leader(&cluster, &first, &live).await;
    let producer = Producer::connect(&cluster, &second, &topic)
        .await
        .with_id(id);
    let expected: Vec<(i64, String)> = offsets
        .iter()
        .enumerate()
        .map(|(i, offset)| (*offset, format!("r{i}")))
        .collect();
    // A new leader can answer its first fetch with an empty success before
    // its commit bound covers the records replicated before the kill, so an
    // empty or short answer is "not settled yet". If the records never
    // arrive within the budget the test still fails, and that is a real bug.
    let producer = tokio::sync::Mutex::new(producer);
    let last: tokio::sync::Mutex<Vec<(i64, String)>> = tokio::sync::Mutex::new(Vec::new());
    let (producer_ref, last_ref, expected_ref, first_offset) =
        (&producer, &last, &expected, offsets[0]);
    wait::until(
        Duration::from_secs(60),
        "the next leader to serve every committed record",
        || async move {
            let got = producer_ref.lock().await.fetch_until_served(first_offset).await;
            // Only a prefix of the committed records may be "not settled yet";
            // any other answer is a wrong record served, and fails at once.
            assert!(
                expected_ref.starts_with(&got),
                "the next leader served records that were not committed: {got:?}, expected {expected_ref:?}"
            );
            let settled = got == *expected_ref;
            *last_ref.lock().await = got;
            settled
        },
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "the next leader moved or lost a committed record: {error}; last answer {:?}, expected {expected:?}",
            last.try_lock().map(|l| l.clone())
        )
    });
    let mut producer = producer.into_inner();
    let latest_before = latest;
    let latest_seen = tokio::sync::Mutex::new(0i64);
    let (producer_ref, seen_ref) = (&tokio::sync::Mutex::new(&mut producer), &latest_seen);
    wait::until(
        Duration::from_secs(60),
        "the next leader's latest offset to cover the old leader's",
        || async move {
            let after = producer_ref.lock().await.latest_until_served().await;
            *seen_ref.lock().await = after;
            after >= latest_before
        },
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "latest went back from {latest}: {error}; last answer {:?}",
            latest_seen.try_lock().map(|v| *v)
        )
    });

    let next = producer.send_ok(5).await;
    assert!(next >= latest, "offset {next} was already handed out");
    assert_eq!(
        producer.fetch_until_served(latest).await,
        [(next, "r5".to_string())]
    );
    cluster.shutdown().await;
}

/// A Kafka connection speaking as one idempotent producer.
struct Producer {
    stream: TcpStream,
    next_correlation: i32,
    topic: String,
    id: i64,
    epoch: i16,
}

impl Producer {
    /// Connect to `node`'s Kafka listener, authenticate, and take a producer
    /// id.
    async fn connect(cluster: &Cluster, node: &str, topic: &str) -> Self {
        let addr = cluster
            .node(node)
            .and_then(|node| node.kafka_addr.clone())
            .expect("kafka listener");
        // Advertised for a container; this test dials from the host.
        let port = addr.rsplit_once(':').expect("host:port").1;
        let stream = TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("connect");
        let mut producer = Self {
            stream,
            next_correlation: 1,
            topic: topic.to_string(),
            id: -1,
            epoch: 0,
        };
        let handshake = producer
            .call(
                &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
                1,
            )
            .await;
        assert_eq!(handshake.error_code, 0);
        let login = producer
            .call(
                &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(format!(
                    "\0{}\0{}",
                    cluster.tenant_id,
                    cluster.client_token()
                ))),
                2,
            )
            .await;
        assert_eq!(login.error_code, 0, "{:?}", login.error_message);
        let init = producer.call(&InitProducerIdRequest::default(), 4).await;
        assert_eq!(init.error_code, 0);
        producer.id = init.producer_id.0;
        producer
    }

    /// Keep producing as `id`: the same producer on a new connection.
    fn with_id(mut self, id: i64) -> Self {
        self.id = id;
        self
    }

    /// Keep producing as the same id at `epoch`, as a KIP-360 bump does.
    fn with_epoch(mut self, epoch: i16) -> Self {
        self.epoch = epoch;
        self
    }

    /// Send record `r{sequence}` as a one-record batch at that sequence, and
    /// return the answer's error and base offset.
    async fn send(&mut self, sequence: i32) -> (i16, i64) {
        self.send_batch(sequence, 1).await
    }

    /// Send records `r{first}` to `r{first + count - 1}` as one batch, each
    /// at its own sequence, and return the answer's error and base offset.
    async fn send_batch(&mut self, first: i32, count: i32) -> (i16, i64) {
        let records: Vec<Record> = (first..first + count)
            .map(|sequence| self.record(sequence))
            .collect();
        self.send_records(&records).await
    }

    fn record(&self, sequence: i32) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: self.id,
            producer_epoch: self.epoch,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from(format!("r{sequence}"))),
            headers: Default::default(),
        }
    }

    async fn send_records(&mut self, records: &[Record]) -> (i16, i64) {
        let mut batch = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch,
            records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode");
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(10_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from_string(self.topic.clone())))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(batch.freeze())),
                    ]),
            ]);
        let response = self.call(&request, 9).await;
        let partition = &response.responses[0].partition_responses[0];
        (partition.error_code, partition.base_offset)
    }

    async fn send_ok(&mut self, sequence: i32) -> i64 {
        let (error, offset) = self.send_until_served(sequence).await;
        assert_eq!(error, 0, "sequence {sequence}");
        offset
    }

    /// [`Self::send`], retried while the broker is still taking the shard
    /// on: a promoted leader or a move's destination opens it a moment after
    /// placement names it.
    async fn send_until_served(&mut self, sequence: i32) -> (i16, i64) {
        self.send_batch_until_served(sequence, 1).await
    }

    async fn send_batch_until_served(&mut self, first: i32, count: i32) -> (i16, i64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let answer = self.send_batch(first, count).await;
            if !RETRIABLE.contains(&answer.0) || tokio::time::Instant::now() >= deadline {
                return answer;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Records from `offset` on, as `(offset, value)`, retried while the
    /// broker is still taking the shard on.
    async fn fetch_until_served(&mut self, offset: i64) -> Vec<(i64, String)> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let request = FetchRequest::default()
                .with_max_wait_ms(500)
                .with_min_bytes(0)
                .with_max_bytes(1 << 24)
                .with_session_epoch(-1)
                .with_topics(vec![
                    FetchTopic::default()
                        .with_topic(TopicName(StrBytes::from_string(self.topic.clone())))
                        .with_partitions(vec![
                            FetchPartition::default()
                                .with_partition(0)
                                .with_fetch_offset(offset)
                                .with_partition_max_bytes(1 << 20),
                        ]),
                ]);
            let response = self.call(&request, 11).await;
            let partition = &response.responses[0].partitions[0];
            if partition.error_code == 0 {
                let Some(bytes) = partition.records.clone().filter(|b| !b.is_empty()) else {
                    return Vec::new();
                };
                return RecordBatchDecoder::decode_all(&mut bytes.clone())
                    .expect("decode")
                    .into_iter()
                    .flat_map(|set| set.records)
                    .filter(|record| record.offset >= offset)
                    .map(|record| {
                        let value = record.value.unwrap_or_default();
                        (record.offset, String::from_utf8_lossy(&value).into_owned())
                    })
                    .collect();
            }
            assert!(
                RETRIABLE.contains(&partition.error_code) && tokio::time::Instant::now() < deadline,
                "fetch from {offset} answered {}",
                partition.error_code
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn list_offset(&mut self, timestamp: i64) -> (i16, i64) {
        let request = ListOffsetsRequest::default().with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_string(self.topic.clone())))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_timestamp(timestamp),
                ]),
        ]);
        let response = self.call(&request, 1).await;
        let partition = &response.topics[0].partitions[0];
        (partition.error_code, partition.offset)
    }

    /// The log end offset, retried while the broker is still taking the
    /// shard on.
    async fn latest_until_served(&mut self) -> i64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let (error, offset) = self.list_offset(-1).await;
            if error == 0 {
                return offset;
            }
            assert!(
                RETRIABLE.contains(&error) && tokio::time::Instant::now() < deadline,
                "ListOffsets answered {error}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn earliest(&mut self) -> i64 {
        let (error, offset) = self.list_offset(-2).await;
        assert_eq!(error, 0);
        offset
    }

    async fn call<R: Request>(&mut self, request: &R, version: i16) -> R::Response {
        let correlation_id = self.next_correlation;
        self.next_correlation += 1;
        let mut body = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("felix-cluster-test")))
            .encode(&mut body, R::header_version(version))
            .expect("encode header");
        request.encode(&mut body, version).expect("encode request");
        let mut frame = BytesMut::with_capacity(body.len() + 4);
        frame.put_i32(body.len() as i32);
        frame.put_slice(&body);
        self.stream.write_all(&frame).await.expect("write");

        let mut size = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(30), self.stream.read_exact(&mut size))
            .await
            .expect("an answer in time")
            .expect("read");
        let mut frame = vec![0u8; i32::from_be_bytes(size) as usize];
        self.stream.read_exact(&mut frame).await.expect("read");
        let mut frame = Bytes::from(frame);
        assert_eq!(frame.get_i32(), correlation_id);
        if R::Response::header_version(version) >= 1 {
            frame.get_u8();
        }
        R::Response::decode(&mut frame, version).expect("decode response")
    }
}

async fn wait_until_replicated(cluster: &Cluster, _leader: &str) {
    cluster
        .wait_for_replication(STREAM, Duration::from_secs(30))
        .await
        .expect("replication should reach the followers of a healthy shard");
}

/// Step placement until the shard's owner is none of `gone`.
async fn new_owner(cluster: &Cluster, gone: &[&str]) -> String {
    cluster
        .promoted_leader(STREAM, gone[0], Duration::from_secs(60))
        .await
        .expect("a new leader")
}

async fn move_shard(cluster: &Cluster, destination: &str) {
    cluster
        .start_move(STREAM, 0, destination)
        .await
        .expect("start the move");
    wait::until(Duration::from_secs(60), "the move to cut over", || async {
        cluster.place_shards().await;
        cluster
            .shard_owner_of("stream", STREAM, 0)
            .await
            .is_ok_and(|owner| owner == destination)
    })
    .await
    .expect("move");
}

/// `(offset, payload)` of the producer's records, replayed over QUIC from
/// `owner`. The harness's own probe records are left out.
async fn read_back(cluster: &Cluster, owner: &str) -> Vec<(u64, String)> {
    let events = cluster
        .whole_replay_of_shard(
            owner,
            STREAM,
            0,
            Duration::from_secs(3),
            Duration::from_secs(30),
        )
        .await
        .expect("replay");
    let mut read = Vec::new();
    for event in events {
        let payload = String::from_utf8_lossy(&event.payload).into_owned();
        if payload.starts_with('r') && payload[1..].parse::<u32>().is_ok() {
            read.push((event.offset.expect("durable offsets"), payload));
        }
    }
    read
}

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
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    InitProducerIdRequest, ProduceRequest, RequestHeader, SaslAuthenticateRequest,
    SaslHandshakeRequest, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, Record, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType,
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

    let first = cluster.owner(STREAM).await.expect("owner");
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

/// A Kafka connection speaking as one idempotent producer.
struct Producer {
    stream: TcpStream,
    next_correlation: i32,
    topic: String,
    id: i64,
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

    /// Send record `r{sequence}` as a one-record batch at that sequence, and
    /// return the answer's error and base offset.
    async fn send(&mut self, sequence: i32) -> (i16, i64) {
        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: self.id,
            producer_epoch: 0,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from(format!("r{sequence}"))),
            headers: Default::default(),
        };
        let mut batch = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch,
            [&record],
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
        let (error, offset) = self.send(sequence).await;
        assert_eq!(error, 0, "sequence {sequence}");
        offset
    }

    /// [`Self::send`], retried while the broker is still taking the shard
    /// on: a promoted leader or a move's destination opens it a moment after
    /// placement names it.
    async fn send_until_served(&mut self, sequence: i32) -> (i16, i64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let answer = self.send(sequence).await;
            if !RETRIABLE.contains(&answer.0) || tokio::time::Instant::now() >= deadline {
                return answer;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
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

async fn wait_until_replicated(cluster: &Cluster, leader: &str) {
    wait::until(Duration::from_secs(30), "every replica level", || async {
        matches!(
            cluster
                .metric(leader, "felix_broker_replication_lag_records")
                .await
                .ok()
                .flatten(),
            Some(lag) if lag == 0.0
        )
    })
    .await
    .expect("replication should reach the followers of a healthy shard");
}

/// Step placement until the shard's owner is none of `gone`.
async fn new_owner(cluster: &Cluster, gone: &[&str]) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        // The harness's control plane does not re-plan on a timer.
        cluster.place_shards().await;
        if let Ok(owner) = cluster.owner(STREAM).await
            && !gone.contains(&owner.as_str())
        {
            return owner;
        }
        assert!(tokio::time::Instant::now() < deadline, "no new leader");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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
    let (_client, mut subscription) = cluster
        .replay_shard(owner, STREAM, 0)
        .await
        .expect("replay");
    let mut read = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        let payload = String::from_utf8_lossy(&event.payload).into_owned();
        if payload.starts_with('r') && payload[1..].parse::<u32>().is_ok() {
            read.push((event.offset.expect("durable offsets"), payload));
        }
    }
    read
}

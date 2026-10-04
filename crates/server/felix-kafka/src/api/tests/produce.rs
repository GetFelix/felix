use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::produce_response::PartitionProduceResponse;
use kafka_protocol::messages::{
    ApiVersionsRequest, InitProducerIdRequest, ProduceRequest, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use std::sync::atomic::Ordering;

use super::{Client, Fixture, REMOTE};
use crate::cluster::{Placement, WriteError};

const TOPIC: &str = "orders.created";
const PUBLISH_ORDERS: &str = "stream.publish:stream:t1/orders/*";
const READ_ORDERS: &str = "stream.subscribe:stream:t1/orders/*";

/// Who wrote a batch: nobody in particular, or an idempotent producer at a
/// record sequence.
#[derive(Clone, Copy)]
enum Producer {
    Plain,
    Idempotent { id: i64, sequence: i32 },
}

fn batch(producer: Producer, values: &[&str], compression: Compression) -> Bytes {
    let (producer_id, producer_epoch, base) = match producer {
        Producer::Plain => (NO_PRODUCER_ID, NO_PRODUCER_EPOCH, NO_SEQUENCE),
        Producer::Idempotent { id, sequence } => (id, 0, sequence),
    };
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, value)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id,
            producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: if base < 0 {
                base
            } else {
                base.wrapping_add(i as i32)
            },
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .expect("encode");
    buf.freeze()
}

fn request(acks: i16, partition: i32, records: Bytes) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(acks)
        .with_timeout_ms(5_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(records)),
                ]),
        ])
}

async fn produce(client: &mut Client, acks: i16, records: Bytes) -> PartitionProduceResponse {
    let response = client.call(&request(acks, 0, records), 9).await;
    response.responses[0].partition_responses[0].clone()
}

/// Everything in shard 0, as `(offset, value)`.
async fn stored(client: &mut Client) -> Vec<(i64, String)> {
    let response = client
        .call(&super::fetch::request(&[(TOPIC, 0, 0)], 0, 0), 12)
        .await;
    super::fetch::records(&response.responses[0].partitions[0])
}

async fn producer_id(client: &mut Client) -> i64 {
    let response = client.call(&InitProducerIdRequest::default(), 4).await;
    assert_eq!(response.error_code, 0);
    assert_eq!(response.producer_epoch, 0);
    response.producer_id.0
}

fn owned(pairs: &[(i64, &str)]) -> Vec<(i64, String)> {
    pairs.iter().map(|(o, v)| (*o, v.to_string())).collect()
}

/// **A produce lands at Felix's next offsets and a consumer reads it back.**
/// Keys are dropped, values kept; every codec a producer may pick decodes.
#[tokio::test]
async fn produced_records_land_at_the_next_offsets_for_every_codec() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["felix"]).await;
    let mut client = fixture.connect();

    let mut expected = vec![(0, "felix")];
    let mut next = 1;
    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let value = format!("{compression:?}");
        let answer = produce(
            &mut client,
            1,
            batch(Producer::Plain, &[&value, "x"], compression),
        )
        .await;
        assert_eq!(answer.error_code, 0, "{compression:?}: {answer:?}");
        assert_eq!(answer.base_offset, next);
        assert!(answer.log_append_time_ms > 0, "the append time is reported");
        expected.push((next, Box::leak(value.into_boxed_str())));
        expected.push((next + 1, "x"));
        next += 2;
    }
    assert_eq!(stored(&mut client).await, owned(&expected));
}

#[tokio::test]
async fn acks_zero_writes_and_answers_nothing() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    client
        .send(
            &request(0, 0, batch(Producer::Plain, &["quiet"], Compression::None)),
            9,
        )
        .await;
    // The next answer on the wire is for the next request.
    let response = client.call(&ApiVersionsRequest::default(), 3).await;
    assert_eq!(response.error_code, 0);
    assert_eq!(stored(&mut client).await, owned(&[(0, "quiet")]));
}

/// `acks=all` waits for the stream's consistency and `acks=1` does not; a
/// quorum that does not confirm in time is a timeout the producer retries.
#[tokio::test]
async fn acks_all_waits_for_the_streams_consistency() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let waits = || fixture.cluster.consistency_waits.load(Ordering::SeqCst);

    produce(
        &mut client,
        1,
        batch(Producer::Plain, &["a"], Compression::None),
    )
    .await;
    assert_eq!(waits(), 0, "acks=1 waited for replicas");
    produce(
        &mut client,
        -1,
        batch(Producer::Plain, &["b"], Compression::None),
    )
    .await;
    assert_eq!(waits(), 1, "acks=all did not wait");

    *fixture.cluster.consistency_error.lock().expect("lock") = Some(WriteError::QuorumTimeout);
    let answer = produce(
        &mut client,
        -1,
        batch(Producer::Plain, &["c"], Compression::None),
    )
    .await;
    assert_eq!(answer.error_code, 7, "REQUEST_TIMED_OUT");
    assert!(answer.error_message.is_some());
}

/// **An idempotent producer's re-send lands once.** Sequences count records,
/// so the second batch starts at 3; sending either batch again writes nothing
/// and answers with its original offset.
#[tokio::test]
async fn an_idempotent_re_send_is_answered_with_the_original_offset() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let id = producer_id(&mut client).await;

    let first = batch(
        Producer::Idempotent { id, sequence: 0 },
        &["a", "b", "c"],
        Compression::Lz4,
    );
    let second = batch(
        Producer::Idempotent { id, sequence: 3 },
        &["d"],
        Compression::None,
    );
    assert_eq!(produce(&mut client, -1, first.clone()).await.base_offset, 0);
    assert_eq!(
        produce(&mut client, -1, second.clone()).await.base_offset,
        3
    );
    let again = produce(&mut client, -1, first).await;
    assert_eq!((again.error_code, again.base_offset), (0, 0));
    let again = produce(&mut client, -1, second).await;
    assert_eq!((again.error_code, again.base_offset), (0, 3));
    assert_eq!(
        stored(&mut client).await,
        owned(&[(0, "a"), (1, "b"), (2, "c"), (3, "d")])
    );
}

#[tokio::test]
async fn sequence_refusals_use_the_codes_librdkafka_acts_on() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let id = producer_id(&mut client).await;
    produce(
        &mut client,
        -1,
        batch(
            Producer::Idempotent { id, sequence: 0 },
            &["a"],
            Compression::None,
        ),
    )
    .await;

    let gap = produce(
        &mut client,
        -1,
        batch(
            Producer::Idempotent { id, sequence: 5 },
            &["x"],
            Compression::None,
        ),
    )
    .await;
    assert_eq!(gap.error_code, 45, "OUT_OF_ORDER_SEQUENCE_NUMBER");
    let other = producer_id(&mut client).await;
    let unknown = produce(
        &mut client,
        -1,
        batch(
            Producer::Idempotent {
                id: other,
                sequence: 9,
            },
            &["x"],
            Compression::None,
        ),
    )
    .await;
    assert_eq!(unknown.error_code, 59, "UNKNOWN_PRODUCER_ID");
    assert_eq!(stored(&mut client).await, owned(&[(0, "a")]));
}

/// Store `values` as `producer`'s records from `first_sequence` on, the way a
/// replica is shipped them. Reaching the wrap by producing 2^31 records is out
/// of reach for a test; a promoted replica holding the tail of them is not.
async fn ship_producer(
    fixture: &super::Fixture,
    producer: i64,
    first_sequence: i32,
    values: &[&str],
) {
    use felix_wire::internal::{ProducerMark, batch_checksum};
    let handle = fixture
        .broker
        .resolve_stream_handle(super::TENANT, "orders", "created", 0)
        .await
        .expect("handle");
    let log = handle.log().expect("durable");
    let from = log.tail_offset().await.expect("tail");
    let payloads: Vec<Bytes> = values
        .iter()
        .map(|value| Bytes::copy_from_slice(value.as_bytes()))
        .collect();
    let marks: Vec<ProducerMark> = (0..values.len())
        .map(|i| ProducerMark::Opens {
            producer_id: producer as u64,
            sequence: first_sequence.wrapping_add(i as i32) as u64,
            len: 1,
        })
        .collect();
    let applied = felix_broker::replication::apply(
        log,
        from,
        batch_checksum(&payloads, &marks, &[]),
        &payloads,
        &marks,
        &[],
    )
    .await
    .expect("apply")
    .expect("in order");
    fixture
        .broker
        .adopt_replicated(
            super::TENANT,
            "orders",
            "created",
            0,
            applied.durable_offset,
        )
        .await
        .expect("adopt");
}

fn idempotent(id: i64, sequence: i32, values: &[&str]) -> Bytes {
    batch(
        Producer::Idempotent { id, sequence },
        values,
        Compression::None,
    )
}

/// **Sequences wrap from `i32::MAX` to 0, as Kafka's do.** After a record at
/// `i32::MAX` the next batch starts at 0; a re-send from either side of the
/// wrap is a duplicate, and a batch past the wrap's next sequence is a gap.
#[tokio::test]
async fn a_sequence_wraps_to_zero_after_i32_max() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let id = producer_id(&mut client).await;
    ship_producer(&fixture, id, i32::MAX - 2, &["a", "b"]).await;

    let last = idempotent(id, i32::MAX, &["c"]);
    let wrapped = idempotent(id, 0, &["d", "e"]);
    let answer = produce(&mut client, -1, last.clone()).await;
    assert_eq!((answer.error_code, answer.base_offset), (0, 2));
    let gap = produce(&mut client, -1, idempotent(id, 1, &["x"])).await;
    assert_eq!(gap.error_code, 45, "OUT_OF_ORDER_SEQUENCE_NUMBER");
    let answer = produce(&mut client, -1, wrapped.clone()).await;
    assert_eq!((answer.error_code, answer.base_offset), (0, 3));

    for (resent, offset) in [(last, 2), (wrapped, 3)] {
        let again = produce(&mut client, -1, resent).await;
        assert_eq!((again.error_code, again.base_offset), (0, offset));
    }
    let gap = produce(&mut client, -1, idempotent(id, 5, &["x"])).await;
    assert_eq!(gap.error_code, 45, "OUT_OF_ORDER_SEQUENCE_NUMBER");
    let answer = produce(&mut client, -1, idempotent(id, 2, &["f"])).await;
    assert_eq!((answer.error_code, answer.base_offset), (0, 5));
    assert_eq!(
        stored(&mut client).await,
        owned(&[(0, "a"), (1, "b"), (2, "c"), (3, "d"), (4, "e"), (5, "f")])
    );
}

/// **A batch may straddle the wrap.** Its records carry `i32::MAX - 1`,
/// `i32::MAX`, 0 and 1; the next batch starts at 2, and a re-send of the
/// straddling batch is a duplicate.
#[tokio::test]
async fn a_batch_spans_the_sequence_wrap() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let id = producer_id(&mut client).await;
    ship_producer(&fixture, id, i32::MAX - 3, &["a", "b"]).await;

    let spanning = idempotent(id, i32::MAX - 1, &["c", "d", "e", "f"]);
    let answer = produce(&mut client, -1, spanning.clone()).await;
    assert_eq!((answer.error_code, answer.base_offset), (0, 2));
    let again = produce(&mut client, -1, spanning).await;
    assert_eq!((again.error_code, again.base_offset), (0, 2));
    let gap = produce(&mut client, -1, idempotent(id, 3, &["x"])).await;
    assert_eq!(gap.error_code, 45, "OUT_OF_ORDER_SEQUENCE_NUMBER");
    let answer = produce(&mut client, -1, idempotent(id, 2, &["g"])).await;
    assert_eq!((answer.error_code, answer.base_offset), (0, 6));
    assert_eq!(
        stored(&mut client).await,
        owned(&[
            (0, "a"),
            (1, "b"),
            (2, "c"),
            (3, "d"),
            (4, "e"),
            (5, "f"),
            (6, "g")
        ])
    );
}

/// Writing needs `stream.publish`, checked before the stream is looked up.
#[tokio::test]
async fn producing_needs_publish_permission() {
    let fixture = Fixture::secured(&[READ_ORDERS]).await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let unauthenticated = produce(
        &mut client,
        1,
        batch(Producer::Plain, &["a"], Compression::None),
    )
    .await;
    assert_eq!(unauthenticated.error_code, 29, "TOPIC_AUTHORIZATION_FAILED");
    assert_eq!(client.login(super::TENANT, super::TOKEN).await, 0);
    let read_only = produce(
        &mut client,
        1,
        batch(Producer::Plain, &["a"], Compression::None),
    )
    .await;
    assert_eq!(read_only.error_code, 29, "TOPIC_AUTHORIZATION_FAILED");

    let fixture = Fixture::secured(&[READ_ORDERS, PUBLISH_ORDERS]).await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    assert_eq!(client.login(super::TENANT, super::TOKEN).await, 0);
    let allowed = produce(
        &mut client,
        1,
        batch(Producer::Plain, &["a"], Compression::None),
    )
    .await;
    assert_eq!(allowed.error_code, 0);
}

#[tokio::test]
async fn a_partition_this_broker_does_not_lead_or_cannot_find_is_refused() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 2, true).await;
    fixture.stream("orders", "memory", 1, false).await;
    fixture.cluster.place(
        "created",
        1,
        Placement::Remote {
            leader: REMOTE.to_string(),
            replicas: Vec::new(),
        },
    );
    let mut client = fixture.connect();
    let records = batch(Producer::Plain, &["a"], Compression::None);
    let answer = |response: kafka_protocol::messages::ProduceResponse| {
        response.responses[0].partition_responses[0].error_code
    };
    assert_eq!(
        answer(client.call(&request(1, 1, records.clone()), 9).await),
        6,
        "NOT_LEADER_OR_FOLLOWER"
    );
    assert_eq!(
        answer(client.call(&request(1, 2, records.clone()), 9).await),
        3,
        "UNKNOWN_TOPIC_OR_PARTITION"
    );
    let mut in_memory = request(1, 0, records);
    in_memory.topic_data[0].name = TopicName(StrBytes::from_static_str("orders.memory"));
    assert_eq!(
        answer(client.call(&in_memory, 9).await),
        3,
        "an in-memory stream is not a topic"
    );
}

#[tokio::test]
async fn legacy_formats_and_transactions_are_refused_with_a_reason() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();

    let mut legacy = BytesMut::from(&batch(Producer::Plain, &["a"], Compression::None)[..]);
    legacy[16] = 1;
    let answer = produce(&mut client, 1, legacy.freeze()).await;
    assert_eq!(answer.error_code, 43, "UNSUPPORTED_FOR_MESSAGE_FORMAT");
    assert!(answer.error_message.is_some());

    let transactional = request(1, 0, batch(Producer::Plain, &["a"], Compression::None))
        .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("tx"))));
    let response = client.call(&transactional, 9).await;
    let answer = &response.responses[0].partition_responses[0];
    assert_eq!(
        answer.error_code, 53,
        "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"
    );
    assert_eq!(
        answer.error_message.as_ref().map(|m| m.as_str()),
        Some(crate::api::transactions::MESSAGE)
    );
    assert!(
        stored(&mut client).await.is_empty(),
        "a refused produce wrote"
    );
}

/// **A tenant over its publish quota is throttled the way Kafka does it**:
/// the produce is written and answered with `throttle_time_ms`, and the
/// connection reads nothing more until that time has passed.
#[tokio::test]
async fn a_produce_over_quota_is_answered_with_a_throttle_and_the_connection_muted() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    *fixture.cluster.produce_hold.lock().expect("lock") = std::time::Duration::from_millis(300);
    let mut client = fixture.connect();

    let response = client
        .call(
            &request(
                1,
                0,
                batch(Producer::Plain, &["ab", "cde"], Compression::None),
            ),
            9,
        )
        .await;
    assert_eq!(response.throttle_time_ms, 300);
    assert_eq!(response.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(
        fixture
            .cluster
            .produce_charges
            .lock()
            .expect("lock")
            .as_slice(),
        &[("t1".to_string(), 2, 5)],
        "charged the tenant's records and payload bytes",
    );

    *fixture.cluster.produce_hold.lock().expect("lock") = std::time::Duration::ZERO;
    let started = std::time::Instant::now();
    let versions = client.call(&ApiVersionsRequest::default(), 3).await;
    assert_eq!(versions.error_code, 0);
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(250),
        "the next request waited out the throttle, after {:?}",
        started.elapsed()
    );
    assert_eq!(stored(&mut client).await, owned(&[(0, "ab"), (1, "cde")]));
}

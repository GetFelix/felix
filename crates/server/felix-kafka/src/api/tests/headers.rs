//! Keys and headers across the Kafka listener: kept when the listener opts in
//! and the broker accepts record headers, dropped otherwise.

use bytes::{Bytes, BytesMut};
use felix_wire::RecordHeaders;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

use super::produce::{produce, request};
use super::{Client, Fixture, TENANT};
use crate::service::Settings;

const TOPIC: &str = "orders.created";

/// A producer's record: a value, an optional key, and headers in order. A
/// `None` header value is Kafka's null.
fn record(
    sequence: i32,
    value: &str,
    key: Option<&str>,
    headers: &[(&str, Option<&str>)],
) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: i64::from(sequence),
        sequence: NO_SEQUENCE,
        timestamp: 1_700_000_000_000,
        key: key.map(|key| Bytes::copy_from_slice(key.as_bytes())),
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: headers
            .iter()
            .map(|(name, value)| {
                (
                    StrBytes::from_string(name.to_string()),
                    value.map(|value| Bytes::copy_from_slice(value.as_bytes())),
                )
            })
            .collect(),
    }
}

fn batch(records: &[Record]) -> Bytes {
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode");
    buf.freeze()
}

/// The same, written by an idempotent producer from `sequence`.
fn idempotent(producer: i64, sequence: i32, records: Vec<Record>) -> Bytes {
    let records: Vec<Record> = records
        .into_iter()
        .enumerate()
        .map(|(i, record)| Record {
            producer_id: producer,
            producer_epoch: 0,
            sequence: sequence + i as i32,
            ..record
        })
        .collect();
    batch(&records)
}

/// What a consumer reads: `(value, key, headers)` per record. A header whose
/// value arrives null is shown as `None`.
type Seen = (String, Option<String>, Vec<(String, Option<String>)>);

async fn fetched(client: &mut Client) -> Vec<Seen> {
    let response = client
        .call(&super::fetch::request(&[(TOPIC, 0, 0)], 0, 0), 12)
        .await;
    let Some(bytes) = response.responses[0].partitions[0]
        .records
        .clone()
        .filter(|bytes| !bytes.is_empty())
    else {
        return Vec::new();
    };
    let text = |bytes: &Bytes| String::from_utf8(bytes.to_vec()).expect("utf8");
    RecordBatchDecoder::decode_all(&mut bytes.clone())
        .expect("decode")
        .into_iter()
        .flat_map(|set| set.records)
        .map(|record| {
            (
                text(&record.value.expect("value")),
                record.key.as_ref().map(text),
                record
                    .headers
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.as_ref().map(text)))
                    .collect(),
            )
        })
        .collect()
}

fn seen(value: &str, key: Option<&str>, headers: &[(&str, Option<&str>)]) -> Seen {
    (
        value.to_string(),
        key.map(str::to_string),
        headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.map(str::to_string)))
            .collect(),
    )
}

/// `opt_in` is the listener's `FELIX_KAFKA_RECORD_HEADERS`, `accept` whether
/// the broker's own gate lets headers through.
async fn stream_fixture(opt_in: bool, accept: bool) -> Fixture {
    let fixture = Fixture::with(Settings {
        anonymous_tenant: Some(TENANT.to_string()),
        cluster_id: "test-cluster".to_string(),
        record_headers: opt_in,
        ..Settings::default()
    })
    .await;
    if accept {
        fixture.accept_headers();
    }
    fixture.stream("orders", "created", 1, true).await;
    fixture
}

/// **A key and headers go in and come back**, in order, for a plain producer
/// and an idempotent one. A null header value comes back empty, and a record
/// with neither comes back with neither.
#[tokio::test]
async fn produced_keys_and_headers_are_fetched_back() {
    let fixture = stream_fixture(true, true).await;
    let mut client = fixture.connect();

    let answer = produce(
        &mut client,
        1,
        batch(&[
            record(
                0,
                "a",
                Some("k1"),
                &[("trace", Some("t-1")), ("nil", None), ("empty", Some(""))],
            ),
            record(1, "b", None, &[]),
            record(2, "c", None, &[("only", Some("h"))]),
        ]),
    )
    .await;
    assert_eq!(answer.error_code, 0, "{answer:?}");
    let answer = produce(
        &mut client,
        1,
        idempotent(
            7,
            0,
            vec![
                record(0, "d", Some("k4"), &[("x", Some("y"))]),
                record(1, "e", None, &[]),
            ],
        ),
    )
    .await;
    assert_eq!(answer.error_code, 0, "{answer:?}");

    assert_eq!(
        fetched(&mut client).await,
        vec![
            seen(
                "a",
                Some("k1"),
                &[
                    ("trace", Some("t-1")),
                    ("nil", Some("")),
                    ("empty", Some(""))
                ]
            ),
            seen("b", None, &[]),
            seen("c", None, &[("only", Some("h"))]),
            seen("d", Some("k4"), &[("x", Some("y"))]),
            seen("e", None, &[]),
        ]
    );
}

/// **A re-sent idempotent batch does not store its headers twice.**
#[tokio::test]
async fn a_re_sent_batch_keeps_one_copy_of_its_headers() {
    let fixture = stream_fixture(true, true).await;
    let mut client = fixture.connect();
    let records = || vec![record(0, "a", Some("k"), &[("h", Some("1"))])];

    for _ in 0..2 {
        let answer = produce(&mut client, 1, idempotent(7, 0, records())).await;
        assert_eq!(answer.error_code, 0, "{answer:?}");
        assert_eq!(answer.base_offset, 0);
    }
    assert_eq!(
        fetched(&mut client).await,
        vec![seen("a", Some("k"), &[("h", Some("1"))])]
    );
}

/// **A record a Felix client published with headers reads the same to a
/// Kafka consumer**, and one published without reads bare.
#[tokio::test]
async fn a_record_published_with_headers_is_fetched_with_them() {
    let fixture = stream_fixture(true, true).await;
    fixture
        .publish_with_blocks(
            "orders",
            "created",
            0,
            &[
                (
                    "a",
                    Some(RecordHeaders {
                        key: Some(Bytes::from_static(b"k")),
                        headers: vec![
                            ("trace".to_string(), Bytes::from_static(b"t")),
                            ("trace".to_string(), Bytes::from_static(b"u")),
                        ],
                    }),
                ),
                ("b", None),
                (
                    "c",
                    Some(RecordHeaders {
                        key: None,
                        headers: vec![("mqtt".to_string(), Bytes::new())],
                    }),
                ),
            ],
        )
        .await;
    let mut client = fixture.connect();

    // Kafka's decoder keeps one value per name; the last one wins.
    assert_eq!(
        fetched(&mut client).await,
        vec![
            seen("a", Some("k"), &[("trace", Some("u"))]),
            seen("b", None, &[]),
            seen("c", None, &[("mqtt", Some(""))]),
        ]
    );
}

/// **Felix's reserved header names are refused the way Kafka refuses an
/// invalid record**, and nothing from the request is written.
#[tokio::test]
async fn a_felix_header_name_is_an_invalid_record() {
    let fixture = stream_fixture(true, true).await;
    let mut client = fixture.connect();

    let response = client
        .call(
            &request(
                1,
                0,
                batch(&[
                    record(0, "fine", None, &[]),
                    record(1, "reserved", None, &[("felix.mqtt.topic", Some("t"))]),
                ]),
            ),
            9,
        )
        .await;
    let answer = &response.responses[0].partition_responses[0];
    assert_eq!(answer.error_code, 87, "INVALID_RECORD: {answer:?}");
    let message = answer.error_message.as_ref().expect("a reason");
    assert!(message.as_str().contains("felix."), "{message}");
    assert!(
        fetched(&mut client).await.is_empty(),
        "a refused produce wrote"
    );
}

/// What a produce of two keyed or headered records drops, as counted.
const DROPPED_KEYS: &str = r#"felix_kafka_produce_dropped_total{field="key"} 1"#;
const DROPPED_HEADERS: &str = r#"felix_kafka_produce_dropped_total{field="headers"} 2"#;

/// Produce the same two records and say what the dropped counter shows.
/// The service runs on this thread, so a thread-local recorder sees it.
async fn produce_two(fixture: &Fixture) -> String {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let rendered = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let mut client = fixture.connect();
    let answer = produce(
        &mut client,
        1,
        batch(&[
            record(0, "a", Some("k"), &[("trace", Some("t"))]),
            record(1, "b", None, &[("felix.mqtt.topic", Some("t"))]),
        ]),
    )
    .await;
    assert_eq!(answer.error_code, 0, "{answer:?}");
    rendered.render()
}

/// **A broker that does not accept headers keeps taking Kafka produces**: the
/// key and headers are dropped and counted, the value is kept, and a reserved
/// name is not looked at.
#[tokio::test]
async fn a_broker_that_does_not_accept_headers_drops_them() {
    let fixture = stream_fixture(true, false).await;
    let rendered = produce_two(&fixture).await;
    assert!(rendered.contains(DROPPED_KEYS), "{rendered}");
    assert!(rendered.contains(DROPPED_HEADERS), "{rendered}");
    let mut client = fixture.connect();
    assert_eq!(
        fetched(&mut client).await,
        vec![seen("a", None, &[]), seen("b", None, &[])]
    );
}

/// **A listener that has not opted in drops keys and headers even when the
/// broker accepts them**, so enabling Kafka never moves the logs to the
/// headers storage format by itself. No block is built, and the drops are
/// counted as before.
#[tokio::test]
async fn a_listener_that_has_not_opted_in_drops_keys_and_headers() {
    let fixture = stream_fixture(false, true).await;
    let rendered = produce_two(&fixture).await;
    assert!(rendered.contains(DROPPED_KEYS), "{rendered}");
    assert!(rendered.contains(DROPPED_HEADERS), "{rendered}");
    let mut client = fixture.connect();
    assert_eq!(
        fetched(&mut client).await,
        vec![seen("a", None, &[]), seen("b", None, &[])]
    );
    let handle = fixture
        .broker
        .resolve_stream_handle("t1", "orders", "created", 0)
        .await
        .expect("handle");
    let stored = handle
        .log()
        .expect("durable")
        .read_from(0, usize::MAX)
        .await
        .expect("read");
    assert_eq!(stored.len(), 2);
    assert!(
        stored.iter().all(|record| record.headers.is_none()),
        "a block was stored: {stored:?}"
    );
}

/// **An opted-in listener on an accepting broker stores the block** and
/// counts nothing as dropped.
#[tokio::test]
async fn an_opted_in_listener_stores_keys_and_headers() {
    let fixture = stream_fixture(true, true).await;
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let rendered = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let mut client = fixture.connect();
    let answer = produce(
        &mut client,
        1,
        batch(&[record(0, "a", Some("k"), &[("trace", Some("t"))])]),
    )
    .await;
    assert_eq!(answer.error_code, 0, "{answer:?}");
    assert!(
        !rendered
            .render()
            .contains("felix_kafka_produce_dropped_total"),
        "{}",
        rendered.render()
    );
    let handle = fixture
        .broker
        .resolve_stream_handle("t1", "orders", "created", 0)
        .await
        .expect("handle");
    let stored = handle
        .log()
        .expect("durable")
        .read_from(0, usize::MAX)
        .await
        .expect("read");
    assert_eq!(stored.len(), 1);
    assert!(stored[0].headers.is_some(), "no block stored: {stored:?}");
}

/// **A kept block is charged with its payload**: the quota counts the bytes
/// the broker stores. The block holds a flag byte, a count, a length and the
/// key, then each name and value with their lengths.
#[tokio::test]
async fn header_bytes_count_toward_the_quota() {
    let fixture = stream_fixture(true, true).await;
    let mut client = fixture.connect();

    let answer = produce(
        &mut client,
        1,
        batch(&[record(0, "ab", Some("k"), &[("h", Some("vv"))])]),
    )
    .await;
    assert_eq!(answer.error_code, 0, "{answer:?}");
    let block = RecordHeaders {
        key: Some(Bytes::from_static(b"k")),
        headers: vec![("h".to_string(), Bytes::from_static(b"vv"))],
    }
    .encode()
    .expect("block");
    assert_eq!(
        fixture
            .cluster
            .produce_charges
            .lock()
            .expect("lock")
            .as_slice(),
        &[("t1".to_string(), 1, 2 + block.len() as u64)],
    );
}

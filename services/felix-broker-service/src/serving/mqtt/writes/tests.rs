//! The write path on a single broker: what lands in the log, and what is
//! refused before anything does.

use felix_broker::{DurableStorage, StreamMetadata};
use felix_mqtt::{MountedTopic, Record};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::RecordHeaders;

use super::*;

mod cluster;

async fn broker(durable: bool, dir: &std::path::Path) -> Arc<Broker> {
    broker_with(durable, felix_broker::ConsistencyLevel::Leader, dir).await
}

async fn broker_with(
    durable: bool,
    consistency: felix_broker::ConsistencyLevel,
    dir: &std::path::Path,
) -> Arc<Broker> {
    let storage = DurableStorage::open(
        dir,
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    broker.register_tenant("acme").await.expect("tenant");
    broker
        .register_namespace("acme", "ns")
        .await
        .expect("namespace");
    broker
        .register_stream(
            "acme",
            "ns",
            "plant",
            StreamMetadata {
                durable,
                consistency,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("stream");
    Arc::new(broker)
}

fn writes(broker: &Arc<Broker>) -> BrokerWrites {
    BrokerWrites::new(Arc::clone(broker), None, None, Duration::from_secs(1))
}

fn batch(sequence: u64, producer_id: u64, topics: &[&str]) -> WriteBatch {
    WriteBatch {
        tenant_id: "acme".into(),
        credential: Some(felix_mqtt::Credential::new("token")),
        target: MountedTopic {
            namespace: "ns".into(),
            stream: "plant".into(),
            shard: 0,
        },
        producer_id,
        sequence,
        records: topics
            .iter()
            .map(|topic| Record {
                payload: Bytes::from(format!("payload of {topic}")),
                headers: RecordHeaders {
                    key: Some(Bytes::copy_from_slice(topic.as_bytes())),
                    headers: vec![("felix.mqtt.qos".to_string(), Bytes::from_static(b"1"))],
                }
                .encode()
                .expect("headers"),
            })
            .collect(),
        acknowledged: true,
    }
}

async fn stored(broker: &Broker) -> Vec<(Bytes, Option<RecordHeaders>)> {
    broker
        .read_durable("acme", "ns", "plant", 0, 0, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| {
            (
                record.payload,
                record
                    .headers
                    .map(|block| RecordHeaders::decode((*block).clone()).expect("block")),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_batch_is_in_the_log_with_its_key_and_headers_when_write_returns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(true, dir.path()).await;
    broker.record_headers_when(|| true);
    let writes = writes(&broker);
    let producer = writes.new_producer_id();

    writes
        .write(batch(0, producer, &["plant/a", "plant/b"]))
        .await
        .expect("first batch");
    writes
        .write(batch(1, producer, &["plant/c"]))
        .await
        .expect("second batch");

    let records = stored(&broker).await;
    assert_eq!(records.len(), 3);
    let (payload, headers) = &records[2];
    assert_eq!(payload, &Bytes::from_static(b"payload of plant/c"));
    let headers = headers.as_ref().expect("headers kept");
    assert_eq!(headers.key, Some(Bytes::from_static(b"plant/c")));
    assert_eq!(headers.headers[0].0, "felix.mqtt.qos");
}

#[tokio::test]
async fn a_batch_sent_twice_is_written_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(true, dir.path()).await;
    broker.record_headers_when(|| true);
    let writes = writes(&broker);
    let producer = writes.new_producer_id();

    for _ in 0..2 {
        writes
            .write(batch(0, producer, &["plant/a"]))
            .await
            .expect("batch");
    }
    assert_eq!(stored(&broker).await.len(), 1);
}

#[tokio::test]
async fn a_broker_that_does_not_accept_headers_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(true, dir.path()).await;
    let writes = writes(&broker);

    let refused = writes
        .write(batch(0, writes.new_producer_id(), &["plant/a"]))
        .await;
    assert_eq!(refused, Err(WriteError::Refused(Refusal::HeadersDisabled)));
    assert!(stored(&broker).await.is_empty());
}

#[tokio::test]
async fn a_stream_without_a_log_or_a_name_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(false, dir.path()).await;
    broker.record_headers_when(|| true);
    let writes = writes(&broker);
    let producer = writes.new_producer_id();

    let refused = writes.write(batch(0, producer, &["plant/a"])).await;
    assert_eq!(
        refused,
        Err(WriteError::Refused(Refusal::StreamUnavailable))
    );

    let mut unknown = batch(0, producer, &["plant/a"]);
    unknown.target.stream = "gone".into();
    assert_eq!(
        writes.write(unknown).await,
        Err(WriteError::Refused(Refusal::StreamUnavailable))
    );
}

#[tokio::test]
async fn a_sequence_gap_is_not_told_to_the_client() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = broker(true, dir.path()).await;
    broker.record_headers_when(|| true);
    let writes = writes(&broker);

    // The log never saw sequence 0 of this producer, so it cannot say what
    // happened: the connection is closed and the client sends it again.
    let result = writes
        .write(batch(5, writes.new_producer_id(), &["plant/a"]))
        .await;
    assert_eq!(result, Err(WriteError::OutcomeUnknown("write_failed")));
    assert!(stored(&broker).await.is_empty());
}

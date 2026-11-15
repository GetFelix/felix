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
    BrokerWrites::new(
        Arc::clone(broker),
        Arc::new(crate::shards::routing::StreamEngine::default()),
        Duration::from_secs(1),
    )
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

/// **An MQTT message for a shard on Raft is proposed to its group** as the
/// connection's idempotent batch, with its key and headers, and a re-send
/// of it lands once.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_message_for_a_raft_shard_lands_once_with_its_headers() {
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker(root.path()).await;
    broker.record_headers_when(|| true);
    broker.read_record_headers_when(|| true);
    broker.allow_raft_record_headers_when(|| true);
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Arc::new(fixture::engine(&shards)),
        Duration::from_secs(1),
    );
    let producer = writes.new_producer_id();
    let message = |sequence, topic| {
        let mut message = batch(sequence, producer, &[topic]);
        message.tenant_id = "t1".into();
        message.target.stream = "orders".into();
        message
    };

    assert_eq!(writes.max_batch_records(), 1);
    writes.write(message(0, "plant/a")).await.expect("written");
    writes
        .write(message(0, "plant/a"))
        .await
        .expect("answered from the log");
    writes.write(message(1, "plant/b")).await.expect("written");

    let stored: Vec<_> = broker
        .read_durable("t1", "ns", "orders", 0, 0, 1 << 20)
        .await
        .expect("read")
        .into_iter()
        .map(|record| {
            let block = record.headers.expect("headers stored");
            RecordHeaders::decode((*block).clone()).expect("block").key
        })
        .collect();
    assert_eq!(
        stored,
        vec![
            Some(Bytes::from_static(b"plant/a")),
            Some(Bytes::from_static(b"plant/b"))
        ]
    );
    EngineShards::shutdown(&*shards).await;
}

/// Forwards nothing: a test whose writes all stay on this broker.
#[cfg(feature = "raft")]
struct NoPeers;

#[cfg(feature = "raft")]
impl felix_replication::peer::PeerRequester for NoPeers {
    async fn request(
        &self,
        node_id: &str,
        _addr: std::net::SocketAddr,
        _message: felix_wire::internal::InternalMessage,
    ) -> Result<felix_wire::internal::InternalMessage, felix_replication::peer::pool::PeerError>
    {
        panic!("forwarded to {node_id}");
    }
}

/// **An MQTT message for a Raft shard between leaders waits for the next
/// one** within the forward budget, as the hybrid's dispatch waits for a
/// shard that is not ready, rather than being refused `not_owner`. Written
/// as the group opens, before it has elected anyone.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_message_for_a_raft_shard_between_leaders_waits_for_the_next() {
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker(root.path()).await;
    broker.record_headers_when(|| true);
    broker.allow_raft_record_headers_when(|| true);
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Arc::new(fixture::engine(&shards)),
        Duration::from_secs(1),
    )
    .with_forwarding(Arc::new(NoPeers), Duration::from_secs(10));
    let mut message = batch(0, writes.new_producer_id(), &["plant/a"]);
    message.tenant_id = "t1".into();
    message.target.stream = "orders".into();

    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    let led_at_once = shards.leading(&orders).is_some();
    let written = writes.write(message).await;

    assert!(
        !led_at_once,
        "the group led before the write: nothing to wait out"
    );
    assert_eq!(written, Ok(()));
    EngineShards::shutdown(&*shards).await;
}

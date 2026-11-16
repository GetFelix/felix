//! What a subscription reads from the log: committed records by offset, with
//! their headers, from the tail a clean session starts at.

use bytes::Bytes;
use felix_broker::{DurableStorage, StreamMetadata};
use felix_mqtt::{MountedTopic, Record, WriteBatch, Writes};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::RecordHeaders;

use super::*;
use crate::serving::mqtt::BrokerWrites;

fn batch(sequence: u64, producer_id: u64, topic: &str, stream: &str) -> WriteBatch {
    WriteBatch {
        tenant_id: "acme".into(),
        credential: None,
        target: MountedTopic {
            namespace: "ns".into(),
            stream: stream.into(),
            shard: 0,
        },
        producer_id,
        sequence,
        records: vec![Record {
            payload: Bytes::from(format!("payload of {topic}")),
            headers: RecordHeaders {
                key: Some(Bytes::copy_from_slice(topic.as_bytes())),
                headers: vec![("felix.mqtt.qos".to_string(), Bytes::from_static(b"1"))],
            }
            .encode()
            .expect("headers"),
        }],
        acknowledged: true,
    }
}

async fn topics_read(reads: &BrokerReads, stream: &str, from: u64) -> (Vec<String>, u64) {
    let page = reads
        .read(
            "acme",
            "ns",
            stream,
            0,
            from,
            1 << 20,
            Duration::from_millis(50),
        )
        .await
        .expect("read");
    let topics = page
        .messages
        .iter()
        .map(|message| {
            let block =
                RecordHeaders::decode(message.headers.clone().expect("headers")).expect("block");
            String::from_utf8(block.key.expect("key").to_vec()).expect("utf8")
        })
        .collect();
    (topics, page.next)
}

#[tokio::test]
async fn reads_start_at_the_tail_and_return_what_is_written_after() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    broker.record_headers_when(|| true);
    broker.read_record_headers_when(|| true);
    broker.register_tenant("acme").await.expect("tenant");
    broker.register_namespace("acme", "ns").await.expect("ns");
    broker
        .register_stream(
            "acme",
            "ns",
            "plant",
            StreamMetadata {
                durable: true,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("stream");
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Arc::new(crate::shards::routing::StreamEngine::default()),
        Duration::from_secs(1),
    );
    let reads = BrokerReads::new(Arc::clone(&broker), None);
    let producer = writes.new_producer_id();
    writes
        .write(batch(0, producer, "plant/old", "plant"))
        .await
        .expect("write");

    let tail = reads.tail("acme", "ns", "plant", 0).await.expect("tail");
    assert_eq!(tail, 1);
    assert_eq!(reads.shard_count("acme", "ns", "plant"), 1);
    // Nothing past the tail yet: the read waits, then comes back empty.
    assert_eq!(topics_read(&reads, "plant", tail).await, (vec![], tail));

    writes
        .write(batch(1, producer, "plant/a", "plant"))
        .await
        .expect("write");
    writes
        .write(batch(2, producer, "plant/b", "plant"))
        .await
        .expect("write");
    let (topics, next) = topics_read(&reads, "plant", tail).await;
    assert_eq!(topics, ["plant/a", "plant/b"]);
    assert_eq!(next, 3);
}

/// **On Raft a subscription reads what the group has committed**, with the
/// headers it was proposed with, from the tail.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_raft_shard_is_read_up_to_its_commit_index() {
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
    let reads = BrokerReads::new(Arc::clone(&broker), None);
    let producer = writes.new_producer_id();
    let message = |sequence, topic| {
        let mut message = batch(sequence, producer, topic, "orders");
        message.tenant_id = "t1".into();
        message
    };
    writes
        .write(message(0, "plant/old"))
        .await
        .expect("written");

    let tail = reads.tail("t1", "ns", "orders", 0).await.expect("tail");
    // The group's own entries take offsets too, so the tail is past the
    // record, not at its offset.
    assert!(tail >= 1);
    writes.write(message(1, "plant/a")).await.expect("written");
    let page = reads
        .read(
            "t1",
            "ns",
            "orders",
            0,
            tail,
            1 << 20,
            Duration::from_secs(1),
        )
        .await
        .expect("read");
    assert_eq!(page.messages.len(), 1);
    assert!(page.messages[0].offset >= tail);
    let block =
        RecordHeaders::decode(page.messages[0].headers.clone().expect("headers")).expect("block");
    assert_eq!(block.key, Some(Bytes::from_static(b"plant/a")));
    EngineShards::shutdown(&*shards).await;
}

/// A read bound a test moves by hand, standing in for the commit index.
#[cfg(feature = "raft")]
#[derive(Debug, Default)]
struct HeldBound(std::sync::atomic::AtomicU64);

#[cfg(feature = "raft")]
impl felix_broker::ReadBounds for HeldBound {
    fn stream_bound(&self, _: &str, _: &str, _: &str, _: u32) -> felix_broker::ReadBound {
        felix_broker::ReadBound::Committed(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
}

/// **A record the group has not committed is not read**, and is once it is.
/// Removing the read bound from `read_range` fails the first assertion.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_raft_shard_withholds_a_record_until_it_is_committed() {
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker(root.path()).await;
    broker.record_headers_when(|| true);
    broker.read_record_headers_when(|| true);
    broker.allow_raft_record_headers_when(|| true);
    let shards = fixture::held_shards(&broker);
    fixture::open_root(&broker, &shards);
    let bound = Arc::new(HeldBound::default());
    broker.set_read_bounds(Arc::clone(&bound) as Arc<dyn felix_broker::ReadBounds>);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    shards.take(&mut fixture::assignments([assigned])).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Arc::new(fixture::engine(&shards)),
        Duration::from_secs(1),
    );
    let reads = BrokerReads::new(Arc::clone(&broker), None);
    let producer = writes.new_producer_id();
    let mut message = batch(0, producer, "plant/a", "orders");
    message.tenant_id = "t1".into();
    writes.write(message).await.expect("written");

    let read = || async {
        reads
            .read(
                "t1",
                "ns",
                "orders",
                0,
                0,
                1 << 20,
                Duration::from_millis(200),
            )
            .await
            .expect("read")
    };
    assert!(
        read().await.messages.is_empty(),
        "an uncommitted record was read"
    );
    bound.0.store(u64::MAX, std::sync::atomic::Ordering::SeqCst);
    // The bound moving wakes the hold; give the release a moment.
    let mut delivered = 0;
    for _ in 0..50 {
        delivered = read().await.messages.len();
        if delivered > 0 {
            break;
        }
    }
    assert_eq!(delivered, 1);
    EngineShards::shutdown(&*shards).await;
}

/// A read waiting on a shard returns as soon as the shard's readers are
/// ended, and says why: moved away, or stopped with nobody named.
#[tokio::test]
async fn a_shard_that_stops_being_served_here_ends_its_reads() {
    use crate::test_support::leader::{self, Leader};

    let mut fixture = Leader::start().await;
    let _lease = fixture.hold_lease();
    let reads = Arc::new(BrokerReads::new(
        Arc::clone(&fixture.broker),
        Some(Arc::clone(&fixture.ingress)),
    ));
    let tail = reads
        .tail(leader::TENANT, leader::NAMESPACE, leader::DURABLE, 0)
        .await
        .expect("served while the lease holds");
    let waiting = |reads: Arc<BrokerReads>| {
        tokio::spawn(async move {
            reads
                .read(
                    leader::TENANT,
                    leader::NAMESPACE,
                    leader::DURABLE,
                    0,
                    tail,
                    1 << 20,
                    Duration::from_secs(3600),
                )
                .await
        })
    };
    let end = |fixture: &Leader| {
        let broker = Arc::clone(&fixture.broker);
        async move {
            broker
                .end_subscriptions(leader::TENANT, leader::NAMESPACE, leader::DURABLE, 0, None)
                .await
        }
    };

    // A move: the shard is someone else's.
    let read = waiting(Arc::clone(&reads));
    tokio::time::sleep(Duration::from_millis(50)).await;
    fixture.fence_move(&leader::stream_key(leader::DURABLE));
    end(&fixture).await;
    let answer = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("the ending wakes the read")
        .expect("join");
    assert_eq!(answer, Err(ReadError::NotHere));

    // Once routes name the new owner, a SUBSCRIBE finds it refused.
    fixture.cut_over(&leader::stream_key(leader::DURABLE));
    assert_eq!(
        reads
            .tail(leader::TENANT, leader::NAMESPACE, leader::DURABLE, 0)
            .await,
        Err(ReadError::NotHere)
    );

    let fixture = Leader::start().await;
    let lease = fixture.hold_lease();
    let reads = Arc::new(BrokerReads::new(
        Arc::clone(&fixture.broker),
        Some(Arc::clone(&fixture.ingress)),
    ));
    // A lapsed lease: nobody to name, so the reader is stopped. Last, as
    // the fixture's lease is bound once.
    let read = waiting(Arc::clone(&reads));
    tokio::time::sleep(Duration::from_millis(50)).await;
    lease.surrender();
    end(&fixture).await;
    let answer = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("the ending wakes the read")
        .expect("join");
    assert_eq!(answer, Err(ReadError::Stopped));
}

//! A stream whose shards are separate logs.
//!
//! The registry used to open shard 0's log whatever shard it was asked for
//! (#286), while the replication driver and shard lifecycle opened the real
//! index.
//! They agreed only for shard 0, which is the only shard anything could reach —
//! so a routing key (#240) would have written a record to one log and shipped
//! another.
//!
//! Run with `cargo test -p felix-broker --test sharded_streams`.
use std::sync::Arc;

use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";
const SHARDS: u32 = 4;

async fn sharded_broker() -> (Arc<Broker>, DurableStorage, TempDir) {
    let dir = tempdir().expect("dir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");

    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage.clone());
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                shards: SHARDS,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("stream");
    (Arc::new(broker), storage, dir)
}

/// Everything on `shard`, read straight from its log.
///
/// The log is the question here — whether two shards share one — so this reads
/// it directly rather than through a subscription, whose replay ring would
/// answer a different question.
async fn replay(broker: &Broker, shard: u32) -> Vec<String> {
    broker
        .read_durable(TENANT, NAMESPACE, STREAM, shard, 0, 1024 * 1024)
        .await
        .expect("read the shard log")
        .into_iter()
        .map(|record| String::from_utf8_lossy(&record.payload).to_string())
        .collect()
}

/// **Each shard is its own log.** A record published to shard 2 must not be
/// visible on shard 0, or the shard dimension is decoration.
#[tokio::test]
async fn shards_of_one_stream_do_not_share_a_log() {
    let (broker, _storage, _dir) = sharded_broker().await;

    broker
        .publish_batch(TENANT, NAMESPACE, STREAM, 0, &[Bytes::from_static(b"zero")])
        .await
        .expect("publish to shard 0");
    broker
        .publish_batch(TENANT, NAMESPACE, STREAM, 2, &[Bytes::from_static(b"two")])
        .await
        .expect("publish to shard 2");

    assert_eq!(replay(&broker, 0).await, vec!["zero".to_string()]);
    assert_eq!(replay(&broker, 2).await, vec!["two".to_string()]);
    assert!(
        replay(&broker, 1).await.is_empty(),
        "a shard nothing was published to must be empty",
    );
}

/// **Offsets are per shard.** Both shards start at zero, because each is a log.
/// A shared log would have given the second record offset 1.
#[tokio::test]
async fn each_shard_numbers_its_own_records() {
    let (broker, _storage, _dir) = sharded_broker().await;

    for shard in [0u32, 3] {
        broker
            .publish_batch(
                TENANT,
                NAMESPACE,
                STREAM,
                shard,
                &[Bytes::from_static(b"first")],
            )
            .await
            .expect("publish");
    }

    for shard in [0u32, 3] {
        let tail = broker
            .cursor_tail(TENANT, NAMESPACE, STREAM, shard)
            .await
            .expect("tail");
        assert_eq!(
            tail.next_seq(),
            1,
            "shard {shard} should hold exactly one record"
        );
    }
}

/// **The publish path and the replication path open the same log.** This is the
/// disagreement #286 was filed for: the registry opened shard 0 whatever it was
/// asked for, while replication opened `key.shard`, so a record published to
/// shard 2 was invisible to the driver shipping shard 2.
#[tokio::test]
async fn a_publish_lands_in_the_log_replication_ships() {
    let (broker, storage, _dir) = sharded_broker().await;

    broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            STREAM,
            2,
            &[Bytes::from_static(b"routed-here")],
        )
        .await
        .expect("publish to shard 2");

    // Opened exactly as `replication::driver` opens it, from the shard key.
    let shipped = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 2)
        .expect("open shard 2 the way replication does");
    assert_eq!(
        shipped.tail_offset().await.expect("tail"),
        1,
        "replication would ship an empty log for a shard that was published to",
    );

    let untouched = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open shard 0");
    assert_eq!(
        untouched.tail_offset().await.expect("tail"),
        0,
        "the record was written to shard 0's log instead of the one it routed to",
    );
}

/// A shard the stream does not have is refused rather than silently created:
/// opening a log for it would leave a directory nothing ever reads.
#[tokio::test]
async fn a_shard_beyond_the_stream_is_refused() {
    let (broker, _storage, _dir) = sharded_broker().await;

    assert!(
        broker
            .publish_batch(
                TENANT,
                NAMESPACE,
                STREAM,
                SHARDS,
                &[Bytes::from_static(b"nowhere")]
            )
            .await
            .is_err(),
        "shard {SHARDS} is out of range for a stream with {SHARDS} shards",
    );
}

/// **A shard this broker lets go of is closed, and comes back from disk.** The
/// log is fenced under anything still holding it, the other shards are left
/// alone, and the next publish reopens the shard where it left off.
#[tokio::test]
async fn a_closed_shard_lets_go_of_its_log_and_reopens_from_disk() {
    let (broker, storage, _dir) = sharded_broker().await;
    for shard in [1u32, 2] {
        broker
            .publish_batch(
                TENANT,
                NAMESPACE,
                STREAM,
                shard,
                &[Bytes::from_static(b"a")],
            )
            .await
            .expect("publish");
    }
    let held = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 1)
        .expect("open shard 1");
    let neighbour = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 2)
        .expect("open shard 2");

    broker
        .close_stream_shard(TENANT, NAMESPACE, STREAM, 1)
        .await
        .expect("close");

    assert!(
        held.read_from(0, 1024).await.is_err(),
        "a handle from before the close still works",
    );
    assert_eq!(neighbour.read_from(0, 1024).await.expect("read").len(), 1);

    // The stream state went with the log: a publish opens both afresh rather
    // than failing against the closed one.
    broker
        .publish_batch(TENANT, NAMESPACE, STREAM, 1, &[Bytes::from_static(b"b")])
        .await
        .expect("publish after the close");
    assert_eq!(replay(&broker, 1).await, ["a", "b"]);
    let tail = broker
        .cursor_tail(TENANT, NAMESPACE, STREAM, 1)
        .await
        .expect("tail");
    assert_eq!(tail.next_seq(), 2);
}

//! Subscriber queues bounded by payload bytes as well as entries.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};

use crate::stream::SubQueuePolicy;
use crate::{Broker, DurableStorage, StartPosition, StreamMetadata, Subscription};

async fn broker_with(broker: Broker, metadata: StreamMetadata) -> Broker {
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "big", metadata)
        .await
        .expect("register");
    broker
}

async fn bounded(bytes: usize, policy: SubQueuePolicy) -> Broker {
    broker_with(
        Broker::new(EphemeralCache::new().into())
            .with_subscriber_queue_policy(policy)
            .with_subscriber_queue_max_bytes(bytes),
        StreamMetadata::default(),
    )
    .await
}

async fn subscribe(broker: &Broker) -> Subscription {
    broker
        .subscribe("t1", "default", "big", 0)
        .await
        .expect("subscribe")
}

async fn publish(broker: &Broker, len: usize) -> usize {
    broker
        .publish("t1", "default", "big", Bytes::from(vec![7u8; len]))
        .await
        .expect("publish")
}

/// The queue has 1024 entries of room, but its bytes run out after two.
#[tokio::test]
async fn a_queue_full_of_bytes_drops_new_batches_until_it_drains() {
    let broker = bounded(100, SubQueuePolicy::DropNew).await;
    let mut sub = subscribe(&broker).await;

    assert_eq!(publish(&broker, 40).await, 1);
    assert_eq!(publish(&broker, 40).await, 1);
    assert_eq!(publish(&broker, 40).await, 0, "over the byte bound");
    assert_eq!(publish(&broker, 20).await, 1, "a smaller one still fits");

    assert_eq!(sub.recv().await.expect("recv").len(), 40);
    assert_eq!(publish(&broker, 40).await, 1, "room again once one is read");
}

#[tokio::test]
async fn a_batch_larger_than_the_bound_goes_into_an_empty_queue() {
    let broker = bounded(100, SubQueuePolicy::DropNew).await;
    let mut sub = subscribe(&broker).await;

    assert_eq!(publish(&broker, 1).await, 1);
    assert_eq!(publish(&broker, 500).await, 0, "the queue is not empty");
    assert_eq!(sub.recv().await.expect("recv").len(), 1);
    assert_eq!(publish(&broker, 500).await, 1, "an empty queue takes it");
    assert_eq!(publish(&broker, 1).await, 0, "and it holds the whole bound");
    assert_eq!(sub.recv().await.expect("recv").len(), 500);
}

/// One subscriber at its bound costs another nothing.
#[tokio::test]
async fn one_subscriber_at_its_byte_bound_does_not_hold_back_another() {
    let broker = bounded(100, SubQueuePolicy::DropNew).await;
    let _slow = subscribe(&broker).await;
    let mut fast = subscribe(&broker).await;

    for _ in 0..10 {
        publish(&broker, 60).await;
        assert_eq!(fast.recv().await.expect("recv").len(), 60);
    }
}

/// The broker-wide budget is a ceiling across subscribers, so a slow one
/// filling it costs a fast one records. That is why it is off by default.
#[tokio::test]
async fn the_broker_budget_is_shared_by_every_subscriber() {
    let broker = broker_with(
        Broker::new(EphemeralCache::new().into())
            .with_subscriber_queue_max_bytes(0)
            .with_subscriber_queue_max_bytes_total(100),
        StreamMetadata::default(),
    )
    .await;
    let _slow = subscribe(&broker).await;
    let mut fast = subscribe(&broker).await;

    assert_eq!(publish(&broker, 50).await, 2);
    fast.recv().await.expect("recv");
    // The slow queue holds 50 of the 100; 60 fits neither queue now.
    assert_eq!(publish(&broker, 60).await, 0);
    assert_eq!(
        publish(&broker, 50).await,
        1,
        "the slow one is the one full"
    );
}

#[tokio::test]
async fn block_waits_for_bytes_to_drain() {
    let broker = bounded(100, SubQueuePolicy::Block).await;
    let mut sub = subscribe(&broker).await;
    publish(&broker, 80).await;

    let blocked = publish(&broker, 80);
    tokio::pin!(blocked);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut blocked)
            .await
            .is_err(),
        "the second batch waits for room"
    );
    assert_eq!(sub.recv().await.expect("recv").len(), 80);
    assert_eq!(blocked.await, 1);
    assert_eq!(sub.recv().await.expect("recv").len(), 80);
}

/// Under `Block`, a subscriber at its bound holds up only its own copy of
/// a batch: one with room has it while the publish still waits.
#[tokio::test]
async fn block_queues_for_subscribers_with_room_before_waiting_on_a_full_one() {
    let broker = Arc::new(bounded(100, SubQueuePolicy::Block).await);
    let mut slow = subscribe(&broker).await;
    let mut fast = subscribe(&broker).await;
    assert_eq!(publish(&broker, 80).await, 2);
    assert_eq!(fast.recv().await.expect("recv").len(), 80);

    let blocked = tokio::spawn({
        let broker = Arc::clone(&broker);
        async move { publish(&broker, 81).await }
    });
    let early = tokio::time::timeout(Duration::from_millis(500), fast.recv()).await;
    assert_eq!(
        early
            .expect("not held behind the full subscriber")
            .expect("recv")
            .len(),
        81
    );
    assert!(
        !blocked.is_finished(),
        "the publish still waits on the full one"
    );

    assert_eq!(slow.recv().await.expect("recv").len(), 80);
    assert_eq!(blocked.await.expect("join"), 2);
    assert_eq!(slow.recv().await.expect("recv").len(), 81);
}

/// The same holds for a queue full by entries rather than bytes.
#[tokio::test]
async fn block_does_not_hold_a_subscriber_with_room_behind_a_full_entry_count() {
    let broker = Arc::new(bounded(0, SubQueuePolicy::Block).await);
    let mut slow = broker
        .subscribe_sized("t1", "default", "big", 0, Some(1))
        .await
        .expect("subscribe");
    let mut fast = subscribe(&broker).await;
    assert_eq!(publish(&broker, 1).await, 2);
    assert_eq!(fast.recv().await.expect("recv").len(), 1);

    let blocked = tokio::spawn({
        let broker = Arc::clone(&broker);
        async move { publish(&broker, 2).await }
    });
    let early = tokio::time::timeout(Duration::from_millis(500), fast.recv()).await;
    assert_eq!(
        early
            .expect("not held behind the full subscriber")
            .expect("recv")
            .len(),
        2
    );
    assert_eq!(slow.recv().await.expect("recv").len(), 1);
    assert_eq!(blocked.await.expect("join"), 2);
    assert_eq!(slow.recv().await.expect("recv").len(), 2);
}

/// Waiting on a full subscriber does not let a later publish reach anyone
/// ahead of an earlier one: offsets arrive in order for every subscriber,
/// with concurrent publishers and a reader that is often full.
#[tokio::test]
async fn block_keeps_each_subscriber_in_offset_order() {
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
    let broker = Arc::new(
        broker_with(
            Broker::new(EphemeralCache::new().into())
                .with_durable_storage(storage)
                .with_subscriber_queue_policy(SubQueuePolicy::Block)
                .with_subscriber_queue_max_bytes(100),
            StreamMetadata {
                durable: true,
                ..StreamMetadata::default()
            },
        )
        .await,
    );
    let mut receivers = Vec::new();
    for _ in 0..3 {
        let resumed = broker
            .subscribe_from("t1", "default", "big", 0, StartPosition::Latest)
            .await
            .expect("subscribe");
        receivers.push(resumed.subscription.into_parts());
    }

    const PUBLISHERS: u64 = 4;
    const EACH: u64 = 25;
    let publishers: Vec<_> = (0..PUBLISHERS)
        .map(|_| {
            let broker = Arc::clone(&broker);
            tokio::spawn(async move {
                for _ in 0..EACH {
                    assert_eq!(publish(&broker, 40).await, 3);
                }
            })
        })
        .collect();
    let mut readers = Vec::new();
    for (i, (mut rx, guard)) in receivers.into_iter().enumerate() {
        readers.push(tokio::spawn(async move {
            let _guard = guard;
            let mut offsets = Vec::new();
            for _ in 0..PUBLISHERS * EACH {
                // The first reader is slow, so its queue is often full.
                if i == 0 {
                    tokio::time::sleep(Duration::from_micros(500)).await;
                }
                offsets.push(
                    rx.recv()
                        .await
                        .expect("recv")
                        .base_offset()
                        .expect("offset"),
                );
            }
            offsets
        }));
    }
    for publisher in publishers {
        publisher.await.expect("join");
    }
    let expected: Vec<u64> = (0..PUBLISHERS * EACH).collect();
    for reader in readers {
        assert_eq!(reader.await.expect("join"), expected);
    }
}

/// A byte-bound drop is a drop like any other: on a durable stream it is
/// an offset gap the subscription reports as a lag.
#[tokio::test]
async fn a_byte_bound_drop_on_a_durable_stream_is_a_lag() {
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
    let broker = broker_with(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            .with_subscriber_queue_max_bytes(100),
        StreamMetadata {
            durable: true,
            ..StreamMetadata::default()
        },
    )
    .await;
    let resumed = broker
        .subscribe_from("t1", "default", "big", 0, StartPosition::Latest)
        .await
        .expect("subscribe");
    let (mut receiver, _guard) = resumed.subscription.into_parts();

    publish(&broker, 80).await;
    publish(&broker, 80).await;
    assert_eq!(receiver.lagged(), Some(1));
    assert_eq!(receiver.recv().await.expect("recv").base_offset(), Some(0));
}

/// A receiver that keeps a batch's bytes keeps the queue's room taken, even
/// with several batches folded into one hold.
#[tokio::test]
async fn bytes_taken_with_a_batch_stay_held_until_dropped() {
    let broker = bounded(100, SubQueuePolicy::DropNew).await;
    let (mut rx, _guard) = subscribe(&broker).await.into_parts();

    assert_eq!(publish(&broker, 40).await, 1);
    assert_eq!(publish(&broker, 40).await, 1);
    let (_, mut held) = rx.recv_held().await.expect("recv");
    let (_, more) = rx.try_recv_held().expect("queued");
    held.absorb(more);
    assert_eq!(publish(&broker, 40).await, 0, "both still held");

    drop(held);
    assert_eq!(publish(&broker, 40).await, 1, "room once given back");
    assert_eq!(publish(&broker, 40).await, 1);
}

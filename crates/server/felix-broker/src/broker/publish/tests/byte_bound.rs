//! Subscriber queues bounded by payload bytes as well as entries.

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

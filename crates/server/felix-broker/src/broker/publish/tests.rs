//! Fanout and queue policy on the publish path. Idempotent publishes are in
//! `tests/idempotent.rs`.

use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use felix_storage::EphemeralCache;

use crate::error::BrokerError;
use crate::stream::SubQueuePolicy;
use crate::{Broker, StreamMetadata};

mod idempotent;

#[tokio::test]
async fn publish_delivers_to_subscriber() {
    // Basic pub/sub flow with a single subscriber.
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"hello"))
        .await
        .expect("publish");
    let msg = sub.recv().await.expect("recv");
    assert_eq!(msg, Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn publish_without_subscribers_returns_zero() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "empty", StreamMetadata::default())
        .await
        .expect("register");
    let delivered = broker
        .publish("t1", "default", "empty", Bytes::from_static(b"payload"))
        .await
        .expect("publish");
    assert_eq!(delivered, 0);
}

#[tokio::test]
async fn stream_delivers_in_order_to_single_subscriber() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "ordered", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "ordered", 0)
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "ordered", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    broker
        .publish("t1", "default", "ordered", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"two"));
}

#[tokio::test]
async fn slow_subscriber_drops_messages_without_blocking_publish() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(1)
        .expect("capacity");
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "laggy", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "laggy", 0)
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "laggy", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    let delivered = broker
        .publish("t1", "default", "laggy", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(delivered, 0);
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    assert!(sub.try_recv().is_err());
}

#[tokio::test]
async fn block_policy_backpressures_publish_when_queue_is_full() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(1)
        .expect("capacity")
        .with_subscriber_queue_policy(SubQueuePolicy::Block);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "blocky", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "blocky", 0)
        .await
        .expect("subscribe");

    broker
        .publish("t1", "default", "blocky", Bytes::from_static(b"one"))
        .await
        .expect("publish");

    let blocked = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        broker.publish("t1", "default", "blocky", Bytes::from_static(b"two")),
    )
    .await;
    assert!(blocked.is_err(), "publish should block on full queue");
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    let sent = broker
        .publish("t1", "default", "blocky", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(sent, 1);
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"two"));
}

/// **Under `Block`, every publisher of a shard waits on the stalled subscriber,
/// and the commit turn has nothing to do with it.**
///
/// The publish path holds a stream's commit turn across fanout on durable
/// streams, and it is tempting to read that as publisher-to-publisher coupling
/// introduced by the ordering. It is not: every publish to a shard delivers to
/// every subscriber of that shard, so a second publisher's *own* enqueue waits
/// on the same full queue. This stream is ephemeral -- no log, no sequencer,
/// no turn -- and the second publisher still waits, and still proceeds in
/// order the moment the subscriber drains. That is `Block`'s contract, and it
/// is per shard whichever path took the record.
#[tokio::test]
async fn block_policy_stalls_every_publisher_of_the_shard_without_a_commit_turn() {
    let broker = std::sync::Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(1)
            .expect("capacity")
            .with_subscriber_queue_policy(SubQueuePolicy::Block),
    );
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "shared", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "shared", 0)
        .await
        .expect("subscribe");

    // Publisher A fills the one slot.
    broker
        .publish("t1", "default", "shared", Bytes::from_static(b"a1"))
        .await
        .expect("publish");

    // Publisher B, a different task, has nothing to do with A and no turn to
    // wait for -- and waits anyway, on the subscriber.
    let b = {
        let broker = std::sync::Arc::clone(&broker);
        tokio::spawn(async move {
            broker
                .publish("t1", "default", "shared", Bytes::from_static(b"b1"))
                .await
                .expect("publish")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert!(
        !b.is_finished(),
        "a second publisher got past a stalled subscriber under Block"
    );

    // The subscriber drains one record and B goes through, behind A.
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"a1"));
    assert_eq!(b.await.expect("join"), 1);
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"b1"));
}

#[tokio::test]
async fn drop_old_policy_is_emulated_as_drop_new() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(1)
        .expect("capacity")
        .with_subscriber_queue_policy(SubQueuePolicy::DropOld);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "drop_old", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "drop_old", 0)
        .await
        .expect("subscribe");

    broker
        .publish("t1", "default", "drop_old", Bytes::from_static(b"one"))
        .await
        .expect("publish");
    let delivered = broker
        .publish("t1", "default", "drop_old", Bytes::from_static(b"two"))
        .await
        .expect("publish");
    assert_eq!(delivered, 0);
    assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
    assert!(sub.try_recv().is_err());
}

#[tokio::test]
async fn small_queue_does_not_grow_unbounded_under_burst() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(2)
        .expect("capacity")
        .with_subscriber_queue_policy(SubQueuePolicy::DropNew);
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "bounded", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub = broker
        .subscribe("t1", "default", "bounded", 0)
        .await
        .expect("subscribe");

    for i in 0..100 {
        let payload = Bytes::from(format!("msg-{i}"));
        let _ = broker
            .publish("t1", "default", "bounded", payload)
            .await
            .expect("publish");
    }

    // Queue is capped at 2; only the earliest buffered items are still available.
    let _ = sub.recv().await.expect("recv");
    let _ = sub.recv().await.expect("recv");
    assert!(sub.try_recv().is_err());
}

#[tokio::test]
async fn multiple_subscribers_receive_payload() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");
    let mut sub_a = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    let mut sub_b = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    broker
        .publish("t1", "default", "orders", Bytes::from_static(b"fanout"))
        .await
        .expect("publish");
    assert_eq!(
        sub_a.recv().await.expect("recv"),
        Bytes::from_static(b"fanout")
    );
    assert_eq!(
        sub_b.recv().await.expect("recv"),
        Bytes::from_static(b"fanout")
    );
}

#[tokio::test]
async fn publish_batch_shares_one_encoded_frame_across_subscribers() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");
    let (mut rx_a, _guard_a) = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe a")
        .into_parts();
    let (mut rx_b, _guard_b) = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe b")
        .into_parts();
    let payloads = [Bytes::from_static(b"one"), Bytes::from_static(b"two")];
    broker
        .publish_batch("t1", "default", "orders", 0, &payloads)
        .await
        .expect("publish");

    let envelope_a = rx_a.recv().await.expect("recv a");
    let envelope_b = rx_b.recv().await.expect("recv b");
    let frame_a = envelope_a.shared_event_frame().expect("encode a");
    let frame_b = envelope_b.shared_event_frame().expect("encode b");

    assert_eq!(frame_a, frame_b);
    assert_eq!(frame_a.as_ptr(), frame_b.as_ptr());
}

#[tokio::test]
async fn queue_depth_returns_to_zero_after_receive_and_receiver_drop() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("register");
    let stream_state = broker
        .get_stream_state("t1", "default", "orders", 0)
        .await
        .expect("stream state");
    let (mut receiver, _guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe")
        .into_parts();
    let payloads = [Bytes::from_static(b"one"), Bytes::from_static(b"two")];

    broker
        .publish_batch("t1", "default", "orders", 0, &payloads)
        .await
        .expect("publish");
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 2);
    receiver.recv().await.expect("receive");
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 0);

    broker
        .publish_batch("t1", "default", "orders", 0, &payloads)
        .await
        .expect("publish");
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 2);
    drop(receiver);
    assert_eq!(stream_state.queued_items.load(Ordering::Relaxed), 0);
}

#[tokio::test]
#[ignore = "microbenchmark: run explicitly for perf validation"]
async fn perf_hot_path_payload_4096_fanout_1_batch_64_binary() {
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(16_384)
        .expect("capacity");
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");

    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    let iterations = 200usize;
    let payloads: Vec<Bytes> = (0..64).map(|_| Bytes::from(vec![0xAB; 4096])).collect();
    let expected = iterations * payloads.len();

    let drain = tokio::spawn(async move {
        for _ in 0..expected {
            let _ = sub.recv().await;
        }
    });

    let stream_state = broker
        .get_stream_state("t1", "default", "orders", 0)
        .await
        .expect("stream_state");

    let mut snapshot_ns = 0u128;
    let mut publish_ns = 0u128;
    let mut encode_ns = 0u128;
    let mut write_ns = 0u128;

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = stream_state.subscriber_snapshot();
        snapshot_ns += start.elapsed().as_nanos();

        let start = Instant::now();
        broker
            .publish_batch("t1", "default", "orders", 0, &payloads)
            .await
            .expect("publish");
        publish_ns += start.elapsed().as_nanos();

        let start = Instant::now();
        let frame = felix_wire::binary::encode_event_batch_bytes(1, &payloads)
            .expect("encode binary event batch");
        encode_ns += start.elapsed().as_nanos();

        let start = Instant::now();
        let mut io_buf = Vec::with_capacity(frame.len());
        io_buf.extend_from_slice(frame.as_ref());
        write_ns += start.elapsed().as_nanos();
    }

    drain.await.expect("drain");

    println!(
        "perf payload=4096 fanout=1 batch=64 binary=true iterations={} snapshot_avg_us={:.2} publish_avg_us={:.2} encode_avg_us={:.2} write_avg_us={:.2}",
        iterations,
        snapshot_ns as f64 / iterations as f64 / 1_000.0,
        publish_ns as f64 / iterations as f64 / 1_000.0,
        encode_ns as f64 / iterations as f64 / 1_000.0,
        write_ns as f64 / iterations as f64 / 1_000.0,
    );
}

#[tokio::test]
async fn publish_to_nonexistent_stream_errors() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    let err = broker
        .publish("t1", "default", "missing", Bytes::from_static(b"data"))
        .await
        .expect_err("stream");
    assert!(matches!(err, BrokerError::StreamNotFound { .. }));
}

#[tokio::test]
async fn resolved_stream_handle_publishes_without_name_lookup() {
    let broker = Broker::new(EphemeralCache::new().into());
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await
        .expect("stream");
    let mut sub = broker
        .subscribe("t1", "default", "orders", 0)
        .await
        .expect("subscribe");
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await
        .expect("handle");

    broker
        .publish_batch_to_handle(&handle, &[Bytes::from_static(b"handled")])
        .await
        .expect("publish");
    assert_eq!(
        sub.recv().await.expect("delivery"),
        Bytes::from_static(b"handled")
    );
}

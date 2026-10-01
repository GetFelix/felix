//! The lane feeder: coalescing single-record publishes into one frame.

use std::sync::atomic::AtomicUsize;

use dashmap::DashMap;

use super::*;

/// A manager with one lane whose commands the test reads directly, so nothing
/// but the feeder decides when a batch goes out.
fn capture_lane() -> (Arc<WriterLaneManager>, mpsc::Receiver<LaneCommand>) {
    let (tx, rx) = mpsc::channel(1024);
    let manager = WriterLaneManager {
        lanes: vec![tx],
        lane_queue_capacity: 1024,
        lane_queue_policy: felix_broker::SubQueuePolicy::Block,
        lane_queue_highwater: vec![AtomicUsize::new(0)],
        connection_writers: DashMap::new(),
        subscriber_connections: DashMap::new(),
        connection_queue_capacity: 1024,
        max_bytes_per_write: 256 * 1024,
        connection_lanes: DashMap::new(),
        subscriber_pins: DashMap::new(),
        shard: crate::config::SubscriberLaneShard::Auto,
        single_writer_per_conn: true,
        rr_counter: AtomicUsize::new(0),
    };
    (Arc::new(manager), rx)
}

async fn settle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn lane_feeder_flushes_a_steady_stream_by_the_batch_deadline() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let (event_rx, _guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();

    let (manager, mut lane_rx) = capture_lane();
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 1,
        max_events: 64,
        max_bytes: 64 * 1024,
        flush_delay: Duration::from_millis(2),
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };
    let feeder = tokio::spawn(run_lane_feeder(
        event_rx,
        Arc::downgrade(&manager),
        0,
        None,
        config,
        Arc::new(SubscriptionLimiter::new()),
        TenantDelivery::for_tenant("t1"),
    ));

    // One publish every 250 us never leaves a 2 ms gap, and 63 stays under the
    // count cap, so only a deadline for the whole batch can flush here.
    let mut first = None;
    for sent in 1..=63_usize {
        broker
            .publish("t1", "default", "orders", make_payload(b"event"))
            .await?;
        tokio::time::advance(Duration::from_micros(250)).await;
        settle().await;
        if let Ok(command) = lane_rx.try_recv() {
            first = Some((sent, command));
            break;
        }
    }
    feeder.abort();

    let (sent, command) = first.expect("no batch from a steady stream");
    let LaneCommand::Delivery { item_count, .. } = command else {
        panic!("expected a delivery, got {command:?}");
    };
    // 2 ms after the first event, plus up to 1 ms because tokio timers fire
    // on millisecond ticks.
    assert!(sent <= 12, "first batch only after {sent} publishes");
    assert_eq!(item_count, sent);
    Ok(())
}

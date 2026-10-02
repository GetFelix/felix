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

/// A broker with one stream, a subscription on it, and a feeder draining that
/// subscription into a capture lane.
struct Harness {
    broker: Arc<Broker>,
    lane_rx: mpsc::Receiver<LaneCommand>,
    feeder: tokio::task::JoinHandle<()>,
    // The feeder holds the manager weakly and the guard unsubscribes on drop.
    _manager: Arc<WriterLaneManager>,
    _guard: felix_broker::SubscriptionGuard,
}

async fn spawn_feeder(flush_delay: Duration) -> Result<Harness> {
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
    let (event_rx, guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();

    let (manager, lane_rx) = capture_lane();
    let config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        subscription_id: 1,
        max_events: 64,
        max_bytes: 64 * 1024,
        flush_delay,
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
    Ok(Harness {
        broker,
        lane_rx,
        feeder,
        _manager: manager,
        _guard: guard,
    })
}

async fn publish(broker: &Broker) -> Result<()> {
    broker
        .publish("t1", "default", "orders", make_payload(b"event"))
        .await?;
    Ok(())
}

fn item_count(command: LaneCommand) -> usize {
    match command {
        LaneCommand::Delivery { item_count, .. } => item_count,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn lane_feeder_sends_a_lone_event_without_waiting_for_the_delay() -> Result<()> {
    let Harness {
        broker,
        mut lane_rx,
        feeder,
        _manager,
        _guard,
    } = spawn_feeder(Duration::from_millis(50)).await?;
    settle().await;

    let start = tokio::time::Instant::now();
    publish(&broker).await?;
    let command = lane_rx.recv().await.expect("lane closed");
    let waited = start.elapsed();
    feeder.abort();

    assert_eq!(item_count(command), 1);
    // Paused time only moves when every task waits on a timer, so any wait
    // for the batch deadline shows up here as the full 50 ms.
    assert!(
        waited < Duration::from_millis(1),
        "lone event waited {waited:?}"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn lane_feeder_coalesces_a_queued_burst() -> Result<()> {
    let Harness {
        broker,
        mut lane_rx,
        feeder,
        _manager,
        _guard,
    } = spawn_feeder(Duration::from_millis(50)).await?;
    // Queue the whole burst before the feeder gets to run.
    const BURST: usize = 100;
    for _ in 0..BURST {
        publish(&broker).await?;
    }

    let mut frames = 0;
    let mut delivered = 0;
    while delivered < BURST {
        delivered += item_count(lane_rx.recv().await.expect("lane closed"));
        frames += 1;
    }
    feeder.abort();

    // 64 is the count cap.
    assert_eq!(frames, 2, "{BURST} queued events took {frames} frames");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn lane_feeder_flushes_a_steady_stream_by_the_batch_deadline() -> Result<()> {
    let Harness {
        broker,
        mut lane_rx,
        feeder,
        _manager,
        _guard,
    } = spawn_feeder(Duration::from_millis(2)).await?;

    // Two events queued at once mark the feeder busy, so the next batch waits.
    publish(&broker).await?;
    publish(&broker).await?;
    settle().await;
    assert_eq!(item_count(lane_rx.try_recv()?), 2);

    // One publish every 250 us never leaves a 2 ms gap, and 63 stays under the
    // count cap, so only a deadline for the whole batch can flush here.
    let mut first = None;
    for sent in 1..=63_usize {
        publish(&broker).await?;
        tokio::time::advance(Duration::from_micros(250)).await;
        settle().await;
        if let Ok(command) = lane_rx.try_recv() {
            first = Some((sent, command));
            break;
        }
    }
    feeder.abort();

    let (sent, command) = first.expect("no batch from a steady stream");
    // 2 ms after the first event, plus up to 1 ms because tokio timers fire
    // on millisecond ticks.
    assert!(sent <= 12, "first batch only after {sent} publishes");
    assert!(sent > 1, "a busy feeder flushed without waiting");
    assert_eq!(item_count(command), sent);
    Ok(())
}

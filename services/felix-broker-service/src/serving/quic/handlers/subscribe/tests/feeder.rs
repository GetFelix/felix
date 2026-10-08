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
        publisher_enabled: false,
        timestamps_enabled: false,
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

#[tokio::test(start_paused = true)]
async fn lane_feeder_with_zero_delay_flushes_a_busy_batch_without_a_timer() -> Result<()> {
    let Harness {
        broker,
        mut lane_rx,
        feeder,
        _manager,
        _guard,
    } = spawn_feeder(Duration::ZERO).await?;

    // Two events queued at once mark the feeder busy.
    publish(&broker).await?;
    publish(&broker).await?;
    settle().await;
    assert_eq!(item_count(lane_rx.try_recv()?), 2);

    // Off a millisecond boundary, any timer would need time to advance to the
    // next tick before it fires.
    tokio::time::advance(Duration::from_micros(500)).await;
    publish(&broker).await?;
    settle().await;
    let flushed = lane_rx.try_recv();
    feeder.abort();

    let command = flushed.expect("busy batch waited on a timer");
    assert_eq!(item_count(command), 1);
    Ok(())
}

/// A durable stream whose subscriber queue holds one batch, and a
/// subscription on it that ends at its first drop, with three publishes made:
/// the first queued, the other two dropped.
async fn lagged_subscription() -> Result<Lagged> {
    let dir = tempfile::tempdir()?;
    let storage = felix_broker::DurableStorage::open(
        dir.path(),
        felix_storage::log::LogConfig {
            fsync_mode: felix_storage::log::FsyncMode::None,
            preallocate_segments: false,
            ..Default::default()
        },
    )?;
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(storage)
        .with_topic_capacity(1)?;
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata {
                durable: true,
                shards: 1,
                ..Default::default()
            },
        )
        .await?;
    let (mut event_rx, guard) = broker
        .subscribe("t1", "default", "orders", 0)
        .await?
        .into_parts();
    event_rx.end_on_lag();
    for _ in 0..3 {
        publish(&broker).await?;
    }
    Ok(Lagged {
        _dir: dir,
        broker,
        event_rx,
        _guard: guard,
    })
}

struct Lagged {
    _dir: tempfile::TempDir,
    broker: Broker,
    event_rx: felix_broker::SubscriptionReceiver,
    _guard: felix_broker::SubscriptionGuard,
}

/// Feed `event_rx` through a lane and return how many records it delivered
/// and the frame that ended it.
async fn feed_to_the_end(
    event_rx: felix_broker::SubscriptionReceiver,
    shard_moved_enabled: bool,
) -> Result<(usize, Message)> {
    let (manager, mut lane_rx) = capture_lane();
    let config = EventWriterConfig {
        offsets_enabled: true,
        skip_enabled: false,
        shard_moved_enabled,
        subscription_id: 1,
        max_events: 64,
        max_bytes: 64 * 1024,
        flush_delay: Duration::ZERO,
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
        publisher_enabled: false,
        timestamps_enabled: false,
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
    let mut delivered = 0;
    let last = loop {
        match tokio::time::timeout(Duration::from_secs(2), lane_rx.recv())
            .await?
            .expect("lane open")
        {
            LaneCommand::Unregister { last, .. } => break last.expect("a last frame"),
            other => delivered += item_count(other),
        }
    };
    tokio::time::timeout(Duration::from_secs(2), feeder).await??;
    Ok((
        delivered,
        Message::decode(felix_wire::Frame::decode(last)?)?,
    ))
}

/// A subscription that asked to end at its first drop sends what was queued
/// before it and then `subscription_lagged`, with nothing published after.
#[tokio::test]
async fn lane_feeder_ends_a_lagged_subscription_with_where_to_resume() -> Result<()> {
    let lagged = lagged_subscription().await?;

    let (delivered, last) = feed_to_the_end(lagged.event_rx, false).await?;
    assert_eq!(delivered, 1);
    assert_eq!(
        last,
        Message::SubscriptionLagged {
            subscription_id: 1,
            resume_from: 1,
        }
    );
    Ok(())
}

/// **A lag outranks a shard move.** The move's `resume_from` is the stream's
/// position at the move, past the records this subscriber's queue dropped, so
/// a client that followed it would skip them. Ending with the lag sends it
/// back to where the drop began.
#[tokio::test]
async fn lane_feeder_reports_a_lag_over_a_shard_move() -> Result<()> {
    let lagged = lagged_subscription().await?;
    let ended = lagged
        .broker
        .end_subscriptions(
            "t1",
            "default",
            "orders",
            0,
            Some(felix_broker::ShardHandoff {
                node_id: Some("b".to_string()),
                addr: None,
                generation: 2,
            }),
        )
        .await;
    assert_eq!(ended, 1);

    let (delivered, last) = feed_to_the_end(lagged.event_rx, true).await?;
    assert_eq!(delivered, 1);
    assert_eq!(
        last,
        Message::SubscriptionLagged {
            subscription_id: 1,
            resume_from: 1,
        }
    );
    Ok(())
}

//! The lane feeder: coalescing single-record publishes into one frame.

use std::sync::atomic::AtomicUsize;

use dashmap::DashMap;

use super::*;

/// A manager with one lane whose commands the test reads directly, so nothing
/// but the feeder decides when a batch goes out.
fn capture_lane() -> (Arc<WriterLaneManager>, mpsc::Receiver<LaneCommand>) {
    capture_lane_bounded(0)
}

/// [`capture_lane`] with a byte bound on the connection's lanes.
fn capture_lane_bounded(egress: usize) -> (Arc<WriterLaneManager>, mpsc::Receiver<LaneCommand>) {
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
        egress: Arc::new(
            crate::serving::quic::handlers::subscribe::egress::EgressBudget::new(egress),
        ),
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
    spawn_feeder_capped(flush_delay, 64 * 1024).await
}

async fn spawn_feeder_capped(flush_delay: Duration, max_bytes: usize) -> Result<Harness> {
    spawn_feeder_on(
        Broker::new(EphemeralCache::new().into()),
        flush_delay,
        max_bytes,
    )
    .await
}

async fn spawn_feeder_on(
    broker: Broker,
    flush_delay: Duration,
    max_bytes: usize,
) -> Result<Harness> {
    let broker = Arc::new(broker);
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let (manager, lane_rx) = capture_lane();
    let (feeder, guard) = feed(&broker, "orders", &manager, flush_delay, max_bytes).await?;
    Ok(Harness {
        broker,
        lane_rx,
        feeder,
        _manager: manager,
        _guard: guard,
    })
}

/// Register `stream` and run a feeder for a new subscription to it into
/// `manager`'s lane.
async fn feed(
    broker: &Arc<Broker>,
    stream: &str,
    manager: &Arc<WriterLaneManager>,
    flush_delay: Duration,
    max_bytes: usize,
) -> Result<(tokio::task::JoinHandle<()>, felix_broker::SubscriptionGuard)> {
    feed_with(broker, stream, manager, flush_delay, max_bytes, |_| {}).await
}

/// [`feed`] with the writer config adjusted by `adjust`.
async fn feed_with(
    broker: &Arc<Broker>,
    stream: &str,
    manager: &Arc<WriterLaneManager>,
    flush_delay: Duration,
    max_bytes: usize,
    adjust: impl FnOnce(&mut EventWriterConfig),
) -> Result<(tokio::task::JoinHandle<()>, felix_broker::SubscriptionGuard)> {
    broker
        .register_stream(
            "t1",
            "default",
            stream,
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let (event_rx, guard) = broker
        .subscribe("t1", "default", stream, 0)
        .await?
        .into_parts();
    let mut config = EventWriterConfig {
        offsets_enabled: false,
        skip_enabled: false,
        shard_moved_enabled: false,
        stream_deleted_enabled: false,
        subscription_id: 1,
        max_events: 64,
        max_bytes,
        flush_delay,
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
        publisher_enabled: false,
        timestamps_enabled: false,
    };
    adjust(&mut config);
    let feeder = tokio::spawn(run_lane_feeder(
        event_rx,
        Arc::downgrade(manager),
        0,
        None,
        config,
        IdentityLimits::unlimited(),
        TenantDelivery::for_tenant("t1"),
        None,
    ));
    Ok((feeder, guard))
}

async fn publish(broker: &Broker) -> Result<()> {
    broker
        .publish("t1", "default", "orders", make_payload(b"event"))
        .await?;
    Ok(())
}

async fn publish_len(broker: &Broker, len: usize) -> Result<usize> {
    Ok(broker
        .publish("t1", "default", "orders", Bytes::from(vec![7u8; len]))
        .await?)
}

async fn publish_to(broker: &Broker, stream: &str, len: usize) -> Result<usize> {
    Ok(broker
        .publish("t1", "default", stream, Bytes::from(vec![7u8; len]))
        .await?)
}

/// A frame in the writer lanes no longer counts against its subscriber's
/// queue bound: that bound is for what waits in the broker queue.
#[tokio::test]
async fn a_frame_in_the_lanes_leaves_the_subscriber_queue_bound() -> Result<()> {
    let broker = Broker::new(EphemeralCache::new().into()).with_subscriber_queue_max_bytes(100);
    let Harness {
        broker,
        mut lane_rx,
        feeder,
        _manager,
        _guard,
    } = spawn_feeder_on(broker, Duration::ZERO, 64 * 1024).await?;
    settle().await;

    assert_eq!(publish_len(&broker, 60).await?, 1);
    settle().await;
    let _frame = lane_rx.try_recv().expect("the feeder sent the batch on");
    assert_eq!(
        publish_len(&broker, 60).await?,
        1,
        "the frame in the lane still counts against the queue bound"
    );
    feeder.abort();
    Ok(())
}

/// A frame keeps holding its subscription's share of the connection's lane
/// budget until it is written. With the share full the feeder stops taking
/// batches, so they wait in the broker queue and its bound sheds them.
#[tokio::test]
async fn a_full_lane_share_leaves_batches_in_the_broker_queue() -> Result<()> {
    let broker =
        Arc::new(Broker::new(EphemeralCache::new().into()).with_subscriber_queue_max_bytes(100));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let (manager, mut lane_rx) = capture_lane_bounded(100);
    let (feeder, _guard) = feed(&broker, "orders", &manager, Duration::ZERO, 64 * 1024).await?;
    settle().await;

    assert_eq!(publish_to(&broker, "orders", 60).await?, 1);
    settle().await;
    let first = lane_rx.try_recv().expect("the first batch went on");
    assert_eq!(publish_to(&broker, "orders", 60).await?, 1);
    settle().await;
    assert!(lane_rx.try_recv().is_err(), "the share has no room for it");
    assert_eq!(
        publish_to(&broker, "orders", 60).await?,
        0,
        "the waiting batch still counts against the queue bound"
    );

    drop(first);
    settle().await;
    assert_eq!(item_count(lane_rx.try_recv().expect("room again")), 1);
    feeder.abort();
    Ok(())
}

/// Each subscription has its own share of the connection's budget, so one
/// whose frames are not being written stops at its share and the others on
/// the connection keep going.
#[tokio::test]
async fn a_stalled_subscription_does_not_hold_up_its_connection_mates() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let (manager, mut lane_rx) = capture_lane_bounded(100);
    let (stalled, _stalled_guard) =
        feed(&broker, "stalled", &manager, Duration::ZERO, 64 * 1024).await?;
    let (healthy, _healthy_guard) =
        feed(&broker, "healthy", &manager, Duration::ZERO, 64 * 1024).await?;
    settle().await;

    // Never written: the stalled subscription keeps these frames.
    let mut kept = Vec::new();
    for _ in 0..4 {
        publish_to(&broker, "stalled", 40).await?;
        settle().await;
        while let Ok(frame) = lane_rx.try_recv() {
            kept.push(frame);
        }
    }

    for _ in 0..8 {
        assert_eq!(publish_to(&broker, "healthy", 40).await?, 1);
        settle().await;
        let frame = lane_rx
            .try_recv()
            .expect("the healthy subscription keeps going");
        drop(frame);
    }
    assert_eq!(
        kept.len(),
        1,
        "the stalled subscription stopped at its share"
    );
    stalled.abort();
    healthy.abort();
    Ok(())
}

/// A manager whose one lane holds `capacity` commands, so a test can stall
/// the feeder's send by leaving it full.
fn capture_lane_holding(capacity: usize) -> (Arc<WriterLaneManager>, mpsc::Receiver<LaneCommand>) {
    let (manager, _) = capture_lane();
    let (tx, rx) = mpsc::channel(capacity);
    let mut manager = Arc::into_inner(manager).expect("only handle");
    manager.lanes = vec![tx];
    manager.lane_queue_capacity = capacity;
    (Arc::new(manager), rx)
}

fn filler() -> LaneCommand {
    let now = Instant::now();
    LaneCommand::Delivery {
        subscriber_id: 0,
        frame: Bytes::new(),
        item_count: 0,
        first_enqueued_at: now,
        enqueue_at: now,
        held: Default::default(),
    }
}

/// A batch counts against its subscriber's queue bound until its frame is
/// handed to the lanes: through a wait for lane room, but not after the send,
/// nor after a failed encode, nor while the feeder waits out the flush delay.
/// With a bound of 100, a 60-byte probe gets through only when the 60 bytes
/// before it have been given back.
#[tokio::test(start_paused = true)]
async fn subscriber_queue_bytes_go_back_once_the_frame_is_handed_on() -> Result<()> {
    let broker =
        Arc::new(Broker::new(EphemeralCache::new().into()).with_subscriber_queue_max_bytes(100));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    // A full lane: the feeder encodes the batch and waits to send it.
    let (manager, mut lane_rx) = capture_lane_holding(1);
    manager.lanes[0]
        .try_send(filler())
        .expect("room for the filler");
    let (stalled, _stalled_guard) =
        feed(&broker, "stalled", &manager, Duration::ZERO, 64 * 1024).await?;
    settle().await;
    assert_eq!(publish_to(&broker, "stalled", 60).await?, 1);
    settle().await;
    assert_eq!(
        publish_to(&broker, "stalled", 60).await?,
        0,
        "held while the feeder waits for lane room"
    );
    let _filler = lane_rx.recv().await.expect("the filler");
    let sent = lane_rx.recv().await.expect("the batch went on");
    assert_eq!(item_count(sent), 1);
    settle().await;
    assert_eq!(
        publish_to(&broker, "stalled", 60).await?,
        1,
        "given back once the frame is in the lane"
    );
    stalled.abort();

    // A publisher too long to encode: the frame is never built.
    let (manager, mut lane_rx) = capture_lane();
    let (failing, _failing_guard) = feed_with(
        &broker,
        "failing",
        &manager,
        Duration::ZERO,
        64 * 1024,
        |config| config.publisher_enabled = true,
    )
    .await?;
    settle().await;
    let handle = broker
        .resolve_stream_handle("t1", "default", "failing", 0)
        .await?;
    let publisher = Bytes::from(vec![b'p'; 300]);
    broker
        .publish_batch_with_outcome(&handle, &[Bytes::from(vec![7u8; 60])], Some(&publisher))
        .await?;
    settle().await;
    assert!(lane_rx.try_recv().is_err(), "the encode failed");
    assert_eq!(
        publish_to(&broker, "failing", 60).await?,
        1,
        "given back when the encode fails"
    );
    failing.abort();

    // A busy feeder waits out the flush delay for more.
    let (manager, mut lane_rx) = capture_lane();
    let (waiting, _waiting_guard) = feed(
        &broker,
        "waiting",
        &manager,
        Duration::from_secs(60),
        64 * 1024,
    )
    .await?;
    settle().await;
    publish_to(&broker, "waiting", 10).await?;
    publish_to(&broker, "waiting", 10).await?;
    settle().await;
    assert_eq!(
        item_count(lane_rx.try_recv()?),
        2,
        "two at once mark it busy"
    );
    assert_eq!(publish_to(&broker, "waiting", 60).await?, 1);
    settle().await;
    assert!(lane_rx.try_recv().is_err(), "waiting out the delay");
    assert_eq!(
        publish_to(&broker, "waiting", 60).await?,
        1,
        "given back before the flush-delay wait"
    );
    waiting.abort();
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
async fn lane_feeder_carries_the_event_past_the_byte_cap_into_the_next_frame() -> Result<()> {
    // Two five-byte events fit under twelve bytes; a third does not.
    let Harness {
        broker,
        mut lane_rx,
        feeder,
        _manager,
        _guard,
    } = spawn_feeder_capped(Duration::from_millis(50), 12).await?;
    for _ in 0..5 {
        publish(&broker).await?;
    }

    let mut frames = Vec::new();
    while frames.iter().sum::<usize>() < 5 {
        frames.push(item_count(lane_rx.recv().await.expect("lane closed")));
    }
    feeder.abort();

    assert_eq!(frames, vec![2, 2, 1]);
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
    let (delivered, last) = feed_until_unregistered(event_rx, shard_moved_enabled, false).await?;
    Ok((delivered, last.expect("a last frame")))
}

/// Like [`feed_to_the_end`], for a subscription that may end with no last
/// frame at all.
async fn feed_until_unregistered(
    event_rx: felix_broker::SubscriptionReceiver,
    shard_moved_enabled: bool,
    stream_deleted_enabled: bool,
) -> Result<(usize, Option<Message>)> {
    let (manager, mut lane_rx) = capture_lane();
    let config = EventWriterConfig {
        offsets_enabled: true,
        skip_enabled: false,
        shard_moved_enabled,
        stream_deleted_enabled,
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
        IdentityLimits::unlimited(),
        TenantDelivery::for_tenant("t1"),
        None,
    ));
    let mut delivered = 0;
    let last = loop {
        match tokio::time::timeout(Duration::from_secs(2), lane_rx.recv())
            .await?
            .expect("lane open")
        {
            LaneCommand::Unregister { last, .. } => break last,
            other => delivered += item_count(other),
        }
    };
    tokio::time::timeout(Duration::from_secs(2), feeder).await??;
    let last = match last {
        Some(frame) => Some(Message::decode(felix_wire::Frame::decode(frame)?)?),
        None => None,
    };
    Ok((delivered, last))
}

/// A subscriber of a deleted stream gets what was queued, then
/// `stream_deleted` when it offered the feature and a plain end otherwise.
#[tokio::test]
async fn lane_feeder_ends_a_deleted_stream_with_stream_deleted() -> Result<()> {
    for offered in [true, false] {
        let broker = Broker::new(EphemeralCache::new().into());
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
        publish(&broker).await?;
        assert!(broker.remove_stream("t1", "default", "orders").await?);

        let (delivered, last) = feed_until_unregistered(event_rx, true, offered).await?;
        assert_eq!(delivered, 1);
        let expected = offered.then_some(Message::StreamDeleted { subscription_id: 1 });
        assert_eq!(last, expected, "offered: {offered}");
    }
    Ok(())
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

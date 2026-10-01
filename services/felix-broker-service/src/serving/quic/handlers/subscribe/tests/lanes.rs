//! Writer lanes: which lane a subscriber lands on, and cleanup when it leaves.

use super::*;

#[tokio::test]
async fn lane_fanout_preserves_order_for_multiple_subscribers() -> Result<()> {
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

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let mut config = test_config();
    config.subscriber_writer_lanes = 4;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
    config.fanout_batch_size = 1;
    config.event_batch_max_events = 1;

    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        for sub_id in [31_u64, 32_u64] {
            let connection = server.accept().await?;
            handle_subscribe_message(
                broker_for_server.clone(),
                connection,
                config.clone(),
                &Arc::new(SubscriptionLimiter::new()),
                &server_lane_manager,
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "orders".to_string(),
                Some(sub_id),
                None,
                None,
                felix_wire::ORIGINAL_V1_FLAGS,
                0,
            )
            .await?;
        }
        Result::<()>::Ok(())
    });

    let client1 = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        make_client_config(cert.clone())?,
        transport.clone(),
    )?;
    let client2 = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection1 = client1.connect(addr, "localhost").await?;
    let connection2 = client2.connect(addr, "localhost").await?;

    let mut subscribed = Vec::new();
    for _ in 0..2 {
        let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
            .await
            .context("ack timeout")?
            .context("ack missing")?;
        match ack {
            Outgoing::Message(Message::Subscribed {
                subscription_id, ..
            }) => {
                subscribed.push(subscription_id);
            }
            Outgoing::Message(other) => panic!("unexpected ack message: {other:?}"),
            Outgoing::CacheMessage(other) => panic!("unexpected cache ack: {other:?}"),
            Outgoing::PublishAck { request_id, .. } => {
                panic!("unexpected publish ack: {request_id}")
            }
        }
    }
    subscribed.sort_unstable();
    assert_eq!(subscribed, vec![31, 32]);

    let mut event_recv_1 = tokio::time::timeout(Duration::from_secs(1), connection1.accept_uni())
        .await
        .context("accept uni timeout 1")??;
    let mut event_recv_2 = tokio::time::timeout(Duration::from_secs(1), connection2.accept_uni())
        .await
        .context("accept uni timeout 2")??;
    let mut scratch_1 = crate::serving::quic::FrameScratch::new();
    let mut scratch_2 = crate::serving::quic::FrameScratch::new();
    let _ = crate::serving::quic::codec::read_message_limited(
        &mut event_recv_1,
        16 * 1024,
        &mut scratch_1,
    )
    .await?
    .expect("hello1");
    let _ = crate::serving::quic::codec::read_message_limited(
        &mut event_recv_2,
        16 * 1024,
        &mut scratch_2,
    )
    .await?
    .expect("hello2");

    let expected = (0..20)
        .map(|i| format!("msg-{i}").into_bytes())
        .collect::<Vec<_>>();
    for payload in &expected {
        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::copy_from_slice(payload.as_slice()),
            )
            .await?;
    }

    let mut recv_1 = Vec::with_capacity(expected.len());
    let mut recv_2 = Vec::with_capacity(expected.len());
    for _ in 0..expected.len() {
        let frame = crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv_1,
            16 * 1024,
            &mut scratch_1,
        )
        .await?
        .expect("event frame 1");
        let payloads = decode_delivery_payloads(&frame).context("decode batch 1")?;
        recv_1.push(payloads[0].to_vec());

        let frame = crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv_2,
            16 * 1024,
            &mut scratch_2,
        )
        .await?
        .expect("event frame 2");
        let payloads = decode_delivery_payloads(&frame).context("decode batch 2")?;
        recv_2.push(payloads[0].to_vec());
    }

    assert_eq!(recv_1, expected);
    assert_eq!(recv_2, expected);

    let _ = felix_broker::timings::take_samples();
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn lane_selection_auto_pins_shared_connection_to_one_lane() -> Result<()> {
    let mut config = test_config();
    config.subscriber_writer_lanes = 8;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
    config.subscriber_single_writer_per_conn = true;
    let manager = WriterLaneManager::new(&config);

    let lane_a = manager.select_lane(100, Some(7));
    let lane_b = manager.select_lane(200, Some(7));
    let lane_c = manager.select_lane(300, Some(7));
    assert_eq!(lane_a, lane_b);
    assert_eq!(lane_b, lane_c);

    Ok(())
}

#[tokio::test]
async fn lane_selection_auto_uses_subscriber_hash_when_conn_pin_disabled() -> Result<()> {
    let mut config = test_config();
    config.subscriber_writer_lanes = 8;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
    config.subscriber_single_writer_per_conn = false;
    let manager = WriterLaneManager::new(&config);

    let lane_a = manager.select_lane(100, Some(7));
    let lane_b = manager.select_lane(200, Some(7));
    let lane_c = manager.select_lane(100, Some(7));
    assert_eq!(lane_a, lane_c);
    assert_ne!(lane_a, lane_b);

    Ok(())
}

#[tokio::test]
async fn round_robin_pin_keeps_subscriber_sticky() -> Result<()> {
    let mut config = test_config();
    config.subscriber_writer_lanes = 4;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::RoundRobinPin;
    let manager = WriterLaneManager::new(&config);

    let first = manager.select_lane(42, None);
    let second = manager.select_lane(42, None);
    let third = manager.select_lane(42, None);
    assert_eq!(first, second);
    assert_eq!(second, third);

    Ok(())
}

// Regression test for the leak the soak harness found (#154).
//
// Subscription teardown enqueues `LaneCommand::Unregister` and then *immediately*
// calls `unregister_subscriber`, which removes the `subscriber_connections` entry.
// The lane worker dequeues afterwards, so when it used to look the connection up
// there it found nothing and skipped cleanup entirely — leaving an entry in
// `ACTIVE_SUB_CONN_COUNTS` and a per-connection metric series behind for every
// subscriber connection ever made. Over a long-lived broker with connection churn
// that is unbounded growth, and it made `felix_sub_active_connections` permanently
// wrong. The command now carries `connection_id` so the lookup is not needed.
#[tokio::test]
async fn lane_unregister_cleans_up_after_teardown_already_removed_the_mapping() {
    let manager = WriterLaneManager::new(&test_config());
    let connection_id = unique_test_connection_id();
    let subscriber_id = connection_id ^ 0x5555;

    connection_subscriber_register(Some(connection_id));
    let map = ACTIVE_SUB_CONN_COUNTS
        .get()
        .expect("counts map should be initialized");
    assert!(map.get(&connection_id).is_some());

    // Reproduce the race: teardown has already dropped the mapping the worker
    // used to depend on, before the worker gets to the command.
    manager.subscriber_connections.remove(&subscriber_id);

    manager
        .enqueue(
            0,
            LaneCommand::Unregister {
                subscriber_id,
                connection_id: Some(connection_id),
                last: None,
            },
        )
        .await
        .expect("enqueue unregister");

    // The worker runs on its own task, so poll rather than assume immediacy.
    let mut cleared = false;
    for _ in 0..200 {
        if map.get(&connection_id).is_none() {
            cleared = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        cleared,
        "lane worker must release the connection's subscriber count even when \
         teardown already removed the subscriber_connections entry"
    );
}

#[tokio::test]
async fn connection_id_hash_falls_back_to_subscriber_hash_without_connection() {
    let mut config = test_config();
    config.subscriber_writer_lanes = 8;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::ConnectionIdHash;
    config.subscriber_single_writer_per_conn = false;
    let manager = WriterLaneManager::new(&config);

    let without_conn = manager.select_lane(555, None);
    let expected = manager.lane_for_subscriber(555);
    assert_eq!(without_conn, expected);

    let with_conn_a = manager.select_lane(555, Some(42));
    let with_conn_b = manager.select_lane(999, Some(42));
    assert_eq!(with_conn_a, with_conn_b);
}

#[tokio::test]
async fn unregister_subscriber_clears_internal_maps() {
    let mut config = test_config();
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::RoundRobinPin;
    let manager = WriterLaneManager::new(&config);
    manager.subscriber_pins.insert(7, 2);
    manager.subscriber_connections.insert(7, 88);
    manager.connection_lanes.insert(88, 1);

    manager.unregister_subscriber(7, Some(88));

    assert!(manager.subscriber_pins.get(&7).is_none());
    assert!(manager.subscriber_connections.get(&7).is_none());
    assert!(manager.connection_lanes.get(&88).is_none());
}

#[tokio::test]
async fn writer_lane_tasks_do_not_retain_manager() {
    let manager = WriterLaneManager::new(&test_config());
    let weak_manager = Arc::downgrade(&manager);

    drop(manager);

    assert!(
        weak_manager.upgrade().is_none(),
        "writer lane tasks must not keep a connection's manager alive"
    );
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn manager_drop_releases_remaining_connection_counts() {
    let manager = WriterLaneManager::new(&test_config());
    let connection_id = unique_test_connection_id();
    let subscriber_id = connection_id ^ 0xaaaa;

    connection_subscriber_register(Some(connection_id));
    manager
        .subscriber_connections
        .insert(subscriber_id, connection_id);

    drop(manager);

    let map = ACTIVE_SUB_CONN_COUNTS
        .get()
        .expect("counts map should be initialized");
    assert!(
        map.get(&connection_id).is_none(),
        "dropping the manager must release registrations whose queued unregister did not run"
    );
}

#[tokio::test]
async fn concurrent_lanes_share_one_connection_writer() {
    let manager = WriterLaneManager::new(&test_config());
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            manager.ensure_connection_writer(42, None)
        }));
    }

    let mut senders = Vec::new();
    for task in tasks {
        senders.push(task.await.expect("connection writer task"));
    }

    assert_eq!(manager.connection_writers.len(), 1);
    assert!(
        senders[1..]
            .iter()
            .all(|sender| sender.same_channel(&senders[0]))
    );
}

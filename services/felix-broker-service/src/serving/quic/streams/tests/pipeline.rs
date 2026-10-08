//! A pipelining stream: its publishes are answered in request order, and its
//! window stops the reads when it is full.

use super::*;
use crate::serving::quic::handlers::publish::AckOrder;

fn pipelining_auth(fixture: &AuthFixture) -> Message {
    Message::Auth {
        tenant_id: fixture.tenant_id.clone(),
        token: fixture.token.clone(),
        client_flags: Some(felix_wire::KNOWN_FLAGS),
        client_features: Some(felix_wire::FEATURE_PUBLISH_PIPELINE),
        client_features_hi: None,
    }
}

fn acked_publish(request_id: u64) -> Message {
    Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
        payload: format!("record-{request_id}").into_bytes(),
        key: None,
        request_id: Some(request_id),
        ack: Some(felix_wire::AckMode::PerMessage),
    }
}

fn answered(outgoing: &Outgoing) -> Option<u64> {
    match outgoing {
        Outgoing::Message(Message::PublishOk { request_id, .. }) => Some(*request_id),
        _ => None,
    }
}

/// A connection to a server that accepts it and does nothing else, for the
/// control loops to run on.
async fn idle_connection() -> Result<QuicConnection> {
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    client.connect(addr, "localhost").await
}

/// A broker holding `t1/default/updates`.
async fn updates_broker() -> Result<Arc<Broker>> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    Ok(broker)
}

/// One pipelining stream's control loop reading `frames`, with no writer:
/// nothing releases its answers, so every slot it takes stays taken until the
/// test hands an answer to the returned order.
fn spawn_pipelining_stream(
    broker: &Arc<Broker>,
    connection: &QuicConnection,
    auth: &AuthFixture,
    publish_ctx: &PublishContext,
    frames: Vec<Frame>,
) -> (
    tokio::task::JoinHandle<Result<bool>>,
    mpsc::Receiver<Outgoing>,
    Arc<AckOrder>,
) {
    let mut source = TestFrameSource::new(frames.into_iter().map(|f| Ok(Some(f))).collect());
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let order = Arc::new(AckOrder::new());
    let loop_order = Arc::clone(&order);
    let broker = Arc::clone(broker);
    let connection = connection.clone();
    let auth = Arc::clone(&auth.auth);
    let publish_ctx = publish_ctx.clone();
    let control = tokio::spawn(async move {
        // Held so the loop's ack waiters have somewhere to go.
        let _ack_waiter_rx = ack_waiter_rx;
        let mut scratch = crate::serving::quic::FrameScratch::new();
        run_control_loop(
            &mut source,
            broker,
            connection,
            BrokerConfig::default(),
            auth,
            publish_ctx,
            HashMap::new(),
            String::new(),
            out_ack_tx,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            ack_throttle_rx,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            cancel_rx,
            Arc::new(Semaphore::new(8)),
            ack_waiter_tx,
            Duration::from_millis(500),
            &mut scratch,
            Default::default(),
            loop_order,
        )
        .await
    });
    (control, out_ack_rx, order)
}

/// Read publish answers off `out_ack_rx` until there are `count`, failing if
/// one takes longer than `wait`. `AuthOk` must grant a window.
async fn read_answers(
    out_ack_rx: &mut mpsc::Receiver<Outgoing>,
    count: usize,
    wait: Duration,
) -> Result<Vec<u64>> {
    let mut answers = Vec::new();
    while answers.len() < count {
        let outgoing = timeout(wait, out_ack_rx.recv())
            .await
            .context("an admitted publish was not answered")?
            .context("control loop ended")?;
        match outgoing {
            Outgoing::Message(Message::AuthOk { publish_window, .. }) => {
                assert!(publish_window.is_some(), "no window granted")
            }
            other => answers.extend(answered(&other)),
        }
    }
    Ok(answers)
}

/// **A full window stops the reads.** With a window of two and no answer
/// ever written, the third publish is not read, let alone answered, until
/// the first answer is released and its slot freed.
#[tokio::test]
async fn a_full_window_stops_reading_until_an_answer_frees_a_slot() -> Result<()> {
    let broker = updates_broker().await?;
    let auth = auth_fixture("t1", default_perms());
    let mut publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    publish_ctx.publish_window = 2;
    let connection = idle_connection().await?;
    let frames = vec![
        frame_from_message(pipelining_auth(&auth)),
        frame_from_message(acked_publish(1)),
        frame_from_message(acked_publish(2)),
        frame_from_message(acked_publish(3)),
    ];
    let (control, mut out_ack_rx, order) =
        spawn_pipelining_stream(&broker, &connection, &auth, &publish_ctx, frames);

    // AuthOk, then the two publishes the window admits.
    assert_eq!(
        read_answers(&mut out_ack_rx, 2, Duration::from_secs(5)).await?,
        vec![1, 2]
    );
    assert!(
        timeout(Duration::from_millis(300), out_ack_rx.recv())
            .await
            .is_err(),
        "a publish past the window was read and answered"
    );

    // What the writer does once it has written the answer to 1.
    let mut ready = Vec::new();
    order.release(
        Outgoing::Message(Message::PublishOk {
            request_id: 1,
            offset: None,
        }),
        &mut ready,
    );
    let third = timeout(Duration::from_secs(5), out_ack_rx.recv())
        .await
        .context("the freed slot did not let the third publish in")?
        .context("control loop ended")?;
    assert_eq!(answered(&third), Some(3));
    let ended = timeout(Duration::from_secs(5), control).await??;
    assert!(ended?, "the stream did not end cleanly");
    Ok(())
}

/// **A stream stuck with a full window does not stall its neighbours.** Two
/// pipelining streams share a connection. One has a full window whose answers
/// never go out, as when its publishes wait on a shard that has stalled; the
/// other's publishes are still read and answered.
#[tokio::test]
async fn a_stuck_stream_does_not_stall_another_on_the_same_connection() -> Result<()> {
    let broker = updates_broker().await?;
    let auth = auth_fixture("t1", default_perms());
    let mut publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    publish_ctx.publish_window = 2;
    let connection = idle_connection().await?;

    let stuck_frames = vec![
        frame_from_message(pipelining_auth(&auth)),
        frame_from_message(acked_publish(1)),
        frame_from_message(acked_publish(2)),
        frame_from_message(acked_publish(3)),
    ];
    let (stuck, mut stuck_rx, _stuck_order) =
        spawn_pipelining_stream(&broker, &connection, &auth, &publish_ctx, stuck_frames);
    assert_eq!(
        read_answers(&mut stuck_rx, 2, Duration::from_secs(5)).await?,
        vec![1, 2]
    );
    assert!(
        timeout(Duration::from_millis(200), stuck_rx.recv())
            .await
            .is_err(),
        "the stuck stream read past its window"
    );

    let healthy_frames = vec![
        frame_from_message(pipelining_auth(&auth)),
        frame_from_message(acked_publish(1)),
        frame_from_message(acked_publish(2)),
    ];
    let (_healthy, mut healthy_rx, _healthy_order) =
        spawn_pipelining_stream(&broker, &connection, &auth, &publish_ctx, healthy_frames);
    assert_eq!(
        read_answers(&mut healthy_rx, 2, Duration::from_secs(2))
            .await
            .context("the stuck stream's window stalled its neighbour")?,
        vec![1, 2]
    );
    stuck.abort();
    Ok(())
}

/// A server that reads one stream to its end and reports every publish it was
/// answered, in the order the bytes arrived.
async fn answers_as_read() -> Result<(QuicConnection, tokio::task::JoinHandle<Result<Vec<u64>>>)> {
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let reader = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (_send, mut recv) = connection.accept_bi().await?;
        let mut scratch = crate::serving::quic::FrameScratch::new();
        let mut ids = Vec::new();
        while let Some(message) =
            crate::serving::quic::read_message_limited(&mut recv, 1 << 20, &mut scratch).await?
        {
            if let Message::PublishOk { request_id, .. } = message {
                ids.push(request_id);
            }
        }
        Ok(ids)
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    Ok((client.connect(addr, "localhost").await?, reader))
}

/// **The writer puts answers back into request order**, whatever order they
/// finish in, and writes anything that is not a publish answer at once.
#[tokio::test]
async fn the_writer_answers_a_pipelining_stream_in_request_order() -> Result<()> {
    let (connection, reader) = answers_as_read().await?;
    let (send, _recv) = connection.open_bi().await?;
    let order = Arc::new(AckOrder::new());
    order.enable();
    for id in 1..=4 {
        order.register(id, None);
    }
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::clone(&order),
        Duration::from_secs(5),
        Arc::new(std::sync::atomic::AtomicUsize::new(4)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    for id in [3, 1, 4, 2] {
        out_ack_tx
            .send(Outgoing::Message(Message::PublishOk {
                request_id: id,
                offset: None,
            }))
            .await?;
    }
    drop(out_ack_tx);
    writer.await?;
    assert_eq!(reader.await??, vec![1, 2, 3, 4]);
    Ok(())
}

/// **An answer that never comes closes the stream** rather than holding the
/// ones behind it forever.
#[tokio::test]
async fn a_lost_answer_closes_a_pipelining_stream() -> Result<()> {
    let (connection, _reader) = answers_as_read().await?;
    let (send, _recv) = connection.open_bi().await?;
    let order = Arc::new(AckOrder::new());
    order.enable();
    order.register(1, None);
    order.register(2, None);
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut cancelled = cancel_tx.subscribe();
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::clone(&order),
        Duration::from_millis(100),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx
        .send(Outgoing::Message(Message::PublishOk {
            request_id: 2,
            offset: None,
        }))
        .await?;
    timeout(Duration::from_secs(5), cancelled.wait_for(|cancel| *cancel))
        .await
        .context("the stalled stream was never closed")??;
    timeout(Duration::from_secs(5), writer).await??;
    Ok(())
}

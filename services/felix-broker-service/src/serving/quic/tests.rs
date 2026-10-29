use std::net::SocketAddr;

use super::*;

#[test]
fn client_transport_config_overrides_windows() {
    let broker_config = BrokerConfig {
        quic_bind: "0.0.0.0:5000".parse::<SocketAddr>().unwrap(),
        metrics_bind: "0.0.0.0:8080".parse::<SocketAddr>().unwrap(),
        controlplane_url: None,
        controlplane_sync_interval_ms: 2000,
        membership: None,
        peer_transport: None,
        ack_on_commit: false,
        ack_durable_on_commit: false,
        max_frame_bytes: 16 * 1024 * 1024,
        publish_queue_wait_timeout_ms: 2000,
        ack_wait_timeout_ms: 2000,
        disable_timings: false,
        control_stream_drain_timeout_ms: 50,
        shutdown_drain_timeout_ms: 25_000,
        pub_conn_recv_window: Some(999),
        pub_stream_recv_window: Some(888),
        cache_send_window: 777,
        event_batch_max_events: 64,
        event_batch_max_bytes: 256 * 1024,
        event_batch_max_delay_us: 250,
        fanout_batch_size: 64,
        pub_workers_per_conn: 4,
        pub_queue_depth: 1024,
        pub_inflight_bytes: 64 * 1024 * 1024,
        pub_conn_inflight_bytes: 16 * 1024 * 1024,
        pub_ingress_wait: false,
        core_shards: 0,
        subscriber_queue_capacity: 128,
        subscriber_queue_capacity_max: 4096,
        max_subscriptions_per_conn: 4096,
        subscriber_queue_policy: felix_broker::SubQueuePolicy::DropNew,
        subscriber_writer_lanes: 4,
        subscriber_lane_queue_depth: 8192,
        subscriber_lane_queue_policy: felix_broker::SubQueuePolicy::Block,
        max_subscriber_writer_lanes: 8,
        subscriber_lane_shard: crate::config::SubscriberLaneShard::Auto,
        subscriber_single_writer_per_conn: true,
        subscriber_flush_max_items: 64,
        subscriber_flush_max_delay_us: 200,
        subscriber_max_bytes_per_write: 256 * 1024,
        sub_streams_per_conn: 4,
        sub_stream_mode: crate::config::SubStreamMode::PerSubscriber,
        ..Default::default()
    };

    let base = TransportConfig::default();
    let result = client_transport_config(&broker_config, base);

    assert_eq!(result.receive_window, 999);
    assert_eq!(result.stream_receive_window, 888);
    assert_eq!(result.send_window, 777);
}

/// The client listeners carry publishes, so by default a connection's
/// receive window matches its publish budget and each stream gets a share of
/// it. The cache-sized 256/64 MiB windows let a connection park far more
/// unread publishes in the broker than ingress backpressure accounts for.
#[test]
fn client_listener_windows_follow_the_publish_budget_by_default() {
    let config = BrokerConfig::default();
    let result = client_transport_config(&config, TransportConfig::default());

    let budget = config.pub_conn_inflight_bytes as u64;
    assert_eq!(result.receive_window, budget);
    assert_eq!(result.stream_receive_window, budget / 4);
}

/// With the default client-listener windows, a stream the broker stops
/// reading is held back by QUIC flow control after its own window, and a
/// second stream on the same connection still gets through. With the stream
/// window equal to the connection window, the stopped stream took all of the
/// connection's credit and the second one never moved.
#[tokio::test]
async fn a_stopped_stream_leaves_credit_for_the_connection_s_other_streams() -> anyhow::Result<()> {
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Context;
    use felix_transport::{QuicClient, QuicServer};
    use rustls::RootCertStore;
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use tokio::time::timeout;

    let config = BrokerConfig::default();
    let transport = client_transport_config(&config, TransportConfig::default());
    let (conn_window, stream_window) = (transport.receive_window, transport.stream_receive_window);

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("server config")?;
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport)?;
    let addr = server.local_addr()?;

    let other_len = 1024 * 1024;
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        // Accepted but never read, like a control stream whose publishes are
        // waiting on a full ingress queue.
        let stopped = connection.accept_bi().await?;
        let (_send, mut recv) = connection.accept_bi().await?;
        let mut body = vec![0u8; other_len];
        recv.read_exact(&mut body)
            .await
            .context("read the other stream")?;
        let _ = stopped_tx.send(());
        drop(stopped);
        anyhow::Ok(connection)
    });

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let client_config = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client_transport = TransportConfig {
        send_window: 4 * conn_window,
        ..TransportConfig::default()
    };
    let client = QuicClient::bind("127.0.0.1:0".parse()?, client_config, client_transport)?;
    let connection = client.connect(addr, "localhost").await?;

    // Fill the stopped stream until flow control holds it.
    let (mut stopped_send, _stopped_recv) = connection.open_bi().await?;
    let chunk = vec![7u8; 64 * 1024];
    let mut sent = 0u64;
    while let Ok(written) = timeout(Duration::from_millis(500), stopped_send.write(&chunk)).await {
        sent += written.context("write the stopped stream")? as u64;
        assert!(
            sent <= stream_window,
            "the broker accepted {sent} unread bytes on one stream, past its {stream_window} window"
        );
    }
    assert!(sent > 0, "the stopped stream sent nothing");

    let (mut other_send, _other_recv) = connection.open_bi().await?;
    timeout(
        Duration::from_secs(5),
        other_send.write_all(&vec![1u8; other_len]),
    )
    .await
    .with_context(|| {
        format!(
            "the other stream stalled: {sent} unread bytes on the stopped stream, \
                 connection window {conn_window}"
        )
    })??;
    timeout(Duration::from_secs(5), stopped_rx)
        .await
        .context("the broker never read the other stream")??;
    let _server_connection = server_task.await??;
    Ok(())
}

//! The control stream's response writer and its ack throttle.

use super::*;
use crate::serving::quic::handlers::publish::AckOrder;

#[tokio::test]
#[serial]
async fn writer_loop_branches() -> Result<()> {
    timings::enable_collection(1);
    timings::set_enabled(true);
    test_hooks::reset();
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        for _ in 0..4 {
            let Ok((_send, mut recv)) = connection.accept_bi().await else {
                break;
            };
            let mut buf = vec![0u8; 1024];
            let _ = recv.read(&mut buf).await;
        }
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, _recv) = connection.open_bi().await?;

    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_throttle_reset(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::new(AckOrder::new()),
        std::time::Duration::from_secs(5),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx
        .send(Outgoing::Message(Message::PublishOk { request_id: 1 }))
        .await?;
    out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    let (send, _recv) = connection.open_bi().await?;
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_write_message_error(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::new(AckOrder::new()),
        std::time::Duration::from_secs(5),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx
        .send(Outgoing::Message(Message::PublishOk { request_id: 2 }))
        .await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    let (send, _recv) = connection.open_bi().await?;
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_cache_encode_error(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::new(AckOrder::new()),
        std::time::Duration::from_secs(5),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    let (send, _recv) = connection.open_bi().await?;
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_write_frame_error(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::new(AckOrder::new()),
        std::time::Duration::from_secs(5),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    server_task.await.context("server task")??;
    Ok(())
}

#[test]
fn should_reset_throttle_true_when_crossing_low_water() {
    assert!(super::super::hooks::should_reset_throttle(Some((
        ACK_HI_WATER,
        ACK_LO_WATER - 1
    ))));
}

#[tokio::test]
#[serial]
async fn writer_loop_cancel_breaks_on_cancel() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (_send, mut recv) = connection.accept_bi().await?;
        let mut buf = vec![0u8; 16];
        let _ = recv.read(&mut buf).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, _recv) = connection.open_bi().await?;

    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::new(AckOrder::new()),
        std::time::Duration::from_secs(5),
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        cancel_tx.clone(),
        cancel_rx,
    ));
    let _ = cancel_tx.send(true);
    writer.await.expect("writer");
    drop(out_ack_tx);

    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn writer_loop_records_timings_when_sampled() -> Result<()> {
    timings::enable_collection(1);
    timings::set_enabled(true);
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (_send, mut recv) = connection.accept_bi().await?;
        let mut buf = vec![0u8; 4096];
        while let Some(n) = recv.read(&mut buf).await? {
            if n == 0 {
                break;
            }
        }
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, _recv) = connection.open_bi().await?;

    let (out_ack_tx, out_ack_rx) = mpsc::channel(256);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Default::default(),
        Arc::new(AckOrder::new()),
        std::time::Duration::from_secs(5),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    for i in 0..50u64 {
        out_ack_tx
            .send(Outgoing::Message(Message::PublishOk { request_id: i }))
            .await?;
    }
    for _ in 0..50u64 {
        out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    }
    drop(out_ack_tx);
    writer.await.expect("writer");

    server_task.await.context("server task")??;
    Ok(())
}

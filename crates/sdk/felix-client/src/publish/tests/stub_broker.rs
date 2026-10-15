//! Publishing through a whole `Client` against a stub broker that answers
//! the way an old or refusing broker would.

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use tokio::time::{Duration, timeout};

use crate::Client;
use crate::frame_io::{read_frame_into, read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env_with_event_pool,
};

#[tokio::test]
#[serial_test::serial]
async fn publish_reports_server_error() -> Result<()> {
    let _env_guard = set_client_env_with_event_pool(0);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let connection = server.accept().await?;
            tasks.push(tokio::spawn(async move {
                let (mut send, mut recv) = connection.accept_bi().await?;
                let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
                let _ = read_message(&mut recv, &mut frame_scratch).await?;
                write_message(&mut send, Message::Ok).await?;
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                if let Some(Message::Publish { request_id, .. }) = next {
                    let id = request_id.unwrap_or(1);
                    write_message(&mut send, Message::publish_error(id, "denied")).await?;
                }
                Ok::<(), anyhow::Error>(())
            }));
        }
        for task in tasks {
            task.await??;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        TransportConfig::default(),
    )
    .await?;

    let publisher = client.publisher().await?;
    let err = publisher
        .publish(
            "t1",
            "default",
            "updates",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("publish error");
    assert!(!err.to_string().is_empty());

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn publish_batch_ack_succeeds() -> Result<()> {
    let _env_guard = set_client_env_with_event_pool(0);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            let next = read_frame_into(&mut recv, &mut frame_scratch, false).await?;
            if next.is_some() {
                write_message(
                    &mut send,
                    Message::PublishOk {
                        request_id: 1,
                        offset: Some(5),
                    },
                )
                .await?;
                let _ = send.finish();
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(())
        }

        let mut tasks = Vec::new();
        let accept_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let now = tokio::time::Instant::now();
            if now >= accept_deadline {
                break;
            }
            let remaining = accept_deadline.saturating_duration_since(now);
            let result = timeout(remaining, server.accept()).await;
            let Ok(Ok(connection)) = result else {
                break;
            };
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        TransportConfig::default(),
    )
    .await?;

    // A broker that never advertised the acked binary frame gets JSON, and
    // the offset in its `publish_ok` comes back all the same.
    let publisher = client.publisher().await?;
    let offset = publisher
        .publish_batch(
            "t1",
            "default",
            "updates",
            vec![b"a".to_vec(), b"b".to_vec()],
            AckMode::PerBatch,
        )
        .await?;
    assert_eq!(offset, Some(5));

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

/// **A plain `Client` keeps one writer per stream.** `HashStream` promises
/// that a stream's publishes share one writer, so an unkeyed and a keyed
/// publish to one stream, pipelined together, arrive on the same QUIC stream.
#[tokio::test]
#[serial_test::serial]
async fn a_plain_client_sends_unkeyed_and_keyed_publishes_on_one_stream() -> Result<()> {
    let _env_guard = set_client_env_with_event_pool(0);
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    // The (connection, stream) each publish arrived on.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let server_seen = std::sync::Arc::clone(&seen);
    let server_task = tokio::spawn(async move {
        while let Ok(connection) = server.accept().await {
            let seen = std::sync::Arc::clone(&server_seen);
            tokio::spawn(async move {
                while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let seen = std::sync::Arc::clone(&seen);
                    let connection_id = connection.info().id;
                    tokio::spawn(async move {
                        let mut scratch = BytesMut::with_capacity(64 * 1024);
                        let _ = read_message(&mut recv, &mut scratch).await?;
                        write_message(&mut send, Message::Ok).await?;
                        while let Some(message) = read_message(&mut recv, &mut scratch).await? {
                            if let Message::PublishBatch {
                                request_id: Some(id),
                                ..
                            } = message
                            {
                                seen.lock()
                                    .expect("seen")
                                    .push((format!("{connection_id:?}"), recv.id()));
                                write_message(
                                    &mut send,
                                    Message::PublishOk {
                                        request_id: id,
                                        offset: None,
                                    },
                                )
                                .await?;
                            }
                        }
                        Ok::<(), anyhow::Error>(())
                    });
                }
            });
        }
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        TransportConfig::default(),
    )
    .await?;
    let publisher = client.publisher().await?;
    let (unkeyed, keyed) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            publisher.publish(
                "t1",
                "default",
                "orders",
                b"a".to_vec(),
                AckMode::PerMessage
            ),
            publisher.publish_keyed(
                "t1",
                "default",
                "orders",
                bytes::Bytes::from_static(b"k"),
                b"b".to_vec(),
                AckMode::PerMessage,
            ),
        )
    })
    .await?;
    unkeyed?;
    keyed?;
    let seen = seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(
        seen[0], seen[1],
        "one stream's publishes went out on two writers"
    );
    server_task.abort();
    Ok(())
}

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

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use tokio::time::{Duration, timeout};
use tracing::debug;

use crate::Client;
use crate::frame_io::{read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env_with_event_pool,
};

#[tokio::test]
#[serial_test::serial]
async fn cache_worker_exits_on_stream_error() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let _env_guard = set_client_env_with_event_pool(0);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<bool> {
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                debug!("test server failed to accept bi stream");
                return Ok(false);
            };
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let auth_msg = read_message(&mut recv, &mut frame_scratch).await;
            debug!(?auth_msg, "test server read auth message");
            let ok_result = write_message(&mut send, Message::Ok).await;
            debug!(?ok_result, "test server sent auth ok");
            let request = timeout(
                Duration::from_millis(200),
                read_message(&mut recv, &mut frame_scratch),
            )
            .await;
            let Ok(Ok(Some(message))) = request else {
                debug!("test server did not receive cache request");
                let _ = send.finish();
                return Ok(false);
            };
            match message {
                Message::CacheGet { .. } | Message::CachePut { .. } => {
                    let write_result =
                        write_message(&mut send, Message::error("cache failure")).await;
                    debug!(?write_result, "test server sent cache error");
                    let _ = send.finish();
                    Ok(true)
                }
                _ => {
                    debug!(?message, "test server received unexpected message");
                    let _ = send.finish();
                    Ok(false)
                }
            }
        }

        let mut tasks: Vec<tokio::task::JoinHandle<Result<bool>>> = Vec::new();
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
            debug!("test server accepted connection");
            tasks.push(tokio::spawn(handle_connection(connection)));
        }

        let mut closed = false;
        for task in tasks {
            if task.await?? {
                closed = true;
            }
        }

        if !closed {
            return Err(anyhow::anyhow!("server did not accept cache stream"));
        }

        Result::<()>::Ok(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        TransportConfig::default(),
    )
    .await?;

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache should fail");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("cache response closed")
            || err_msg.contains("cache worker closed")
            || err_msg.contains("connection lost")
            || err_msg.contains("cache error"),
        "unexpected cache error: {err_msg}"
    );

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache worker should be closed");
    assert!(err.to_string().contains("cache worker closed"));

    server_task.await.context("server task join")??;
    Ok(())
}

/// A refusal the broker answers is a whole answer: the worker keeps its
/// stream, and the next request on it is served. Only one worker, so the
/// second request can only go where the first was refused.
#[tokio::test]
#[serial_test::serial]
async fn cache_worker_keeps_its_stream_after_a_refusal() -> Result<()> {
    let _env_guard = set_client_env_with_event_pool(0);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<usize> {
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(0);
            };
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let _auth = read_message(&mut recv, &mut frame_scratch).await;
            write_message(&mut send, Message::Ok).await?;
            let mut puts = 0;
            while let Ok(Ok(Some(message))) = timeout(
                Duration::from_secs(2),
                read_message(&mut recv, &mut frame_scratch),
            )
            .await
            {
                let Message::CachePut {
                    request_id: Some(request_id),
                    ..
                } = message
                else {
                    break;
                };
                puts += 1;
                let answer = if puts == 1 {
                    Message::Error {
                        message: "this broker is still opening the shard".to_string(),
                        code: Some(felix_wire::ErrorCode::ShardUnavailable),
                        retry: None,
                        detail: None,
                    }
                } else {
                    Message::CacheOk { request_id }
                };
                write_message(&mut send, answer).await?;
                if puts == 2 {
                    break;
                }
            }
            let _ = send.finish();
            // Held open until the client hangs up, so the last answer is not
            // lost with the connection.
            let _ = timeout(
                Duration::from_secs(2),
                read_message(&mut recv, &mut frame_scratch),
            )
            .await;
            Ok(puts)
        }

        let mut tasks = Vec::new();
        while let Ok(Ok(connection)) = timeout(Duration::from_secs(2), server.accept()).await {
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        let mut served = 0;
        for task in tasks {
            served = served.max(task.await??);
        }
        Result::<usize>::Ok(served)
    });

    let mut config = build_client_config_with_overrides(cert, 0)?;
    config.cache_conn_pool = 1;
    config.cache_streams_per_conn = 1;
    let client =
        Client::connect_with_transport(addr, "localhost", config, TransportConfig::default())
            .await?;

    let err = client
        .cache_put("t1", "default", "cache", "key", "v".into(), None)
        .await
        .expect_err("the first put is refused");
    assert!(
        err.downcast_ref::<crate::error::BrokerError>().is_some(),
        "{err:#}"
    );
    client
        .cache_put("t1", "default", "cache", "key", "v".into(), None)
        .await
        .context("the second put on the same worker")?;

    drop(client);
    assert_eq!(server_task.await.context("server task join")??, 2);
    Ok(())
}

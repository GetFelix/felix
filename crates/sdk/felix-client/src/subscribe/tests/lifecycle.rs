use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use tokio::time::{Duration, timeout};

use crate::Client;
use crate::frame_io::{read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env,
};

/// The client no longer generates its own subscription id and no longer requires
/// the server to echo a requested id back. It sends `subscription_id: None` and
/// adopts whatever globally-unique id the broker assigns.
///
/// This matters for correctness, not just tidiness: the client's old per-`Client`
/// counter started at 1 in every instance, so two independent clients against one
/// broker both requested id 1, 2, ... The broker keys its subscription/lane
/// bookkeeping on the requested id, and the client silently drops any event batch
/// whose subscription_id doesn't match its own, so colliding ids made events
/// vanish with no error surfaced anywhere.
#[tokio::test]
#[serial_test::serial]
async fn subscribe_requests_no_id_and_adopts_server_assigned_id() -> Result<()> {
    let _env_guard = set_client_env();

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    // The id the "broker" assigns, deliberately NOT 1, so a client that still
    // generated its own id (and demanded an exact echo) would fail this.
    const SERVER_ASSIGNED_ID: u64 = 4242;

    let (observed_tx, observed_rx) = tokio::sync::oneshot::channel::<Option<u64>>();
    let server_task = tokio::spawn(async move {
        let observed_tx = Arc::new(tokio::sync::Mutex::new(Some(observed_tx)));
        async fn handle_connection(
            connection: felix_transport::QuicConnection,
            observed_tx: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<Option<u64>>>>>,
        ) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            loop {
                let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                    break;
                };
                let _ = read_message(&mut recv, &mut frame_scratch).await?;
                write_message(&mut send, Message::Ok).await?;
                if let Some(Message::Subscribe {
                    subscription_id, ..
                }) = read_message(&mut recv, &mut frame_scratch).await?
                {
                    if let Some(tx) = observed_tx.lock().await.take() {
                        let _ = tx.send(subscription_id);
                    }
                    write_message(
                        &mut send,
                        Message::Subscribed {
                            subscription_id: SERVER_ASSIGNED_ID,
                            start_offset: None,
                            live_offset: None,
                            queue_capacity: None,
                        },
                    )
                    .await?;
                    let mut uni = connection.open_uni().await?;
                    write_message(
                        &mut uni,
                        Message::EventStreamHello {
                            subscription_id: SERVER_ASSIGNED_ID,
                        },
                    )
                    .await?;
                }
                let _ = recv.read_to_end(usize::MAX).await;
                let _ = send.finish();
            }
            Ok(())
        }

        let mut tasks = Vec::new();
        let accept_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < accept_deadline {
            let remaining = accept_deadline - tokio::time::Instant::now();
            let Ok(Ok(connection)) = timeout(remaining, server.accept()).await else {
                break;
            };
            tasks.push(tokio::spawn(handle_connection(
                connection,
                Arc::clone(&observed_tx),
            )));
        }
        for task in tasks {
            drop(task);
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;

    // Succeeding at all is the assertion for id adoption: the server replies with
    // SERVER_ASSIGNED_ID (deliberately != 1) and only hands over an event stream
    // under that id, so a client that still demanded its own requested id back
    // would error out here instead of returning a live subscription.
    let _subscription = timeout(
        Duration::from_secs(5),
        client.subscribe("t1", "default", "updates"),
    )
    .await
    .context("subscribe timed out")?
    .context("subscribe failed")?;

    let observed = timeout(Duration::from_secs(5), observed_rx)
        .await
        .context("did not observe a Subscribe message")??;
    assert_eq!(
        observed, None,
        "client must send subscription_id: None and let the broker assign a globally-unique id"
    );

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_stream_close_returns_none() -> Result<()> {
    let _env_guard = set_client_env();

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
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                                start_offset: None,
                                live_offset: None,
                                queue_capacity: None,
                            },
                        )
                        .await?;
                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
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
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let result = timeout(Duration::from_secs(1), subscription.next_event()).await??;
    assert!(result.is_none());

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

/// A broker that goes away mid-subscription is not a stream that ended: a
/// `while let Some(event) = sub.next_event().await?` loop has to see an error.
#[tokio::test]
#[serial_test::serial]
async fn subscription_connection_loss_is_an_error_not_an_end() -> Result<()> {
    let _env_guard = set_client_env();

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
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                                start_offset: None,
                                live_offset: None,
                                queue_capacity: None,
                            },
                        )
                        .await?;
                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        // Let the subscribe complete, then die rather than end
                        // the stream.
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        connection.close(0u32.into(), b"gone");
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
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
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let err = match timeout(Duration::from_secs(1), subscription.next_event()).await? {
        Ok(event) => panic!(
            "a dropped connection read as {}",
            if event.is_some() {
                "an event"
            } else {
                "a clean end"
            }
        ),
        Err(err) => err,
    };
    assert!(
        err.downcast_ref::<crate::SubscriptionLost>().is_some(),
        "expected SubscriptionLost, got {err:#}"
    );

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_empty_event_batch_returns_none() -> Result<()> {
    let _env_guard = set_client_env();

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
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                                start_offset: None,
                                live_offset: None,
                                queue_capacity: None,
                            },
                        )
                        .await?;

                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        write_message(
                            &mut uni,
                            Message::EventBatch {
                                base_offset: None,
                                tenant_id: "t1".to_string(),
                                namespace: "default".to_string(),
                                stream: "updates".to_string(),
                                payloads: Vec::new(),
                            },
                        )
                        .await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
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
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let value = timeout(Duration::from_secs(1), subscription.next_event()).await??;
    assert!(value.is_none());

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

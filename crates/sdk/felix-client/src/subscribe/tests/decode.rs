use anyhow::Result;
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::{FrameHeader, Message};
use tokio::time::{Duration, timeout};

use crate::Client;
use crate::frame_io::{read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env,
};

#[tokio::test]
#[serial_test::serial]
async fn subscription_binary_batch_mismatched_id_errors() -> Result<()> {
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
                        let sub_id = subscription_id.unwrap_or(7);
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
                        // Encode a binary event batch with the wrong subscription id to force an error.
                        let payloads = vec![Bytes::from_static(b"a")];
                        let encoded =
                            felix_wire::binary::encode_event_batch_bytes(sub_id + 1, &payloads)?;
                        uni.write_all(&encoded).await?;
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
    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected subscription id mismatch"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("subscription id mismatch"));

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_decode_error_on_invalid_frame() -> Result<()> {
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
                        // Send an invalid binary message frame to trigger decode error handling.
                        let header = FrameHeader::new(0, 2);
                        let mut header_bytes = [0u8; FrameHeader::LEN];
                        header.encode_into(&mut header_bytes);
                        uni.write_all(&header_bytes).await?;
                        uni.write_all(&[0, 5]).await?;
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
    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected decode error"),
        Err(err) => err,
    };
    assert!(!err.to_string().is_empty());

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_legacy_event_paths_and_unexpected_message_error() -> Result<()> {
    let _env_guard = set_client_env();
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

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
                            Message::Event {
                                offset: None,
                                tenant_id: "t1".to_string(),
                                namespace: "default".to_string(),
                                stream: "updates".to_string(),
                                payload: b"legacy-one".to_vec(),
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
                                payloads: vec![b"legacy-two".to_vec(), b"legacy-three".to_vec()],
                            },
                        )
                        .await?;
                        // Force the "unexpected message" branch in Subscription::next_event.
                        write_message(&mut uni, Message::Ok).await?;
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

    let first = subscription.next_event().await?.expect("first event");
    assert_eq!(first.payload, Bytes::from_static(b"legacy-one"));

    let second = subscription.next_event().await?.expect("second event");
    assert_eq!(second.payload, Bytes::from_static(b"legacy-two"));

    // This one comes from the cached batch path.
    let third = subscription.next_event().await?.expect("third event");
    assert_eq!(third.payload, Bytes::from_static(b"legacy-three"));

    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected unexpected-message error"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("unexpected message on subscription stream")
    );

    let _ = shutdown_tx.send(());
    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_binary_batch_decode_error_on_invalid_payload() -> Result<()> {
    let _env_guard = set_client_env();
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

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
                        // Set the binary event-batch flag with an invalid payload to force
                        // decode_event_batch failure in Subscription::next_event.
                        let header = FrameHeader::new(felix_wire::FLAG_BINARY_EVENT_BATCH, 2);
                        let mut header_bytes = [0u8; FrameHeader::LEN];
                        header.encode_into(&mut header_bytes);
                        uni.write_all(&header_bytes).await?;
                        uni.write_all(&[0, 0]).await?;
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
    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected binary batch decode error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("decode binary event batch"));

    let _ = shutdown_tx.send(());
    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_binary_batch_success_records_decode_timing() -> Result<()> {
    let _env_guard = set_client_env();
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

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
                        let sub_id = subscription_id.unwrap_or(42);
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
                        let payloads = vec![
                            Bytes::from_static(b"binary-a"),
                            Bytes::from_static(b"binary-b"),
                        ];
                        let encoded =
                            felix_wire::binary::encode_event_batch_bytes(sub_id, &payloads)?;
                        uni.write_all(&encoded).await?;
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
    let first = subscription.next_event().await?.expect("first");
    assert_eq!(first.payload, Bytes::from_static(b"binary-a"));
    let second = subscription.next_event().await?.expect("second");
    assert_eq!(second.payload, Bytes::from_static(b"binary-b"));

    let _ = shutdown_tx.send(());
    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

/// A `shard_moved` frame ends the subscription after the events before it,
/// and says where the shard went.
#[tokio::test]
#[serial_test::serial]
async fn subscription_ends_with_shard_moved_after_its_events() -> Result<()> {
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
            while let Some(message) = read_message(&mut recv, &mut frame_scratch).await? {
                let Message::Subscribe {
                    subscription_id, ..
                } = message
                else {
                    continue;
                };
                let sub_id = subscription_id.unwrap_or(9);
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
                let encoded = felix_wire::binary::encode_event_batch_bytes_with_offset(
                    sub_id,
                    &[Bytes::from_static(b"before-move")],
                    7,
                )?;
                uni.write_all(&encoded).await?;
                write_message(
                    &mut uni,
                    Message::ShardMoved {
                        subscription_id: sub_id,
                        resume_from: Some(8),
                        node_id: Some("node-b".to_string()),
                        addr: Some("127.0.0.1:5001".to_string()),
                        generation: 4,
                    },
                )
                .await?;
                let _ = uni.finish();
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            Ok(())
        }

        let mut tasks = Vec::new();
        let accept_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while let Ok(Ok(connection)) =
            tokio::time::timeout_at(accept_deadline, server.accept()).await
        {
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        let _ = shutdown_rx.await;
        drop(tasks);
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
    let event = timeout(Duration::from_secs(5), subscription.next_event())
        .await??
        .expect("the event before the move");
    assert_eq!(event.payload, Bytes::from_static(b"before-move"));
    assert_eq!(event.offset, Some(7));
    let end = timeout(Duration::from_secs(5), subscription.next_event()).await??;
    assert!(end.is_none(), "the subscription ends after shard_moved");
    assert_eq!(
        subscription.shard_moved(),
        Some(&crate::ShardMoved {
            resume_from: Some(8),
            node_id: Some("node-b".to_string()),
            addr: Some("127.0.0.1:5001".to_string()),
            generation: 4,
        })
    );

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

/// A batch that follows a generation start says how many offsets before it
/// held no event, and only its first event carries the count.
#[tokio::test]
#[serial_test::serial]
async fn a_skip_count_reaches_the_first_event_of_its_batch() -> Result<()> {
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
            while let Some(message) = read_message(&mut recv, &mut frame_scratch).await? {
                let Message::Subscribe {
                    subscription_id, ..
                } = message
                else {
                    continue;
                };
                let sub_id = subscription_id.unwrap_or(9);
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
                let before = felix_wire::binary::encode_event_batch_bytes_with_offset(
                    sub_id,
                    &[Bytes::from_static(b"a")],
                    7,
                )?;
                uni.write_all(&before).await?;
                let after = felix_wire::binary::encode_shared_event_batch_bytes_with_skip(
                    &[Bytes::from_static(b"b"), Bytes::from_static(b"c")],
                    9,
                    1,
                )?;
                uni.write_all(&after).await?;
                let _ = uni.finish();
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            Ok(())
        }

        let mut tasks = Vec::new();
        let accept_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while let Ok(Ok(connection)) =
            tokio::time::timeout_at(accept_deadline, server.accept()).await
        {
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        let _ = shutdown_rx.await;
        drop(tasks);
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
    let mut seen = Vec::new();
    for _ in 0..3 {
        let event = timeout(Duration::from_secs(5), subscription.next_event())
            .await??
            .expect("an event");
        seen.push((event.offset, event.skipped_before));
    }
    // 7 then 9 with one skipped offset between: no drop.
    assert_eq!(seen, vec![(Some(7), 0), (Some(9), 1), (Some(10), 0)]);

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

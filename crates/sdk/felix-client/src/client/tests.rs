use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use tokio::time::{Duration, timeout};

use super::Client;
use crate::frame_io::{read_frame_into, read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env,
};

#[tokio::test]
#[serial_test::serial]
async fn quic_publish_subscribe_cache_success() -> Result<()> {
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

    // This stub deliberately answers `Auth` with a plain `Ok` and never advertises
    // capabilities, so it stands in for a broker that predates negotiation. The
    // client must therefore fall back to the JSON encoding for acked publishes;
    // `binary_acked_seen` proves it did, because the binary branch below is the
    // only thing that could set it.
    let binary_acked_seen = Arc::new(AtomicUsize::new(0));
    let json_acked_seen = Arc::new(AtomicUsize::new(0));
    // The same contract for a keyed publish: it is binary against a broker that
    // advertised 0x0040 and JSON against one that did not.
    let binary_keyed_seen = Arc::new(AtomicUsize::new(0));
    let json_keyed_seen = Arc::new(AtomicUsize::new(0));
    #[derive(Clone)]
    struct StubCounters {
        binary_acked: Arc<AtomicUsize>,
        json_acked: Arc<AtomicUsize>,
        binary_keyed: Arc<AtomicUsize>,
        json_keyed: Arc<AtomicUsize>,
    }
    let counters_task = StubCounters {
        binary_acked: Arc::clone(&binary_acked_seen),
        json_acked: Arc::clone(&json_acked_seen),
        binary_keyed: Arc::clone(&binary_keyed_seen),
        json_keyed: Arc::clone(&json_keyed_seen),
    };

    let server_task = tokio::spawn(async move {
        async fn handle_connection(
            connection: felix_transport::QuicConnection,
            counters: StubCounters,
        ) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            loop {
                let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                    break;
                };
                let auth = read_message(&mut recv, &mut frame_scratch).await?;
                match auth {
                    Some(Message::Auth { .. }) => {
                        write_message(&mut send, Message::Ok).await?;
                    }
                    _ => {
                        write_message(&mut send, Message::error("missing auth")).await?;
                        continue;
                    }
                }
                loop {
                    // Read at frame level: acked publishes now default to the binary
                    // encoding, which is not a JSON `Message` at all, so this stub has
                    // to branch on the flags exactly as the real broker does.
                    let Some(frame) = read_frame_into(&mut recv, &mut frame_scratch, false).await?
                    else {
                        break;
                    };
                    if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_KEYED != 0 {
                        counters
                            .binary_keyed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACKED != 0 {
                        counters
                            .binary_acked
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let batch = felix_wire::binary::decode_acked_publish_batch(&frame)?;
                        send.write_all(&felix_wire::binary::encode_publish_ack_bytes(
                            batch.request_id,
                            None,
                        )?)
                        .await?;
                        continue;
                    }
                    let next = Some(Message::decode(frame)?);
                    match next {
                        Some(Message::Publish {
                            request_id: Some(id),
                            ..
                        }) => {
                            write_message(
                                &mut send,
                                Message::PublishOk {
                                    request_id: id,
                                    offset: None,
                                },
                            )
                            .await?;
                        }
                        Some(Message::PublishBatch {
                            request_id: Some(id),
                            key,
                            ..
                        }) => {
                            let counter = if key.is_some() {
                                &counters.json_keyed
                            } else {
                                &counters.json_acked
                            };
                            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            write_message(
                                &mut send,
                                Message::PublishOk {
                                    request_id: id,
                                    offset: None,
                                },
                            )
                            .await?;
                        }
                        Some(Message::Subscribe {
                            subscription_id, ..
                        }) => {
                            let sub_id = subscription_id.unwrap_or(0);
                            write_message(
                                &mut send,
                                Message::Subscribed {
                                    subscription_id: sub_id,
                                    start_offset: None,
                                    live_offset: None,
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
                                    payloads: vec![b"a".to_vec(), b"b".to_vec()],
                                },
                            )
                            .await?;
                            let _ = uni.finish();
                        }
                        Some(Message::CachePut { request_id, .. }) => {
                            let id = request_id.unwrap_or(1);
                            write_message(&mut send, Message::CacheOk { request_id: id }).await?;
                        }
                        Some(Message::CacheGet { request_id, .. }) => {
                            write_message(
                                &mut send,
                                Message::CacheValue {
                                    tenant_id: "t1".to_string(),
                                    namespace: "default".to_string(),
                                    cache: "cache".to_string(),
                                    key: "key".to_string(),
                                    value: Some(Bytes::from_static(b"value")),
                                    request_id,
                                },
                            )
                            .await?;
                        }
                        None => break,
                        _ => {}
                    }
                }
                let _ = send.finish();
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
            tasks.push(tokio::spawn(handle_connection(
                connection,
                counters_task.clone(),
            )));
        }
        for task in tasks {
            task.await??;
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

    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "updates",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    // The negotiation contract: against a broker that advertises nothing, an
    // acked publish must go out as JSON and must not use the 0x0008 frame that
    // such a broker would misparse.
    use std::sync::atomic::Ordering as AtomicOrdering;
    assert_eq!(
        binary_acked_seen.load(AtomicOrdering::Relaxed),
        0,
        "client sent an acked binary frame to a broker that never advertised support for it"
    );
    assert_eq!(
        json_acked_seen.load(AtomicOrdering::Relaxed),
        1,
        "expected the acked publish to fall back to the JSON encoding"
    );

    publisher
        .publish_keyed(
            "t1",
            "default",
            "updates",
            Bytes::from_static(b"customer-1"),
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!(
        binary_keyed_seen.load(AtomicOrdering::Relaxed),
        0,
        "client sent a keyed binary frame to a broker that never advertised 0x0040; \
         such a broker reads the key prefix as a tenant length"
    );
    assert_eq!(
        json_keyed_seen.load(AtomicOrdering::Relaxed),
        1,
        "expected the keyed publish to fall back to the JSON encoding, key intact"
    );

    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let first = subscription.next_event().await?.expect("event");
    let second = subscription.next_event().await?.expect("event");
    assert_eq!(first.payload, Bytes::from_static(b"a"));
    assert_eq!(second.payload, Bytes::from_static(b"b"));

    client
        .cache_put(
            "t1",
            "default",
            "cache",
            "key",
            Bytes::from_static(b"value"),
            None,
        )
        .await?;
    let value = client
        .cache_get("t1", "default", "cache", "key")
        .await?
        .expect("value");
    assert_eq!(value, Bytes::from_static(b"value"));

    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

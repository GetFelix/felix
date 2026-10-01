//! The checks with hand-built frames over raw QUIC, so they test the wire
//! protocol rather than the reference client's use of it.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use felix_broker_service::serving::quic;
use felix_wire::{AckMode, Message};

use super::MAX_TEST_FRAME_BYTES;
use super::checks::{
    ensure_cache_expired, ensure_cache_value, ensure_event_order, ensure_ok_response,
    ensure_publish_ok, parse_cache_get_response, parse_subscribe_response,
};
use super::fixture::AuthFixture;
use super::frames::{handle_event_frame, read_frame};

pub(crate) async fn run_pubsub(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running pub/sub checks...");
    let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
    let mut frame_scratch = quic::FrameScratch::new();
    quic::write_message(
        &mut sub_send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut sub_recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut sub_send,
        Message::Subscribe {
            start: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "conformance".to_string(),
            subscription_id: None,
            shard: None,
        },
    )
    .await?;
    sub_send.finish()?;
    let response =
        quic::read_message_limited(&mut sub_recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    let expected_id = parse_subscribe_response(response)?;
    let mut event_recv = connection.accept_uni().await?;

    publish(connection, auth, "conformance", b"alpha").await?;
    publish(connection, auth, "conformance", b"beta").await?;

    let mut pending = VecDeque::new();
    if let Some(frame) = read_frame(&mut event_recv).await? {
        handle_event_frame(expected_id, frame, &mut pending, true)?;
    }

    let mut received = Vec::new();
    for _ in 0..2 {
        let payload = if let Some(payload) = pending.pop_front() {
            payload
        } else {
            let frame = read_frame(&mut event_recv)
                .await?
                .ok_or_else(|| anyhow!("subscription ended early"))?;
            handle_event_frame(expected_id, frame, &mut pending, false)?;
            pending
                .pop_front()
                .ok_or_else(|| anyhow!("subscription ended early"))?
        };
        received.push(payload);
    }

    ensure_event_order(&received)?;
    Ok(())
}

pub(crate) async fn run_cache(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running cache checks...");
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut frame_scratch = quic::FrameScratch::new();
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut send,
        Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "conformance-key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: None,
            ttl_ms: Some(100),
        },
    )
    .await?;
    send.finish()?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(response, "cache put")?;

    let value = cache_get(connection, auth, "conformance-key").await?;
    ensure_cache_value(value.clone(), Bytes::from_static(b"value"), "cache get")?;

    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = cache_get(connection, auth, "conformance-key").await?;
    ensure_cache_expired(expired, "cache entry should be expired")?;
    Ok(())
}

pub(crate) async fn publish(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
    stream: &str,
    payload: &[u8],
) -> Result<()> {
    static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut frame_scratch = quic::FrameScratch::new();
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: stream.to_string(),
            payload: payload.to_vec(),
            request_id: Some(request_id),
            ack: Some(AckMode::PerMessage),
            key: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_publish_ok(response, request_id)?;
    Ok(())
}

async fn cache_get(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
    key: &str,
) -> Result<Option<Bytes>> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut frame_scratch = quic::FrameScratch::new();
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Legacy handshake: no capabilities offered, so the broker
            // answers with a plain `Ok`.
            client_flags: None,
            client_features: None,
        },
    )
    .await?;
    let auth_response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    ensure_ok_response(auth_response, "auth")?;
    quic::write_message(
        &mut send,
        Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: key.to_string(),
            request_id: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut frame_scratch).await?;
    parse_cache_get_response(response)
}

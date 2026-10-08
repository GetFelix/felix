//! What a subscriber sees when its shard moves: its last events, then
//! `shard_moved` if it offered `FEATURE_SHARD_MOVED`, then the end of the
//! stream. A client that did not offer the feature must see the stream end
//! exactly as it did before the frame existed.
//!
//! Ending a shard's subscriptions ends every subscriber of that stream, so
//! this runs on a stream of its own.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use felix_broker::{Broker, ShardHandoff};
use felix_broker_service::serving::quic;
use felix_wire::{FLAG_BINARY_EVENT_BATCH, FLAG_BINARY_EVENT_BATCH_SHARED, Message};
use quinn::RecvStream;

use super::MAX_TEST_FRAME_BYTES;
use super::checks::{ensure_ok_response, parse_subscribe_response};
use super::fixture::AuthFixture;
use super::frames::read_frame;
use super::raw::publish;

pub(crate) const MOVED_STREAM: &str = "conformance-moved";

const READ_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn run_shard_move(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
    broker: &Arc<Broker>,
) -> Result<()> {
    println!("Running shard move checks...");
    let (offered_id, mut offered) =
        subscribe(connection, auth, Some(felix_wire::FEATURE_SHARD_MOVED)).await?;
    let (plain_id, mut plain) = subscribe(connection, auth, None).await?;

    publish(connection, auth, MOVED_STREAM, b"before-move").await?;
    let handoff = ShardHandoff {
        node_id: Some("conformance-next".to_string()),
        addr: Some("127.0.0.1:5999".to_string()),
        generation: 3,
    };
    let ended = broker
        .end_subscriptions("t1", "default", MOVED_STREAM, 0, Some(handoff.clone()))
        .await;
    if ended != 2 {
        return Err(anyhow!("ended {ended} subscriptions, expected 2"));
    }

    let expected_moved = Message::ShardMoved {
        subscription_id: offered_id,
        // An in-memory stream: its sequence means nothing on another broker.
        resume_from: None,
        node_id: handoff.node_id,
        addr: handoff.addr,
        generation: handoff.generation,
    };
    ensure_moved_tail(
        &read_to_end(&mut offered, offered_id).await?,
        Some(&expected_moved),
    )
    .context("subscriber that offered FEATURE_SHARD_MOVED")?;
    ensure_moved_tail(&read_to_end(&mut plain, plain_id).await?, None)
        .context("subscriber that did not offer FEATURE_SHARD_MOVED")?;
    Ok(())
}

/// One thing read off an event stream after its hello.
#[derive(Debug, PartialEq)]
pub(crate) enum Received {
    Event(Vec<u8>),
    Message(Message),
}

/// The event, then `shard_moved` when one is expected, then nothing: the
/// stream has already ended by the time this sees it.
pub(crate) fn ensure_moved_tail(received: &[Received], moved: Option<&Message>) -> Result<()> {
    let mut expected = vec![Received::Event(b"before-move".to_vec())];
    if let Some(moved) = moved {
        expected.push(Received::Message(moved.clone()));
    }
    if received != expected.as_slice() {
        return Err(anyhow!(
            "expected {expected:?} then end of stream, got {received:?}"
        ));
    }
    Ok(())
}

async fn subscribe(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
    client_features: Option<u32>,
) -> Result<(u64, RecvStream)> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut scratch = quic::FrameScratch::new();
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            // Features without flags: the broker remembers them and still
            // answers the plain `Ok` a flagless client expects.
            client_flags: None,
            client_features,
            client_features_hi: None,
        },
    )
    .await?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await?;
    ensure_ok_response(response, "auth")?;
    quic::write_message(
        &mut send,
        Message::Subscribe {
            start: None,
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: MOVED_STREAM.to_string(),
            subscription_id: None,
            shard: None,
        },
    )
    .await?;
    send.finish()?;
    let response =
        quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await?;
    let subscription_id = parse_subscribe_response(response)?;

    // Accepted and read before the next subscribe, so each uni stream is
    // matched to its own subscription by the hello.
    let mut events = tokio::time::timeout(READ_TIMEOUT, connection.accept_uni())
        .await
        .context("accept event stream")??;
    let hello = tokio::time::timeout(READ_TIMEOUT, read_frame(&mut events))
        .await
        .context("read hello")??
        .ok_or_else(|| anyhow!("event stream ended before its hello"))?;
    match Message::decode(hello).context("decode hello")? {
        Message::EventStreamHello {
            subscription_id: id,
        } if id == subscription_id => Ok((subscription_id, events)),
        other => Err(anyhow!(
            "expected hello for {subscription_id}, got {other:?}"
        )),
    }
}

async fn read_to_end(events: &mut RecvStream, subscription_id: u64) -> Result<Vec<Received>> {
    let mut received = Vec::new();
    loop {
        let frame = tokio::time::timeout(READ_TIMEOUT, read_frame(events))
            .await
            .context("the event stream did not end")??;
        let Some(frame) = frame else {
            return Ok(received);
        };
        if frame.header.flags & FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
            let batch = felix_wire::binary::decode_shared_event_batch(&frame)?;
            received.extend(batch.payloads.iter().map(|p| Received::Event(p.to_vec())));
        } else if frame.header.flags & FLAG_BINARY_EVENT_BATCH != 0 {
            let batch = felix_wire::binary::decode_event_batch(&frame)?;
            if batch.subscription_id != subscription_id {
                return Err(anyhow!(
                    "batch for subscription {}, expected {subscription_id}",
                    batch.subscription_id
                ));
            }
            received.extend(batch.payloads.iter().map(|p| Received::Event(p.to_vec())));
        } else {
            received.push(Received::Message(Message::decode(frame)?));
        }
    }
}

#[cfg(test)]
mod tests;

//! Writes that name their shard instead of hashing a key.
//!
//! The scenarios are the `explicit_shard.*` entries in `scenarios.toml`. The
//! suite's broker is a single node, so every stream has one shard: shard 0 is
//! in range and anything above it is not.

use anyhow::{Result, bail};
use bytes::Bytes;
use felix_broker_service::serving::quic;
use felix_wire::{ErrorCode, Message, StateChange};

use super::MAX_TEST_FRAME_BYTES;
use super::commit::COMMIT_STREAM;
use super::fixture::AuthFixture;

pub(crate) async fn run_explicit_shard(
    connection: &felix_transport::QuicConnection,
    auth: &AuthFixture,
) -> Result<()> {
    println!("Running explicit shard checks...");
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut scratch = quic::FrameScratch::new();
    let (features, features_hi) = felix_wire::offer_features(
        felix_wire::FEATURE_ERROR_CODES,
        felix_wire::FEATURE_EXPLICIT_SHARD,
    );
    quic::write_message(
        &mut send,
        Message::Auth {
            tenant_id: auth.tenant_id.clone(),
            token: auth.token.clone(),
            client_flags: Some(felix_wire::KNOWN_FLAGS),
            client_features: Some(features),
            client_features_hi: features_hi,
        },
    )
    .await?;
    // explicit_shard.negotiated
    match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
        Some(Message::AuthOk {
            server_features_hi: Some(hi),
            ..
        }) if felix_wire::supports_feature(hi, felix_wire::FEATURE_EXPLICIT_SHARD) => {}
        other => bail!("auth_ok did not advertise FEATURE_EXPLICIT_SHARD: {other:?}"),
    }

    // explicit_shard.commit_lands_on_the_named_shard
    quic::write_message(
        &mut send,
        Message::Commit {
            tenant_id: auth.tenant_id.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            entity_key: Bytes::from_static(b"district-7"),
            event: Bytes::from_static(b"tick"),
            changes: vec![StateChange::Put {
                key: "district-7".to_string(),
                value: Bytes::from_static(b"tick"),
            }],
            request_id: 1,
            expected_offset: None,
            shard: Some(0),
        },
    )
    .await?;
    let offset =
        match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
            Some(Message::CommitOk {
                request_id: 1,
                offset,
            }) => offset,
            other => bail!("a commit to shard 0 was not answered with commit_ok: {other:?}"),
        };
    quic::write_message(
        &mut send,
        Message::StateGet {
            tenant_id: auth.tenant_id.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            entity_key: Bytes::new(),
            key: "district-7".to_string(),
            request_id: 2,
            shard: Some(0),
        },
    )
    .await?;
    match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
        Some(Message::StateValue {
            value: Some(value),
            version: Some(version),
            request_id: 2,
            ..
        }) if value == "tick" && version == offset => {}
        other => bail!("state_get on shard 0 did not return the commit at {offset}: {other:?}"),
    }

    // explicit_shard.out_of_range_is_refused
    quic::write_message(
        &mut send,
        Message::PublishBatch {
            tenant_id: auth.tenant_id.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            payloads: vec![b"lost".to_vec()],
            key: None,
            shard: Some(1),
            request_id: Some(3),
            ack: Some(felix_wire::AckMode::PerBatch),
        },
    )
    .await?;
    match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
        Some(Message::PublishError {
            request_id: 3,
            code: Some(ErrorCode::InvalidRequest),
            ..
        }) => {}
        other => bail!("a publish to shard 1 of a one-shard stream was not refused: {other:?}"),
    }
    quic::write_message(
        &mut send,
        Message::Commit {
            tenant_id: auth.tenant_id.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            entity_key: Bytes::from_static(b"district-7"),
            event: Bytes::from_static(b"lost"),
            changes: Vec::new(),
            request_id: 4,
            expected_offset: Some(offset + 1),
            shard: Some(1),
        },
    )
    .await?;
    match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
        Some(Message::Error {
            code: Some(ErrorCode::InvalidRequest),
            ..
        }) => {}
        other => bail!("a commit to shard 1 of a one-shard stream was not refused: {other:?}"),
    }
    // Nothing the refusals sent was written: the shard's next offset is still
    // the one after the commit.
    quic::write_message(
        &mut send,
        Message::PublishIf {
            tenant_id: auth.tenant_id.clone(),
            namespace: "default".to_string(),
            stream: COMMIT_STREAM.to_string(),
            payloads: vec![b"next".to_vec()],
            key: None,
            shard: Some(0),
            expected_offset: offset + 1,
            request_id: 5,
        },
    )
    .await?;
    match quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES, &mut scratch).await? {
        Some(Message::PublishOk {
            request_id: 5,
            offset: Some(at),
        }) if at == offset + 1 => {}
        other => bail!("a refused write was written after all: {other:?}"),
    }
    send.finish()?;
    Ok(())
}

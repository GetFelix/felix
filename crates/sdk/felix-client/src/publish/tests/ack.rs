use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use quinn::RecvStream;

use crate::publish::ack::{maybe_wait_for_ack, read_ack_message_with_timing, wait_for_ack};
use crate::test_support::{build_server_config, quinn_client_config};

async fn open_ack_stream(
    message: Option<Message>,
) -> Result<(
    RecvStream,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let bytes = message.map(|message| {
        let frame = message.encode().context("encode")?;
        Ok::<_, anyhow::Error>(frame.encode().to_vec())
    });
    open_ack_stream_bytes(bytes.transpose()?).await
}

async fn open_ack_stream_bytes(
    bytes: Option<Vec<u8>>,
) -> Result<(
    RecvStream,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let (server_config, cert_der) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, _recv) = connection.accept_bi().await?;
        if let Some(bytes) = bytes {
            send.write_all(&bytes).await.context("write ack")?;
        }
        send.finish().context("finish ack")?;
        let _ = shutdown_rx.await;
        Result::<()>::Ok(())
    });

    let quinn = quinn_client_config(cert_der)?;
    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, recv) = connection.open_bi().await?;
    send.finish().context("finish client send")?;
    Ok((recv, shutdown_tx, server_task))
}

#[tokio::test]
async fn maybe_wait_for_ack_none_returns_ok() -> Result<()> {
    let (mut recv, shutdown_tx, server_task) = open_ack_stream(None).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    maybe_wait_for_ack(&mut recv, AckMode::None, None, &mut scratch).await?;
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

#[tokio::test]
async fn maybe_wait_for_ack_missing_request_id() -> Result<()> {
    let (mut recv, shutdown_tx, server_task) = open_ack_stream(None).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    assert!(
        maybe_wait_for_ack(&mut recv, AckMode::PerMessage, None, &mut scratch)
            .await
            .is_err()
    );
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

#[tokio::test]
async fn maybe_wait_for_ack_ok() -> Result<()> {
    crate::timings::enable_collection(1);
    let request_id = 42;
    let (mut recv, shutdown_tx, server_task) = open_ack_stream(Some(Message::PublishOk {
        request_id,
        offset: None,
    }))
    .await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    maybe_wait_for_ack(
        &mut recv,
        AckMode::PerMessage,
        Some(request_id),
        &mut scratch,
    )
    .await?;
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

#[tokio::test]
async fn maybe_wait_for_ack_error() -> Result<()> {
    crate::timings::enable_collection(1);
    let request_id = 7;
    let (mut recv, shutdown_tx, server_task) =
        open_ack_stream(Some(Message::publish_error(request_id, "nope"))).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    assert!(
        maybe_wait_for_ack(
            &mut recv,
            AckMode::PerMessage,
            Some(request_id),
            &mut scratch,
        )
        .await
        .is_err()
    );
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

#[tokio::test]
async fn maybe_wait_for_ack_unexpected_message() -> Result<()> {
    crate::timings::enable_collection(1);
    let request_id = 9;
    let (mut recv, shutdown_tx, server_task) = open_ack_stream(Some(Message::PublishOk {
        request_id: 8,
        offset: None,
    }))
    .await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    assert!(
        maybe_wait_for_ack(
            &mut recv,
            AckMode::PerMessage,
            Some(request_id),
            &mut scratch,
        )
        .await
        .is_err()
    );
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

#[tokio::test]
async fn read_ack_message_with_timing_none() -> Result<()> {
    crate::timings::enable_collection(1);
    let (mut recv, shutdown_tx, server_task) = open_ack_stream(None).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let response = read_ack_message_with_timing(
        &mut recv,
        &mut scratch,
        crate::config::DEFAULT_MAX_FRAME_BYTES,
    )
    .await?;
    assert!(response.is_none());
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

#[tokio::test]
async fn read_ack_message_with_timing_decode_error() -> Result<()> {
    crate::timings::enable_collection(1);
    let payload = b"not-json";
    let mut header_bytes = [0u8; felix_wire::FrameHeader::LEN];
    let header = felix_wire::FrameHeader::new(0, payload.len() as u32);
    header.encode_into(&mut header_bytes);
    let mut bytes = Vec::with_capacity(header_bytes.len() + payload.len());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(payload);
    let (mut recv, shutdown_tx, server_task) = open_ack_stream_bytes(Some(bytes)).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    assert!(
        read_ack_message_with_timing(
            &mut recv,
            &mut scratch,
            crate::config::DEFAULT_MAX_FRAME_BYTES,
        )
        .await
        .is_err()
    );
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

/// A coded refusal is typed; the same refusal without a code keeps its text
/// and is not.
#[tokio::test]
async fn a_coded_publish_error_is_a_broker_error() -> Result<()> {
    let request_id = 8;
    let coded = Message::PublishError {
        request_id,
        message: "no majority".to_string(),
        code: Some(felix_wire::ErrorCode::QuorumTimeout),
        retry: Some(felix_wire::RetryClass::OutcomeUnknown),
        detail: None,
    };
    for (message, typed) in [
        (coded, true),
        (Message::publish_error(request_id, "no majority"), false),
    ] {
        let (mut recv, shutdown_tx, server_task) = open_ack_stream(Some(message)).await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        let err = maybe_wait_for_ack(
            &mut recv,
            AckMode::PerMessage,
            Some(request_id),
            &mut scratch,
        )
        .await
        .expect_err("refused");
        assert_eq!(err.to_string(), "publish failed: no majority");
        let broker = err.downcast_ref::<crate::BrokerError>();
        assert_eq!(broker.is_some(), typed);
        if let Some(broker) = broker {
            assert_eq!(broker.code, felix_wire::ErrorCode::QuorumTimeout);
            assert_eq!(broker.retry, felix_wire::RetryClass::OutcomeUnknown);
        }
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
    }
    Ok(())
}

#[tokio::test]
async fn a_binary_ack_carries_its_code_to_the_caller() -> Result<()> {
    let bytes = felix_wire::binary::encode_publish_ack_bytes_coded(
        9,
        Some("shed"),
        Some((
            &felix_wire::ErrorCode::Overloaded,
            felix_wire::RetryClass::RetryAfter,
        )),
        None,
    )?;
    let (mut recv, shutdown_tx, server_task) = open_ack_stream_bytes(Some(bytes.to_vec())).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let err = maybe_wait_for_ack(&mut recv, AckMode::PerMessage, Some(9), &mut scratch)
        .await
        .expect_err("refused");
    let broker = err
        .downcast_ref::<crate::BrokerError>()
        .expect("typed refusal");
    assert_eq!(broker.code, felix_wire::ErrorCode::Overloaded);
    assert_eq!(broker.retry, felix_wire::RetryClass::RetryAfter);
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

/// A binary publisher learns why a shard refused it and how long to wait,
/// which is what `ClusterClient` backs off on.
#[tokio::test]
async fn a_binary_ack_carries_its_detail_to_the_caller() -> Result<()> {
    let bytes = felix_wire::binary::encode_publish_ack_bytes_detailed(
        10,
        Some("publish to t1/ns/orders refused: shard is moving to another broker"),
        Some((
            &felix_wire::ErrorCode::ShardUnavailable,
            felix_wire::RetryClass::Retry,
        )),
        Some(&felix_wire::ErrorDetail {
            reason: Some("moving".to_string()),
            retry_after_ms: Some(100),
        }),
        None,
    )?;
    let (mut recv, shutdown_tx, server_task) = open_ack_stream_bytes(Some(bytes.to_vec())).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let err = maybe_wait_for_ack(&mut recv, AckMode::PerMessage, Some(10), &mut scratch)
        .await
        .expect_err("refused");
    let broker = err
        .downcast_ref::<crate::BrokerError>()
        .expect("typed refusal");
    assert_eq!(broker.code, felix_wire::ErrorCode::ShardUnavailable);
    assert_eq!(broker.reason(), Some("moving"));
    assert_eq!(
        broker.detail.as_ref().and_then(|d| d.retry_after_ms),
        Some(100)
    );
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}

/// The offset a successful ack reports, read off the wire.
async fn acked_offset(bytes: Vec<u8>, request_id: u64) -> Result<Option<u64>> {
    let (mut recv, shutdown_tx, server_task) = open_ack_stream_bytes(Some(bytes)).await?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let acked = wait_for_ack(
        &mut recv,
        request_id,
        &mut scratch,
        crate::config::DEFAULT_MAX_FRAME_BYTES,
    )
    .await??;
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(acked.offset)
}

#[tokio::test]
async fn a_binary_ack_carries_its_offset_to_the_caller() -> Result<()> {
    let at = felix_wire::binary::encode_publish_ack_bytes_at(11, None, None, None, Some(99), None)?;
    assert_eq!(acked_offset(at.to_vec(), 11).await?, Some(99));
    // Without the flag bit there is no offset, rather than a zero.
    let plain = felix_wire::binary::encode_publish_ack_bytes(11, None)?;
    assert_eq!(acked_offset(plain.to_vec(), 11).await?, None);
    Ok(())
}

#[tokio::test]
async fn a_json_publish_ok_carries_its_offset_to_the_caller() -> Result<()> {
    for offset in [Some(12), None] {
        let frame = Message::PublishOk {
            request_id: 3,
            offset,
        }
        .encode()?;
        assert_eq!(acked_offset(frame.encode().to_vec(), 3).await?, offset);
    }
    Ok(())
}

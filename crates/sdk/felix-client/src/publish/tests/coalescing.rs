//! The writer sends what is queued as one write and does not stop writing to
//! wait for acks.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use quinn::{RecvStream, SendStream};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::test_publish_permit;
use crate::config::DEFAULT_MAX_FRAME_BYTES;
use crate::publish::AckOutcome;
use crate::publish::writer::{PublishRequest, WriteStats, run_publisher_writer_counted};
use crate::test_support::{build_server_config, quinn_client_config};

const FRAME_LEN: usize = 10;

/// A connected stream pair: the client halves, and the broker side's halves
/// delivered once the server task has accepted.
async fn connect() -> Result<(
    SendStream,
    RecvStream,
    oneshot::Receiver<(SendStream, RecvStream)>,
)> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let (halves_tx, halves_rx) = oneshot::channel();
    tokio::spawn(async move {
        let connection = server.accept().await?;
        let halves = connection.accept_bi().await?;
        let _ = halves_tx.send(halves);
        // Keep the connection alive until the test ends.
        std::future::pending::<()>().await;
        anyhow::Ok(())
    });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, recv) = connection.open_bi().await?;
    // A stream is only visible to the peer once it carries data.
    send.write_all(b"hello").await?;
    // The connection must outlive this function.
    std::mem::forget(connection);
    std::mem::forget(client);
    Ok((send, recv, halves_rx))
}

fn unacked_frame() -> (PublishRequest, oneshot::Receiver<AckOutcome>) {
    let (response, receiver) = oneshot::channel();
    let request = PublishRequest::BinaryBytes {
        bytes: Bytes::from_static(&[7u8; FRAME_LEN]),
        item_count: 1,
        sample: false,
        declaration: None,
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response,
    };
    (request, receiver)
}

fn acked_batch(request_id: u64) -> (PublishRequest, oneshot::Receiver<AckOutcome>) {
    let (response, receiver) = oneshot::channel();
    let request = PublishRequest::Message {
        message: Message::PublishBatch {
            tenant_id: "t".into(),
            namespace: "ns".into(),
            stream: "s".into(),
            payloads: vec![b"x".to_vec()],
            key: None,
            request_id: Some(request_id),
            ack: Some(AckMode::PerBatch),
        },
        ack: AckMode::PerBatch,
        request_id: Some(request_id),
        _permit: test_publish_permit(),
        response,
    };
    (request, receiver)
}

async fn read_hello(recv: &mut RecvStream) -> Result<()> {
    let mut hello = [0u8; 5];
    recv.read_exact(&mut hello).await.context("read hello")?;
    Ok(())
}

#[tokio::test]
async fn queued_publishes_go_out_in_fewer_writes_than_publishes() -> Result<()> {
    const FRAMES: usize = 32;
    let (send, recv, halves) = connect().await?;
    let (tx, rx) = mpsc::channel(FRAMES + 1);
    // Queue everything before the writer starts, so all of it is waiting
    // when the writer first looks.
    let mut responses = Vec::new();
    for _ in 0..FRAMES {
        let (request, response) = unacked_frame();
        tx.send(request).await.ok().context("queue")?;
        responses.push(response);
    }
    let stats = Arc::new(WriteStats::default());
    let writer = tokio::spawn(run_publisher_writer_counted(
        send,
        recv,
        rx,
        16 * 1024,
        DEFAULT_MAX_FRAME_BYTES,
        None,
        Arc::clone(&stats),
    ));
    for response in responses {
        tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .context("no answer")??
            .context("publish failed")?;
    }
    assert_eq!(stats.frames.load(Ordering::Relaxed), FRAMES as u64);
    let writes = stats.writes.load(Ordering::Relaxed);
    assert!(
        writes < FRAMES as u64,
        "{FRAMES} frames took {writes} writes"
    );

    // All of it arrives, in order and whole.
    let (_server_send, mut server_recv) = halves.await?;
    read_hello(&mut server_recv).await?;
    let mut received = vec![0u8; FRAMES * FRAME_LEN];
    server_recv.read_exact(&mut received).await?;
    assert!(received.iter().all(|byte| *byte == 7));
    writer.abort();
    Ok(())
}

#[tokio::test]
async fn a_lone_publish_is_written_without_waiting_for_company() -> Result<()> {
    let (send, recv, halves) = connect().await?;
    let (tx, rx) = mpsc::channel(8);
    let stats = Arc::new(WriteStats::default());
    let writer = tokio::spawn(run_publisher_writer_counted(
        send,
        recv,
        rx,
        16 * 1024,
        DEFAULT_MAX_FRAME_BYTES,
        None,
        Arc::clone(&stats),
    ));
    let (request, response) = unacked_frame();
    tx.send(request).await.ok().context("queue")?;
    tokio::time::timeout(Duration::from_secs(2), response)
        .await
        .context("a lone publish waited for more")??
        .context("publish failed")?;
    assert_eq!(stats.writes.load(Ordering::Relaxed), 1);
    assert_eq!(stats.frames.load(Ordering::Relaxed), 1);

    let (_server_send, mut server_recv) = halves.await?;
    read_hello(&mut server_recv).await?;
    let mut received = [0u8; FRAME_LEN];
    server_recv.read_exact(&mut received).await?;
    writer.abort();
    Ok(())
}

/// The broker holds its ack for publish 1 until publish 2 has arrived. A
/// writer that settles its acks whenever the queue runs dry would sit on
/// publish 1's ack and never take publish 2.
#[tokio::test]
async fn the_writer_keeps_taking_publishes_while_an_ack_is_outstanding() -> Result<()> {
    let (send, recv, halves) = connect().await?;
    let (tx, rx) = mpsc::channel(8);
    let writer = tokio::spawn(run_publisher_writer_counted(
        send,
        recv,
        rx,
        16 * 1024,
        DEFAULT_MAX_FRAME_BYTES,
        None,
        Arc::default(),
    ));
    let server = tokio::spawn(async move {
        let (mut send, mut recv) = halves.await?;
        read_hello(&mut recv).await?;
        let mut scratch = BytesMut::with_capacity(4096);
        let mut ids = Vec::new();
        for _ in 0..2 {
            let frame = crate::frame_io::read_frame_into_with_limit(
                &mut recv,
                &mut scratch,
                false,
                1 << 20,
            )
            .await?
            .context("stream closed")?;
            match Message::decode(frame)? {
                Message::PublishBatch { request_id, .. } => ids.extend(request_id),
                other => anyhow::bail!("unexpected {other:?}"),
            }
        }
        for request_id in ids {
            let ok = Message::PublishOk {
                request_id,
                offset: None,
            };
            send.write_all(&ok.encode()?.encode()).await?;
        }
        std::future::pending::<()>().await;
        anyhow::Ok(())
    });

    let (first, first_response) = acked_batch(1);
    tx.send(first).await.ok().context("queue")?;
    // Long enough for the writer to find the queue empty with 1 outstanding.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (second, second_response) = acked_batch(2);
    tx.send(second).await.ok().context("queue")?;
    for response in [first_response, second_response] {
        tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .context("the writer stalled behind an ack barrier")??
            .context("publish failed")?;
    }
    server.abort();
    writer.abort();
    Ok(())
}

fn spawn_writer(
    send: SendStream,
    recv: RecvStream,
    rx: mpsc::Receiver<PublishRequest>,
    window: Option<Arc<Semaphore>>,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(run_publisher_writer_counted(
        send,
        recv,
        rx,
        16 * 1024,
        DEFAULT_MAX_FRAME_BYTES,
        window,
        Arc::default(),
    ))
}

/// What a caller sees: the writer's own error, not a dropped channel.
async fn expect_write_error(response: oneshot::Receiver<AckOutcome>) -> Result<()> {
    let answer = tokio::time::timeout(Duration::from_secs(10), response)
        .await
        .context("no answer")?
        .context("the response channel was dropped instead of answered")?;
    answer.expect_err("the publish should have failed");
    Ok(())
}

#[tokio::test]
async fn a_failed_write_answers_every_publish_in_the_burst() -> Result<()> {
    let (mut send, recv, _halves) = connect().await?;
    send.finish()?;
    let (tx, rx) = mpsc::channel(8);
    let mut responses = Vec::new();
    for _ in 0..4 {
        let (request, response) = unacked_frame();
        tx.send(request).await.ok().context("queue")?;
        responses.push(response);
    }
    let writer = spawn_writer(send, recv, rx, None);
    for response in responses {
        expect_write_error(response).await?;
    }
    // The failed writer drains its queue until the senders are gone.
    drop(tx);
    assert!(writer.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn a_publish_waiting_for_a_window_slot_gets_the_write_error() -> Result<()> {
    let (mut send, recv, _halves) = connect().await?;
    send.finish()?;
    let (tx, rx) = mpsc::channel(8);
    let (first, first_response) = acked_batch(1);
    let (second, second_response) = acked_batch(2);
    tx.send(first).await.ok().context("queue")?;
    tx.send(second).await.ok().context("queue")?;
    // One slot: the second publish waits for it with the first still staged,
    // so the writer flushes, and that write fails.
    let writer = spawn_writer(send, recv, rx, Some(Arc::new(Semaphore::new(1))));
    expect_write_error(first_response).await?;
    expect_write_error(second_response).await?;
    // The failed writer drains its queue until the senders are gone.
    drop(tx);
    assert!(writer.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn a_finish_taken_into_a_failing_burst_is_answered() -> Result<()> {
    let (mut send, recv, _halves) = connect().await?;
    send.finish()?;
    let (tx, rx) = mpsc::channel(8);
    let (request, response) = unacked_frame();
    let (finish, finish_response) = oneshot::channel();
    tx.send(request).await.ok().context("queue")?;
    tx.send(PublishRequest::Finish { response: finish })
        .await
        .ok()
        .context("queue")?;
    let writer = spawn_writer(send, recv, rx, None);
    expect_write_error(response).await?;
    expect_write_error(finish_response).await?;
    // The failed writer drains its queue until the senders are gone.
    drop(tx);
    assert!(writer.await?.is_err());
    Ok(())
}

#[tokio::test]
async fn a_finish_in_a_burst_settles_the_publishes_before_it() -> Result<()> {
    let (send, recv, halves) = connect().await?;
    let (tx, rx) = mpsc::channel(8);
    let (request, response) = acked_batch(1);
    let (finish, finish_response) = oneshot::channel();
    tx.send(request).await.ok().context("queue")?;
    tx.send(PublishRequest::Finish { response: finish })
        .await
        .ok()
        .context("queue")?;
    let writer = spawn_writer(send, recv, rx, None);
    let server = tokio::spawn(async move {
        let (mut send, mut recv) = halves.await?;
        read_hello(&mut recv).await?;
        let mut scratch = BytesMut::with_capacity(4096);
        crate::frame_io::read_frame_into_with_limit(&mut recv, &mut scratch, false, 1 << 20)
            .await?
            .context("stream closed")?;
        let ok = Message::PublishOk {
            request_id: 1,
            offset: None,
        };
        send.write_all(&ok.encode()?.encode()).await?;
        send.finish()?;
        let _ = recv.read_to_end(1024).await;
        anyhow::Ok(())
    });
    tokio::time::timeout(Duration::from_secs(10), response)
        .await
        .context("no answer")??
        .context("publish failed")?;
    tokio::time::timeout(Duration::from_secs(10), finish_response)
        .await
        .context("no finish answer")??
        .context("finish failed")?;
    writer.await??;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn out_of_order_acks_in_one_burst_reach_their_own_callers() -> Result<()> {
    let (send, recv, halves) = connect().await?;
    let (tx, rx) = mpsc::channel(8);
    let mut responses = Vec::new();
    for id in 1..=3u64 {
        let (request, response) = acked_batch(id);
        tx.send(request).await.ok().context("queue")?;
        responses.push(response);
    }
    let writer = spawn_writer(send, recv, rx, None);
    let server = tokio::spawn(async move {
        let (mut send, mut recv) = halves.await?;
        read_hello(&mut recv).await?;
        let mut scratch = BytesMut::with_capacity(4096);
        for _ in 0..3 {
            crate::frame_io::read_frame_into_with_limit(&mut recv, &mut scratch, false, 1 << 20)
                .await?
                .context("stream closed")?;
        }
        for answer in [
            Message::PublishOk {
                request_id: 3,
                offset: None,
            },
            Message::publish_error(1, "refused"),
            Message::PublishOk {
                request_id: 2,
                offset: None,
            },
        ] {
            send.write_all(&answer.encode()?.encode()).await?;
        }
        std::future::pending::<()>().await;
        anyhow::Ok(())
    });
    let mut answers = Vec::new();
    for response in responses {
        answers.push(
            tokio::time::timeout(Duration::from_secs(10), response)
                .await
                .context("no answer")??,
        );
    }
    assert!(answers[0].is_err(), "request 1 was refused");
    assert!(answers[1].is_ok());
    assert!(answers[2].is_ok());
    server.abort();
    writer.abort();
    Ok(())
}

/// An ack arrives in two pieces with a new publish queued between them. The
/// writer takes the publish and still finishes reading the ack.
#[tokio::test]
async fn an_ack_half_read_when_a_publish_arrives_is_not_lost() -> Result<()> {
    let (send, recv, halves) = connect().await?;
    let (tx, rx) = mpsc::channel(8);
    let writer = spawn_writer(send, recv, rx, None);
    let (half_written_tx, half_written) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut send, mut recv) = halves.await?;
        read_hello(&mut recv).await?;
        let mut scratch = BytesMut::with_capacity(4096);
        crate::frame_io::read_frame_into_with_limit(&mut recv, &mut scratch, false, 1 << 20)
            .await?
            .context("stream closed")?;
        let first = Message::PublishOk {
            request_id: 1,
            offset: None,
        }
        .encode()?
        .encode();
        send.write_all(&first[..4]).await?;
        let _ = half_written_tx.send(());
        crate::frame_io::read_frame_into_with_limit(&mut recv, &mut scratch, false, 1 << 20)
            .await?
            .context("stream closed")?;
        send.write_all(&first[4..]).await?;
        let second = Message::PublishOk {
            request_id: 2,
            offset: None,
        };
        send.write_all(&second.encode()?.encode()).await?;
        std::future::pending::<()>().await;
        anyhow::Ok(())
    });
    let (first, first_response) = acked_batch(1);
    tx.send(first).await.ok().context("queue")?;
    half_written.await?;
    // The writer is now inside the ack read.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (second, second_response) = acked_batch(2);
    tx.send(second).await.ok().context("queue")?;
    for response in [first_response, second_response] {
        tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .context("no answer")??
            .context("publish failed")?;
    }
    server.abort();
    writer.abort();
    Ok(())
}

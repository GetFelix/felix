use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::test_publish_permit;
use crate::publish::writer::{
    PublishRequest, PublishWorker, drain_publish_queue, finish_publisher_stream,
    run_publisher_writer,
};
use crate::publish::{PublishSharding, Publisher, PublisherInner};
use crate::test_support::{build_server_config, quinn_client_config};

#[tokio::test]
async fn acked_publishes_pipeline_without_waiting_per_ack() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    // The server refuses to ack anything until all three publish frames
    // have arrived. A writer that awaits each ack inline can never send
    // frame 2 before frame 1 is acked, so it deadlocks here (and this
    // test times out); the pipelined writer sends all three back to back.
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let mut scratch = BytesMut::with_capacity(4096);
        let mut request_ids = Vec::new();
        for _ in 0..3 {
            let frame = crate::frame_io::read_frame_into_with_limit(
                &mut recv,
                &mut scratch,
                false,
                1 << 20,
            )
            .await?
            .context("publish stream closed before all frames arrived")?;
            match Message::decode(frame).context("decode publish frame")? {
                Message::PublishBatch { request_id, .. } => {
                    request_ids.push(request_id.context("acked batch missing request_id")?);
                }
                other => anyhow::bail!("unexpected message: {other:?}"),
            }
        }
        for request_id in request_ids {
            let frame = Message::PublishOk {
                request_id,
                offset: None,
            }
            .encode()?;
            send.write_all(&frame.encode()).await.context("write ack")?;
        }
        // Close like the broker does: finish the ack side so the client's
        // EOF drain completes, then wait out the client's own finish.
        send.finish()?;
        let _ = recv.read_to_end(1024).await;
        let _ = send.stopped().await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, recv) = connection.open_bi().await?;
    let (tx, rx) = mpsc::channel(8);
    let worker = tokio::spawn(run_publisher_writer(send, recv, rx, 16 * 1024));

    let admission = Arc::new(Semaphore::new(1 << 20));
    let mut responses = Vec::new();
    for request_id in 1..=3u64 {
        let permit = admission
            .clone()
            .acquire_many_owned(64)
            .await
            .context("admission")?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(PublishRequest::Message {
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
            _permit: permit,
            response: response_tx,
        })
        .await
        .context("queue publish")?;
        responses.push(response_rx);
    }

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for response in responses {
            response.await.context("worker dropped response")??;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("acked publishes did not pipeline (inline ack wait deadlocks this server)")??;

    let (finish_tx, finish_rx) = oneshot::channel();
    tx.send(PublishRequest::Finish {
        response: finish_tx,
    })
    .await
    .context("queue finish")?;
    finish_rx.await.context("finish dropped")??;
    worker.await.context("join worker")??;
    server_task.await.context("join server")??;
    Ok(())
}

/// The broker answers a stream's acked publishes as each completes, so a
/// `Quorum` or forwarded publish can be answered after one sent behind it.
/// Each answer goes to the request it names.
#[tokio::test]
async fn acks_answered_out_of_order_reach_their_own_requests() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let mut scratch = BytesMut::with_capacity(4096);
        for _ in 0..3 {
            crate::frame_io::read_frame_into_with_limit(&mut recv, &mut scratch, false, 1 << 20)
                .await?
                .context("publish stream closed before all frames arrived")?;
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
            send.write_all(&answer.encode()?.encode())
                .await
                .context("write ack")?;
        }
        send.finish()?;
        let _ = recv.read_to_end(1024).await;
        let _ = send.stopped().await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, recv) = connection.open_bi().await?;
    let (tx, rx) = mpsc::channel(8);
    let worker = tokio::spawn(run_publisher_writer(send, recv, rx, 16 * 1024));

    let mut responses = Vec::new();
    for request_id in 1..=3u64 {
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(PublishRequest::Message {
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
            _permit: super::test_publish_permit(),
            response: response_tx,
        })
        .await
        .context("queue publish")?;
        responses.push(response_rx);
    }

    let mut answers = Vec::new();
    for response in responses {
        answers.push(
            tokio::time::timeout(std::time::Duration::from_secs(10), response)
                .await
                .context("no answer")?
                .context("worker dropped response")?,
        );
    }
    let refusal = answers[0].as_ref().expect_err("request 1 was refused");
    assert!(format!("{refusal:#}").contains("refused"), "{refusal:#}");
    assert!(answers[1].is_ok(), "request 2: {:?}", answers[1]);
    assert!(answers[2].is_ok(), "request 3: {:?}", answers[2]);

    let (finish_tx, finish_rx) = oneshot::channel();
    tx.send(PublishRequest::Finish {
        response: finish_tx,
    })
    .await
    .context("queue finish")?;
    finish_rx.await.context("finish dropped")??;
    worker.await.context("join worker")??;
    server_task.await.context("join server")??;
    Ok(())
}

#[tokio::test]
async fn finish_publisher_stream_drains_recv() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let _ = recv.read_to_end(1024).await?;
        send.write_all(b"pong").await?;
        send.finish()?;
        let _ = send.stopped().await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(b"ping").await?;

    finish_publisher_stream(&mut send, &mut recv).await?;
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn finish_skips_finish_request_when_channel_closed() {
    let (tx, rx) = mpsc::channel::<PublishRequest>(1);
    drop(rx);
    let handle = tokio::spawn(async { Ok(()) });
    let publisher = Publisher {
        inner: Arc::new(PublisherInner::new(
            Arc::new(vec![PublishWorker {
                tx,
                handle: tokio::sync::Mutex::new(Some(handle)),
                request_counter: AtomicU64::new(1),
                server_flags: felix_wire::KNOWN_FLAGS,
                publish_window: 0,
            }]),
            PublishSharding::RoundRobin,
        )),
    };
    publisher.finish().await.expect("finish");
}

#[tokio::test]
async fn run_publisher_writer_publish_batch_no_ack_ok() -> Result<()> {
    crate::timings::enable_collection(1);
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        recv.read_to_end(1024 * 1024).await?;
        send.finish()?;
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (client_send, client_recv) = connection.open_bi().await?;
    let (tx, rx) = mpsc::channel(4);
    let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

    let (response_tx, response_rx) = oneshot::channel();
    tx.send(PublishRequest::Message {
        message: Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payloads: vec![b"a".to_vec(), b"b".to_vec()],
            key: None,
            request_id: None,
            ack: Some(AckMode::None),
        },
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response: response_tx,
    })
    .await
    .context("send request")?;

    let response = response_rx.await.context("response dropped");
    drop(tx);
    let writer_result = writer_task.await.context("writer join");
    let _ = shutdown_tx.send(());
    let server_result = server_task.await.context("server join");
    if let Err(err) = server_result {
        return Err(err);
    }
    if let Err(err) = writer_result {
        return Err(err);
    }
    if let Err(err) = response {
        return Err(err.context("publish batch response"));
    }
    writer_result.unwrap()?;
    server_result.unwrap()?;
    Ok(())
}

#[tokio::test]
async fn run_publisher_writer_binary_bytes_success() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        recv.read_to_end(1024 * 1024).await?;
        send.finish()?;
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (client_send, client_recv) = connection.open_bi().await?;
    let (tx, rx) = mpsc::channel(4);
    let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

    let payloads = vec![b"p1".to_vec(), b"p2".to_vec()];
    let (bytes, _) =
        felix_wire::binary::encode_publish_batch_bytes_with_stats("t1", "ns", "s", &payloads)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(PublishRequest::BinaryBytes {
        bytes,
        item_count: payloads.len(),
        sample: false,
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response: response_tx,
    })
    .await
    .context("send binary request")?;

    let response = response_rx.await.context("response dropped");
    drop(tx);
    let writer_result = writer_task.await.context("writer join");
    let _ = shutdown_tx.send(());
    let server_result = server_task.await.context("server join");
    if let Err(err) = server_result {
        return Err(err);
    }
    if let Err(err) = writer_result {
        return Err(err);
    }
    if let Err(err) = response {
        return Err(err.context("binary batch response"));
    }
    writer_result.unwrap()?;
    server_result.unwrap()?;
    Ok(())
}

#[tokio::test]
async fn run_publisher_writer_non_publish_message_ok() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let _ = recv.read_to_end(1024 * 1024).await;
        send.finish().context("finish server send")?;
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, recv) = connection.open_bi().await?;

    let (tx, rx) = mpsc::channel(2);
    let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

    let (resp_tx, resp_rx) = oneshot::channel();
    tx.send(PublishRequest::Message {
        message: Message::error("oops"),
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response: resp_tx,
    })
    .await
    .context("send error message")?;
    drop(tx);

    resp_rx.await.context("response dropped")??;
    writer_task.await.context("writer join")??;
    let _ = shutdown_tx.send(());
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn drain_publish_queue_returns_errors() {
    let (tx, mut rx) = mpsc::channel::<PublishRequest>(4);
    let (resp_tx1, resp_rx1) = oneshot::channel();
    let (resp_tx2, resp_rx2) = oneshot::channel();
    let (resp_tx3, resp_rx3) = oneshot::channel();

    tx.send(PublishRequest::Message {
        message: Message::Publish {
            tenant_id: "t".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payload: vec![],
            key: None,
            request_id: None,
            ack: Some(AckMode::None),
        },
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response: resp_tx1,
    })
    .await
    .expect("send message");
    tx.send(PublishRequest::BinaryBytes {
        bytes: Bytes::from_static(b"bin"),
        item_count: 1,
        sample: false,
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response: resp_tx2,
    })
    .await
    .expect("send binary");
    tx.send(PublishRequest::Finish { response: resp_tx3 })
        .await
        .expect("send finish");
    drop(tx);

    drain_publish_queue(&mut rx, "closed").await;

    let err1 = resp_rx1.await.expect("resp1").expect_err("expected error");
    let err2 = resp_rx2.await.expect("resp2").expect_err("expected error");
    let err3 = resp_rx3.await.expect("resp3").expect_err("expected error");
    assert!(err1.to_string().contains("closed"));
    assert!(err2.to_string().contains("closed"));
    assert!(err3.to_string().contains("closed"));
}

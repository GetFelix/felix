use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use tokio::sync::{mpsc, oneshot};

use super::test_publish_permit;
use crate::publish::writer::{PublishRequest, run_publisher_writer};
use crate::test_support::{build_server_config, quinn_client_config};

#[tokio::test]
async fn run_publisher_writer_error_drains_queue() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let mut scratch = BytesMut::new();
        let frame = crate::frame_io::read_frame_into(&mut recv, &mut scratch, false)
            .await?
            .context("missing publish frame")?;
        let message = Message::decode(frame).context("decode message")?;
        let request_id = match message {
            Message::Publish { request_id, .. } => request_id,
            other => return Err(anyhow::anyhow!("unexpected message: {other:?}")),
        }
        .context("missing request_id")?;
        let ack = Message::publish_error(request_id, "denied");
        let frame = ack.encode().context("encode ack")?;
        send.write_all(&frame.encode()).await.context("write ack")?;
        send.finish().context("finish ack")?;
        let _ = recv.read_to_end(1024 * 1024).await;
        // A pipelined client sends its whole flight (and FIN) up front, so
        // reaching EOF here no longer implies the ack was transmitted.
        // Dropping the connection discards untransmitted data; wait until
        // the client has read the ack stream to completion.
        let _ = send.stopped().await;
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, recv) = connection.open_bi().await?;

    let (tx, rx) = mpsc::channel(4);
    let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

    let (resp_tx1, resp_rx1) = oneshot::channel();
    let (resp_tx2, resp_rx2) = oneshot::channel();
    tx.send(PublishRequest::Message {
        message: Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payload: b"bad".to_vec(),
            key: None,
            request_id: Some(1),
            ack: Some(AckMode::PerMessage),
        },
        ack: AckMode::PerMessage,
        request_id: Some(1),
        _permit: test_publish_permit(),
        response: resp_tx1,
    })
    .await
    .context("send request1")?;
    tx.send(PublishRequest::Message {
        message: Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payload: b"queued".to_vec(),
            key: None,
            request_id: Some(2),
            ack: Some(AckMode::PerMessage),
        },
        ack: AckMode::PerMessage,
        request_id: Some(2),
        _permit: test_publish_permit(),
        response: resp_tx2,
    })
    .await
    .context("send request2")?;
    drop(tx);

    let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
    assert!(!err1.to_string().is_empty());
    let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
    assert!(!err2.to_string().is_empty());

    let writer_err = writer_task.await.context("writer join")?;
    assert!(writer_err.is_err());
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_publisher_writer_ack_mismatch_drains_queue() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    // The connection must outlive the assertions: a pipelined client sends
    // its whole flight (and FIN) up front, so server-side EOF no longer
    // implies the client has consumed the ack, and dropping the connection
    // races the client's read of buffered stream data.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let mut scratch = BytesMut::new();
        let frame = crate::frame_io::read_frame_into(&mut recv, &mut scratch, false)
            .await?
            .context("missing publish frame")?;
        let message = Message::decode(frame).context("decode message")?;
        let request_id = match message {
            Message::Publish { request_id, .. } => request_id,
            other => return Err(anyhow::anyhow!("unexpected message: {other:?}")),
        }
        .context("missing request_id")?;
        // Neither 9 nor 10: an ack for a request nobody is waiting on.
        let ack = Message::PublishOk {
            request_id: request_id + 100,
            offset: None,
        };
        let frame = ack.encode().context("encode ack")?;
        send.write_all(&frame.encode()).await.context("write ack")?;
        send.finish().context("finish ack")?;
        let _ = recv.read_to_end(1024 * 1024).await;
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, recv) = connection.open_bi().await?;

    let (tx, rx) = mpsc::channel(4);
    let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

    let (resp_tx1, resp_rx1) = oneshot::channel();
    let (resp_tx2, resp_rx2) = oneshot::channel();
    tx.send(PublishRequest::Message {
        message: Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payload: b"bad".to_vec(),
            key: None,
            request_id: Some(9),
            ack: Some(AckMode::PerMessage),
        },
        ack: AckMode::PerMessage,
        request_id: Some(9),
        _permit: test_publish_permit(),
        response: resp_tx1,
    })
    .await
    .context("send request1")?;
    tx.send(PublishRequest::Message {
        message: Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payload: b"queued".to_vec(),
            key: None,
            request_id: Some(10),
            ack: Some(AckMode::PerMessage),
        },
        ack: AckMode::PerMessage,
        request_id: Some(10),
        _permit: test_publish_permit(),
        response: resp_tx2,
    })
    .await
    .context("send request2")?;
    drop(tx);

    let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
    assert!(err1.to_string().contains("publish failed"));
    let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
    assert!(err2.to_string().contains("publish failed"));

    let writer_err = writer_task.await.context("writer join")?;
    assert!(writer_err.is_err());
    let _ = shutdown_tx.send(());
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_publisher_writer_binary_bytes_write_error_drains_queue() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (_send, mut recv) = connection.accept_bi().await?;
        let drain_task = tokio::spawn(async move {
            let _ = recv.read_to_end(1024 * 1024).await;
        });
        let _ = shutdown_rx.await;
        drain_task.abort();
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut client_send, client_recv) = connection.open_bi().await?;
    client_send.finish().context("finish client send")?;

    let (tx, rx) = mpsc::channel(4);
    let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

    let payloads = vec![b"p1".to_vec()];
    let (bytes, _) =
        felix_wire::binary::encode_publish_batch_bytes_with_stats("t1", "ns", "s", &payloads)?;
    let (resp_tx1, resp_rx1) = oneshot::channel();
    let (resp_tx2, resp_rx2) = oneshot::channel();
    tx.send(PublishRequest::BinaryBytes {
        bytes,
        item_count: payloads.len(),
        sample: false,
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response: resp_tx1,
    })
    .await
    .context("send binary request")?;
    tx.send(PublishRequest::Finish { response: resp_tx2 })
        .await
        .context("send finish request")?;
    drop(tx);

    let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
    assert!(!err1.to_string().is_empty());
    let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
    assert!(!err2.to_string().is_empty());

    let writer_err = writer_task.await.context("writer join")?;
    assert!(writer_err.is_err());
    let _ = shutdown_tx.send(());
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_publisher_writer_send_closed_drains_queue() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let _ = recv.read_to_end(1024 * 1024).await;
        send.finish().context("finish server send")?;
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut client_send, client_recv) = connection.open_bi().await?;
    client_send.finish().context("finish client send")?;

    let (tx, rx) = mpsc::channel(4);
    let writer_task = tokio::spawn(run_publisher_writer(client_send, client_recv, rx, 1024));

    let (resp_tx1, resp_rx1) = oneshot::channel();
    let (resp_tx2, resp_rx2) = oneshot::channel();
    tx.send(PublishRequest::Message {
        message: Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payload: b"bad".to_vec(),
            key: None,
            request_id: Some(7),
            ack: Some(AckMode::PerMessage),
        },
        ack: AckMode::PerMessage,
        request_id: Some(7),
        _permit: test_publish_permit(),
        response: resp_tx1,
    })
    .await
    .context("send publish request")?;
    tx.send(PublishRequest::Finish { response: resp_tx2 })
        .await
        .context("send finish request")?;
    drop(tx);

    let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
    assert!(!err1.to_string().is_empty());
    let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
    assert!(!err2.to_string().is_empty());

    let writer_err = writer_task.await.context("writer join")?;
    assert!(writer_err.is_err());
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_publisher_writer_publish_batch_error_drains_queue() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (mut send, mut recv) = connection.accept_bi().await?;
        let mut scratch = BytesMut::new();
        let _ = crate::frame_io::read_frame_into(&mut recv, &mut scratch, false)
            .await?
            .context("missing publish batch frame")?;
        let ack = Message::publish_error(99, "denied");
        let frame = ack.encode().context("encode ack")?;
        send.write_all(&frame.encode()).await.context("write ack")?;
        send.finish().context("finish ack")?;
        let _ = recv.read_to_end(1024 * 1024).await;
        Ok::<(), anyhow::Error>(())
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, recv) = connection.open_bi().await?;

    let (tx, rx) = mpsc::channel(4);
    let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

    let (resp_tx1, resp_rx1) = oneshot::channel();
    let (resp_tx2, resp_rx2) = oneshot::channel();
    tx.send(PublishRequest::Message {
        message: Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payloads: vec![b"a".to_vec(), b"b".to_vec()],
            key: None,
            request_id: Some(99),
            ack: Some(AckMode::PerMessage),
        },
        ack: AckMode::PerMessage,
        request_id: Some(99),
        _permit: test_publish_permit(),
        response: resp_tx1,
    })
    .await
    .context("send request1")?;
    tx.send(PublishRequest::Message {
        message: Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "s".to_string(),
            payload: b"queued".to_vec(),
            key: None,
            request_id: Some(100),
            ack: Some(AckMode::PerMessage),
        },
        ack: AckMode::PerMessage,
        request_id: Some(100),
        _permit: test_publish_permit(),
        response: resp_tx2,
    })
    .await
    .context("send request2")?;
    drop(tx);

    let err1 = resp_rx1.await.context("resp1 drop")?.expect_err("err1");
    assert!(!err1.to_string().is_empty());
    let err2 = resp_rx2.await.context("resp2 drop")?.expect_err("err2");
    assert!(!err2.to_string().is_empty());

    let writer_err = writer_task.await.context("writer join")?;
    assert!(writer_err.is_err());
    server_task.await.context("server join")??;
    Ok(())
}

/// A refusal answers one request; the publishes after it on the same stream
/// must still get their own answers, not a copy of the refusal. Checked with
/// and without a code, since a broker that predates codes sends plain text.
#[tokio::test]
async fn a_refused_publish_leaves_the_stream_serving_later_ones() -> Result<()> {
    let coded = |request_id| Message::PublishError {
        request_id,
        message: "stream not found: absent".to_string(),
        code: Some(felix_wire::ErrorCode::NotFound),
        retry: None,
        detail: None,
    };
    let uncoded = |request_id| Message::publish_error(request_id, "stream not found: absent");
    for refusal in [coded as fn(u64) -> Message, uncoded] {
        let (server_config, cert) = build_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let mut scratch = BytesMut::new();
            while let Some(frame) =
                crate::frame_io::read_frame_into(&mut recv, &mut scratch, false).await?
            {
                let (stream, request_id) = match Message::decode(frame).context("decode")? {
                    Message::Publish {
                        stream, request_id, ..
                    } => (stream, request_id.context("missing request_id")?),
                    other => return Err(anyhow::anyhow!("unexpected message: {other:?}")),
                };
                let ack = if stream == "absent" {
                    refusal(request_id)
                } else {
                    Message::PublishOk {
                        request_id,
                        offset: None,
                    }
                };
                let frame = ack.encode().context("encode ack")?;
                send.write_all(&frame.encode()).await.context("write ack")?;
            }
            send.finish().context("finish ack")?;
            let _ = send.stopped().await;
            Ok::<(), anyhow::Error>(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let (send, recv) = connection.open_bi().await?;
        let (tx, rx) = mpsc::channel(4);
        let writer_task = tokio::spawn(run_publisher_writer(send, recv, rx, 1024));

        let publish = |stream: &str, request_id| {
            let (response, answer) = oneshot::channel();
            let request = PublishRequest::Message {
                message: Message::Publish {
                    tenant_id: "t1".to_string(),
                    namespace: "ns".to_string(),
                    stream: stream.to_string(),
                    payload: b"x".to_vec(),
                    key: None,
                    request_id: Some(request_id),
                    ack: Some(AckMode::PerMessage),
                },
                ack: AckMode::PerMessage,
                request_id: Some(request_id),
                _permit: test_publish_permit(),
                response,
            };
            (request, answer)
        };

        let (request, answer) = publish("absent", 1);
        tx.send(request).await.context("send refused publish")?;
        let err = answer
            .await
            .context("refusal dropped")?
            .expect_err("refused");
        assert!(err.to_string().contains("absent"), "{err:#}");

        let (request, answer) = publish("present", 2);
        tx.send(request).await.context("send later publish")?;
        answer
            .await
            .context("later answer dropped")?
            .expect("a publish to another stream was answered with the refusal");

        drop(tx);
        writer_task.await.context("writer join")??;
        server_task.await.context("server join")??;
    }
    Ok(())
}

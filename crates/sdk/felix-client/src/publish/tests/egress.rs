//! Publish streams of one connection are written together, and one stream
//! that cannot write never holds up another.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicClient, QuicConnection, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use tokio::sync::{Notify, mpsc, oneshot};

use super::coalescing::acked_batch;
use super::test_publish_permit;
use crate::config::DEFAULT_MAX_FRAME_BYTES;
use crate::publish::writer::{PublishRequest, run_publisher_writer_counted};
use crate::publish::{AckOutcome, Egress};
use crate::test_support::{build_server_config, quinn_client_config};

/// A broker that counts the frames it reads on every stream and answers the
/// acked ones. The first stream it accepts can be held: never read.
struct Rig {
    connection: QuicConnection,
    frames: Arc<AtomicUsize>,
}

async fn rig(stream_window: Option<u64>, hold_first: Option<Arc<Notify>>) -> Result<Rig> {
    let (server_config, cert) = build_server_config()?;
    let mut transport = TransportConfig::default();
    let mut server_transport = transport.clone();
    if let Some(window) = stream_window {
        server_transport.stream_receive_window = window;
    }
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, server_transport)?;
    let addr = server.local_addr()?;
    let frames = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&frames);
    tokio::spawn(async move {
        let connection = server.accept().await?;
        let mut first = true;
        loop {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let hold = if std::mem::take(&mut first) {
                hold_first.clone()
            } else {
                None
            };
            let counted = Arc::clone(&counted);
            tokio::spawn(async move {
                if let Some(release) = hold {
                    release.notified().await;
                }
                let mut scratch = BytesMut::with_capacity(4096);
                while let Some(frame) = crate::frame_io::read_frame_into_with_limit(
                    &mut recv,
                    &mut scratch,
                    false,
                    1 << 20,
                )
                .await?
                {
                    counted.fetch_add(1, Ordering::SeqCst);
                    if let Message::PublishBatch {
                        request_id: Some(request_id),
                        ..
                    } = Message::decode(frame)?
                    {
                        let ok = Message::PublishOk {
                            request_id,
                            offset: None,
                        };
                        send.write_all(&ok.encode()?.encode()).await?;
                    }
                }
                anyhow::Ok(())
            });
        }
        #[allow(unreachable_code)]
        anyhow::Ok(())
    });
    transport.max_streams = 256;
    let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    std::mem::forget(client);
    Ok(Rig { connection, frames })
}

/// A writer on a new stream of the rig's connection, written through `egress`.
async fn open_writer(
    rig: &Rig,
    egress: &Arc<Egress>,
) -> Result<(
    mpsc::Sender<PublishRequest>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let (send, recv) = rig.connection.open_bi().await?;
    let (tx, rx) = mpsc::channel(8);
    let writer = tokio::spawn(run_publisher_writer_counted(
        egress.attach(send),
        recv,
        rx,
        16 * 1024,
        DEFAULT_MAX_FRAME_BYTES,
        None,
        Arc::default(),
    ));
    Ok((tx, writer))
}

fn small_unacked() -> (PublishRequest, oneshot::Receiver<AckOutcome>) {
    let (response, receiver) = oneshot::channel();
    let request = PublishRequest::Message {
        message: Message::PublishBatch {
            tenant_id: "t".into(),
            namespace: "ns".into(),
            stream: "s".into(),
            payloads: vec![vec![b'x'; 100]],
            key: None,
            request_id: None,
            ack: None,
        },
        ack: AckMode::None,
        request_id: None,
        _permit: test_publish_permit(),
        response,
    };
    (request, receiver)
}

async fn settled(response: oneshot::Receiver<AckOutcome>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), response)
        .await
        .context("no answer")??
        .context("publish failed")?;
    Ok(())
}

/// Datagrams the client sent to publish one small frame on each of `streams`
/// streams of one connection, queued back to back.
async fn datagrams_for_one_frame_per_stream(streams: usize, shared: bool) -> Result<u64> {
    let rig = rig(None, None).await?;
    let shared_egress = Egress::spawn();
    let mut writers = Vec::new();
    for _ in 0..streams {
        let egress = if shared {
            Arc::clone(&shared_egress)
        } else {
            Egress::spawn()
        };
        writers.push(open_writer(&rig, &egress).await?);
    }
    // Let the handshake and stream opens settle so only the burst is counted.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = rig.connection.stats().udp_tx.datagrams;
    let mut responses = Vec::new();
    for (tx, _) in &writers {
        let (request, response) = small_unacked();
        tx.send(request).await.ok().context("queue")?;
        responses.push(response);
    }
    for response in responses {
        settled(response).await?;
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while rig.frames.load(Ordering::SeqCst) < streams {
        anyhow::ensure!(Instant::now() < deadline, "broker never got every frame");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let sent = rig.connection.stats().udp_tx.datagrams - before;
    for (_, writer) in writers {
        writer.abort();
    }
    Ok(sent)
}

/// quinn builds packets in the connection driver, woken by the first stream
/// write. Streams written in one pass of the egress task are in the stream
/// state by the time it runs, and share packets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frames_of_many_streams_share_packets() -> Result<()> {
    const STREAMS: usize = 200;
    let (mut shared, mut separate) = (0, 0);
    // Totals over several runs, so scheduling noise in one does not decide it.
    for _ in 0..5 {
        shared += datagrams_for_one_frame_per_stream(STREAMS, true).await?;
        separate += datagrams_for_one_frame_per_stream(STREAMS, false).await?;
    }
    eprintln!("datagrams for {STREAMS} one-frame streams x5: shared {shared}, separate {separate}");
    assert!(
        shared * 2 <= separate,
        "shared egress sent {shared} datagrams, one egress per stream sent {separate}"
    );
    Ok(())
}

/// One stream's write is blocked on flow control (the broker is not reading
/// it). Another stream on the same connection still writes and is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_blocked_on_flow_control_does_not_hold_up_another() -> Result<()> {
    let release = Arc::new(Notify::new());
    let rig = rig(Some(8 * 1024), Some(Arc::clone(&release))).await?;
    let egress = Egress::spawn();
    let (blocked_tx, blocked_writer) = open_writer(&rig, &egress).await?;
    let (healthy_tx, healthy_writer) = open_writer(&rig, &egress).await?;

    // 256 KiB into a stream whose window is 8 KiB and which nobody reads.
    let (response, blocked_response) = oneshot::channel();
    blocked_tx
        .send(PublishRequest::Message {
            message: Message::PublishBatch {
                tenant_id: "t".into(),
                namespace: "ns".into(),
                stream: "s".into(),
                payloads: vec![vec![b'b'; 256 * 1024]],
                key: None,
                request_id: Some(1),
                ack: Some(AckMode::PerBatch),
            },
            ack: AckMode::PerBatch,
            request_id: Some(1),
            _permit: test_publish_permit(),
            response,
        })
        .await
        .ok()
        .context("queue")?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    for id in 1..=5u64 {
        let (request, response) = acked_batch(id);
        healthy_tx.send(request).await.ok().context("queue")?;
        tokio::time::timeout(Duration::from_secs(2), response)
            .await
            .context("a write behind the blocked stream was held up")???;
    }
    let mut blocked_response = blocked_response;
    assert!(
        blocked_response.try_recv().is_err(),
        "the blocked stream was not blocked"
    );

    release.notify_one();
    settled(blocked_response).await?;
    blocked_writer.abort();
    healthy_writer.abort();
    Ok(())
}

/// A publish on an idle connection is written at once, however many other
/// streams share the egress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_publish_on_an_idle_connection_is_not_delayed() -> Result<()> {
    let rig = rig(None, None).await?;
    let egress = Egress::spawn();
    let mut writers = Vec::new();
    for _ in 0..8 {
        writers.push(open_writer(&rig, &egress).await?);
    }
    let started = Instant::now();
    let (request, response) = small_unacked();
    writers[3].0.send(request).await.ok().context("queue")?;
    settled(response).await?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while rig.frames.load(Ordering::SeqCst) < 1 {
        anyhow::ensure!(Instant::now() < deadline, "broker never got the frame");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(started.elapsed() < Duration::from_millis(500));
    for (_, writer) in writers {
        writer.abort();
    }
    Ok(())
}

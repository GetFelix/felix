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
/// acked ones. The first streams it accepts can be held: not read until released.
struct Rig {
    connection: QuicConnection,
    frames: Arc<AtomicUsize>,
    /// Streams the broker's read of failed, as a reset does.
    read_errors: Arc<AtomicUsize>,
}

async fn rig(stream_window: Option<u64>, hold: Option<(Arc<Notify>, usize)>) -> Result<Rig> {
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
    let read_errors = Arc::new(AtomicUsize::new(0));
    let errored = Arc::clone(&read_errors);
    tokio::spawn(async move {
        let connection = server.accept().await?;
        let mut accepted = 0usize;
        loop {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let hold = hold
                .as_ref()
                .filter(|(_, count)| accepted < *count)
                .map(|(release, _)| Arc::clone(release));
            accepted += 1;
            let counted = Arc::clone(&counted);
            let errored = Arc::clone(&errored);
            tokio::spawn(async move {
                if let Some(release) = hold {
                    release.notified().await;
                }
                let mut scratch = BytesMut::with_capacity(4096);
                loop {
                    let frame = match crate::frame_io::read_frame_into_with_limit(
                        &mut recv,
                        &mut scratch,
                        false,
                        1 << 20,
                    )
                    .await
                    {
                        Ok(Some(frame)) => frame,
                        Ok(None) => break,
                        Err(err) => {
                            errored.fetch_add(1, Ordering::SeqCst);
                            return Err(err);
                        }
                    };
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
    Ok(Rig {
        connection,
        frames,
        read_errors,
    })
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
        shared * 3 <= separate * 2,
        "shared egress sent {shared} datagrams, one egress per stream sent {separate}"
    );
    Ok(())
}

/// One stream's write is blocked on flow control (the broker is not reading
/// it). Another stream on the same connection still writes and is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_blocked_on_flow_control_does_not_hold_up_another() -> Result<()> {
    let release = Arc::new(Notify::new());
    let rig = rig(Some(8 * 1024), Some((Arc::clone(&release), 1))).await?;
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

fn big_acked(request_id: u64, bytes: usize) -> (PublishRequest, oneshot::Receiver<AckOutcome>) {
    let (response, receiver) = oneshot::channel();
    let request = PublishRequest::Message {
        message: Message::PublishBatch {
            tenant_id: "t".into(),
            namespace: "ns".into(),
            stream: "s".into(),
            payloads: vec![vec![b'b'; bytes]],
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

/// Median time of acked publishes sent one at a time on stream 0 of
/// `streams` attached to one egress.
async fn median_round_trip(streams: usize) -> Result<(Duration, Arc<Egress>)> {
    let rig = rig(None, None).await?;
    let egress = Egress::spawn();
    let mut writers = Vec::new();
    for _ in 0..streams {
        writers.push(open_writer(&rig, &egress).await?);
    }
    let mut times = Vec::new();
    for id in 1..=60u64 {
        let (request, response) = acked_batch(id);
        let started = Instant::now();
        writers[0].0.send(request).await.ok().context("queue")?;
        settled(response).await?;
        times.push(started.elapsed());
    }
    for (_, writer) in writers {
        writer.abort();
    }
    times.sort();
    Ok((times[times.len() / 2], egress))
}

/// A lone publish on an idle connection pays one task hop for the egress,
/// not a wait for company.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_publish_costs_no_more_than_a_direct_write() -> Result<()> {
    let (direct, alone) = median_round_trip(1).await?;
    let (relayed, shared) = median_round_trip(8).await?;
    assert_eq!(alone.stats.relayed_writes.load(Ordering::Relaxed), 0);
    assert!(shared.stats.relayed_writes.load(Ordering::Relaxed) > 0);
    eprintln!("median round trip: direct {direct:?}, through the egress {relayed:?}");
    assert!(
        relayed <= direct + Duration::from_millis(3),
        "through the egress {relayed:?}, direct {direct:?}"
    );
    Ok(())
}

/// A stream alone on its connection has nothing to share packets with and
/// is written directly; the second stream turns the egress on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_stream_writes_directly() -> Result<()> {
    let rig = rig(None, None).await?;
    let egress = Egress::spawn();
    let (first, first_writer) = open_writer(&rig, &egress).await?;
    let (request, response) = small_unacked();
    first.send(request).await.ok().context("queue")?;
    settled(response).await?;
    assert_eq!(egress.stats.direct_writes.load(Ordering::Relaxed), 1);
    assert_eq!(egress.stats.relayed_writes.load(Ordering::Relaxed), 0);

    let (second, second_writer) = open_writer(&rig, &egress).await?;
    for tx in [&first, &second] {
        let (request, response) = small_unacked();
        tx.send(request).await.ok().context("queue")?;
        settled(response).await?;
    }
    assert_eq!(egress.stats.relayed_writes.load(Ordering::Relaxed), 2);
    first_writer.abort();
    second_writer.abort();
    Ok(())
}

/// Many streams are blocked on flow control. A healthy stream's writes poll
/// only the healthy stream's write, not every blocked one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_writes_are_not_polled_for_another_streams_write() -> Result<()> {
    const BLOCKED: usize = 40;
    const HEALTHY_PUBLISHES: u64 = 20;
    let release = Arc::new(Notify::new());
    let rig = rig(Some(8 * 1024), Some((Arc::clone(&release), BLOCKED))).await?;
    let egress = Egress::spawn();
    let mut blocked = Vec::new();
    for id in 0..BLOCKED as u64 {
        let (tx, writer) = open_writer(&rig, &egress).await?;
        let (request, response) = big_acked(id + 1, 64 * 1024);
        tx.send(request).await.ok().context("queue")?;
        blocked.push((tx, writer, response));
        // The broker numbers streams by first data, so order them.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (healthy, healthy_writer) = open_writer(&rig, &egress).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = egress.stats.flight_polls.load(Ordering::Relaxed);
    for id in 1..=HEALTHY_PUBLISHES {
        let (request, response) = acked_batch(id);
        healthy.send(request).await.ok().context("queue")?;
        settled(response).await?;
    }
    let polls = egress.stats.flight_polls.load(Ordering::Relaxed) - before;
    eprintln!("{polls} flight polls for {HEALTHY_PUBLISHES} publishes beside {BLOCKED} blocked");
    assert!(
        polls <= HEALTHY_PUBLISHES as usize * 4,
        "{polls} flight polls for {HEALTHY_PUBLISHES} publishes"
    );
    release.notify_waiters();
    for (_, writer, _) in blocked {
        writer.abort();
    }
    healthy_writer.abort();
    Ok(())
}

/// A writer that goes away while its write is blocked resets the stream, so
/// the peer is told and the buffered burst is freed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_dropped_mid_write_resets_its_stream() -> Result<()> {
    let release = Arc::new(Notify::new());
    let rig = rig(Some(8 * 1024), Some((Arc::clone(&release), 1))).await?;
    let egress = Egress::spawn();
    let (tx, writer) = open_writer(&rig, &egress).await?;
    let (other, other_writer) = open_writer(&rig, &egress).await?;
    let (request, _response) = big_acked(1, 256 * 1024);
    tx.send(request).await.ok().context("queue")?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    writer.abort();
    let _ = writer.await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    release.notify_waiters();
    let deadline = Instant::now() + Duration::from_secs(5);
    while rig.read_errors.load(Ordering::SeqCst) == 0 {
        anyhow::ensure!(Instant::now() < deadline, "the broker never saw a reset");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The egress is still serving the others.
    let (request, response) = acked_batch(1);
    other.send(request).await.ok().context("queue")?;
    settled(response).await?;
    other_writer.abort();
    Ok(())
}

/// The connection drops with one write stuck on flow control and others
/// waiting for acks: every caller gets an error, none hangs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_connection_answers_every_waiting_writer() -> Result<()> {
    let release = Arc::new(Notify::new());
    let rig = rig(Some(8 * 1024), Some((release, 6))).await?;
    let egress = Egress::spawn();
    let mut writers = Vec::new();
    let mut responses = Vec::new();
    for index in 0..6u64 {
        let (tx, writer) = open_writer(&rig, &egress).await?;
        // Three stick in the egress; three are written whole and wait for
        // an ack the broker never sends.
        let bytes = if index < 3 { 256 * 1024 } else { 100 };
        let (request, response) = big_acked(index + 1, bytes);
        tx.send(request).await.ok().context("queue")?;
        responses.push(response);
        writers.push((tx, writer));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    rig.connection.close(quinn::VarInt::from_u32(0), b"gone");
    for response in responses {
        tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .context("a writer hung after the connection was lost")?
            .context("the response was dropped")?
            .expect_err("the publish cannot have succeeded");
    }
    for (tx, writer) in writers {
        drop(tx);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), writer)
                .await
                .context("a writer never ended")??
                .is_err()
        );
    }
    Ok(())
}

/// A finish needs no write through the egress, so it is not held up by
/// another stream that is stuck in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finish_is_not_held_up_by_a_stalled_stream() -> Result<()> {
    let release = Arc::new(Notify::new());
    let rig = rig(Some(8 * 1024), Some((release, 1))).await?;
    let egress = Egress::spawn();
    let (stalled, stalled_writer) = open_writer(&rig, &egress).await?;
    let (request, _response) = big_acked(1, 256 * 1024);
    stalled.send(request).await.ok().context("queue")?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (closing, closing_writer) = open_writer(&rig, &egress).await?;
    let (request, response) = acked_batch(1);
    closing.send(request).await.ok().context("queue")?;
    settled(response).await?;
    let (finish, finished) = oneshot::channel();
    closing
        .send(PublishRequest::Finish { response: finish })
        .await
        .ok()
        .context("queue")?;
    tokio::time::timeout(Duration::from_secs(5), finished)
        .await
        .context("the finish was held up")??
        .context("finish failed")?;
    closing_writer.await??;
    stalled_writer.abort();
    Ok(())
}

/// The task ends with the connection's last handle, not at process exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_egress_task_ends_when_its_last_handle_is_dropped() -> Result<()> {
    let rig = rig(None, None).await?;
    let egress = Egress::spawn();
    let stats = Arc::clone(&egress.stats);
    let (tx, writer) = open_writer(&rig, &egress).await?;
    let (other, other_writer) = open_writer(&rig, &egress).await?;
    for tx in [&tx, &other] {
        let (request, response) = small_unacked();
        tx.send(request).await.ok().context("queue")?;
        settled(response).await?;
    }
    drop(egress);
    // The writers' streams still hold it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!stats.finished.load(Ordering::SeqCst));
    for (tx, writer) in [(tx, writer), (other, other_writer)] {
        drop(tx);
        let _ = writer.await?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while !stats.finished.load(Ordering::SeqCst) {
        anyhow::ensure!(Instant::now() < deadline, "the egress task never ended");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

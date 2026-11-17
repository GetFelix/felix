//! A publisher held to a fixed rate, for the tail cells: it must keep its rate
//! whatever the acks do, so the write side and the ack side run apart.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::sleep_until;

use super::{Tally, open, publish_packet};

/// How long after the window the last acks are waited for.
const DRAIN: Duration = Duration::from_secs(10);

struct Acks {
    sent_at: Vec<Option<Instant>>,
    tally: Tally,
}

/// The gap between messages for one connection's share of the rate.
pub(super) fn gap(per_connection_rate: f64) -> Duration {
    Duration::from_secs_f64(1.0 / per_connection_rate.max(f64::MIN_POSITIVE))
}

/// Publish `per_connection_rate` messages a second from `start` for `duration`.
/// A send that falls behind is made up, so the achieved rate stays at the
/// target unless the broker or the window cannot keep up.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publisher(
    addr: String,
    credentials: Option<(String, String)>,
    index: usize,
    topic: String,
    window: usize,
    payload_bytes: usize,
    per_connection_rate: f64,
    start: Instant,
    duration: Duration,
) -> Result<Tally> {
    let stream = open(&addr, credentials.as_ref(), &format!("loadgen-{index}")).await?;
    let (rd, mut wr) = stream.into_split();
    let acks = Arc::new(Mutex::new(Acks {
        sent_at: vec![None; 65536],
        tally: Tally {
            acked: 0,
            samples: crate::stats::Samples::with_capacity(200_000),
        },
    }));
    let acked = Arc::new(AtomicU64::new(0));
    let reader = tokio::spawn(read_acks(rd, acks.clone(), acked.clone()));

    let body = vec![0xa5u8; payload_bytes];
    let step = gap(per_connection_rate);
    let end = start + duration;
    let mut due = start;
    let mut sent = 0u64;
    let mut next_id: u16 = 1;
    let mut buf = Vec::new();
    while due < end {
        sleep_until(due.into()).await;
        while sent - acked.load(Ordering::Acquire) >= window as u64 {
            if reader.is_finished() {
                return Err(reader_error(reader).await);
            }
            sleep_until((Instant::now() + Duration::from_micros(100)).into()).await;
        }
        buf.clear();
        publish_packet(&topic, next_id, &body, &mut buf);
        acks.lock().await.sent_at[next_id as usize] = Some(Instant::now());
        wr.write_all(&buf).await?;
        next_id = if next_id == u16::MAX { 1 } else { next_id + 1 };
        sent += 1;
        due += step;
    }
    let drain_end = Instant::now() + DRAIN;
    while acked.load(Ordering::Acquire) < sent && Instant::now() < drain_end {
        if reader.is_finished() {
            return Err(reader_error(reader).await);
        }
        sleep_until((Instant::now() + Duration::from_millis(5)).into()).await;
    }
    reader.abort();
    let mut state = acks.lock().await;
    Ok(Tally {
        acked: state.tally.acked,
        samples: std::mem::take(&mut state.tally.samples),
    })
}

async fn read_acks(
    mut rd: tokio::net::tcp::OwnedReadHalf,
    acks: Arc<Mutex<Acks>>,
    acked: Arc<AtomicU64>,
) -> Result<()> {
    loop {
        let mut ack = [0u8; 4];
        rd.read_exact(&mut ack).await?;
        if ack[0] != 0x40 {
            bail!("expected PUBACK, got {ack:?}");
        }
        let id = u16::from_be_bytes([ack[2], ack[3]]) as usize;
        let mut state = acks.lock().await;
        if let Some(t0) = state.sent_at[id].take() {
            state.tally.samples.record(t0.elapsed());
        }
        state.tally.acked += 1;
        acked.fetch_add(1, Ordering::Release);
    }
}

async fn reader_error(reader: tokio::task::JoinHandle<Result<()>>) -> anyhow::Error {
    match reader.await {
        Ok(Err(e)) => e.context("reading PUBACKs"),
        Ok(Ok(())) => anyhow::anyhow!("ack reader ended"),
        Err(e) => anyhow::anyhow!("ack reader: {e}"),
    }
}

//! MQTT QoS 1 publish throughput against a mounted stream.
//!
//! Plain MQTT 3.1.1 over TCP, hand-encoded: the broker side must run with
//! `FELIX_MQTT_TLS=false` and `FELIX_MQTT_ANONYMOUS_TENANT`. Each connection
//! keeps `window` publishes unacknowledged, which is what the broker's
//! `FELIX_MQTT_MAX_INFLIGHT` allows, so the latency reported is a window-full
//! one, not an unloaded round trip.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::Common;
use crate::stats::{Samples, report};

/// The MQTT variable-length integer.
pub(super) fn remaining_length(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            return;
        }
    }
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

pub(super) fn connect_packet(client_id: &str) -> Vec<u8> {
    let mut body = Vec::new();
    put_str(&mut body, "MQTT");
    body.extend_from_slice(&[4, 0x02, 0x00, 0x3c]); // 3.1.1, clean session, keepalive 60
    put_str(&mut body, client_id);
    let mut out = vec![0x10];
    remaining_length(body.len(), &mut out);
    out.extend(body);
    out
}

/// A QoS 1 PUBLISH. `id` must not be zero.
pub(super) fn publish_packet(topic: &str, id: u16, payload: &[u8], out: &mut Vec<u8>) {
    out.push(0x32);
    remaining_length(2 + topic.len() + 2 + payload.len(), out);
    put_str(out, topic);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
}

struct Tally {
    acked: u64,
    samples: Samples,
}

async fn publisher(
    addr: String,
    index: usize,
    topic: String,
    window: usize,
    payload_bytes: usize,
    duration: Duration,
) -> Result<Tally> {
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connect {addr}"))?;
    stream.set_nodelay(true)?;
    stream
        .write_all(&connect_packet(&format!("loadgen-{index}")))
        .await?;
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack).await.context("CONNACK")?;
    if connack[0] != 0x20 || connack[3] != 0 {
        bail!("CONNECT refused: {connack:?}");
    }
    let body = vec![0xa5u8; payload_bytes];
    let mut sent_at = vec![None::<Instant>; 65536];
    let mut next_id: u16 = 1;
    let mut inflight = 0usize;
    let mut tally = Tally {
        acked: 0,
        samples: Samples::with_capacity(200_000),
    };
    let end = Instant::now() + duration;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        while inflight < window && Instant::now() < end {
            publish_packet(&topic, next_id, &body, &mut buf);
            sent_at[next_id as usize] = Some(Instant::now());
            next_id = if next_id == u16::MAX { 1 } else { next_id + 1 };
            inflight += 1;
        }
        if !buf.is_empty() {
            stream.write_all(&buf).await?;
        }
        if inflight == 0 {
            return Ok(tally);
        }
        let mut ack = [0u8; 4];
        stream.read_exact(&mut ack).await.context("PUBACK")?;
        if ack[0] != 0x40 {
            bail!("expected PUBACK, got {ack:?}");
        }
        let id = u16::from_be_bytes([ack[2], ack[3]]) as usize;
        if let Some(t0) = sent_at[id].take() {
            tally.samples.record(t0.elapsed());
        }
        inflight -= 1;
        tally.acked += 1;
    }
}

pub(crate) async fn mqtt_publish(
    common: &Common,
    addrs: &[String],
    topic_prefix: &str,
    window: usize,
    duration: Duration,
) -> Result<serde_json::Value> {
    if addrs.is_empty() {
        bail!("mqtt-publish needs --mqtt-addrs");
    }
    let started = Instant::now();
    let mut tasks = Vec::new();
    for i in 0..common.concurrency.max(1) {
        let addr = addrs[i % addrs.len()].clone();
        let topic = format!("{topic_prefix}c{i}");
        tasks.push(tokio::spawn(publisher(
            addr,
            i,
            topic,
            window.max(1),
            common.payload_bytes,
            duration,
        )));
    }
    let mut acked = 0u64;
    let mut samples = Samples::default();
    for task in tasks {
        let tally = task.await.context("publisher task")??;
        acked += tally.acked;
        samples.merge(tally.samples);
    }
    let secs = started.elapsed().as_secs_f64();
    let p = samples.percentiles();
    let msg_s = acked as f64 / secs;
    report!(
        common,
        "mqtt publish: {msg_s:.0} msg/s, ack p50 {} us p99 {} us",
        p.p50_us,
        p.p99_us
    );
    Ok(serde_json::json!({
        "scenario": "mqtt-publish",
        "environment": common.environment,
        "connections": common.concurrency,
        "window": window,
        "payload_bytes": common.payload_bytes,
        "acked_records": acked,
        "duration_s": secs,
        "throughput_msg_s": msg_s,
        "throughput_mb_s": msg_s * common.payload_bytes as f64 / 1_000_000.0,
        "batch_ack_latency_us": { "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us },
    }))
}

#[cfg(test)]
mod tests;

//! Tail subscribers over MQTT: connect, SUBSCRIBE at QoS 1, then count what is
//! delivered inside a fixed window while something else publishes.
//!
//! A broker only serves a SUBSCRIBE for a shard it leads, so `--mqtt-addrs`
//! must name the shard's leader.

use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout_at;

use super::{Common, open, put_str, remaining_length, start_instant};
use crate::stats::report;

/// A SUBSCRIBE for one filter at QoS 1.
pub(super) fn subscribe_packet(id: u16, filter: &str) -> Vec<u8> {
    let mut body = id.to_be_bytes().to_vec();
    put_str(&mut body, filter);
    body.push(1);
    let mut out = vec![0x82];
    remaining_length(body.len(), &mut out);
    out.extend(body);
    out
}

/// What a PUBLISH from the broker carries that the loadgen needs: the packet
/// id to acknowledge, if it is QoS 1, and the payload length.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Delivery {
    pub(super) ack_id: Option<u16>,
    pub(super) payload_len: usize,
}

/// Read a PUBLISH's first byte and body (3.1.1 layout: topic, packet id for
/// QoS above 0, payload).
pub(super) fn parse_publish(first: u8, body: &[u8]) -> Result<Delivery> {
    let qos = (first >> 1) & 3;
    let topic_len = u16::from_be_bytes(
        body.get(..2)
            .context("short PUBLISH")?
            .try_into()
            .expect("two bytes"),
    ) as usize;
    let mut at = 2 + topic_len;
    let ack_id = if qos > 0 {
        let id = body.get(at..at + 2).context("short PUBLISH")?;
        at += 2;
        Some(u16::from_be_bytes([id[0], id[1]]))
    } else {
        None
    };
    let payload = body.get(at..).context("short PUBLISH")?;
    Ok(Delivery {
        ack_id,
        payload_len: payload.len(),
    })
}

/// The next packet: its first byte and body.
pub(super) async fn read_packet(stream: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let first = stream.read_u8().await?;
    let mut len = 0usize;
    for shift in [0, 7, 14, 21] {
        let b = stream.read_u8().await?;
        len |= ((b & 0x7f) as usize) << shift;
        if b & 0x80 == 0 {
            let mut body = vec![0u8; len];
            stream.read_exact(&mut body).await?;
            return Ok((first, body));
        }
    }
    bail!("malformed remaining length")
}

struct Tally {
    delivered: u64,
    bytes: u64,
}

async fn subscriber(
    addr: String,
    credentials: Option<(String, String)>,
    index: usize,
    filter: String,
    start: Instant,
    end: Instant,
) -> Result<Tally> {
    let mut stream = open(&addr, credentials.as_ref(), &format!("loadgen-sub-{index}")).await?;
    stream.write_all(&subscribe_packet(1, &filter)).await?;
    // Anything the broker sends before the SUBACK is not a delivery.
    loop {
        let (first, body) = read_packet(&mut stream).await.context("SUBACK")?;
        if first == 0x90 {
            match body.last() {
                Some(0x80) | None => bail!("SUBSCRIBE to {filter} refused: {body:?}"),
                Some(_) => break,
            }
        }
    }
    let mut tally = Tally {
        delivered: 0,
        bytes: 0,
    };
    loop {
        let packet = match timeout_at(end.into(), read_packet(&mut stream)).await {
            Ok(packet) => packet.context("reading deliveries")?,
            Err(_) => return Ok(tally),
        };
        let (first, body) = packet;
        if first >> 4 != 3 {
            continue;
        }
        let delivery = parse_publish(first, &body)?;
        if let Some(id) = delivery.ack_id {
            stream
                .write_all(&[0x40, 2, (id >> 8) as u8, id as u8])
                .await?;
        }
        let now = Instant::now();
        if now >= start && now < end {
            tally.delivered += 1;
            tally.bytes += delivery.payload_len as u64;
        }
    }
}

/// `common.fanout` subscribers, each on every topic under `topic_prefix`,
/// counting deliveries from `start_at` for `duration`.
pub(crate) async fn mqtt_subscribe(
    common: &Common,
    addrs: &[String],
    topic_prefix: &str,
    duration: Duration,
    start_at: Option<SystemTime>,
) -> Result<serde_json::Value> {
    if addrs.is_empty() {
        bail!("mqtt-subscribe needs --mqtt-addrs");
    }
    let start = start_instant(start_at, "mqtt-subscribe");
    let end = start + duration;
    let filter = format!("{topic_prefix}#");
    let subscribers = common.fanout.max(1);
    let mut tasks = Vec::new();
    for i in 0..subscribers {
        let credentials =
            (!common.token.is_empty()).then(|| (common.tenant.clone(), common.token.clone()));
        tasks.push(tokio::spawn(subscriber(
            addrs[i % addrs.len()].clone(),
            credentials,
            i,
            filter.clone(),
            start,
            end,
        )));
    }
    let (mut delivered, mut bytes) = (0u64, 0u64);
    let (mut min, mut max) = (u64::MAX, 0u64);
    for task in tasks {
        let tally = task.await.context("subscriber task")??;
        delivered += tally.delivered;
        bytes += tally.bytes;
        min = min.min(tally.delivered);
        max = max.max(tally.delivered);
    }
    let secs = duration.as_secs_f64();
    let msg_s = delivered as f64 / secs;
    report!(
        common,
        "mqtt subscribe: {delivered} deliveries to {subscribers} subscribers, {msg_s:.0} msg/s"
    );
    Ok(serde_json::json!({
        "scenario": "mqtt-subscribe",
        "environment": common.environment,
        "fanout": subscribers,
        "duration_s": secs,
        "received": delivered,
        "received_bytes": bytes,
        "received_per_sub_min": min,
        "received_per_sub_max": max,
        "delivered_throughput_msg_s": msg_s,
        "delivered_throughput_mb_s": bytes as f64 / secs / 1_000_000.0,
    }))
}

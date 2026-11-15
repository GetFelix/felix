//! Leader-kill recovery: how long a publisher and a subscriber are cut off.
//!
//! One publisher acks single records back to back and one subscriber follows
//! the live tail. The harness kills the shard's leader while this runs. Both
//! numbers come from this process's own clock, so no cross-machine clock sync
//! is involved.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use felix_wire::AckMode;

use super::connect::cluster;
use super::framing::{payload, read_header};
use super::{Common, start_instant};
use crate::stats::report;

/// What one kill cost, in milliseconds since the run's start.
#[derive(Debug, PartialEq)]
pub(super) struct Outage {
    /// The last ack before the longest silence.
    pub gap_start_ms: f64,
    /// Silence until the first ack after it: leader kill to first commit.
    pub first_commit_ms: f64,
    /// From the same start until the subscriber had a record published after
    /// the recovery. `None` when it never did.
    pub caught_up_ms: Option<f64>,
}

/// Find the longest silence between acks and the subscriber's recovery after
/// it. `acks` and `deliveries` are `(seq, ms)` in time order.
pub(super) fn analyze(acks: &[(u64, f64)], deliveries: &[(u64, f64)]) -> Option<Outage> {
    let (i, gap) = acks
        .windows(2)
        .enumerate()
        .map(|(i, w)| (i, w[1].1 - w[0].1))
        .max_by(|a, b| a.1.total_cmp(&b.1))?;
    let start = acks[i].1;
    let resume_seq = acks[i + 1].0;
    let caught_up_ms = deliveries
        .iter()
        .find(|(seq, ms)| *seq >= resume_seq && *ms >= start)
        .map(|(_, ms)| ms - start);
    Some(Outage {
        gap_start_ms: start,
        first_commit_ms: gap,
        caught_up_ms,
    })
}

pub(crate) async fn failover(
    common: &Common,
    stream: &str,
    duration: Duration,
    start_at: Option<SystemTime>,
) -> Result<serde_json::Value> {
    let epoch = Instant::now();
    let client = Arc::new(cluster(common).await?);

    // A sustained run of acks first, so every broker's routing snapshot has
    // caught up and the measured window opens on a settled cluster.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut consecutive = 0;
    while consecutive < 50 {
        let body = payload(u64::MAX, epoch, common.payload_bytes);
        match client
            .publish(
                &common.tenant,
                &common.namespace,
                stream,
                body,
                AckMode::PerMessage,
            )
            .await
        {
            Ok(_) => consecutive += 1,
            Err(err) if Instant::now() < deadline => {
                consecutive = 0;
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = err;
            }
            Err(err) => return Err(err.context("cluster never became ready to publish")),
        }
    }

    let started = start_instant(start_at, "failover");
    let end = started + duration;
    let deliveries: Arc<Mutex<Vec<(u64, f64)>>> = Arc::default();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let follower = {
        let (client, deliveries, stop) = (
            Arc::clone(&client),
            Arc::clone(&deliveries),
            Arc::clone(&stop),
        );
        let (tenant, ns, stream) = (
            common.tenant.clone(),
            common.namespace.clone(),
            stream.to_string(),
        );
        tokio::spawn(async move {
            // A subscription that ends is replaced at the live tail, the way
            // an application would; the gap it leaves is part of the cost.
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let Ok(mut sub) = client.subscribe(&tenant, &ns, &stream).await else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                while let Ok(Ok(Some(event))) =
                    tokio::time::timeout(Duration::from_secs(10), sub.next_event()).await
                {
                    if let Some((seq, _)) = read_header(&event.payload) {
                        let ms = started.elapsed().as_secs_f64() * 1000.0;
                        deliveries.lock().expect("deliveries").push((seq, ms));
                    }
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            }
        })
    };
    // Let the subscription attach before the first measured publish.
    tokio::time::sleep(Duration::from_millis(500)).await;
    tokio::time::sleep_until(started.into()).await;

    let mut acks: Vec<(u64, f64)> = Vec::new();
    let mut failures = 0u64;
    let mut seq = 0u64;
    while Instant::now() < end {
        seq += 1;
        let body = payload(seq, epoch, common.payload_bytes);
        let sent = client.publish(
            &common.tenant,
            &common.namespace,
            stream,
            body,
            AckMode::PerMessage,
        );
        match tokio::time::timeout(Duration::from_secs(5), sent).await {
            Ok(Ok(_)) => acks.push((seq, started.elapsed().as_secs_f64() * 1000.0)),
            _ => {
                failures += 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    }
    // Give the subscriber time to deliver the tail.
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    follower.abort();
    let deliveries = deliveries.lock().expect("deliveries").clone();
    let outage = analyze(&acks, &deliveries).context("fewer than two acks")?;
    report!(
        common,
        "failover: first commit {:.0} ms after the last ack, subscriber caught up {}",
        outage.first_commit_ms,
        outage
            .caught_up_ms
            .map_or("never".to_string(), |ms| format!("{ms:.0} ms"))
    );
    Ok(serde_json::json!({
        "scenario": "failover",
        "environment": common.environment,
        "stream": stream,
        "duration_s": duration.as_secs_f64(),
        "acked": acks.len(),
        "failed_sends": failures,
        "delivered": deliveries.len(),
        "gap_start_s": outage.gap_start_ms / 1000.0,
        "first_commit_ms": outage.first_commit_ms,
        "caught_up_ms": outage.caught_up_ms,
    }))
}

#[cfg(test)]
mod tests;

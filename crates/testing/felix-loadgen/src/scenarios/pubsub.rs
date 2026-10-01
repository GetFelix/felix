//! Publish/subscribe latency: acknowledgement and publish-to-delivery.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_client::{ClusterClient, Publisher};
use felix_wire::AckMode;

use super::connect::{client, cluster};
use super::framing::{payload, read_header};
use super::{Common, is_retriable_transient};
use crate::stats::{Percentiles, Samples, emit_json, fmt_us};

/// Where single publishes go.
enum Route {
    /// To the shard's owner, as a client following the assignment does. The
    /// latency then does not depend on which broker leads the shard.
    Owner(Box<ClusterClient>),
    /// Through the first broker, which relays to the owner when it is not.
    Entry(Publisher),
}

impl Route {
    fn name(&self) -> &'static str {
        match self {
            Route::Owner(_) => "owner",
            Route::Entry(_) => "entry",
        }
    }

    async fn publish(
        &self,
        common: &Common,
        stream: &str,
        body: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        match self {
            Route::Owner(cluster) => cluster
                .publish(&common.tenant, &common.namespace, stream, body, ack)
                .await
                .map(|_| ()),
            Route::Entry(publisher) => publisher
                .publish(&common.tenant, &common.namespace, stream, body, ack)
                .await
                .map(|_| ()),
        }
    }
}

/// Publish/subscribe: `fanout` subscribers, one publisher, and both latencies
/// that matter — the acknowledgement round trip (batch 1) and the
/// publish-to-delivery path (always).
///
/// Single publishes go to the shard's owner unless `via_entry`; batches always
/// go through the first broker.
pub(crate) async fn pubsub(
    common: &Common,
    stream: &str,
    binary: bool,
    via_entry: bool,
) -> Result<()> {
    let epoch = Instant::now();
    let use_ack = common.batch <= 1 && !binary;

    let publisher_client = client(common, common.brokers[0]).await?;
    let publisher = publisher_client.publisher().await?;
    let route = if via_entry || common.batch > 1 {
        Route::Entry(publisher.clone())
    } else {
        Route::Owner(Box::new(cluster(common).await?))
    };

    // Readiness pre-flight, *before* subscribers exist so its sentinels reach
    // nobody and pollute no count. A broker that does not yet hold the routing
    // snapshot for this stream cannot forward a publish to the owner and
    // answers "stream not found" — and the several brokers learn a fresh
    // stream registration through the control-plane watch at slightly
    // different times, so one success proves only that one path converged.
    // Require a sustained run of them, so every broker's snapshot has caught
    // up before the measured window opens: inside the run, a publish error is
    // an error, never a cold snapshot.
    {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut consecutive = 0;
        while consecutive < 50 {
            match route
                .publish(
                    common,
                    stream,
                    payload(u64::MAX, epoch, common.payload_bytes),
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
    }

    // Subscribers now, each following redirects to the shard's owner, and
    // each at the live tail — so the pre-flight sentinel above is already
    // behind them.
    let subs_cluster = Arc::new(cluster(common).await?);
    let mut receivers = Vec::new();
    for _ in 0..common.fanout.max(1) {
        let subscription = subs_cluster
            .subscribe(&common.tenant, &common.namespace, stream)
            .await
            .context("subscribe")?;
        receivers.push(subscription);
    }
    // What the subscribers cost in connections, summed across brokers.
    let subscriber_connections: usize = subs_cluster
        .connections_per_node()
        .await
        .iter()
        .map(|(_, count)| count)
        .sum();

    let expected_per_sub = (common.warmup + common.total) as u64;
    let delivered = Arc::new(AtomicU64::new(0));
    let mut collectors = Vec::new();
    // The slow subscribers are the highest indices, so index 0 — the one whose
    // delivery latency is sampled — is always healthy.
    let fanout = common.fanout.max(1);
    let slow_from = fanout.saturating_sub(common.slow_subscribers);
    for (index, mut subscription) in receivers.into_iter().enumerate() {
        let delivered = Arc::clone(&delivered);
        let warmup = common.warmup as u64;
        let is_slow = common.slow_subscribers > 0 && index >= slow_from;
        let slow_delay = common.slow_delay;
        collectors.push(tokio::spawn(async move {
            // Sample delivery latency on the first subscriber only; count
            // deliveries on all. Sampling everywhere multiplies memory for a
            // curve that fanout does not change shape of.
            let mut samples = Samples::with_capacity(if index == 0 { 200_000 } else { 0 });
            let mut received = 0u64;
            let mut highest_seq = 0u64;
            while received < expected_per_sub {
                match tokio::time::timeout(Duration::from_secs(30), subscription.next_event()).await
                {
                    Ok(Ok(Some(event))) => {
                        received += 1;
                        delivered.fetch_add(1, Ordering::Relaxed);
                        if let Some((seq, t0)) = read_header(&event.payload) {
                            highest_seq = highest_seq.max(seq);
                            if index == 0 && seq >= warmup {
                                let now = epoch.elapsed().as_nanos() as u64;
                                samples.record(Duration::from_nanos(now.saturating_sub(t0)));
                            }
                        }
                        // A slow subscriber drains behind the publisher; under
                        // DropNew its queue overflows and the broker sheds its
                        // events, which is exactly the isolation being probed.
                        if is_slow && !slow_delay.is_zero() {
                            tokio::time::sleep(slow_delay).await;
                        }
                    }
                    Ok(Ok(None)) | Ok(Err(_)) => break,
                    // Quiet too long: the run is over as far as this
                    // subscriber can tell. `unaccounted` reports the gap.
                    Err(_) => break,
                }
            }
            drop(subscription);
            (samples, received, highest_seq, is_slow)
        }));
    }

    // Publish. Ack latency is per-publish round trip; without acks the loop
    // paces itself only by the client's own backpressure.
    let mut ack_samples = Samples::with_capacity(common.total);
    let mut publish_retries = 0u64;
    let publish_started = Instant::now();
    let mut measured_started = None;
    let mut seq = 0u64;
    let total = common.warmup + common.total;
    while (seq as usize) < total {
        if seq as usize == common.warmup {
            measured_started = Some(Instant::now());
        }
        if common.batch <= 1 {
            let ack = if use_ack {
                AckMode::PerMessage
            } else {
                AckMode::None
            };
            // The latency recorded is the final, successful attempt: a routing
            // transient the instrument had to retry is an artifact of a
            // snapshot swap, not the system's steady-state round trip, so it
            // is excluded from the sample the way a warmup message is — and
            // counted, so the exclusion is visible.
            let started = loop {
                let body = payload(seq, epoch, common.payload_bytes);
                let at = Instant::now();
                match route.publish(common, stream, body, ack).await {
                    Ok(_) => break at,
                    Err(err) if is_retriable_transient(&err) => {
                        publish_retries += 1;
                        if publish_retries > (total as u64).max(1) {
                            return Err(err.context("routing never stabilised"));
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(err) => return Err(err.context(format!("publish seq {seq}"))),
                }
            };
            if use_ack && seq as usize >= common.warmup {
                ack_samples.record(started.elapsed());
            }
            seq += 1;
        } else {
            let count = common.batch.min(total - seq as usize);
            loop {
                let batch: Vec<Vec<u8>> = (0..count)
                    .map(|i| payload(seq + i as u64, epoch, common.payload_bytes))
                    .collect();
                match publisher
                    .publish_batch(
                        &common.tenant,
                        &common.namespace,
                        stream,
                        batch,
                        AckMode::None,
                    )
                    .await
                {
                    Ok(_) => break,
                    Err(err) if is_retriable_transient(&err) => {
                        publish_retries += 1;
                        if publish_retries > (total as u64).max(1) {
                            return Err(err.context("routing never stabilised"));
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(err) => {
                        return Err(err.context(format!("publish batch at seq {seq}")));
                    }
                }
            }
            seq += count as u64;
        }
    }
    let publish_elapsed = publish_started.elapsed();
    let measured_elapsed = measured_started
        .map(|at| at.elapsed())
        .unwrap_or(publish_elapsed);

    let mut delivery_samples = Samples::default();
    let mut received_total = 0u64;
    let mut healthy_received = 0u64;
    let mut slow_received = 0u64;
    for collector in collectors {
        let (samples, received, _highest, is_slow) = collector.await.context("collector")?;
        delivery_samples.merge(samples);
        received_total += received;
        if is_slow {
            slow_received += received;
        } else {
            healthy_received += received;
        }
    }
    let slow_count = common.slow_subscribers.min(fanout);
    let healthy_count = fanout - slow_count;

    let expected_total = expected_per_sub * common.fanout.max(1) as u64;
    let unaccounted = expected_total.saturating_sub(received_total);
    let publish_throughput = total as f64 / publish_elapsed.as_secs_f64();
    let measured_throughput = common.total as f64 / measured_elapsed.as_secs_f64();
    let delivered_throughput = received_total as f64 / publish_elapsed.as_secs_f64();
    let per_sub = delivered_throughput / common.fanout.max(1) as f64;

    let ack = if use_ack {
        Some(ack_samples.percentiles())
    } else {
        None
    };
    let delivery = if delivery_samples.is_empty() {
        None
    } else {
        Some(delivery_samples.percentiles())
    };
    // The latency the matrix files: acknowledgement round trip when the run
    // has one (the request-latency configuration), delivery latency otherwise.
    let headline = ack.or(delivery).unwrap_or(Percentiles {
        p50_us: 0,
        p99_us: 0,
        p999_us: 0,
        max_us: 0,
    });

    // The stdout shape scripts/perf already parses.
    println!(
        "Results (publish n = {}, sampled {}, received {}, unaccounted {}, payload {} bytes, fanout {}, batch {}, binary {})",
        total,
        common.total,
        received_total,
        unaccounted,
        common.payload_bytes,
        common.fanout,
        common.batch,
        binary,
    );
    println!("  delivered total = {received_total}");
    println!("  p50 = {}", fmt_us(headline.p50_us));
    println!("  p99 = {}", fmt_us(headline.p99_us));
    println!("  p999 = {}", fmt_us(headline.p999_us));
    println!("  throughput = {publish_throughput:.1} msg/s");
    println!("  effective throughput = {measured_throughput:.1} msg/s");
    println!("  delivered throughput = {delivered_throughput:.1} msg/s");
    println!("  delivered per-sub throughput = {per_sub:.1} msg/s");
    if common.slow_subscribers > 0 {
        println!(
            "  isolation: {healthy_count} healthy sub(s) got {healthy_received} ({}/sub of {expected_per_sub} expected); {slow_count} slow sub(s) got {slow_received} ({}/sub) — slow-delay {}ms",
            if healthy_count > 0 {
                healthy_received / healthy_count as u64
            } else {
                0
            },
            if slow_count > 0 {
                slow_received / slow_count as u64
            } else {
                0
            },
            common.slow_delay.as_millis(),
        );
    }
    if let (Some(ack), Some(delivery)) = (ack, delivery) {
        println!(
            "  delivery (publish -> subscriber): p50 = {}, p99 = {}, p999 = {} (ack latency above)",
            fmt_us(delivery.p50_us),
            fmt_us(delivery.p99_us),
            fmt_us(delivery.p999_us),
        );
        let _ = ack;
    }

    emit_json(&serde_json::json!({
        "scenario": "pubsub",
        "environment": common.environment,
        "stream": stream,
        "payload_bytes": common.payload_bytes,
        "fanout": common.fanout,
        "batch": common.batch,
        "binary": binary,
        "publish_route": route.name(),
        "warmup": common.warmup,
        "total": common.total,
        "received": received_total,
        "subscriber_connections": subscriber_connections,
        "unaccounted": unaccounted,
        "publish_retries": publish_retries,
        "publish_throughput_msg_s": publish_throughput,
        "effective_throughput_msg_s": measured_throughput,
        "delivered_throughput_msg_s": delivered_throughput,
        "slow_subscribers": common.slow_subscribers,
        "slow_delay_ms": common.slow_delay.as_millis() as u64,
        "healthy_subscribers": healthy_count,
        "healthy_received": healthy_received,
        "slow_received": slow_received,
        "healthy_received_per_sub": if healthy_count > 0 { healthy_received / healthy_count as u64 } else { 0 },
        "slow_received_per_sub": if slow_count > 0 { slow_received / slow_count as u64 } else { 0 },
        "expected_per_sub": expected_per_sub,
        "ack_latency_us": ack.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
            "samples": common.total,
        })),
        "delivery_latency_us": delivery.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
        })),
    }));
    Ok(())
}

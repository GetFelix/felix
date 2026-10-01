//! Aggregate ingest throughput across many publishers.

use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use felix_client::{Client, Publisher};
use felix_wire::AckMode;
use tokio::task::JoinSet;

use super::connect::client;
use super::{Common, is_retriable_transient};
use crate::stats::{Samples, emit_json, fmt_us};

/// The `ingest`-only flags.
#[derive(Default)]
pub(crate) struct IngestOptions {
    /// Distinct routing keys; 0 publishes unkeyed.
    pub(crate) keys: usize,
    /// Acked batches each publisher keeps outstanding; 0 is fire-and-forget.
    pub(crate) in_flight: usize,
    /// Publish for this long instead of a fixed `--total`.
    pub(crate) duration: Option<Duration>,
    /// Start publishing at this wall-clock time, so generators on separate
    /// machines overlap for the whole run instead of starting seconds apart.
    pub(crate) start_at: Option<SystemTime>,
}

/// Where an ingest publisher writes.
#[derive(Clone)]
struct Target {
    tenant: String,
    namespace: String,
    stream: String,
}

/// Aggregate ingest ceiling — the write throughput measured the way a
/// multi-partition system quotes it. `concurrency` publishers, each on its own
/// connection spread across the brokers, hammer `stream` (give it as many
/// shards as brokers, or more) with fire-and-forget **binary** batches and no
/// subscribers, so nothing on the delivery side can false-bottleneck the
/// number. Reports aggregate msg/s and MB/s, and the per-publisher figure so
/// scaling is visible. A single publisher on one shard is the least-parallel
/// configuration possible; this is the opposite, and it is what actually
/// stresses the brokers.
///
/// `keys` spreads batches over that many routing keys. At 0 the batches are
/// unkeyed, and a record with no key resolves to shard 0 -- so a 12-shard
/// stream is exercised as a single log, which is how every multi-shard
/// measurement before this one was really a single-shard one.
///
/// `in_flight` above 0 switches to acknowledged batches: each publisher keeps
/// up to that many `PerBatch` publishes unanswered on its connection, which is
/// what the broker's publish window allows, and the ack latency is reported.
/// On a durable stream with ack-on-commit this is the rate a client that waits
/// for durability gets, not the broker's enqueue rate.
pub(crate) async fn ingest(common: &Common, stream: &str, opts: &IngestOptions) -> Result<()> {
    let IngestOptions {
        keys, in_flight, ..
    } = *opts;
    let publishers = common.concurrency.max(1);
    let per = (common.total / publishers).max(1);
    let batch = common.batch.max(1);
    let payload_bytes = common.payload_bytes.max(1);

    // Readiness pre-flight on one connection, so a cold routing snapshot does
    // not land inside the measured window (same discipline as `pubsub`).
    {
        let pf = client(common, common.brokers[0]).await?;
        let publisher = pf.publisher().await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut consecutive = 0;
        while consecutive < 50 {
            match publisher
                .publish(
                    &common.tenant,
                    &common.namespace,
                    stream,
                    vec![0u8; payload_bytes],
                    AckMode::PerMessage,
                )
                .await
            {
                Ok(_) => consecutive += 1,
                Err(_) if Instant::now() < deadline => {
                    consecutive = 0;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(err) => return Err(err.context("cluster never became ready to ingest")),
            }
        }
    }

    // Wall clock to the local monotonic clock once, so every publisher waits
    // for the same instant.
    let started = match opts.start_at {
        Some(at) => match at.duration_since(SystemTime::now()) {
            Ok(wait) => Instant::now() + wait,
            Err(late) => {
                eprintln!(
                    "ingest: --start-at passed {:?} ago; starting now",
                    late.duration()
                );
                Instant::now()
            }
        },
        None => Instant::now(),
    };
    let deadline = opts.duration.map(|d| started + d);
    let mut tasks = Vec::new();
    for p in 0..publishers {
        // Each publisher owns a connection to a different broker in round-robin,
        // so the write load is spread across the fleet, not funnelled through
        // one ingress.
        let addr = common.brokers[p % common.brokers.len()];
        let target = Target {
            tenant: common.tenant.clone(),
            namespace: common.namespace.clone(),
            stream: stream.to_string(),
        };
        let token = common.token.clone();
        tasks.push(tokio::spawn(async move {
            let config = crate::tls::client_config(&target.tenant, &token)?;
            let client = Client::connect(addr, "localhost", config)
                .await
                .with_context(|| format!("connect to {addr}"))?;
            let publisher = client.publisher().await?;
            tokio::time::sleep_until(started.into()).await;
            let template = vec![0u8; payload_bytes];
            let mut sent = 0usize;
            let mut retries = 0u64;
            let mut samples = Samples::default();
            let mut pending = JoinSet::new();
            while match deadline {
                Some(deadline) => Instant::now() < deadline,
                None => sent < per,
            } {
                let this = match deadline {
                    Some(_) => batch,
                    None => batch.min(per - sent),
                };
                // The key decides the shard, so cycling keys spreads the load
                // across logs instead of piling it on shard 0.
                let key = (keys > 0).then(|| format!("k{}", (p * 1_000_003 + sent / batch) % keys));
                sent += this;
                if in_flight == 0 {
                    retries +=
                        send(&publisher, &target, key, &template, this, AckMode::None).await?;
                    continue;
                }
                if pending.len() >= in_flight {
                    let (r, took) = pending.join_next().await.expect("pending")??;
                    retries += r;
                    samples.record(took);
                }
                let (publisher, target, template) =
                    (publisher.clone(), target.clone(), template.clone());
                pending.spawn(async move {
                    let started = Instant::now();
                    let r =
                        send(&publisher, &target, key, &template, this, AckMode::PerBatch).await?;
                    Ok::<_, anyhow::Error>((r, started.elapsed()))
                });
            }
            while let Some(done) = pending.join_next().await {
                let (r, took) = done??;
                retries += r;
                samples.record(took);
            }
            Ok::<(usize, u64, Samples), anyhow::Error>((sent, retries, samples))
        }));
    }

    let mut published = 0usize;
    let mut retries = 0u64;
    let mut acks = Samples::default();
    for task in tasks {
        let (sent, r, samples) = task.await.context("publisher task")??;
        published += sent;
        retries += r;
        acks.merge(samples);
    }
    let elapsed = started.elapsed();
    let msg_s = published as f64 / elapsed.as_secs_f64();
    let mb_s = msg_s * payload_bytes as f64 / 1_000_000.0;
    println!(
        "ingest: publishers = {publishers}, in_flight = {in_flight}, published = {published} in {:.1} s, {msg_s:.0} msg/s, {mb_s:.1} MB/s, publish_retries = {retries}",
        elapsed.as_secs_f64()
    );
    let ack = (!acks.is_empty()).then(|| acks.percentiles());
    if let Some(ack) = ack {
        println!(
            "  batch ack: p50 = {}, p99 = {}, p999 = {}, max = {}",
            fmt_us(ack.p50_us),
            fmt_us(ack.p99_us),
            fmt_us(ack.p999_us),
            fmt_us(ack.max_us)
        );
    }
    emit_json(&serde_json::json!({
        "scenario": "ingest",
        "environment": common.environment,
        "stream": stream,
        "publishers": publishers,
        "batch": batch,
        "payload_bytes": payload_bytes,
        "published": published,
        "publish_retries": retries,
        "throughput_msg_s": msg_s,
        "throughput_mb_s": mb_s,
        "per_publisher_msg_s": msg_s / publishers as f64,
        "in_flight": in_flight,
        "duration_s": elapsed.as_secs_f64(),
        "batch_ack_latency_us": ack.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
        })),
    }));
    Ok(())
}

/// Publish a batch of `count` copies of `template`, retrying the transient
/// refusals [`is_retriable_transient`] names. Returns how many retries it took.
async fn send(
    publisher: &Publisher,
    target: &Target,
    key: Option<String>,
    template: &[u8],
    count: usize,
    ack: AckMode,
) -> Result<u64> {
    let mut retries = 0u64;
    loop {
        let payloads: Vec<Vec<u8>> = std::iter::repeat_with(|| template.to_vec())
            .take(count)
            .collect();
        let result = match &key {
            Some(key) => {
                publisher
                    .publish_batch_keyed(
                        &target.tenant,
                        &target.namespace,
                        &target.stream,
                        bytes::Bytes::from(key.clone().into_bytes()),
                        payloads,
                        ack,
                    )
                    .await
            }
            None => {
                publisher
                    .publish_batch(
                        &target.tenant,
                        &target.namespace,
                        &target.stream,
                        payloads,
                        ack,
                    )
                    .await
            }
        };
        match result {
            Ok(_) => return Ok(retries),
            Err(err) if is_retriable_transient(&err) => {
                retries += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => return Err(err.context("ingest publish")),
        }
    }
}

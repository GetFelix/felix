//! felix-loadgen's `pubsub` latency scenario, run against NATS.
//!
//! A port of `crates/testing/felix-loadgen/src/scenarios/pubsub.rs` for batch
//! 1: the same readiness pre-flight, the same 16-byte payload header
//! (sequence, then monotonic nanos since this process's epoch), one publish in
//! flight, the same warmup and total, the same sort-based percentiles, and the
//! same `LOADGEN_JSON` field names, so `summarize.py` reads both.
//!
//! `--mode js` publishes to a JetStream stream and waits for each PubAck;
//! subscribers read through an ordered push consumer at the live tail, so a
//! record reaches them only after the stream stored it, as Felix fans out only
//! after the append commits. `--mode core` publishes with core NATS and
//! subscribes to the subject; its "ack" is a flush (PING/PONG), the nearest
//! thing core NATS has to one.

mod args;
mod framing;
mod stats;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::push::OrderedConfig};
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::args::{Args, Mode, parse_args};
use crate::framing::{payload, read_header};
use crate::stats::{Samples, emit_json, fmt_us};

fn main() -> Result<()> {
    // felix-loadgen's runtime: tokio's multi-threaded default.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build runtime")?;
    runtime.block_on(run(parse_args()?))
}

async fn connect(args: &Args, name: &str) -> Result<async_nats::Client> {
    let mut options = async_nats::ConnectOptions::new()
        .name(name)
        .connection_timeout(Duration::from_secs(10));
    if let Some(ca) = &args.tlsca {
        options = options.add_root_certificates(ca.into()).require_tls(true);
    }
    options
        .connect(args.server.as_str())
        .await
        .with_context(|| format!("connect to {}", args.server))
}

/// One publish, returning once the server has confirmed it.
enum Publisher {
    Js(jetstream::Context),
    Core(async_nats::Client),
}

impl Publisher {
    async fn publish(&self, subject: &str, body: Vec<u8>) -> Result<()> {
        match self {
            Publisher::Js(js) => {
                js.publish(subject.to_string(), Bytes::from(body))
                    .await
                    .context("publish")?
                    .await
                    .context("puback")?;
            }
            Publisher::Core(client) => {
                client
                    .publish(subject.to_string(), Bytes::from(body))
                    .await
                    .context("publish")?;
                client.flush().await.context("flush")?;
            }
        }
        Ok(())
    }
}

type Deliveries = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

async fn subscribe(args: &Args, client: &async_nats::Client) -> Result<Deliveries> {
    match args.mode {
        Mode::Js => {
            let js = jetstream::new(client.clone());
            let stream = js
                .get_stream(&args.stream)
                .await
                .with_context(|| format!("stream {}", args.stream))?;
            let consumer = stream
                .create_consumer(OrderedConfig {
                    deliver_subject: client.new_inbox(),
                    filter_subject: args.subject.clone(),
                    deliver_policy: DeliverPolicy::New,
                    ..Default::default()
                })
                .await
                .context("create ordered consumer")?;
            let messages = consumer.messages().await.context("consumer messages")?;
            Ok(Box::pin(messages.map(|m| {
                m.map(|m| m.message.payload.clone())
                    .map_err(|e| anyhow::anyhow!("{e}"))
            })))
        }
        Mode::Core => {
            let sub = client
                .subscribe(args.subject.clone())
                .await
                .context("subscribe")?;
            // The server holds the interest before the first publish.
            client.flush().await.context("flush after subscribe")?;
            Ok(Box::pin(sub.map(|m| Ok(m.payload))))
        }
    }
}

async fn run(args: Args) -> Result<()> {
    let epoch = Instant::now();

    let publisher_client = connect(&args, "nats-latency-pub").await?;
    let publisher = match args.mode {
        Mode::Js => Publisher::Js(
            jetstream::context::ContextBuilder::new()
                .timeout(Duration::from_secs(30))
                .ack_timeout(Duration::from_secs(30))
                .build(publisher_client.clone()),
        ),
        Mode::Core => Publisher::Core(publisher_client.clone()),
    };

    if let (Mode::Js, Some(storage)) = (&args.mode, &args.ensure_stream) {
        let storage = match storage.as_str() {
            "file" => jetstream::stream::StorageType::File,
            "memory" => jetstream::stream::StorageType::Memory,
            other => bail!("--ensure-stream is file or memory, not {other}"),
        };
        jetstream::new(publisher_client.clone())
            .get_or_create_stream(jetstream::stream::Config {
                name: args.stream.clone(),
                subjects: vec![args.subject.clone()],
                storage,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .context("create stream")?;
    }

    // felix-loadgen's readiness pre-flight: 50 confirmed publishes in a row,
    // before any subscriber exists, so they reach nobody.
    {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut consecutive = 0;
        while consecutive < 50 {
            match publisher
                .publish(&args.subject, payload(u64::MAX, epoch, args.payload_bytes))
                .await
            {
                Ok(()) => consecutive += 1,
                Err(_) if Instant::now() < deadline => {
                    consecutive = 0;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(err) => return Err(err.context("server never became ready to publish")),
            }
        }
    }

    // Subscribers share one connection, separate from the publisher's, as
    // felix-loadgen's share one cluster client.
    let fanout = args.fanout.max(1);
    let subs_client = connect(&args, "nats-latency-sub").await?;
    let mut receivers = Vec::new();
    for _ in 0..fanout {
        receivers.push(subscribe(&args, &subs_client).await?);
    }

    let expected_per_sub = (args.warmup + args.total) as u64;
    let delivered = Arc::new(AtomicU64::new(0));
    let mut collectors = Vec::new();
    for (index, mut deliveries) in receivers.into_iter().enumerate() {
        let delivered = Arc::clone(&delivered);
        let warmup = args.warmup as u64;
        collectors.push(tokio::spawn(async move {
            // Sampled on the first subscriber only, counted on all.
            let mut samples = Samples::with_capacity(if index == 0 { 200_000 } else { 0 });
            let mut received = 0u64;
            while received < expected_per_sub {
                match tokio::time::timeout(Duration::from_secs(30), deliveries.next()).await {
                    Ok(Some(Ok(body))) => {
                        received += 1;
                        delivered.fetch_add(1, Ordering::Relaxed);
                        if let Some((seq, t0)) = read_header(&body)
                            && index == 0
                            && seq >= warmup
                        {
                            let now = epoch.elapsed().as_nanos() as u64;
                            samples.record(Duration::from_nanos(now.saturating_sub(t0)));
                        }
                    }
                    Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
                }
            }
            (samples, received)
        }));
    }

    // Closed loop, one in flight: the next publish starts when the last is
    // confirmed.
    let mut ack_samples = Samples::with_capacity(args.total);
    let publish_started = Instant::now();
    let mut measured_started = None;
    let total = args.warmup + args.total;
    for seq in 0..total as u64 {
        if seq as usize == args.warmup {
            measured_started = Some(Instant::now());
        }
        let body = payload(seq, epoch, args.payload_bytes);
        let at = Instant::now();
        publisher
            .publish(&args.subject, body)
            .await
            .with_context(|| format!("publish seq {seq}"))?;
        if seq as usize >= args.warmup {
            ack_samples.record(at.elapsed());
        }
    }
    let publish_elapsed = publish_started.elapsed();
    let measured_elapsed = measured_started
        .map(|at| at.elapsed())
        .unwrap_or(publish_elapsed);

    let mut delivery_samples = Samples::default();
    let mut received_total = 0u64;
    for collector in collectors {
        let (samples, received) = collector.await.context("collector")?;
        delivery_samples.merge(samples);
        received_total += received;
    }
    if delivery_samples.is_empty() {
        bail!("no deliveries sampled (received {received_total})");
    }

    let expected_total = expected_per_sub * fanout as u64;
    let unaccounted = expected_total.saturating_sub(received_total);
    let publish_throughput = total as f64 / publish_elapsed.as_secs_f64();
    let measured_throughput = args.total as f64 / measured_elapsed.as_secs_f64();
    let delivered_throughput = received_total as f64 / publish_elapsed.as_secs_f64();
    let per_sub = delivered_throughput / fanout as f64;
    let ack = ack_samples.percentiles();
    let delivery = delivery_samples.percentiles();

    println!(
        "Results (publish n = {}, sampled {}, received {}, unaccounted {}, payload {} bytes, fanout {}, batch 1, binary false)",
        total, args.total, received_total, unaccounted, args.payload_bytes, args.fanout,
    );
    println!("  delivered total = {received_total}");
    println!("  p50 = {}", fmt_us(ack.p50_us));
    println!("  p99 = {}", fmt_us(ack.p99_us));
    println!("  p999 = {}", fmt_us(ack.p999_us));
    println!("  throughput = {publish_throughput:.1} msg/s");
    println!("  effective throughput = {measured_throughput:.1} msg/s");
    println!("  delivered throughput = {delivered_throughput:.1} msg/s");
    println!("  delivered per-sub throughput = {per_sub:.1} msg/s");
    println!(
        "  delivery (publish -> subscriber): p50 = {}, p99 = {}, p999 = {} (ack latency above)",
        fmt_us(delivery.p50_us),
        fmt_us(delivery.p99_us),
        fmt_us(delivery.p999_us),
    );

    let (ack_kind, delivery_path, stream) = match args.mode {
        Mode::Js => (
            "jetstream-puback",
            "jetstream-ordered-push-consumer",
            args.stream.as_str(),
        ),
        Mode::Core => ("core-flush", "core-subscription", ""),
    };
    emit_json(&serde_json::json!({
        "scenario": "pubsub",
        "system": "nats",
        "nats_mode": args.mode.name(),
        "ack_kind": ack_kind,
        "delivery_path": delivery_path,
        "environment": args.environment,
        "stream": stream,
        "subject": args.subject,
        "tls": args.tlsca.is_some(),
        "payload_bytes": args.payload_bytes,
        "fanout": args.fanout,
        "batch": 1,
        "binary": false,
        "publish_route": "nats",
        "warmup": args.warmup,
        "total": args.total,
        "received": received_total,
        "subscriber_connections": 1,
        "unaccounted": unaccounted,
        "publish_retries": 0,
        "publish_throughput_msg_s": publish_throughput,
        "effective_throughput_msg_s": measured_throughput,
        "delivered_throughput_msg_s": delivered_throughput,
        "expected_per_sub": expected_per_sub,
        "ack_latency_us": {
            "p50": ack.p50_us, "p99": ack.p99_us, "p999": ack.p999_us, "max": ack.max_us,
            "samples": args.total,
        },
        "delivery_latency_us": {
            "p50": delivery.p50_us, "p99": delivery.p99_us, "p999": delivery.p999_us,
            "max": delivery.max_us,
        },
    }));
    Ok(())
}

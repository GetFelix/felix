//! Aggregate ingest throughput across many publishers.

use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use felix_client::{Client, Publisher};
use felix_wire::AckMode;
use felix_wire::routing::{ShardRouting, shard_for_routing};
use tokio::task::JoinSet;

use super::connect::client;
use super::framing::{SEND_TIME, stamp_send_time};
use super::{Common, is_retriable_transient, start_instant};
use crate::stats::{Samples, fmt_us, report};

/// The `ingest`-only flags.
#[derive(Default)]
pub struct IngestOptions {
    /// Distinct routing keys; 0 publishes unkeyed.
    pub keys: usize,
    /// The stream's shard count. Absent, the broker is asked.
    pub shards: Option<u32>,
    /// Acked batches each publisher keeps outstanding; 0 is fire-and-forget.
    pub in_flight: usize,
    /// Publish for this long instead of a fixed `--total`.
    pub duration: Option<Duration>,
    /// Start publishing at this wall-clock time, so generators on separate
    /// machines overlap for the whole run instead of starting seconds apart.
    pub start_at: Option<SystemTime>,
    /// Put the wall-clock send time, Unix micros, in each payload's first 8
    /// bytes, so a `subscribe` run in another process can report delivery
    /// latency.
    pub stamp_send_time: bool,
}

/// Where an ingest publisher writes.
#[derive(Clone)]
struct Target {
    tenant: String,
    namespace: String,
    stream: String,
}

/// What one publisher got through before it stopped.
#[derive(Default)]
struct Tally {
    /// Records in sends that were started and not cut off by the deadline.
    published: usize,
    /// Records whose send call returned. Fire-and-forget, that means written
    /// to the connection, not appended.
    completed: usize,
    /// Records the broker acknowledged. Zero when fire-and-forget.
    acked: usize,
    /// Records in sends abandoned at the deadline.
    cut_off: usize,
    retries: u64,
    samples: Samples,
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
/// measurement before this one was really a single-shard one. When the shard
/// count is known the key names are chosen so they cover the shards evenly;
/// see [`spread_keys`].
///
/// With a duration, every send is bounded by the deadline. A fire-and-forget
/// send can otherwise wait in client admission until the broker drains its
/// backlog, long after the run should have ended.
///
/// `in_flight` above 0 switches to acknowledged batches: each publisher keeps
/// up to that many `PerBatch` publishes unanswered on its connection, which is
/// what the broker's publish window allows, and the ack latency is reported.
/// On a durable stream with ack-on-commit this is the rate a client that waits
/// for durability gets, not the broker's enqueue rate.
pub(crate) async fn ingest(
    common: &Common,
    stream: &str,
    opts: &IngestOptions,
) -> Result<serde_json::Value> {
    let IngestOptions {
        in_flight,
        stamp_send_time: stamp,
        ..
    } = *opts;
    let publishers = common.concurrency.max(1);
    let per = (common.total / publishers).max(1);
    let batch = common.batch.max(1);
    let payload_bytes = common.payload_bytes.max(if stamp { SEND_TIME } else { 1 });

    // Readiness pre-flight on one connection, so a cold routing snapshot does
    // not land inside the measured window (same discipline as `pubsub`).
    let key_names = {
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
        key_names(&pf, common, stream, opts).await
    };

    let started = start_instant(opts.start_at, "ingest");
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
        let key_names = key_names.clone();
        tasks.push(tokio::spawn(async move {
            let config = crate::tls::client_config(&target.tenant, &token)?;
            let client = Client::connect(addr, "localhost", config)
                .await
                .with_context(|| format!("connect to {addr}"))?;
            let publisher = client.publisher().await?;
            tokio::time::sleep_until(started.into()).await;
            let template = vec![0u8; payload_bytes];
            let mut tally = Tally::default();
            let mut batches = 0usize;
            // Records in acked sends not yet answered, which the deadline
            // cuts off if it arrives first.
            let mut outstanding = 0usize;
            let mut pending = JoinSet::new();
            while match deadline {
                Some(deadline) => Instant::now() < deadline,
                None => tally.published < per,
            } {
                let this = match deadline {
                    Some(_) => batch,
                    None => batch.min(per - tally.published),
                };
                // The key decides the shard, so cycling keys spreads the load
                // across logs instead of piling it on shard 0.
                let key = (!key_names.is_empty())
                    .then(|| key_names[(p * 1_000_003 + batches) % key_names.len()].clone());
                batches += 1;
                if in_flight == 0 {
                    let send = send(
                        &publisher,
                        &target,
                        key,
                        &template,
                        this,
                        AckMode::None,
                        stamp,
                    );
                    match until(deadline, send).await {
                        Some(r) => {
                            tally.retries += r?;
                            tally.published += this;
                            tally.completed += this;
                        }
                        None => {
                            tally.cut_off += this;
                            break;
                        }
                    }
                    continue;
                }
                if pending.len() >= in_flight {
                    match until(deadline, pending.join_next()).await {
                        Some(done) => {
                            let (r, took, n) = done.expect("pending")??;
                            tally.acked_one(r, took, n);
                            outstanding -= n;
                        }
                        None => break,
                    }
                }
                tally.published += this;
                outstanding += this;
                let (publisher, target, template) =
                    (publisher.clone(), target.clone(), template.clone());
                pending.spawn(async move {
                    let started = Instant::now();
                    let r = send(
                        &publisher,
                        &target,
                        key,
                        &template,
                        this,
                        AckMode::PerBatch,
                        stamp,
                    )
                    .await?;
                    Ok::<_, anyhow::Error>((r, started.elapsed(), this))
                });
            }
            loop {
                match until(deadline, pending.join_next()).await {
                    Some(Some(done)) => {
                        let (r, took, n) = done??;
                        tally.acked_one(r, took, n);
                        outstanding -= n;
                    }
                    Some(None) => break,
                    None => {
                        // Abandoned sends are not counted as published.
                        pending.abort_all();
                        tally.published -= outstanding;
                        tally.cut_off += outstanding;
                        break;
                    }
                }
            }
            Ok::<Tally, anyhow::Error>(tally)
        }));
    }

    let mut total = Tally::default();
    for task in tasks {
        let t = task.await.context("publisher task")??;
        total.published += t.published;
        total.completed += t.completed;
        total.acked += t.acked;
        total.cut_off += t.cut_off;
        total.retries += t.retries;
        total.samples.merge(t.samples);
    }
    let Tally {
        published,
        completed,
        acked,
        cut_off,
        retries,
        samples: mut acks,
    } = total;
    let elapsed = started.elapsed();
    let secs = elapsed.as_secs_f64();
    let msg_s = published as f64 / secs;
    let mb_s = msg_s * payload_bytes as f64 / 1_000_000.0;
    // Fire-and-forget has no acked count: a completed send there is one the
    // client wrote, which says nothing about what the broker appended.
    let acked_records = (in_flight > 0).then_some(acked);
    let acked_msg_s = acked_records.map(|n| n as f64 / secs);
    report!(
        common,
        "ingest: publishers = {publishers}, in_flight = {in_flight}, published = {published} in {secs:.1} s, {msg_s:.0} msg/s, {mb_s:.1} MB/s, publish_retries = {retries}, cut_off = {cut_off}"
    );
    match acked_msg_s {
        Some(rate) => report!(common, "  acked = {acked} ({rate:.0} msg/s)"),
        None => report!(
            common,
            "  fire-and-forget: published counts sends written, not records appended"
        ),
    }
    let ack = (!acks.is_empty()).then(|| acks.percentiles());
    if let Some(ack) = ack {
        report!(
            common,
            "  batch ack: p50 = {}, p99 = {}, p999 = {}, max = {}",
            fmt_us(ack.p50_us),
            fmt_us(ack.p99_us),
            fmt_us(ack.p999_us),
            fmt_us(ack.max_us)
        );
    }
    Ok(serde_json::json!({
        "scenario": "ingest",
        "environment": common.environment,
        "stream": stream,
        "publishers": publishers,
        "batch": batch,
        "payload_bytes": payload_bytes,
        "published": published,
        "completed_records": completed,
        "acked_records": acked_records,
        "acked_throughput_msg_s": acked_msg_s,
        "acked_throughput_mb_s": acked_msg_s.map(|r| r * payload_bytes as f64 / 1_000_000.0),
        "cut_off_records": cut_off,
        "publish_retries": retries,
        "throughput_msg_s": msg_s,
        "throughput_mb_s": mb_s,
        "per_publisher_msg_s": msg_s / publishers as f64,
        "in_flight": in_flight,
        "stamp_send_time": stamp,
        "duration_s": secs,
        "batch_ack_latency_us": ack.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
        })),
    }))
}

/// Publish a batch of `count` copies of `template`, retrying the transient
/// refusals [`is_retriable_transient`] names. Returns how many retries it took.
/// With `stamp`, each attempt carries the time it was sent, not the time the
/// first attempt was.
async fn send(
    publisher: &Publisher,
    target: &Target,
    key: Option<String>,
    template: &[u8],
    count: usize,
    ack: AckMode,
    stamp: bool,
) -> Result<u64> {
    let mut retries = 0u64;
    loop {
        let payloads: Vec<Vec<u8>> = std::iter::repeat_with(|| {
            let mut body = template.to_vec();
            if stamp {
                stamp_send_time(&mut body);
            }
            body
        })
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

impl Tally {
    fn acked_one(&mut self, retries: u64, took: Duration, records: usize) {
        self.retries += retries;
        self.completed += records;
        self.acked += records;
        self.samples.record(took);
    }
}

/// Run `f` to completion, or until `deadline`. `None` means the deadline won.
async fn until<F: Future>(deadline: Option<Instant>, f: F) -> Option<F::Output> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline.into(), f).await.ok(),
        None => Some(f.await),
    }
}

/// The routing key names an ingest run cycles through. Spread across the
/// stream's shards when the shard count is known, else `k0..k{n}`.
async fn key_names(
    client: &Client,
    common: &Common,
    stream: &str,
    opts: &IngestOptions,
) -> Vec<String> {
    if opts.keys == 0 {
        return Vec::new();
    }
    let learned = if client.supports_stream_shards() {
        client
            .stream_routing(&common.tenant, &common.namespace, stream)
            .await
            .ok()
    } else {
        None
    };
    if let (Some(given), Some((asked, _))) = (opts.shards, learned)
        && given != asked
    {
        eprintln!(
            "ingest: --shards {given}, but the broker says {stream} has {asked}; using {given}"
        );
    }
    let routing = learned.map(|(_, r)| r).unwrap_or_default();
    match opts.shards.or(learned.map(|(s, _)| s)) {
        Some(shards) if shards > 1 => spread_keys(opts.keys, shards, routing),
        _ => (0..opts.keys).map(|i| format!("k{i}")).collect(),
    }
}

/// `keys` key names that land on the shards as evenly as possible: every
/// shard gets one before any gets two, and fewer keys than shards land on
/// distinct shards. Found by trying `k0, k1, ...` against the stream's own
/// routing, since sequential names hash unevenly: 12 of them on 12 shards
/// reach only 8. Returned round-robin by shard, so consecutive keys hit
/// different shards.
pub(crate) fn spread_keys(keys: usize, shards: u32, routing: ShardRouting) -> Vec<String> {
    let n = shards as usize;
    let want = |shard: usize| keys / n + usize::from(shard < keys % n);
    let mut by_shard: Vec<Vec<String>> = vec![Vec::new(); n];
    let mut found = 0;
    let mut candidate = 0u64;
    while found < keys {
        let name = format!("k{candidate}");
        let shard = shard_for_routing(routing, shards, Some(name.as_bytes())) as usize;
        if by_shard[shard].len() < want(shard) {
            by_shard[shard].push(name);
            found += 1;
        }
        candidate += 1;
    }
    let rounds = keys.div_ceil(n);
    (0..rounds)
        .flat_map(|round| by_shard.iter().filter_map(move |names| names.get(round)))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;

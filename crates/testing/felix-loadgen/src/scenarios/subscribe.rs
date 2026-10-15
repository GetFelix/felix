//! Subscribers on their own, for a fixed time, while something else publishes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use felix_client::{ClusterClient, ClusterSubscription, Event, ShardEvent, ShardedSubscription};
use tokio::task::JoinSet;

use super::connect::{client, cluster};
use super::framing::{read_send_time, unix_micros};
use super::{Common, start_instant};
use crate::stats::{Reservoir, fmt_us, report};

/// Latency samples kept across all subscribers. A timed run has no fixed
/// count, so each subscriber keeps a uniform sample of its share.
const LATENCY_SAMPLES: usize = 1_000_000;

/// The `subscribe`-only flags.
pub struct SubscribeOptions {
    /// How long the measured window lasts.
    pub duration: Duration,
    /// Open the subscriptions, then start counting at this wall-clock time,
    /// so a publishing generator started with the same `--start-at` is
    /// measured from its first record.
    pub start_at: Option<SystemTime>,
    /// Read the send time `ingest --stamp-send-time` put in each payload and
    /// report delivery latency against this machine's clock.
    pub stamp_send_time: bool,
}

/// One subscription, over one shard or all of them.
enum Feed {
    One(Box<ClusterSubscription>),
    All(ShardedSubscription),
}

/// What [`Feed::next`] produced.
enum Item {
    Record(u32, Event),
    /// A shard's owner stopped answering; a sharded subscription re-establishes it.
    ShardLost,
    Other,
    Ended,
}

impl Feed {
    async fn next(&mut self) -> Result<Item> {
        match self {
            Feed::One(sub) => Ok(match sub.next_event().await? {
                Some(event) => Item::Record(0, event),
                None => Item::Ended,
            }),
            Feed::All(sub) => Ok(match sub.next().await {
                Some(ShardEvent::Record { shard, event }) => Item::Record(shard, event),
                Some(ShardEvent::ShardLost { .. }) => Item::ShardLost,
                Some(_) => Item::Other,
                None => Item::Ended,
            }),
        }
    }
}

/// What one subscriber saw inside the window.
struct Tally {
    delivered: u64,
    bytes: u64,
    /// Records the offsets say were skipped: `offset - previous - 1 -
    /// skipped_before`, summed per shard.
    gaps: u64,
    /// Records that came without an offset, so no gap could be seen.
    unordered: u64,
    shard_losses: u64,
    /// Send stamps later than this machine's receive time, which only clock
    /// skew explains. Left out of the latency sample.
    ahead_of_clock: u64,
    latency: Reservoir,
    ended_early: bool,
}

/// Subscribe-only: `fanout` subscriptions at the live tail, spread over
/// `concurrency` cluster clients, each counting what it is delivered for
/// `duration`. Nothing is published, so the run reports delivered numbers
/// only; pair it with `ingest` on another generator for read-heavy runs.
///
/// A drop shows as a jump in log offsets, so gaps are counted per shard of
/// each subscription. An in-memory stream has no offsets, and its records are
/// counted as `records_without_offset` instead.
///
/// With `stamp_send_time`, delivery latency is receive time minus the send
/// stamp, two different machines' wall clocks. It is only as good as their
/// sync, and the report says so.
pub(crate) async fn subscribe(
    common: &Common,
    stream: &str,
    opts: &SubscribeOptions,
) -> Result<serde_json::Value> {
    let subscribers = common.fanout.max(1);
    let clients = common.concurrency.clamp(1, subscribers);

    let shards = {
        let probe = client(common, common.brokers[0]).await?;
        if probe.supports_stream_shards() {
            probe
                .stream_routing(&common.tenant, &common.namespace, stream)
                .await
                .map(|(shards, _)| shards)
                .with_context(|| format!("look up {stream}"))?
        } else {
            1
        }
    };
    if shards == 0 {
        bail!("the broker does not know stream {stream}");
    }

    let mut cluster_clients = Vec::with_capacity(clients);
    for _ in 0..clients {
        cluster_clients.push(Arc::new(cluster(common).await?));
    }
    // Opened before the window, so the first record a publisher started with
    // the same --start-at sends is already behind every subscription.
    let mut feeds = Vec::with_capacity(subscribers);
    for index in 0..subscribers {
        let cluster: &Arc<ClusterClient> = &cluster_clients[index % clients];
        let feed = if shards > 1 {
            Feed::All(
                cluster
                    .subscribe_sharded(&common.tenant, &common.namespace, stream, None)
                    .await
                    .context("subscribe")?,
            )
        } else {
            Feed::One(Box::new(
                cluster
                    .subscribe(&common.tenant, &common.namespace, stream)
                    .await
                    .context("subscribe")?,
            ))
        };
        feeds.push(feed);
    }
    let mut subscriber_connections = 0;
    for cluster in &cluster_clients {
        subscriber_connections += cluster
            .connections_per_node()
            .await
            .iter()
            .map(|(_, count)| count)
            .sum::<usize>();
    }

    let started = start_instant(opts.start_at, "subscribe");
    let deadline = started + opts.duration;
    let per_sub_samples = (LATENCY_SAMPLES / subscribers).max(1_000);
    let mut tasks = JoinSet::new();
    for (index, feed) in feeds.into_iter().enumerate() {
        let stamped = opts.stamp_send_time;
        tasks.spawn(watch_feed(
            feed,
            started,
            deadline,
            stamped,
            Reservoir::new(per_sub_samples, index as u64 + 1),
        ));
    }

    let mut delivered = 0u64;
    let mut bytes = 0u64;
    let mut gaps = 0u64;
    let mut subscribers_with_gaps = 0usize;
    let mut unordered = 0u64;
    let mut shard_losses = 0u64;
    let mut ahead_of_clock = 0u64;
    let mut ended_early = 0usize;
    let mut stamped_records = 0u64;
    let mut latency = crate::stats::Samples::default();
    let mut per_sub = Vec::with_capacity(subscribers);
    while let Some(done) = tasks.join_next().await {
        let t = done.context("subscriber task")??;
        delivered += t.delivered;
        bytes += t.bytes;
        gaps += t.gaps;
        subscribers_with_gaps += usize::from(t.gaps > 0);
        unordered += t.unordered;
        shard_losses += t.shard_losses;
        ahead_of_clock += t.ahead_of_clock;
        ended_early += usize::from(t.ended_early);
        stamped_records += t.latency.seen();
        latency.merge(t.latency.into_samples());
        per_sub.push(t.delivered);
    }
    // Keep the connections up until every subscriber is done.
    drop(cluster_clients);

    let secs = opts.duration.as_secs_f64();
    let msg_s = delivered as f64 / secs;
    let mb_s = bytes as f64 / secs / 1_000_000.0;
    let per_sub_msg_s = msg_s / subscribers as f64;
    let (min_per_sub, max_per_sub) = (
        per_sub.iter().copied().min().unwrap_or(0),
        per_sub.iter().copied().max().unwrap_or(0),
    );
    let delivery = (!latency.is_empty()).then(|| latency.percentiles());

    report!(
        common,
        "subscribe: subscribers = {subscribers}, clients = {clients}, shards = {shards}, delivered = {delivered} in {secs:.1} s"
    );
    report!(common, "  delivered throughput = {msg_s:.1} msg/s");
    report!(
        common,
        "  delivered per-sub throughput = {per_sub_msg_s:.1} msg/s"
    );
    report!(
        common,
        "  delivered per sub: min = {min_per_sub}, max = {max_per_sub}; {mb_s:.1} MB/s in all"
    );
    report!(
        common,
        "  gaps = {gaps} ({subscribers_with_gaps} subscriber(s)), records without offset = {unordered}, shard losses = {shard_losses}"
    );
    if ended_early > 0 {
        report!(
            common,
            "  {ended_early} subscription(s) ended before the window closed"
        );
    }
    match delivery {
        Some(p) => report!(
            common,
            "  delivery (send stamp -> subscriber, wall clocks, only as good as their sync): p50 = {}, p99 = {}, p999 = {}, max = {}; {ahead_of_clock} stamp(s) ahead of this clock left out",
            fmt_us(p.p50_us),
            fmt_us(p.p99_us),
            fmt_us(p.p999_us),
            fmt_us(p.max_us),
        ),
        None if opts.stamp_send_time => {
            report!(common, "  no delivery latency: no stamped record arrived")
        }
        None => {}
    }

    Ok(serde_json::json!({
        "scenario": "subscribe",
        "environment": common.environment,
        "stream": stream,
        "shards": shards,
        "fanout": subscribers,
        "clients": clients,
        "subscriber_connections": subscriber_connections,
        "duration_s": secs,
        "received": delivered,
        "received_bytes": bytes,
        "received_per_sub_min": min_per_sub,
        "received_per_sub_max": max_per_sub,
        "delivered_throughput_msg_s": msg_s,
        "delivered_throughput_mb_s": mb_s,
        "delivered_per_sub_msg_s": per_sub_msg_s,
        "gaps": gaps,
        "subscribers_with_gaps": subscribers_with_gaps,
        "records_without_offset": unordered,
        "shard_losses": shard_losses,
        "ended_early": ended_early,
        "stamp_send_time": opts.stamp_send_time,
        "latency_clock": opts.stamp_send_time.then_some("wall clocks of two machines; needs them in sync"),
        "stamped_records": stamped_records,
        "stamps_ahead_of_clock": ahead_of_clock,
        "delivery_latency_us": delivery.map(|p| serde_json::json!({
            "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us,
            "samples": latency.len(),
        })),
    }))
}

/// Count one subscription's deliveries between `started` and `deadline`.
/// Records that arrive before `started` are not counted, but still set the
/// offset a later gap is measured from.
async fn watch_feed(
    mut feed: Feed,
    started: Instant,
    deadline: Instant,
    stamped: bool,
    latency: Reservoir,
) -> Result<Tally> {
    let mut tally = Tally {
        delivered: 0,
        bytes: 0,
        gaps: 0,
        unordered: 0,
        shard_losses: 0,
        ahead_of_clock: 0,
        latency,
        ended_early: false,
    };
    let mut last_offset: HashMap<u32, u64> = HashMap::new();
    loop {
        let item = match tokio::time::timeout_at(deadline.into(), feed.next()).await {
            Ok(item) => item?,
            Err(_) => break,
        };
        let counting = Instant::now() >= started;
        let (shard, event) = match item {
            Item::Record(shard, event) => (shard, event),
            Item::ShardLost => {
                tally.shard_losses += u64::from(counting);
                continue;
            }
            Item::Other => continue,
            Item::Ended => {
                tally.ended_early = true;
                break;
            }
        };
        let gap = event.offset.map(|offset| {
            dropped(
                last_offset.insert(shard, offset),
                offset,
                event.skipped_before,
            )
        });
        if !counting {
            continue;
        }
        tally.delivered += 1;
        tally.bytes += event.payload.len() as u64;
        match gap {
            Some(gap) => tally.gaps += gap,
            None => tally.unordered += 1,
        }
        // Zero is an unstamped record, such as ingest's readiness publishes.
        if stamped && let Some(sent) = read_send_time(&event.payload).filter(|&t| t != 0) {
            match unix_micros().checked_sub(sent) {
                Some(micros) => tally.latency.offer(Duration::from_micros(micros)),
                None => tally.ahead_of_clock += 1,
            }
        }
    }
    Ok(tally)
}

/// Records dropped between the `previous` offset delivered from a shard and
/// `offset`, given the `skipped_before` offsets that hold no event. The first
/// record from a shard has nothing to compare with.
fn dropped(previous: Option<u64>, offset: u64, skipped_before: u64) -> u64 {
    previous.map_or(0, |previous| {
        offset
            .saturating_sub(previous + 1)
            .saturating_sub(skipped_before)
    })
}

#[cfg(test)]
mod tests;

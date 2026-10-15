//! Keyed watch fanout latency.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use super::connect::client;
use super::framing::{payload, read_header};
use super::{Common, scope};
use crate::stats::{Samples, fmt_us, report};

/// Keyed watch: `fanout` watchers on one key, a writer putting `total` values
/// through it, and the latency from each put landing to each watcher seeing
/// it — the composed-semantics fanout claim, measured.
pub(crate) async fn watch(common: &Common, cache: &str) -> Result<serde_json::Value> {
    let epoch = Instant::now();
    let (tenant, namespace, name) = scope(common, cache);
    let key = "watched";

    // A watch is served by the owner and redirected elsewhere; follow by
    // trying each broker, exactly as a redirect-following client would.
    let mut watches = Vec::new();
    for _ in 0..common.fanout.max(1) {
        let mut opened = None;
        for addr in &common.brokers {
            let candidate = client(common, *addr).await?;
            match candidate
                .watch_cache(
                    &tenant,
                    &namespace,
                    &name,
                    felix_client::CacheWatchFilter::Key(key.to_string()),
                    None,
                )
                .await
            {
                Ok(watch) => {
                    opened = Some((candidate, watch));
                    break;
                }
                Err(err) if err.downcast_ref::<felix_client::NotLeaderError>().is_some() => {
                    continue;
                }
                Err(err) => return Err(err.context("open watch")),
            }
        }
        watches.push(opened.context("no broker served the watch")?);
    }

    let expected = (common.warmup + common.total) as u64;
    let mut collectors = Vec::new();
    for (index, (client, mut watch)) in watches.into_iter().enumerate() {
        let warmup = common.warmup as u64;
        collectors.push(tokio::spawn(async move {
            let mut samples = Samples::with_capacity(200_000);
            let mut received = 0u64;
            while received < expected {
                match tokio::time::timeout(Duration::from_secs(30), watch.recv()).await {
                    Ok(Some(felix_client::CacheWatchItem::Change(change))) => {
                        received += 1;
                        if let Some(value) = &change.value
                            && let Some((seq, t0)) = read_header(value)
                            && seq >= warmup
                        {
                            let now = epoch.elapsed().as_nanos() as u64;
                            samples.record(Duration::from_nanos(now.saturating_sub(t0)));
                        }
                    }
                    Ok(Some(felix_client::CacheWatchItem::Lagged { resume_from })) => {
                        bail!("watcher {index} lagged at offset {resume_from}: raise the pace or lower fanout");
                    }
                    Ok(Some(felix_client::CacheWatchItem::ShardMoved(moved))) => {
                        bail!("watcher {index} ended because its shard moved: {moved:?}");
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            drop(client);
            Ok::<(Samples, u64), anyhow::Error>((samples, received))
        }));
    }

    let writer = client(common, common.brokers[0]).await?;
    for seq in 0..expected {
        let body = payload(seq, epoch, common.payload_bytes);
        writer
            .cache_put(
                &tenant,
                &namespace,
                &name,
                key,
                bytes::Bytes::from(body),
                None,
            )
            .await
            .with_context(|| format!("put seq {seq}"))?;
    }

    let mut all = Samples::default();
    let mut received_total = 0u64;
    for collector in collectors {
        let (samples, received) = collector.await.context("watcher")??;
        all.merge(samples);
        received_total += received;
    }
    let expected_total = expected * common.fanout.max(1) as u64;
    let percentiles = all.percentiles();
    report!(
        common,
        "watch delivery: watchers = {}, puts = {expected}, delivered = {received_total}/{expected_total}, p50 = {}, p99 = {}, p999 = {}",
        common.fanout.max(1),
        fmt_us(percentiles.p50_us),
        fmt_us(percentiles.p99_us),
        fmt_us(percentiles.p999_us),
    );
    Ok(serde_json::json!({
        "scenario": "watch",
        "environment": common.environment,
        "cache": name,
        "watchers": common.fanout.max(1),
        "payload_bytes": common.payload_bytes,
        "puts": expected,
        "delivered": received_total,
        "expected": expected_total,
        "delivery_latency_us": { "p50": percentiles.p50_us, "p99": percentiles.p99_us,
                                  "p999": percentiles.p999_us, "max": percentiles.max_us },
    }))
}

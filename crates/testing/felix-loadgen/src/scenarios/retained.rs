//! Retained delivery: how long a late joiner takes to receive the whole roster.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::connect::client;
use super::{Common, scope};
use crate::stats::{fmt_us, report};

/// Retained delivery (#349): the MQTT-style join-and-hold-roster. Seed a roster
/// of `total` retained keys, then open a retained watch and measure the
/// time-to-complete-state — how long a late joiner takes to receive the whole
/// roster before it is live. Run it at `--total` 100 / 1000 / 10000 to trace
/// the curve the design doc asks for. Each roster size gets its own key prefix,
/// so the retained replay is exactly the roster and nothing else in the cache.
pub(crate) async fn retained(common: &Common, cache: &str) -> Result<serde_json::Value> {
    let (tenant, namespace, name) = scope(common, cache);
    let roster = common.total.max(1);
    let prefix = format!("roster-{roster}/");

    // Seed the roster: one retained value per key.
    let writer = client(common, common.brokers[0]).await?;
    let value = bytes::Bytes::from(vec![0u8; common.payload_bytes.max(1)]);
    for i in 0..roster {
        writer
            .cache_put(
                &tenant,
                &namespace,
                &name,
                &format!("{prefix}{i}"),
                value.clone(),
                None,
            )
            .await
            .with_context(|| format!("seed roster key {i}"))?;
    }

    // Join: open a retained watch (owner serves; others redirect) and time how
    // long until the full roster has replayed.
    let mut opened = None;
    for addr in &common.brokers {
        let candidate = client(common, *addr).await?;
        match candidate
            .watch_cache_retained(
                &tenant,
                &namespace,
                &name,
                felix_client::CacheWatchFilter::Prefix(prefix.clone()),
            )
            .await
        {
            Ok(watch) => {
                opened = Some((candidate, watch));
                break;
            }
            Err(err) if err.downcast_ref::<felix_client::NotLeaderError>().is_some() => continue,
            Err(err) => return Err(err.context("open retained watch")),
        }
    }
    let (held, mut watch) = opened.context("no broker served the retained watch")?;

    let started = Instant::now();
    let mut received = 0u64;
    while received < roster as u64 {
        match tokio::time::timeout(Duration::from_secs(60), watch.recv()).await {
            Ok(Some(felix_client::CacheWatchItem::Change(_))) => received += 1,
            Ok(Some(felix_client::CacheWatchItem::Lagged { .. })) => continue,
            // The watch ended with its shard; the count stays short.
            Ok(Some(felix_client::CacheWatchItem::ShardMoved(_))) => break,
            Ok(None) | Err(_) => break,
        }
    }
    let elapsed = started.elapsed();
    drop(held);

    let complete = received >= roster as u64;
    report!(
        common,
        "retained join: roster = {roster}, received = {received}/{roster}, time-to-complete-state = {}",
        fmt_us(elapsed.as_micros() as u64),
    );
    Ok(serde_json::json!({
        "scenario": "retained",
        "environment": common.environment,
        "cache": name,
        "roster": roster,
        "received": received,
        "complete": complete,
        "payload_bytes": common.payload_bytes,
        "time_to_complete_state_us": elapsed.as_micros() as u64,
    }))
}

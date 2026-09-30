//! Aggregate ingest throughput across many publishers.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_client::Client;
use felix_wire::AckMode;

use super::connect::client;
use super::{Common, is_retriable_transient};
use crate::stats::emit_json;

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
pub(crate) async fn ingest(common: &Common, stream: &str, keys: usize) -> Result<()> {
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

    let started = Instant::now();
    let mut tasks = Vec::new();
    for p in 0..publishers {
        // Each publisher owns a connection to a different broker in round-robin,
        // so the write load is spread across the fleet, not funnelled through
        // one ingress.
        let addr = common.brokers[p % common.brokers.len()];
        let tenant = common.tenant.clone();
        let namespace = common.namespace.clone();
        let token = common.token.clone();
        let stream = stream.to_string();
        tasks.push(tokio::spawn(async move {
            let config = crate::tls::client_config(&tenant, &token)?;
            let client = Client::connect(addr, "localhost", config)
                .await
                .with_context(|| format!("connect to {addr}"))?;
            let publisher = client.publisher().await?;
            let template = vec![0u8; payload_bytes];
            let mut sent = 0usize;
            let mut retries = 0u64;
            while sent < per {
                let this = batch.min(per - sent);
                let payloads: Vec<Vec<u8>> = std::iter::repeat_with(|| template.clone())
                    .take(this)
                    .collect();
                let sent_batches = sent / batch.max(1);
                match if keys > 0 {
                    // The key decides the shard, so cycling keys spreads the
                    // load across logs instead of piling it on shard 0.
                    let key = format!("k{}", (p * 1_000_003 + sent_batches) % keys);
                    publisher
                        .publish_batch_keyed(
                            &tenant,
                            &namespace,
                            &stream,
                            bytes::Bytes::from(key.into_bytes()),
                            payloads,
                            AckMode::None,
                        )
                        .await
                } else {
                    publisher
                        .publish_batch(&tenant, &namespace, &stream, payloads, AckMode::None)
                        .await
                } {
                    Ok(_) => sent += this,
                    Err(err) if is_retriable_transient(&err) => {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(err) => return Err(err.context("ingest publish")),
                }
            }
            Ok::<(usize, u64), anyhow::Error>((sent, retries))
        }));
    }

    let mut published = 0usize;
    let mut retries = 0u64;
    for task in tasks {
        let (sent, r) = task.await.context("publisher task")??;
        published += sent;
        retries += r;
    }
    let elapsed = started.elapsed();
    let msg_s = published as f64 / elapsed.as_secs_f64();
    let mb_s = msg_s * payload_bytes as f64 / 1_000_000.0;
    println!(
        "ingest: publishers = {publishers}, published = {published}, {msg_s:.0} msg/s, {mb_s:.1} MB/s, publish_retries = {retries}",
    );
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
    }));
    Ok(())
}

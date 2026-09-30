//! A consumer group draining a stream.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_wire::AckMode;

use super::connect::client;
use super::framing::{payload, read_header};
use super::{Common, is_retriable_transient};
use crate::stats::{Samples, emit_json, fmt_us};

/// Queue semantics: a consumer group draining a stream. Publish `warmup+total`
/// records into `stream`, then poll them through one consumer group and ack —
/// measuring enqueue-to-delivery latency (the record header's publish timestamp
/// against poll time, one clock) and the drain throughput. Redeliveries
/// (`attempts > 1`) are counted, not hidden. A cumulative ack of each batch's
/// highest offset finishes it, which is how a real drain settles a run of work.
pub(crate) async fn queue(common: &Common, stream: &str) -> Result<()> {
    let epoch = Instant::now();
    let group = "perf-cg";
    let shard = 0u32;
    let expected = (common.warmup + common.total) as u64;
    let warmup = common.warmup as u64;

    // Enqueue the whole run first (acked, so every record is durably in the log
    // before the drain we are measuring begins).
    let publisher_client = client(common, common.brokers[0]).await?;
    let publisher = publisher_client.publisher().await?;
    for seq in 0..expected {
        let body = payload(seq, epoch, common.payload_bytes);
        loop {
            match publisher
                .publish(
                    &common.tenant,
                    &common.namespace,
                    stream,
                    body.clone(),
                    AckMode::PerMessage,
                )
                .await
            {
                Ok(_) => break,
                Err(err) if is_retriable_transient(&err) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => return Err(err.context(format!("queue publish seq {seq}"))),
            }
        }
    }

    let mut samples = Samples::with_capacity(common.total);
    let mut seen = std::collections::HashSet::new();
    let mut redeliveries = 0u64;
    let started = Instant::now();

    // Only the shard leader serves its group; find it, and account for the
    // first batch it hands back — a poll leases records, so they must be acked.
    let mut consumer = None;
    for addr in &common.brokers {
        let candidate = client(common, *addr).await?;
        match candidate
            .group_poll_wait(
                &common.tenant,
                &common.namespace,
                stream,
                shard,
                group,
                512,
                Duration::from_secs(2),
            )
            .await
        {
            Ok(records) => {
                let max_off = account_group(
                    &records,
                    warmup,
                    epoch,
                    &mut samples,
                    &mut seen,
                    &mut redeliveries,
                );
                if !records.is_empty() {
                    candidate
                        .group_ack(
                            &common.tenant,
                            &common.namespace,
                            stream,
                            shard,
                            group,
                            max_off,
                        )
                        .await
                        .context("group ack")?;
                }
                consumer = Some(candidate);
                break;
            }
            Err(err) if is_not_leader(&err) => continue,
            Err(err) => return Err(err.context("group poll (owner probe)")),
        }
    }
    let consumer = consumer.context("no broker served the group")?;

    let mut idle = 0;
    while (seen.len() as u64) < expected {
        let records = consumer
            .group_poll_wait(
                &common.tenant,
                &common.namespace,
                stream,
                shard,
                group,
                512,
                Duration::from_secs(5),
            )
            .await
            .context("group poll")?;
        if records.is_empty() {
            idle += 1;
            if idle >= 3 {
                break; // nothing left after several bounded waits: drained
            }
            continue;
        }
        idle = 0;
        let max_off = account_group(
            &records,
            warmup,
            epoch,
            &mut samples,
            &mut seen,
            &mut redeliveries,
        );
        consumer
            .group_ack(
                &common.tenant,
                &common.namespace,
                stream,
                shard,
                group,
                max_off,
            )
            .await
            .context("group ack")?;
    }

    let elapsed = started.elapsed();
    let delivered = seen.len() as u64;
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        delivered as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let p = samples.percentiles();
    println!(
        "queue drain: delivered = {delivered}/{expected}, redeliveries = {redeliveries}, p50 = {}, p99 = {}, p999 = {}, throughput = {throughput:.1} msg/s",
        fmt_us(p.p50_us),
        fmt_us(p.p99_us),
        fmt_us(p.p999_us),
    );
    emit_json(&serde_json::json!({
        "scenario": "queue",
        "environment": common.environment,
        "stream": stream,
        "group": group,
        "payload_bytes": common.payload_bytes,
        "published": expected,
        "delivered": delivered,
        "redeliveries": redeliveries,
        "drain_throughput_msg_s": throughput,
        "delivery_latency_us": { "p50": p.p50_us, "p99": p.p99_us, "p999": p.p999_us, "max": p.max_us },
    }));
    Ok(())
}

/// True when a group poll was refused because this broker does not lead the
/// shard — the redirect a consumer follows to find the owner.
fn is_not_leader(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<felix_client::NotLeaderError>().is_some() {
        return true;
    }
    let text = format!("{err:#}").to_lowercase();
    // A broker that does not lead a shard refuses a group poll and names the
    // owner: "group poll not served: shard 0 of <stream> is served by <node>".
    text.contains("not leader")
        || text.contains("not the leader")
        || text.contains("does not lead")
        || text.contains("not served")
        || text.contains("served by")
}

/// Fold one batch of delivered records into the latency sample and the seen-set.
/// Deduplicates by header sequence (a redelivery is counted, never re-measured)
/// and returns the highest offset in the batch, which is what a cumulative ack
/// finishes. A free function so the drain loop can borrow `samples`/`seen`
/// mutably without closure gymnastics.
fn account_group(
    records: &[felix_wire::GroupRecord],
    warmup: u64,
    epoch: Instant,
    samples: &mut Samples,
    seen: &mut std::collections::HashSet<u64>,
    redeliveries: &mut u64,
) -> u64 {
    let mut max_off = 0u64;
    for rec in records {
        if rec.attempts > 1 {
            *redeliveries += 1;
        }
        if let Some((seq, t0)) = read_header(&rec.payload)
            && seen.insert(seq)
            && seq >= warmup
        {
            let now = epoch.elapsed().as_nanos() as u64;
            samples.record(Duration::from_nanos(now.saturating_sub(t0)));
        }
        if rec.offset > max_off {
            max_off = rec.offset;
        }
    }
    max_off
}

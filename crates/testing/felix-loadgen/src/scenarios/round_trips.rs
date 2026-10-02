//! The shared engine for request/response scenarios.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use felix_client::Client;

use super::Common;
use super::connect::client;
use crate::stats::{Percentiles, Samples, fmt_us, report};

/// Request/response scenarios share one engine: `concurrency` workers, each a
/// sequential loop of round trips against its own routed path.
pub(super) async fn round_trips<F, Fut>(
    common: &Common,
    label: &str,
    op: Arc<F>,
) -> Result<(Percentiles, f64, u64)>
where
    F: Fn(Arc<Client>, u64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let per_worker = (common.warmup + common.total).div_ceil(common.concurrency.max(1));
    let warmup_per_worker = common.warmup.div_ceil(common.concurrency.max(1));
    let started = Instant::now();
    let mut workers = Vec::new();
    for worker in 0..common.concurrency.max(1) {
        // One client per worker, spread across the brokers: the routed path —
        // possibly through a non-owner that forwards — is the deployment's
        // path, and measuring only the owner would flatter it.
        let addr = common.brokers[worker % common.brokers.len()];
        let client = Arc::new(client(common, addr).await?);
        let op = Arc::clone(&op);
        workers.push(tokio::spawn(async move {
            let mut samples = Samples::with_capacity(per_worker);
            for i in 0..per_worker {
                let key = (worker * per_worker + i) as u64;
                let at = Instant::now();
                op(Arc::clone(&client), key).await?;
                if i >= warmup_per_worker {
                    samples.record(at.elapsed());
                }
            }
            Ok::<Samples, anyhow::Error>(samples)
        }));
    }
    let mut all = Samples::default();
    for worker in workers {
        all.merge(worker.await.context("worker")??);
    }
    let elapsed = started.elapsed();
    let done = all.len() as u64;
    let throughput = (per_worker * common.concurrency.max(1)) as f64 / elapsed.as_secs_f64();
    let percentiles = all.percentiles();
    report!(
        common,
        "{label}: n = {done}, concurrency = {}, p50 = {}, p99 = {}, p999 = {}, max = {}, throughput = {throughput:.1} op/s",
        common.concurrency.max(1),
        fmt_us(percentiles.p50_us),
        fmt_us(percentiles.p99_us),
        fmt_us(percentiles.p999_us),
        fmt_us(percentiles.max_us),
    );
    Ok((percentiles, throughput, done))
}

//! Running a load phase, then waiting for resources to settle.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use felix_common::lifecycle::{DrainBudget, Readiness};

use crate::config::SoakConfig;
use crate::fixture::{AuthFixture, BrokerHarness};
use crate::load::{LoadStats, spawn_publishers, spawn_subscribers};
use crate::resources::ResourceSample;

/// How long a resource reading must hold steady before it counts as settled.
pub(crate) const SETTLE_POLL: Duration = Duration::from_millis(500);

pub(crate) const SETTLE_CONSECUTIVE: usize = 4;

/// Run one phase with resource sampling alongside the workload.
pub(crate) async fn run_phase<F, Fut>(
    name: &'static str,
    _config: &SoakConfig,
    workload: F,
) -> Result<PhaseReport>
where
    F: FnOnce(Arc<LoadStats>, Arc<AtomicBool>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    println!("\n[{name}]");
    let stats = Arc::new(LoadStats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn(sample_until(
        Arc::clone(&sampler_stop),
        Duration::from_millis(500),
    ));

    workload(Arc::clone(&stats), Arc::clone(&stop)).await?;

    sampler_stop.store(true, Ordering::Relaxed);
    let samples = sampler.await?;
    let report = PhaseReport {
        name,
        samples,
        published: stats.published.load(Ordering::Relaxed),
        received: stats.received.load(Ordering::Relaxed),
        errors: stats.publish_errors.load(Ordering::Relaxed)
            + stats.connect_errors.load(Ordering::Relaxed),
    };
    println!(
        "  published={} received={} errors={} peak_rss={} KiB peak_fds={} peak_tasks={}",
        report.published,
        report.received,
        report.errors,
        report.peak_rss_kb(),
        report.peak_fds(),
        report.peak_tasks()
    );
    Ok(report)
}

/// Run the *same* load several times and report peak RSS per cycle.
///
/// This is the check that actually distinguishes a leak from allocator
/// retention. A single load phase always leaves RSS elevated — allocators do not
/// return freed pages promptly, so that on its own proves nothing. A genuine
/// leak instead shows up as peak RSS climbing on every identical cycle, while
/// mere retention plateaus after the first.
pub(crate) async fn run_repeated_load_cycles(
    harness: &BrokerHarness,
    auth: &AuthFixture,
    config: &SoakConfig,
) -> Result<Vec<u64>> {
    println!("\n[repeated_load_cycles]");
    let mut peaks = Vec::new();
    let cycle_secs = (config.phase_secs / 2).max(3);
    for cycle in 0..config.load_cycles {
        let stats = Arc::new(LoadStats::default());
        let stop = Arc::new(AtomicBool::new(false));
        let sampler_stop = Arc::new(AtomicBool::new(false));
        let sampler = tokio::spawn(sample_until(
            Arc::clone(&sampler_stop),
            Duration::from_millis(250),
        ));

        let mut handles = spawn_subscribers(
            harness,
            auth,
            Arc::clone(&stats),
            Arc::clone(&stop),
            config.subscribers,
            false,
        )
        .await?;
        handles.extend(
            spawn_publishers(
                harness,
                auth,
                Arc::clone(&stats),
                Arc::clone(&stop),
                config.publishers,
                config.payload_bytes,
            )
            .await?,
        );
        tokio::time::sleep(Duration::from_secs(cycle_secs)).await;
        stop.store(true, Ordering::Relaxed);
        for handle in handles {
            let _ = handle.await;
        }
        // Let each cycle settle so the peak reflects the cycle, not the tail of
        // the previous one.
        tokio::time::sleep(Duration::from_secs(3)).await;
        sampler_stop.store(true, Ordering::Relaxed);

        let samples = sampler.await?;
        let peak = samples.iter().map(|s| s.rss_kb).max().unwrap_or(0);
        peaks.push(peak);
        if let Ok(dir) = std::env::var("SOAK_PROF_DIR")
            && matches!(cycle, 2 | 9 | 10 | 11 | 12)
        {
            crate::resources::dump_heap_profile(&format!("{dir}/heap.{cycle}.prof"));
        }
        let heap = crate::resources::heap_stats();
        let rss_now = crate::resources::read_rss_kb();
        let trimmed = if std::env::var_os("SOAK_TRIM_AFTER_CYCLE").is_some() {
            crate::resources::trim_heap();
            crate::resources::read_rss_kb()
        } else {
            0
        };
        println!(
            "  cycle {cycle}: published={} peak_rss={} KiB rss_end={} KiB heap_live={} KiB \
             heap_resident={} KiB rss_after_trim={} KiB",
            stats.published.load(Ordering::Relaxed),
            peak,
            rss_now,
            heap.live_kb,
            heap.resident_kb,
            trimmed
        );
    }
    Ok(peaks)
}

pub(crate) struct PhaseReport {
    pub(crate) name: &'static str,
    pub(crate) samples: Vec<ResourceSample>,
    pub(crate) published: u64,
    pub(crate) received: u64,
    pub(crate) errors: u64,
}

impl PhaseReport {
    pub(crate) fn peak_rss_kb(&self) -> u64 {
        self.samples.iter().map(|s| s.rss_kb).max().unwrap_or(0)
    }
    pub(crate) fn peak_fds(&self) -> u64 {
        self.samples.iter().map(|s| s.open_fds).max().unwrap_or(0)
    }
    pub(crate) fn peak_tasks(&self) -> usize {
        self.samples
            .iter()
            .map(|s| s.alive_tasks)
            .max()
            .unwrap_or(0)
    }
    pub(crate) fn last(&self) -> Option<&ResourceSample> {
        self.samples.last()
    }
}

/// Result of waiting for process resources to come to rest.
pub(crate) struct SettleOutcome {
    /// Whether the target was reached before the cap expired.
    pub(crate) settled: bool,
    pub(crate) waited: Duration,
    pub(crate) sample: ResourceSample,
}

/// Wait until the *idle broker* stops changing, then treat that as baseline.
///
/// # Why not just sample immediately
/// `start_broker` returns as soon as the listener is bound, but the runtime keeps
/// allocating for a moment afterwards — the accept task spawns, epoll registers,
/// timers arm. Sampling right then captures a baseline lower than the broker's
/// real idle state, which later makes an honest quiesced reading look like a leak.
/// This was visible across platforms: the same idle broker sampled 2 tasks on
/// Linux and 10 on macOS, purely from where the sample landed in startup.
pub(crate) async fn settle_to_stable(cap: Duration) -> SettleOutcome {
    let started = Instant::now();
    let mut previous = ResourceSample::capture();
    let mut stable = 0usize;
    while started.elapsed() < cap {
        tokio::time::sleep(SETTLE_POLL).await;
        let current = ResourceSample::capture();
        if current.open_fds == previous.open_fds && current.alive_tasks == previous.alive_tasks {
            stable += 1;
            if stable >= SETTLE_CONSECUTIVE {
                return SettleOutcome {
                    settled: true,
                    waited: started.elapsed(),
                    sample: current,
                };
            }
        } else {
            stable = 0;
        }
        previous = current;
    }
    SettleOutcome {
        settled: false,
        waited: started.elapsed(),
        sample: previous,
    }
}

/// Wait until process resources fall back to `baseline`, or the cap expires.
///
/// # Why polling rather than a fixed sleep
/// Teardown latency is not a constant. It varies with load, platform, and how
/// busy the machine is, so any fixed `--quiesce-secs` is either too short
/// somewhere (a false leak report) or wastes minutes everywhere. Measured
/// directly: an identical workload failed at a 15s sleep roughly half the time
/// and passed 3/3 at 60s. Polling makes the result depend on the system reaching
/// rest rather than on guessing how long that takes, and it returns as soon as it
/// does — so raising the cap costs nothing on a healthy run.
///
/// Reaching `<=` baseline is the success condition, not `==`: the broker may
/// legitimately hold fewer resources at rest than during startup.
pub(crate) async fn settle_to_baseline(baseline: &ResourceSample, cap: Duration) -> SettleOutcome {
    let started = Instant::now();
    let mut stable = 0usize;
    let mut last = ResourceSample::capture();
    while started.elapsed() < cap {
        if last.open_fds <= baseline.open_fds && last.alive_tasks <= baseline.alive_tasks {
            stable += 1;
            if stable >= SETTLE_CONSECUTIVE {
                return SettleOutcome {
                    settled: true,
                    waited: started.elapsed(),
                    sample: last,
                };
            }
        } else {
            stable = 0;
        }
        tokio::time::sleep(SETTLE_POLL).await;
        last = ResourceSample::capture();
    }
    SettleOutcome {
        settled: false,
        waited: started.elapsed(),
        sample: last,
    }
}

/// Sample resources on a fixed cadence until `stop` flips.
pub(crate) async fn sample_until(stop: Arc<AtomicBool>, interval: Duration) -> Vec<ResourceSample> {
    let mut samples = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        samples.push(ResourceSample::capture());
        tokio::time::sleep(interval).await;
    }
    // Always capture a final sample so a phase is never reported empty.
    samples.push(ResourceSample::capture());
    samples
}

pub(crate) async fn drain_broker(harness: BrokerHarness, deadline: Duration) -> Vec<&'static str> {
    // Exercise the same sequence main.rs uses, so the soak validates the real
    // drain rather than a bespoke teardown.
    let readiness = Readiness::ready();
    readiness.begin_draining();
    harness.accept_shutdown.cancel();

    let mut budget = DrainBudget::new(deadline);
    harness.connections.close();
    budget
        .drain("quic_connections", harness.connections.wait())
        .await;
    let mut accept_task = harness.accept_task;
    if !budget
        .drain("quic_accept_loop", async {
            let _ = (&mut accept_task).await;
        })
        .await
    {
        accept_task.abort();
    }
    budget.unfinished().to_vec()
}

#[cfg(test)]
mod tests;

//! Soak and resource-leak harness for the broker.
//!
//! Produces the empirical evidence for the broker's concurrency and
//! resource-leak criterion (#154). It drives the broker through sustained
//! load, connection churn, slow-subscriber saturation, and repeated process
//! restarts, sampling resource counters throughout, then checks that
//! everything returns to a steady-state envelope once load stops.
//!
//! # Why a binary rather than a test
//! A soak is a measurement, not an assertion about a single code path. It needs
//! to run for minutes, emit a time series, and produce a report a human reviews.
//! `cargo test` is the wrong shape for that. Regression tests for anything this
//! *finds* belong in the normal test suite.
//!
//! # What it exercises
//! - The real accept loop (`serving::quic::serve_with_shutdown`) and the real drain, not
//!   a synthetic shutdown future.
//! - Real QUIC connections over loopback, with the same auth path production
//!   uses (Ed25519-signed Felix tokens verified against a JWKS).
//! - A genuine `SIGTERM` to a genuine child process, which is the gap
//!   `services/felix-broker-service/tests/graceful_shutdown.rs` could not cover in-process.
//!
//! ```text
//! cargo run --release -p felix-broker-service --bin soak -- --duration-secs 60
//! cargo run --release -p felix-broker-service --bin soak -- --serve-child   # internal
//! ```
//! Exits non-zero if any steady-state check fails, so CI can gate on it.

#[cfg(all(unix, feature = "soak-jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod child;
mod config;
mod fixture;
mod load;
mod phase;
mod report;
mod resources;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::child::{run_restart_cycles, run_serve_child};
use crate::config::parse_args;
use crate::fixture::{build_auth_fixture, start_broker};
use crate::load::{run_connection_churn, spawn_publishers, spawn_subscribers};
use crate::phase::{
    drain_broker, run_phase, run_repeated_load_cycles, settle_to_baseline, settle_to_stable,
};
use crate::report::{SoakOutcome, evaluate, report, write_timeseries};

#[tokio::main]
async fn main() -> Result<()> {
    let (config, serve_child) = parse_args()?;
    if serve_child {
        return run_serve_child().await;
    }

    // A recorder must be installed for the broker's gauges to be readable.
    let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("install metrics recorder")?;

    println!("== Felix soak harness ==");
    println!(
        "phases {}s, settle cap {}s, {} publishers, {} subscribers, {} churn cycles, {} restart cycles",
        config.phase_secs,
        config.quiesce_secs,
        config.publishers,
        config.subscribers,
        config.churn_cycles,
        config.restart_cycles
    );

    let auth = build_auth_fixture()?;
    // Baseline is captured after the broker is listening *and* has come to rest,
    // so the listener socket and the runtime's own startup allocations are part
    // of the baseline rather than surfacing later as a phantom leak.
    let harness = start_broker(&auth).await?;
    let baseline_settle = settle_to_stable(Duration::from_secs(30)).await;
    let baseline = baseline_settle.sample;
    println!(
        "baseline (broker listening, no clients, settled in {:?}{}): rss={} KiB fds={} tasks={}",
        baseline_settle.waited,
        if baseline_settle.settled {
            ""
        } else {
            "; NOT STABLE"
        },
        baseline.rss_kb,
        baseline.open_fds,
        baseline.alive_tasks
    );

    let mut phases = Vec::new();

    // Phase 1 — sustained publish/subscribe load.
    phases.push(
        run_phase("sustained_load", &config, |stats, stop| {
            let harness = &harness;
            let auth = &auth;
            let config = &config;
            async move {
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
                tokio::time::sleep(Duration::from_secs(config.phase_secs)).await;
                stop.store(true, Ordering::Relaxed);
                for handle in handles {
                    let _ = handle.await;
                }
                Ok(())
            }
        })
        .await?,
    );

    // Phase 2 — connection churn.
    phases.push(
        run_phase("connection_churn", &config, |stats, stop| {
            let harness = &harness;
            let auth = &auth;
            let config = &config;
            async move {
                run_connection_churn(
                    harness,
                    auth,
                    Arc::clone(&stats),
                    config.churn_cycles,
                    config.payload_bytes,
                )
                .await?;
                stop.store(true, Ordering::Relaxed);
                Ok(())
            }
        })
        .await?,
    );

    // Phase 3 — slow subscribers driving queue saturation and the drop policy.
    phases.push(
        run_phase("slow_subscribers", &config, |stats, stop| {
            let harness = &harness;
            let auth = &auth;
            let config = &config;
            async move {
                let mut handles = spawn_subscribers(
                    harness,
                    auth,
                    Arc::clone(&stats),
                    Arc::clone(&stop),
                    config.subscribers,
                    true,
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
                tokio::time::sleep(Duration::from_secs(config.phase_secs)).await;
                stop.store(true, Ordering::Relaxed);
                for handle in handles {
                    let _ = handle.await;
                }
                Ok(())
            }
        })
        .await?,
    );

    // Phase 4 — identical repeated cycles, the actual memory-leak check.
    let cycle_peaks = run_repeated_load_cycles(&harness, &auth, &config).await?;

    // Phase 5 — wait for cleanup to actually finish, rather than sleeping a fixed
    // amount and hoping. `quiesce_secs` is the cap, not the wait.
    println!(
        "\n[quiesce] waiting for resources to settle (cap {}s)",
        config.quiesce_secs
    );
    let quiesce_settle =
        settle_to_baseline(&baseline, Duration::from_secs(config.quiesce_secs)).await;
    let quiesced = quiesce_settle.sample;
    let gauges = resources::scrape_gauges(&metrics.render());
    println!(
        "quiesced after {:?}{}: rss={} KiB fds={} tasks={}",
        quiesce_settle.waited,
        if quiesce_settle.settled {
            ""
        } else {
            " (CAP EXPIRED)"
        },
        quiesced.rss_kb,
        quiesced.open_fds,
        quiesced.alive_tasks
    );

    // Phase 6 — repeated real-process SIGTERM restarts under traffic.
    println!("\n[restart_cycles]");
    let restart_findings = run_restart_cycles(&config, &auth).await?;

    let unfinished = drain_broker(harness, Duration::from_secs(20)).await;

    let outcome = SoakOutcome {
        baseline,
        quiesced,
        phases,
        gauges,
        restart_findings,
        unfinished,
        cycle_peaks,
        settled: quiesce_settle.settled,
    };
    let findings = evaluate(&config, &outcome);
    report(&outcome, &findings);

    if let Some(path) = &config.timeseries_path {
        write_timeseries(path, &outcome.phases)
            .with_context(|| format!("write timeseries to {path}"))?;
        println!("\ntime series written to {path}");
    }

    if findings.is_empty() {
        Ok(())
    } else {
        bail!("{} soak finding(s); see report above", findings.len())
    }
}

#[cfg(test)]
mod tests;

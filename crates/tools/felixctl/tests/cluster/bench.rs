//! `felixctl bench` runs felix-loadgen's scenarios and summarizes them.
//!
//! Shapes only, never magnitudes: this is loopback on a shared CI machine.

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

use crate::Env;

fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new("perf", 1)],
        caches: vec![CacheSpec::new("perf", 1)],
        ..Default::default()
    }
}

#[tokio::test]
#[serial]
async fn bench_cache_and_ingest_report_rates_and_latency() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let env = Env::new(&cluster);

    let run = env
        .felixctl(
            &cluster,
            &[
                "bench",
                "cache",
                "perf",
                "--total",
                "200",
                "--warmup",
                "20",
                "--concurrency",
                "2",
                "--json",
            ],
        )
        .await
        .ok();
    let report = run.json();
    assert_eq!(report["bench"], "cache");
    assert_eq!(report["result"]["scenario"], "cache");
    assert_eq!(report["result"]["environment"], "felix-cli");
    for line in report["summary"].as_array().unwrap() {
        assert!(line["rate"].as_f64().unwrap_or(0.0) > 0.0, "{report}");
        assert!(line["p50_us"].as_u64().unwrap_or(0) > 0, "{report}");
    }

    let run = env
        .felixctl(
            &cluster,
            &[
                "bench",
                "ingest",
                "perf",
                "--total",
                "2000",
                "--batch",
                "16",
                "--concurrency",
                "2",
            ],
        )
        .await
        .ok();
    assert!(run.stdout.starts_with("ingest\n"), "{}", run.stdout);
    assert!(run.stdout.contains("msg/s"), "{}", run.stdout);
}

/// `#[ignore]` for the reason felix-loadgen's own pubsub test is: an acked
/// publish loop can overrun the harness broker's small ingress queue under
/// loopback burst. Run with `--ignored` when changing the summary.
#[tokio::test]
#[serial]
#[ignore = "flaky against the in-process harness under loopback burst, like the loadgen's pubsub test"]
async fn bench_latency_and_fanout_report_delivery() {
    let cluster = Cluster::start(config()).await.expect("start cluster");
    let env = Env::new(&cluster);
    for (bench, extra) in [("latency", None), ("fanout", Some("3"))] {
        let mut args = vec![
            "bench", bench, "perf", "--total", "300", "--warmup", "30", "--json",
        ];
        if let Some(fanout) = extra {
            args.extend(["--fanout", fanout]);
        }
        let run = env.felixctl(&cluster, &args).await.ok();
        let report = run.json();
        assert_eq!(report["result"]["unaccounted"], 0, "{report}");
        assert!(
            report["summary"][1]["p50_us"].as_u64().unwrap_or(0) > 0,
            "{report}"
        );
    }
}

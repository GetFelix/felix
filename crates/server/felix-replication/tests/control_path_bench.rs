//! Scratch microbench: fence round trips over the real peer transport, for the
//! listener's control-stream path (#1796 follow-up). Not part of the PR.
//!
//! The server runs on its own 4-worker runtime, as a broker would; the
//! requester on a second runtime. Each scenario prints one `BENCH_JSON` line.

use std::sync::Arc;
use std::time::{Duration, Instant};

use felix_replication::peer::{PeerPool, PeerRequestHandler, PeerServer, PeerTransportConfig};
use felix_wire::internal::{Fence, FenceOk, InternalMessage, ReplicaLog, ShardRef};
use tokio_util::sync::CancellationToken;

struct Immediate;

#[async_trait::async_trait]
impl PeerRequestHandler for Immediate {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        InternalMessage::FenceOk(FenceOk {
            correlation_id: request.correlation_id(),
            log_end: 7,
            commit_offset: 7,
            last_generation: 1,
        })
    }
}

fn fence() -> InternalMessage {
    InternalMessage::Fence(Fence {
        correlation_id: 0,
        shard: ShardRef {
            tenant_id: "t1".into(),
            namespace: "ns".into(),
            stream: "cache".into(),
            shard: 0,
            generation: 1,
            incarnation: None,
        },
        log: ReplicaLog::Stream,
        election: None,
    })
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    let i = ((sorted.len() as f64 - 1.0) * p / 100.0).round() as usize;
    sorted[i] as f64 / 1000.0
}

#[test]
#[ignore = "measurement"]
fn fence_round_trips() {
    let side = std::env::var("BENCH_SIDE").unwrap_or_else(|_| "?".into());
    let round = std::env::var("BENCH_ROUND").unwrap_or_else(|_| "?".into());
    let n: usize = std::env::var("BENCH_FENCES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40_000);
    let config = PeerTransportConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        request_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let client_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let addr = server_rt.block_on(async {
        let server = PeerServer::bind("server".into(), &config, Arc::new(Immediate)).unwrap();
        let addr = server.local_addr().unwrap();
        tokio::spawn(server.serve(shutdown.clone()));
        addr
    });

    for (scenario, requesters, busy) in [
        ("serial", 1usize, 0usize),
        ("c4", 4, 0),
        ("serial_busy", 1, 8),
        ("c4_busy", 4, 8),
    ] {
        // Tasks that keep the server's workers busy, yielding each turn, so a
        // spawned answer queues behind them as on a loaded broker.
        let stop_busy = CancellationToken::new();
        for _ in 0..busy {
            let stop = stop_busy.clone();
            server_rt.spawn(async move {
                while !stop.is_cancelled() {
                    std::hint::black_box((0..2_000u64).fold(0u64, |a, b| a.wrapping_mul(31) ^ b));
                    tokio::task::yield_now().await;
                }
            });
        }
        let config = config.clone();
        let (mut samples, elapsed) = client_rt.block_on(async move {
            let pool = PeerPool::new("client".into(), config, CancellationToken::new()).unwrap();
            pool.capabilities("server", addr).await.expect("handshake");
            for _ in 0..2_000 {
                pool.request("server", addr, fence()).await.expect("warmup");
            }
            let started = Instant::now();
            let mut tasks = Vec::new();
            for _ in 0..requesters {
                let pool = Arc::clone(&pool);
                tasks.push(tokio::spawn(async move {
                    let mut out = Vec::with_capacity(n / requesters);
                    for _ in 0..n / requesters {
                        let t = Instant::now();
                        let answer = pool.request("server", addr, fence()).await.expect("fence");
                        assert!(matches!(answer, InternalMessage::FenceOk(_)));
                        out.push(t.elapsed().as_nanos() as u64);
                    }
                    out
                }));
            }
            let mut all = Vec::new();
            for task in tasks {
                all.extend(task.await.unwrap());
            }
            let elapsed = started.elapsed();
            pool.shutdown().await;
            (all, elapsed)
        });
        stop_busy.cancel();
        samples.sort_unstable();
        let mean = samples.iter().sum::<u64>() as f64 / samples.len() as f64 / 1000.0;
        println!(
            "BENCH_JSON {{\"side\":\"{side}\",\"round\":\"{round}\",\"scenario\":\"{scenario}\",\"n\":{},\"rps\":{:.0},\"mean_us\":{mean:.2},\"p50_us\":{:.2},\"p90_us\":{:.2},\"p99_us\":{:.2},\"p999_us\":{:.2}}}",
            samples.len(),
            samples.len() as f64 / elapsed.as_secs_f64(),
            pct(&samples, 50.0),
            pct(&samples, 90.0),
            pct(&samples, 99.0),
            pct(&samples, 99.9),
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    shutdown.cancel();
}

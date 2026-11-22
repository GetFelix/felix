//! A measurement, not a test: process CPU per `write_many` call against the
//! entries per call, on one single-member group, through the propose path's
//! own `write` and responders. Ignored; run it alone:
//!
//! BENCH_N=16 BENCH_FSYNC=oncommit|periodic BENCH_INFLIGHT=1 BENCH_SECS=8 \
//!   cargo test --release -p felix-replication --features raft --lib \
//!   raft::tests::bench::write_many_cost -- --ignored --nocapture

use std::collections::VecDeque;
use std::time::Instant;

use openraft::ServerState;
use tokio::sync::oneshot;

use super::super::propose::{respond, write};
use super::*;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn cpu_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the struct it is handed.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let ns = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    ns(usage.ru_utime) + ns(usage.ru_stime)
}

fn command(payload: &Bytes) -> Command {
    Command {
        timestamp_micros: 1_700_000_000,
        payload: payload.clone(),
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        group: false,
        publisher: None,
        headers: None,
        written: None,
        cond: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement; run alone with --ignored"]
async fn write_many_cost() {
    let entries = env("BENCH_N", 1) as usize;
    let inflight = env("BENCH_INFLIGHT", 1) as usize;
    let secs = env("BENCH_SECS", 8);
    let periodic = std::env::var("BENCH_FSYNC").as_deref() == Ok("periodic");
    let cluster = Cluster::new(true);
    let root = cluster.root.path().join("a");
    std::fs::create_dir_all(&root).expect("root");
    let config = if periodic {
        felix_storage::run_state::begin(&root, true, None, None, || false, || [7; 16])
            .expect("a relaxed run");
        LogConfig {
            fsync_mode: FsyncMode::Periodic {
                interval: Duration::from_millis(10),
            },
            ..log_config()
        }
    } else {
        log_config()
    };
    let runtime = cluster.runtime("a");
    runtime
        .open(group(0), 1, open_log(cluster.root.path(), "a", 0, config))
        .await
        .expect("open");
    runtime
        .initialize(&shard(0), [MemberId::new("a", 1)].into_iter().collect())
        .await
        .expect("initialize");
    let member = runtime.group(&shard(0)).expect("group");
    let raft = member.raft().clone();
    raft.wait(Some(PATIENCE))
        .state(ServerState::Leader, "lead")
        .await
        .expect("leader");
    let payload = Bytes::from(vec![7u8; 256]);

    let run = |until: Duration| {
        let raft = raft.clone();
        let payload = payload.clone();
        async move {
            let start = Instant::now();
            let mut calls = 0u64;
            let mut pending = VecDeque::new();
            while start.elapsed() < until {
                let (tx, rx) = oneshot::channel();
                let commands = (0..entries).map(|_| command(&payload)).collect();
                write(&raft, commands, respond(vec![(entries, tx)], false), None)
                    .await
                    .expect("write");
                pending.push_back(rx);
                calls += 1;
                while pending.len() >= inflight {
                    let rx = pending.pop_front().expect("pending");
                    rx.await.expect("answer").expect("written");
                }
            }
            for rx in pending {
                rx.await.expect("answer").expect("written");
            }
            (calls, start.elapsed())
        }
    };
    run(Duration::from_secs(1)).await;
    let cpu = cpu_ns();
    let (calls, wall) = run(Duration::from_secs(secs)).await;
    let cpu = cpu_ns() - cpu;
    println!(
        "BENCH entries={entries} inflight={inflight} fsync={} calls={calls} cpu_ns_per_call={} \
         cpu_ns_per_entry={} wall_us_per_call={:.1}",
        if periodic { "periodic" } else { "oncommit" },
        cpu / calls,
        cpu / (calls * entries as u64),
        wall.as_secs_f64() * 1e6 / calls as f64,
    );
}

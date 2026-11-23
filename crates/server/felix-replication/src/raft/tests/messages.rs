//! Raft messages per acknowledged record under a steady load across
//! several groups, counted at the transport. A measurement, ignored by
//! default:
//!
//! ```text
//! cargo test -p felix-replication --features raft,testing,felix-common/sim \
//!     --release --lib raft::tests::messages -- --ignored --nocapture
//! ```
//!
//! A heartbeat batch counts as one message. Every group is led by broker
//! a, so a cross-group batch would find more company here than on a
//! cluster with leaders spread out.

use std::sync::atomic::Ordering::Relaxed;

use futures::StreamExt;
use futures::stream::FuturesUnordered;

use super::*;
use crate::raft::{AckAt, Batch, RaftEngine, Record};

const GROUPS: u32 = 6;
const PER_WRITE: usize = 16;

fn batch(payload: &Bytes, ack: AckAt) -> Batch {
    Batch {
        records: (0..PER_WRITE)
            .map(|_| Record {
                timestamp_micros: 1_700_000_000,
                payload: payload.clone(),
                headers: None,
            })
            .collect(),
        ack,
        term: None,
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        publisher: None,
    }
}

async fn quorum_write(engine: RaftEngine, n: u32, payload: Bytes, ack: AckAt) {
    let written = engine
        .propose(&shard(n), batch(&payload, ack))
        .await
        .expect("written");
    engine
        .await_quorum_ack(&shard(n), written, ack, PATIENCE)
        .await
        .expect("acked");
}

/// Messages per record while `in_flight` Quorum writes of 16 records each
/// run across six groups, all led by broker a.
async fn measure(relaxed: bool, in_flight: usize, writes: usize) {
    let mut cluster = Cluster::new(true);
    let all: &[&str] = if relaxed { &BROKERS } else { &[] };
    for n in 0..GROUPS {
        cluster.form_relaxed(n, all).await;
    }
    let ack = if relaxed {
        AckAt::Written
    } else {
        AckAt::Flushed
    };
    let engine = RaftEngine::new(cluster.runtime("a"));
    let payload = Bytes::from(vec![7u8; 1024]);
    let run = |count: usize| {
        let engine = engine.clone();
        let payload = payload.clone();
        async move {
            let mut next = 0u32;
            let mut flying = FuturesUnordered::new();
            for _ in 0..in_flight.min(count) {
                flying.push(quorum_write(
                    engine.clone(),
                    next % GROUPS,
                    payload.clone(),
                    ack,
                ));
                next += 1;
            }
            let mut started = flying.len();
            while flying.next().await.is_some() {
                if started < count {
                    flying.push(quorum_write(
                        engine.clone(),
                        next % GROUPS,
                        payload.clone(),
                        ack,
                    ));
                    next += 1;
                    started += 1;
                }
            }
        }
    };
    run(200).await;
    let net = &cluster.net;
    let (messages, appends, empty, written) = (
        net.messages(),
        net.appends.load(Relaxed),
        net.empty_appends.load(Relaxed),
        net.written_reports.lock().len(),
    );
    let (began, spent) = (std::time::Instant::now(), cpu());
    run(writes).await;
    let (elapsed, spent) = (began.elapsed(), cpu() - spent);
    let records = (writes * PER_WRITE) as f64;
    println!(
        "relaxed {relaxed}, {in_flight} in flight: {:.4} messages, {:.4} appends, {:.4} empty appends, {:.4} written reports per record; {:.2} us CPU per record; {:.0} records/s",
        (net.messages() - messages) as f64 / records,
        (net.appends.load(Relaxed) - appends) as f64 / records,
        (net.empty_appends.load(Relaxed) - empty) as f64 / records,
        (net.written_reports.lock().len() - written) as f64 / records,
        spent.as_secs_f64() * 1e6 / records,
        records / elapsed.as_secs_f64(),
    );
    cluster.stop().await;
}

/// User plus system CPU this process has used.
fn cpu() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the struct it is handed.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let time = |t: libc::timeval| {
        Duration::from_secs(t.tv_sec as u64) + Duration::from_micros(t.tv_usec as u64)
    };
    time(usage.ru_utime) + time(usage.ru_stime)
}

/// One run, as `FELIX_CPU_RELAXED` (default 1) and `FELIX_CPU_IN_FLIGHT`
/// (default 32) say, for setting beside `driver::tests::cpu`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement"]
async fn cpu_per_record() {
    let relaxed = std::env::var("FELIX_CPU_RELAXED").map_or(true, |v| v == "1");
    let in_flight = std::env::var("FELIX_CPU_IN_FLIGHT").map_or(32, |v| v.parse().expect("n"));
    measure(relaxed, in_flight, 3_000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement"]
async fn messages_per_record() {
    for relaxed in [false, true] {
        for in_flight in [1, 32] {
            measure(relaxed, in_flight, 3_000).await;
        }
    }
}

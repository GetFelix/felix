//! What idle Raft groups cost a broker in CPU, measured two ways.
//!
//! - Stub peers (the default): one runtime leads every group and its peers
//!   are stubs that agree, so each group pays a leader's timers and
//!   heartbeats and nothing else. The worst case for one broker.
//! - `RAFT_IDLE_PEERS=real`: three runtimes in one process, every group on
//!   all three, each leading a third. A broker then pays for the groups it
//!   leads and for answering the heartbeats of the ones it follows. The
//!   process's CPU is divided by three. There is no network in it.
//! - `RAFT_IDLE_PEERS=transport`: the same three runtimes, each sending
//!   through its own peer pool to the others' peer listeners on loopback, so
//!   the figure includes framing, QUIC and the listener.
//!
//! Ignored by default, since it is a measurement. Run it optimised:
//!
//! ```text
//! RAFT_IDLE_GROUPS=1500 RAFT_IDLE_PEERS=real cargo test --release \
//!     -p felix-replication --test raft_idle_cpu -- --ignored --nocapture
//! ```
//!
//! `RAFT_IDLE_HEARTBEAT_MS` sets the idle heartbeat and
//! `RAFT_IDLE_WINDOW_SECS` how long the measurement runs (30 s).
//!
//! On Linux each run also prints the process's resident memory, divided by
//! three. `RAFT_IDLE_PEERS=none` opens the same logs with no Raft member on
//! them and writes two records to each, as a new member does, which is the
//! figure to take off: what is left is what Raft adds.
//!
//! `RAFT_IDLE_BURST_SECS` adds a write burst after the idle window: every
//! leader takes pipelined writes for that long (`RAFT_IDLE_BURST_DEPTH` in
//! flight per group, 32), then the groups go idle again. The run prints
//! resident memory at the end of the burst and once idle, which is what a
//! member keeps after it has been busy. Use fewer groups for it: every log
//! then holds its newest records in memory.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use felix_replication::peer::{PeerPool, PeerServer, PeerTransportConfig, UnavailableHandler};
use felix_replication::raft::{
    AckAt, AgreeingPeers, Batch, GroupId, GroupRuntime, MemberId, PeerAddresses, PeerTransport,
    RaftEngine, RaftReply, RaftRequest, RaftTransport, Record, Refused, Role, RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::DiskLog;
use felix_storage::log::{AppendOnlyLog, AppendRecord, FsyncMode, LogConfig, RecordMark};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio_util::sync::CancellationToken;

fn shard(n: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: n,
        kind: ShardKind::Stream,
    }
}

/// CPU time this process has used, user and system.
fn cpu() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` fills the struct it is given.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let time = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    time(usage.ru_utime) + time(usage.ru_stime)
}

/// Resident memory of this process in megabytes, where `/proc` says.
fn resident_mb() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1024.0)
}

const BROKERS: [&str; 3] = ["a", "b", "c"];

/// The logs of `groups` shards on three brokers, written to and left open
/// with no Raft member on them.
async fn logs_alone(groups: u32, config: LogConfig) {
    let root = tempfile::tempdir().expect("dir");
    let mut logs = Vec::new();
    for n in 0..groups {
        for broker in BROKERS {
            let dir = root.path().join(broker).join(n.to_string());
            std::fs::create_dir_all(&dir).expect("dir");
            let log = DiskLog::open(dir, format!("t/ns/orders/{n}"), config.clone()).expect("log");
            for _ in 0..2 {
                let record = AppendRecord {
                    payload: bytes::Bytes::from_static(b"record"),
                    timestamp_micros: 1_700_000_000,
                    mark: RecordMark::None,
                    publisher: None,
                    headers: None,
                };
                log.append(&[record]).await.expect("append");
            }
            logs.push(log);
        }
    }
    tokio::time::sleep(Duration::from_secs(15)).await;
    if let Some(resident) = resident_mb() {
        println!(
            "{groups} shards, logs alone: {:.0} MB resident per broker",
            resident / BROKERS.len() as f64
        );
    }
}

/// Hands a message to the runtime of the broker it names.
#[derive(Default)]
struct Net {
    brokers: parking_lot::Mutex<HashMap<String, GroupRuntime>>,
}

#[async_trait]
impl RaftTransport for Net {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let runtime = self.brokers.lock().get(&*request.to().broker).cloned();
        match runtime {
            Some(runtime) => runtime.handle(request).await,
            None => Err(Refused::Unreachable("no such broker".into())),
        }
    }
}

/// Three runtimes joined by real peer pools and listeners on loopback.
async fn over_the_transport(timers: &RuntimeConfig, stop: &CancellationToken) -> Vec<GroupRuntime> {
    let config = PeerTransportConfig {
        bind: "127.0.0.1:0".parse().expect("addr"),
        ..Default::default()
    };
    let book: Arc<parking_lot::Mutex<HashMap<String, SocketAddr>>> = Arc::default();
    let mut runtimes = Vec::new();
    for broker in BROKERS {
        let pool = PeerPool::new(broker.into(), config.clone(), stop.clone()).expect("pool");
        let addresses: Arc<dyn PeerAddresses> = {
            let book = Arc::clone(&book);
            Arc::new(move |broker: &str| book.lock().get(broker).copied())
        };
        let transport = Arc::new(PeerTransport::new(pool, addresses));
        let runtime = GroupRuntime::start(broker, timers.clone(), transport).expect("timers");
        let server = PeerServer::bind(broker.into(), &config, Arc::new(UnavailableHandler))
            .expect("bind")
            .with_raft(Arc::new(runtime.clone()));
        book.lock()
            .insert(broker.into(), server.local_addr().expect("addr"));
        tokio::spawn(server.serve(stop.clone()));
        runtimes.push(runtime);
    }
    runtimes
}

/// Keep `depth` writes in flight on every group `runtime` leads until
/// `length` has passed. Returns the writes that were acknowledged.
async fn burst(runtime: &GroupRuntime, groups: u32, depth: usize, length: Duration) -> u64 {
    let engine = RaftEngine::new(runtime.clone());
    let deadline = tokio::time::Instant::now() + length;
    let led =
        (0..groups).filter(|n| runtime.status(&shard(*n)).map(|s| s.role) == Some(Role::Leader));
    let writers: Vec<_> = led
        .map(|n| {
            let engine = engine.clone();
            tokio::spawn(async move {
                let write = || {
                    engine.propose(
                        &shard(n),
                        Batch {
                            records: vec![Record {
                                timestamp_micros: 1_700_000_000,
                                payload: Bytes::from_static(&[7; 100]),
                            }],
                            ack: AckAt::Written,
                        },
                    )
                };
                let mut flying: FuturesUnordered<_> = (0..depth).map(|_| write()).collect();
                let mut written = 0u64;
                while let Some(result) = flying.next().await {
                    // A leader too loaded to hear a majority in time refuses
                    // the write, or loses the group. Either is part of a
                    // burst on a small machine; give it a moment.
                    match result {
                        Ok(_) => written += 1,
                        Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                    }
                    if tokio::time::Instant::now() < deadline {
                        flying.push(write());
                    }
                }
                written
            })
        })
        .collect();
    let mut written = 0;
    for writer in writers {
        written += writer.await.expect("writer");
    }
    written
}

fn env<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement; see the module docs"]
async fn idle_groups_cost_little_cpu() {
    let groups: u32 = env("RAFT_IDLE_GROUPS").unwrap_or(1500);
    let peers = std::env::var("RAFT_IDLE_PEERS").unwrap_or_default();
    let transport = peers == "transport";
    let real = peers == "real" || transport;
    let mut timers = RuntimeConfig::default();
    if let Some(millis) = env("RAFT_IDLE_HEARTBEAT_MS") {
        timers.idle_heartbeat = Duration::from_millis(millis);
    }
    if std::env::var("RAFT_IDLE_PEERS").as_deref() == Ok("none") {
        let plain = LogConfig {
            fsync_mode: FsyncMode::OnCommit,
            preallocate_segments: false,
            ..LogConfig::default()
        };
        return logs_alone(groups, plain).await;
    }
    let root = tempfile::tempdir().expect("dir");
    let net = Arc::new(Net::default());
    let brokers: &[&str] = if real { &BROKERS } else { &BROKERS[..1] };
    let stop = CancellationToken::new();
    let runtimes: Vec<GroupRuntime> = if transport {
        over_the_transport(&timers, &stop).await
    } else {
        brokers
            .iter()
            .map(|broker| {
                let transport: Arc<dyn RaftTransport> = match real {
                    true => net.clone(),
                    false => Arc::new(AgreeingPeers),
                };
                let runtime =
                    GroupRuntime::start(*broker, timers.clone(), transport).expect("timers");
                net.brokers.lock().insert((*broker).into(), runtime.clone());
                runtime
            })
            .collect()
    };
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };
    let members: BTreeSet<_> = BROKERS.map(|b| MemberId::new(b, 1)).into();
    let led = |n: u32| {
        runtimes
            .iter()
            .any(|runtime| runtime.status(&shard(n)).map(|s| s.role) == Some(Role::Leader))
    };

    for n in 0..groups {
        for (broker, runtime) in brokers.iter().zip(&runtimes) {
            let dir = root.path().join(broker).join(n.to_string());
            std::fs::create_dir_all(&dir).expect("dir");
            let log = DiskLog::open(dir, format!("t/ns/orders/{n}"), config.clone()).expect("log");
            let group = GroupId::new(shard(n), 0);
            runtime.open(group, 1, log).await.expect("open");
        }
        // The member told first stands at once and wins, so leaders spread
        // evenly over the brokers.
        let first = n as usize % runtimes.len();
        runtimes[first]
            .initialize(&shard(n), members.clone())
            .await
            .expect("initialize");
        while !led(n) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        for runtime in &runtimes {
            runtime
                .initialize(&shard(n), members.clone())
                .await
                .expect("initialize");
        }
    }
    let leaders = |runtime: &GroupRuntime| {
        (0..groups)
            .filter(|n| runtime.status(&shard(*n)).map(|s| s.role) == Some(Role::Leader))
            .count() as u32
    };
    let leading = || -> Vec<u32> { runtimes.iter().map(leaders).collect() };
    // Let the first entries commit and every group go idle.
    tokio::time::sleep(timers.idle_after + Duration::from_secs(10)).await;

    let window = Duration::from_secs(env("RAFT_IDLE_WINDOW_SECS").unwrap_or(30));
    let ticks = || -> u64 {
        runtimes
            .iter()
            .flat_map(|runtime| (0..groups).filter_map(|n| runtime.status(&shard(n))))
            .map(|status| status.ticks)
            .sum()
    };
    let terms = || -> u64 {
        (0..groups)
            .filter_map(|n| runtimes[0].status(&shard(n)))
            .map(|status| status.term)
            .sum()
    };
    let before = leading();
    let (cpu_before, ticks_before, terms_before) = (cpu(), ticks(), terms());
    let resident_before = resident_mb();
    tokio::time::sleep(window).await;
    let used = cpu() - cpu_before;
    let sent = ticks() - ticks_before;
    let each = runtimes.len() as f64;
    println!(
        "{groups} idle groups, {} peers, idle heartbeat {:?}, leaders per broker {before:?}: \
         {:.2}% of one core per broker, {:.0} timer checks a second per broker",
        if transport {
            "transport"
        } else if real {
            "real"
        } else {
            "stub"
        },
        timers.idle_heartbeat,
        100.0 * used.as_secs_f64() / window.as_secs_f64() / each,
        sent as f64 / window.as_secs_f64() / each,
    );
    if let (Some(start), Some(end)) = (resident_before, resident_mb()) {
        println!(
            "{groups} idle groups: {:.0} MB resident per broker, {:.0} MB {window:?} earlier",
            end / each,
            start / each
        );
    }
    assert_eq!(leading(), before, "a group changed leader while idle");
    assert_eq!(terms(), terms_before, "an election ran while idle");
    assert_eq!(before.iter().sum::<u32>(), groups, "a group has no leader");

    if let Some(secs) = env::<u64>("RAFT_IDLE_BURST_SECS") {
        let depth = env("RAFT_IDLE_BURST_DEPTH").unwrap_or(32);
        let idle = resident_mb();
        let calls = || -> u64 {
            runtimes
                .iter()
                .flat_map(|runtime| (0..groups).filter_map(|n| runtime.status(&shard(n))))
                .map(|status| status.client_writes)
                .sum()
        };
        let calls_before = calls();
        let length = Duration::from_secs(secs);
        let written: u64 = futures::future::join_all(
            runtimes
                .iter()
                .map(|runtime| burst(runtime, groups, depth, length)),
        )
        .await
        .into_iter()
        .sum();
        let busy = resident_mb();
        let calls = calls() - calls_before;
        tokio::time::sleep(timers.idle_after + Duration::from_secs(20)).await;
        let members = f64::from(groups) * each;
        if let (Some(idle), Some(busy), Some(after)) = (idle, busy, resident_mb()) {
            println!(
                "{groups} groups after a {length:?} burst, {} writes in {} calls per group: \
                 {:.0} MB resident per broker at its end and {:.0} MB once idle again, \
                 {:.0} KB per member over the {:.0} MB before it",
                written / u64::from(groups),
                calls / u64::from(groups),
                busy / each,
                after / each,
                1024.0 * (after - idle) / members,
                idle / each,
            );
        }
        assert_eq!(
            leading().iter().sum::<u32>(),
            groups,
            "a group has no leader"
        );
    }
    for runtime in &runtimes {
        runtime.shutdown().await;
    }
    stop.cancel();
}

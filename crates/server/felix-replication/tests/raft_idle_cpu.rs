//! What idle Raft groups cost a broker in CPU, measured two ways.
//!
//! - Stub peers (the default): one runtime leads every group and its peers
//!   are stubs that agree, so each group pays a leader's timers and
//!   heartbeats and nothing else. The worst case for one broker.
//! - `RAFT_IDLE_PEERS=real`: three runtimes in one process, every group on
//!   all three, each leading a third. A broker then pays for the groups it
//!   leads and for answering the heartbeats of the ones it follows. The
//!   process's CPU is divided by three. There is no network in it.
//!
//! Ignored by default, since it is a measurement. Run it optimised:
//!
//! ```text
//! RAFT_IDLE_GROUPS=1500 RAFT_IDLE_PEERS=real cargo test --release \
//!     -p felix-replication --test raft_idle_cpu -- --ignored --nocapture
//! ```
//!
//! `RAFT_IDLE_HEARTBEAT_MS` sets the idle heartbeat.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use felix_replication::raft::{
    AgreeingPeers, GroupId, GroupRuntime, MemberId, RaftReply, RaftRequest, RaftTransport, Refused,
    Role, RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};

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

const BROKERS: [&str; 3] = ["a", "b", "c"];

/// Hands a message to the runtime of the broker it names.
#[derive(Default)]
struct Net {
    brokers: parking_lot::Mutex<HashMap<String, GroupRuntime>>,
}

#[async_trait]
impl RaftTransport for Net {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let runtime = self.brokers.lock().get(&request.to().broker).cloned();
        match runtime {
            Some(runtime) => runtime.handle(request).await,
            None => Err(Refused::Unreachable("no such broker".into())),
        }
    }
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
    let real = std::env::var("RAFT_IDLE_PEERS").as_deref() == Ok("real");
    let mut timers = RuntimeConfig::default();
    if let Some(millis) = env("RAFT_IDLE_HEARTBEAT_MS") {
        timers.idle_heartbeat = Duration::from_millis(millis);
    }
    let root = tempfile::tempdir().expect("dir");
    let net = Arc::new(Net::default());
    let brokers: &[&str] = if real { &BROKERS } else { &BROKERS[..1] };
    let runtimes: Vec<GroupRuntime> = brokers
        .iter()
        .map(|broker| {
            let transport: Arc<dyn RaftTransport> = match real {
                true => net.clone(),
                false => Arc::new(AgreeingPeers),
            };
            let runtime = GroupRuntime::start(*broker, timers.clone(), transport).expect("timers");
            net.brokers.lock().insert((*broker).into(), runtime.clone());
            runtime
        })
        .collect();
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
            let group = GroupId {
                shard: shard(n),
                epoch: 0,
            };
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

    let window = Duration::from_secs(30);
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
    tokio::time::sleep(window).await;
    let used = cpu() - cpu_before;
    let sent = ticks() - ticks_before;
    let each = runtimes.len() as f64;
    println!(
        "{groups} idle groups, {} peers, idle heartbeat {:?}, leaders per broker {before:?}: \
         {:.2}% of one core per broker, {:.0} timer checks a second per broker",
        if real { "real" } else { "stub" },
        timers.idle_heartbeat,
        100.0 * used.as_secs_f64() / window.as_secs_f64() / each,
        sent as f64 / window.as_secs_f64() / each,
    );
    assert_eq!(leading(), before, "a group changed leader while idle");
    assert_eq!(terms(), terms_before, "an election ran while idle");
    assert_eq!(before.iter().sum::<u32>(), groups, "a group has no leader");
    for runtime in &runtimes {
        runtime.shutdown().await;
    }
}

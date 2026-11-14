//! How long many Raft groups take to elect when they all lose their leader
//! in the same moment, and how many terms that costs.
//!
//! Three runtimes in one process, every group on all three. Broker `a`
//! leads every group and is then shut down, so each group is left with two
//! voters that must agree. With `RAFT_STORM_MODE=form` the groups are
//! instead formed at once, every member told its group in the same moment.
//!
//! Ignored by default, since it is a measurement. Run it optimised:
//!
//! ```text
//! RAFT_STORM_GROUPS=2000 RAFT_STORM_FSYNC_MS=5 cargo test --release \
//!     -p felix-replication --features raft --test raft_election_storm \
//!     -- --ignored --nocapture
//! ```
//!
//! - `RAFT_STORM_GROUPS`: how many groups (200).
//! - `RAFT_STORM_FSYNC_MS`: a delay added to every fsync, as a slow disk
//!   has. The three brokers share the process's blocking threads, so their
//!   flushes compete as they would on one device.
//! - `RAFT_STORM_DISK_WIDTH`: how many flushes run at once (512). With the
//!   delay it sets what the disk gets through: 4 at 10 ms is 400 a second.
//! - `RAFT_STORM_IDLE=1`: wait for every group to go idle before the leader
//!   is lost. Idle followers wait longer and by a fixed spread.
//! - `RAFT_STORM_ELECTIONS`: the runtime's bound on elections in flight
//!   (`RuntimeConfig::elections`). 0 takes the bound off.
//! - `RAFT_STORM_SETTLE_SECS`: how long the groups get (300).
//!
//! There is no network in it: a message is a call into the other runtime.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use felix_replication::raft::{
    GroupId, GroupRuntime, MemberId, RaftReply, RaftRequest, RaftTransport, Refused, Role,
    RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};

const BROKERS: [&str; 3] = ["a", "b", "c"];

fn shard(n: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: n,
        kind: ShardKind::Stream,
    }
}

fn env<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
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

/// The leader of group `n` among `runtimes`, once one leads and the others
/// follow it.
fn leader(runtimes: &[GroupRuntime], n: u32) -> Option<MemberId> {
    let statuses: Vec<_> = runtimes
        .iter()
        .filter_map(|runtime| runtime.status(&shard(n)))
        .collect();
    let leader = statuses
        .iter()
        .find(|status| status.role == Role::Leader)?
        .member
        .clone();
    statuses
        .iter()
        .all(|status| status.leader.as_ref() == Some(&leader))
        .then_some(leader)
}

fn highest_term(runtimes: &[GroupRuntime], groups: u32) -> u64 {
    (0..groups)
        .flat_map(|n| runtimes.iter().filter_map(move |r| r.status(&shard(n))))
        .map(|status| status.term)
        .max()
        .unwrap_or(0)
}

/// Terms above `base` summed over the groups: each is an election round
/// some member started.
fn rounds(runtimes: &[GroupRuntime], groups: u32, base: u64) -> u64 {
    (0..groups)
        .map(|n| {
            let term = runtimes.iter().filter_map(|r| r.status(&shard(n)));
            term.map(|status| status.term).max().unwrap_or(base)
        })
        .map(|term| term.saturating_sub(base))
        .sum()
}

/// Tell every one of `runtimes` the first members of `groups`, all at once.
async fn tell(runtimes: &[GroupRuntime], groups: &[u32], members: &BTreeSet<MemberId>) {
    let told = groups.iter().flat_map(|n| {
        runtimes.iter().map(move |runtime| async move {
            runtime
                .initialize(&shard(*n), members.clone())
                .await
                .expect("initialize");
        })
    });
    futures::future::join_all(told).await;
}

#[test]
#[ignore = "a measurement; see the module docs"]
fn many_groups_lose_their_leader_at_once() {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.worker_threads(8).enable_all();
    // Every fsync runs on a blocking thread, so their number is how many
    // flushes the disk takes at once.
    if let Some(width) = env("RAFT_STORM_DISK_WIDTH") {
        runtime.max_blocking_threads(width);
    }
    runtime.build().expect("runtime").block_on(storm());
}

async fn storm() {
    let groups: u32 = env("RAFT_STORM_GROUPS").unwrap_or(200);
    let fsync = Duration::from_millis(env("RAFT_STORM_FSYNC_MS").unwrap_or(0));
    let idle = env::<u32>("RAFT_STORM_IDLE") == Some(1);
    let form = std::env::var("RAFT_STORM_MODE").as_deref() == Ok("form");
    let settle = Duration::from_secs(env("RAFT_STORM_SETTLE_SECS").unwrap_or(300));
    let mut timers = RuntimeConfig::default();
    if let Some(bound) = env("RAFT_STORM_ELECTIONS") {
        timers.elections = bound;
    }
    if !idle {
        // Groups never slow down, so every follower waits the short gap.
        timers.idle_heartbeat = timers.heartbeat;
    }

    let root = tempfile::tempdir().expect("dir");
    let net = Arc::new(Net::default());
    let runtimes: Vec<GroupRuntime> = BROKERS
        .iter()
        .map(|broker| {
            let transport: Arc<dyn RaftTransport> = net.clone();
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

    for n in 0..groups {
        for (broker, runtime) in BROKERS.iter().zip(&runtimes) {
            let dir = root.path().join(broker).join(n.to_string());
            std::fs::create_dir_all(&dir).expect("dir");
            let log = DiskLog::open(dir, format!("t/ns/orders/{n}"), config.clone()).expect("log");
            runtime
                .open(GroupId::new(shard(n), 0), 1, log)
                .await
                .expect("open");
        }
    }

    let (survivors, base) = if form {
        felix_storage::fault::set_fsync_delay(fsync);
        (runtimes.clone(), 0)
    } else {
        // `a` is told first, stands at once and wins every group.
        for batch in (0..groups).collect::<Vec<_>>().chunks(64) {
            tell(&runtimes[..1], batch, &members).await;
            while !batch
                .iter()
                .all(|n| runtimes[0].status(&shard(*n)).map(|s| s.role) == Some(Role::Leader))
            {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            tell(&runtimes[1..], batch, &members).await;
        }
        while !(0..groups).all(|n| leader(&runtimes, n).is_some()) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if idle {
            // Every leader slows down and every follower has heard so.
            tokio::time::sleep(timers.idle_after + 3 * timers.idle_heartbeat).await;
        } else {
            tokio::time::sleep(2 * timers.heartbeat).await;
        }
        let base = highest_term(&runtimes, groups);
        felix_storage::fault::set_fsync_delay(fsync);
        net.brokers.lock().remove(BROKERS[0]);
        runtimes[0].shutdown().await;
        (runtimes[1..].to_vec(), base)
    };

    let started = Instant::now();
    if form {
        tell(&runtimes, &(0..groups).collect::<Vec<_>>(), &members).await;
    }
    // When each group was first led, and when all were led at once.
    let mut first_led: Vec<Option<Duration>> = vec![None; groups as usize];
    let all_led = loop {
        let mut led = 0;
        for n in 0..groups {
            if leader(&survivors, n).is_some() {
                led += 1;
                first_led[n as usize].get_or_insert_with(|| started.elapsed());
            }
        }
        if led == groups {
            break Some(started.elapsed());
        }
        if started.elapsed() > settle {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    // Terms that still rise after every group is led are a storm too.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let highest = highest_term(&survivors, groups);
    let rounds = rounds(&survivors, groups, base);
    let still_led = (0..groups)
        .filter(|n| leader(&survivors, *n).is_some())
        .count();
    felix_storage::fault::set_fsync_delay(Duration::ZERO);

    let mut times: Vec<Duration> = first_led.iter().flatten().copied().collect();
    times.sort();
    let at = |q: f64| {
        let last = times.len().saturating_sub(1);
        times
            .get(((last as f64) * q) as usize)
            .copied()
            .unwrap_or_default()
    };
    println!(
        "{groups} groups, {}, fsync +{fsync:?}{}, elections bound {}: all led after {}, \
         first led p50 {:.1?} p99 {:.1?} max {:.1?}, highest term {highest} (was {base}), \
         {rounds} election rounds, {still_led} led 5 s later",
        match (form, idle) {
            (true, _) => "formed at once",
            (false, true) => "idle leader lost",
            (false, false) => "leader lost",
        },
        match env::<usize>("RAFT_STORM_DISK_WIDTH") {
            Some(width) => format!(" x{width}"),
            None => String::new(),
        },
        match timers.elections {
            0 => "off".to_string(),
            bound => bound.to_string(),
        },
        match all_led {
            Some(after) => format!("{after:.1?}"),
            None => format!("more than {settle:?} ({} led)", times.len()),
        },
        at(0.5),
        at(0.99),
        at(1.0),
    );
    assert!(all_led.is_some(), "not every group was led in {settle:?}");
    for runtime in &survivors {
        runtime.shutdown().await;
    }
}

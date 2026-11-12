//! Three runtimes in one process, joined by a transport that hands each
//! message straight to the runtime of the broker it names.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use felix_storage::DiskLog;
use felix_storage::log::{AppendOnlyLog, FsyncMode, LogConfig};
use felix_storage::raft_log::EntryCodec;
use openraft::raft::VoteRequest;
use openraft::type_config::alias::{LeaderIdOf, VoteOf};
use openraft::vote::{RaftLeaderId, RaftVote};
use tempfile::{TempDir, tempdir};

use super::codec::{ShardCodec, StoredMembership};
use super::network::Request;
use super::types::{Command, Membership, ShardTypes};
use super::*;
use crate::{ShardKey, ShardKind};

const BROKERS: [&str; 3] = ["a", "b", "c"];
const PATIENCE: Duration = Duration::from_secs(120);

/// The shipped timers. Shorter ones churn on a loaded machine, where an
/// fsync can outlast a short election timeout.
fn timers() -> RuntimeConfig {
    RuntimeConfig::default()
}

fn shard(n: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: n,
        kind: ShardKind::Stream,
    }
}

fn group(n: u32) -> GroupId {
    GroupId::new(shard(n), 0)
}

fn members() -> BTreeSet<MemberId> {
    BROKERS
        .iter()
        .map(|broker| MemberId::new(*broker, 1))
        .collect()
}

fn log_config() -> LogConfig {
    LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    }
}

fn open_log(root: &Path, broker: &str, n: u32, config: LogConfig) -> DiskLog {
    let dir = root.join(broker).join(n.to_string());
    std::fs::create_dir_all(&dir).expect("dir");
    DiskLog::open(dir, format!("t/ns/orders/{n}"), config).expect("open")
}

/// Hands a message to the runtime of the broker it names, unless that
/// broker is cut off.
#[derive(Default)]
struct Net {
    brokers: parking_lot::Mutex<HashMap<String, GroupRuntime>>,
    cut: parking_lot::Mutex<HashSet<String>>,
    /// How many more appends that name the idle gap are lost on the way.
    lose_idle: std::sync::atomic::AtomicU32,
    lost_idle: std::sync::atomic::AtomicU32,
}

#[async_trait]
impl RaftTransport for Net {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        use std::sync::atomic::Ordering::Relaxed;
        let broker: &str = &request.to().broker;
        if request.beat > timers().heartbeat.as_millis() as u64
            && self
                .lose_idle
                .try_update(Relaxed, Relaxed, |left| left.checked_sub(1))
                .is_ok()
        {
            self.lost_idle.fetch_add(1, Relaxed);
            return Err(Refused::Unreachable("lost".into()));
        }
        let runtime = match self.cut.lock().contains(broker) {
            true => None,
            false => self.brokers.lock().get(broker).cloned(),
        };
        match runtime {
            Some(runtime) => runtime.handle(request).await,
            None => Err(Refused::Unreachable(format!("{broker} is down"))),
        }
    }
}

struct Cluster {
    root: TempDir,
    net: Arc<Net>,
    timers: RuntimeConfig,
    logs: HashMap<(String, u32), DiskLog>,
}

impl Cluster {
    /// Three brokers with their tickers running, or not.
    fn new(ticking: bool) -> Self {
        Self::with(timers(), ticking)
    }

    fn with(timers: RuntimeConfig, ticking: bool) -> Self {
        let cluster = Self {
            root: tempdir().expect("dir"),
            net: Arc::default(),
            timers,
            logs: HashMap::new(),
        };
        cluster.start(ticking);
        cluster
    }

    fn start(&self, ticking: bool) {
        for broker in BROKERS {
            let transport: Arc<dyn RaftTransport> = self.net.clone();
            let runtime = GroupRuntime::without_ticker(broker, self.timers.clone(), transport)
                .expect("timers");
            if ticking {
                runtime.start_ticker();
            }
            self.net.brokers.lock().insert(broker.into(), runtime);
        }
    }

    fn runtime(&self, broker: &str) -> GroupRuntime {
        self.net.brokers.lock()[broker].clone()
    }

    fn runtimes(&self) -> Vec<GroupRuntime> {
        BROKERS.iter().map(|broker| self.runtime(broker)).collect()
    }

    /// Open group `n` on every broker, without forming it.
    async fn open(&mut self, n: u32) {
        for broker in BROKERS {
            let log = open_log(self.root.path(), broker, n, log_config());
            self.logs.insert((broker.into(), n), log.clone());
            self.runtime(broker)
                .open(group(n), 1, log)
                .await
                .expect("open");
        }
    }

    /// Open group `n` and form it. The first member told stands at once
    /// and is elected before the others are told, so no vote splits and a
    /// test that forms many groups does not wait out an election timeout,
    /// and a storm of vote fsyncs, for each.
    async fn form(&mut self, n: u32) {
        self.open(n).await;
        for runtime in self.runtimes() {
            runtime
                .initialize(&shard(n), members())
                .await
                .expect("initialize");
            self.elected(n).await;
        }
    }

    fn statuses(&self, n: u32) -> Vec<GroupStatus> {
        self.runtimes()
            .iter()
            .filter_map(|runtime| runtime.status(&shard(n)))
            .collect()
    }

    /// The member every reachable member of group `n` follows, once they
    /// agree on one.
    fn leader(&self, n: u32) -> Option<MemberId> {
        let cut = self.net.cut.lock().clone();
        let statuses: Vec<_> = self
            .statuses(n)
            .into_iter()
            .filter(|status| !cut.contains(&*status.member.broker))
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

    async fn elected(&self, n: u32) -> MemberId {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            if let Some(leader) = self.leader(n) {
                return leader;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "group {n} has no leader: {:#?}",
                self.statuses(n)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until every member of group `n` knows `index` committed.
    async fn committed_everywhere(&self, n: u32, index: u64) {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let statuses = self.statuses(n);
            if statuses
                .iter()
                .all(|status| status.committed >= Some(index))
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "group {n} has not committed {index} everywhere: {statuses:#?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Commit one record through group `n`'s leader, whoever that is by
    /// the time it is written. Returns the record's index.
    async fn write(&self, n: u32, payload: &'static [u8]) -> u64 {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            let leader = self.elected(n).await;
            let member = self
                .runtime(&leader.broker)
                .group(&shard(n))
                .expect("group");
            let command = Command {
                timestamp_micros: 1_700_000_000,
                payload: Bytes::from_static(payload),
            };
            match member.raft().client_write(command).await {
                Ok(written) => return written.log_id.index,
                Err(err) if tokio::time::Instant::now() < deadline => {
                    tracing::debug!(error = %err, "the write found no leader, trying again");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("no write went through: {err}"),
            }
        }
    }

    /// Stop every broker and close every log, as a process exit does.
    async fn stop(&mut self) {
        for runtime in self.runtimes() {
            runtime.shutdown().await;
        }
        self.net.brokers.lock().clear();
        for (_, log) in self.logs.drain() {
            log.close().await.expect("close");
        }
    }
}

/// Poll `check` until it answers.
async fn eventually<T>(mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if let Some(found) = check() {
            return found;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "it did not happen in {PATIENCE:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_groups_elect_and_replicate() {
    const GROUPS: u32 = 12;
    let mut cluster = Cluster::new(true);
    for n in 0..GROUPS {
        cluster.form(n).await;
    }
    assert_eq!(cluster.runtime("a").groups(), GROUPS as usize);

    for n in 0..GROUPS {
        cluster.elected(n).await;
        cluster.write(n, b"first").await;
        let last = cluster.write(n, b"second").await;
        // Every copy holds both records and knows them committed.
        cluster.committed_everywhere(n, last).await;
        for broker in BROKERS {
            let log = &cluster.logs[&(broker.to_string(), n)];
            assert!(log.tail_offset().await.expect("tail") > last, "{broker}");
            // A follower can hear of the commit before its own flush ends.
            eventually(|| (log.durable_offset() > last).then_some(())).await;
        }
    }
    cluster.stop().await;
}

/// Every member is told the first set, as the control plane tells them. Told
/// together they each stand, the votes split, and a later round elects one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn members_told_together_elect_one_leader() {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    let [a, b, c] = BROKERS.map(|broker| cluster.runtime(broker));
    let key = shard(0);
    let (a, b, c) = tokio::join!(
        a.initialize(&key, members()),
        b.initialize(&key, members()),
        c.initialize(&key, members()),
    );
    for told in [a, b, c] {
        told.expect("initialize");
    }
    cluster.elected(0).await;
    cluster.write(0, b"record").await;
    cluster.stop().await;
}

/// openraft notices a timeout only in its timer check, and the ticker is
/// the only thing that asks for one. Forming a group needs no timer: the
/// member told to form it stands at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_the_ticker_no_timeout_is_noticed() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    cluster
        .runtime("a")
        .initialize(&shard(0), members())
        .await
        .expect("initialize");
    let first = cluster.elected(0).await;
    assert_eq!(first, MemberId::new("a", 1));
    let term = cluster.statuses(0)[0].term;

    // The leader is gone, and nobody looks at the clock.
    cluster.net.cut.lock().insert("a".into());
    tokio::time::sleep(timers().election_max * 3).await;
    for broker in ["b", "c"] {
        let status = cluster.runtime(broker).status(&shard(0)).expect("status");
        assert_eq!(status.role, Role::Follower, "{status:?}");
        assert_eq!(status.leader.as_ref(), Some(&first), "{status:?}");
        assert_eq!(status.term, term, "{status:?}");
    }

    for broker in ["b", "c"] {
        cluster.runtime(broker).start_ticker();
    }
    let second = cluster.elected(0).await;
    assert_ne!(second, first);
    cluster.stop().await;
}

/// The ticker asks a group for a timer check only when its timer can have
/// run out: a leader once per heartbeat, a follower that hears its leader
/// never.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_group_whose_timer_is_due_is_ticked() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let ticks = |cluster: &Cluster| -> HashMap<MemberId, u64> {
        cluster
            .statuses(0)
            .into_iter()
            .map(|status| (status.member, status.ticks))
            .collect()
    };

    // A group just written to is not idle.
    cluster.write(0, b"record").await;
    let window = Duration::from_secs(3);
    let before = ticks(&cluster);
    tokio::time::sleep(window).await;
    let after = ticks(&cluster);
    assert_eq!(
        cluster.leader(0),
        Some(leader.clone()),
        "the leader changed"
    );

    let every_interval = (window.as_millis() / timers().tick.as_millis()) as u64;
    for (member, after) in after {
        let sent = after - before[&member];
        if member == leader {
            // About one per heartbeat, 15 here, and far from one per interval.
            assert!((8..=20).contains(&sent), "{member}: {sent} checks");
        } else {
            assert!(sent <= 3, "{member}: {sent} checks of {every_interval}");
        }
    }
    cluster.stop().await;
}

fn pace() -> (super::group::Pace, super::group::Timers) {
    let millis = |duration: Duration| duration.as_millis() as u64;
    let config = timers();
    let timers = super::group::Timers {
        started: tokio::time::Instant::now(),
        tick: millis(config.tick),
        heartbeat: millis(config.heartbeat),
        idle_heartbeat: millis(config.idle_heartbeat),
        idle_after: millis(config.idle_after),
        election_min: millis(config.election_min),
        election_max: millis(config.election_max),
    };
    let pace = super::group::Pace::new(timers, &group(0), &MemberId::new("a", 1));
    (pace, timers)
}

/// A busy group heartbeats every 200 ms and an idle one every 2 s, and a
/// follower of an idle leader waits three of those before it may stand.
#[tokio::test(start_paused = true)]
async fn a_busy_group_beats_at_200_ms_and_an_idle_one_at_2_s() {
    let (pace, timers) = pace();
    let [b, c] = ["b", "c"].map(|broker| MemberId::new(broker, 1));
    let later = |millis: u64| tokio::time::advance(Duration::from_millis(millis));

    let entry = pace.sending(&b, true);
    pace.answered(&b, &entry);
    pace.answered(&c, &pace.sending(&c, true));
    assert_eq!(pace.leader_gap(timers.now()), 200);

    later(timers.idle_after - timers.heartbeat).await;
    pace.answered(&b, &pace.sending(&b, false));
    pace.answered(&c, &pace.sending(&c, false));
    later(timers.heartbeat).await;
    let (to_b, to_c) = (pace.sending(&b, false), pace.sending(&c, false));
    assert_eq!((to_b.gap, to_c.gap), (2000, 2000));
    pace.answered(&b, &to_b);
    pace.answered(&c, &to_c);
    assert_eq!(pace.leader_gap(timers.now()), 2000);

    pace.followed(Some(2000));
    let wait = pace.follower_wait();
    assert!((6000..6000 + timers.election_max).contains(&wait), "{wait}");
}

/// The leader's side of the idle rule: it slows down only once every
/// follower has answered a heartbeat that named the idle gap, an entry
/// voids those answers, and a follower long silent no longer counts.
#[tokio::test(start_paused = true)]
async fn a_leader_slows_down_only_once_its_followers_know() {
    let (pace, timers) = pace();
    let [b, c] = ["b", "c"].map(|broker| MemberId::new(broker, 1));
    let (short, idle) = (timers.heartbeat, timers.idle_heartbeat);
    let later = |millis: u64| tokio::time::advance(Duration::from_millis(millis));

    // A new leader's first entry.
    let entry = pace.sending(&b, true);
    assert_eq!(entry.gap, short);
    pace.answered(&b, &entry);
    pace.answered(&c, &pace.sending(&c, true));
    assert_eq!(pace.leader_gap(timers.now()), short);

    // Idle: the heartbeats name the idle gap, at the short interval until
    // both followers have answered one.
    later(timers.idle_after - short).await;
    pace.answered(&b, &pace.sending(&b, false));
    pace.answered(&c, &pace.sending(&c, false));
    later(short).await;
    let to_b = pace.sending(&b, false);
    let to_c = pace.sending(&c, false);
    assert_eq!((to_b.gap, to_c.gap), (idle, idle));
    assert_eq!(pace.leader_gap(timers.now()), short);
    pace.answered(&b, &to_b);
    assert_eq!(pace.leader_gap(timers.now()), short, "c has not heard");
    pace.answered(&c, &to_c);
    assert_eq!(pace.leader_gap(timers.now()), idle);

    // An entry: short again at once, and the old answers count for nothing
    // when the group next goes idle.
    later(10_000).await;
    let entry = pace.sending(&b, true);
    assert_eq!(entry.gap, short);
    pace.answered(&b, &entry);
    pace.answered(&c, &pace.sending(&c, true));
    assert_eq!(pace.leader_gap(timers.now()), short);
    // Heartbeats at the short gap, answered, until the group is idle again.
    later(timers.idle_after - short).await;
    pace.answered(&b, &pace.sending(&b, false));
    pace.answered(&c, &pace.sending(&c, false));
    later(short).await;
    assert_eq!(pace.leader_gap(timers.now()), short, "nobody has heard yet");
    pace.answered(&b, &pace.sending(&b, false));

    // c never answers. Once it has been silent past any election timeout
    // it has stood or is gone, and it stops holding the leader back.
    later(timers.election_min + 2 * timers.election_max).await;
    pace.answered(&b, &pace.sending(&b, false));
    assert_eq!(pace.leader_gap(timers.now()), idle);
}

/// The follower's side: it waits by the gap its leader last named, three
/// idle heartbeats at least, and a vote or a short gap ends the long wait.
#[tokio::test(start_paused = true)]
async fn a_follower_waits_by_the_gap_its_leader_named() {
    let (pace, timers) = pace();
    assert_eq!(pace.follower_wait(), timers.election_min);
    pace.followed(Some(timers.idle_heartbeat));
    let patient = pace.follower_wait();
    assert!(patient >= 3 * timers.idle_heartbeat, "{patient}");
    assert!(patient < 3 * timers.idle_heartbeat + timers.election_max);
    // An append from a build that names no gap is taken as the short one.
    pace.followed(Some(0));
    assert_eq!(pace.follower_wait(), timers.election_min);
    pace.followed(Some(timers.idle_heartbeat));
    pace.voted();
    assert_eq!(pace.follower_wait(), timers.election_min);
}

/// A group with no writes heartbeats at the idle interval, and a write
/// brings the short one back. The first idle heartbeats are lost on the way:
/// the leader stays fast until its followers have heard, so nobody stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_group_heartbeats_slowly_until_it_is_written_to() {
    use std::sync::atomic::Ordering::Relaxed;
    let idle = RuntimeConfig {
        idle_after: Duration::from_secs(1),
        ..timers()
    };
    let mut cluster = Cluster::with(idle, true);
    cluster.net.lose_idle.store(6, Relaxed);
    cluster.form(0).await;
    cluster.write(0, b"record").await;
    let leader = cluster.elected(0).await;
    let term = cluster.statuses(0)[0].term;
    let checks = |cluster: &Cluster| -> u64 {
        let status = cluster.runtime(&leader.broker).status(&shard(0));
        status.expect("group").ticks
    };

    // Idle after a second, slow once the lost messages have been resent.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(cluster.net.lost_idle.load(Relaxed), 6);
    let window = Duration::from_secs(4);
    let before = checks(&cluster);
    tokio::time::sleep(window).await;
    let slow = checks(&cluster) - before;
    // One a second, where the short interval makes 20.
    assert!((2..=7).contains(&slow), "{slow} checks while idle");

    // Write a little after a slow check, once the ticker has seen that the
    // next is a whole idle interval away and has stopped looking at the
    // group. The write has to bring the next check forward all the same.
    let seen = checks(&cluster);
    while checks(&cluster) == seen {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(3 * timers().tick).await;
    let before = checks(&cluster);
    cluster.write(0, b"record").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let soon = checks(&cluster) - before;
    assert!(soon >= 1, "no check within 600 ms of a write");
    tokio::time::sleep(Duration::from_millis(600)).await;
    let fast = checks(&cluster) - before;
    assert!(fast >= 3, "{fast} checks after a write");

    assert_eq!(
        cluster.leader(0),
        Some(leader.clone()),
        "the leader changed"
    );
    for status in cluster.statuses(0) {
        assert_eq!(status.term, term, "{}: an election ran", status.member);
    }
    cluster.stop().await;
}

/// Many groups formed in the same moment all elect: a round of votes, each
/// with its own fsyncs, does not outlast the election timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(
    target_os = "macos",
    ignore = "creating and renaming the vote files is too slow there (#2317)"
)]
async fn many_groups_told_at_once_all_elect() {
    const GROUPS: u32 = 40;
    let mut cluster = Cluster::new(true);
    for n in 0..GROUPS {
        cluster.open(n).await;
    }
    let told = (0..GROUPS).flat_map(|n| {
        cluster
            .runtimes()
            .into_iter()
            .map(move |runtime| async move {
                runtime
                    .initialize(&shard(n), members())
                    .await
                    .expect("initialize");
            })
    });
    futures::future::join_all(told).await;
    let settle = Duration::from_secs(30);
    let all = async {
        for n in 0..GROUPS {
            cluster.elected(n).await;
            cluster.write(n, b"record").await;
        }
    };
    let started = std::time::Instant::now();
    let settled = tokio::time::timeout(settle, all).await;
    let terms: Vec<u64> = (0..GROUPS)
        .flat_map(|n| cluster.statuses(n))
        .map(|status| status.term)
        .collect();
    let led = (0..GROUPS).filter(|n| cluster.leader(*n).is_some()).count();
    assert!(
        settled.is_ok(),
        "{led} of {GROUPS} groups led after {:?}, highest term {:?}",
        started.elapsed(),
        terms.iter().max()
    );
    cluster.stop().await;
}

/// An append is reported written, then flushed, at the entry's index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_is_reported_written_and_flushed() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let runtime = cluster.runtime(&leader.broker);
    let mut positions = runtime.log_positions(&shard(0)).expect("group");
    let index = cluster.write(0, b"record").await;
    let flushed = positions
        .wait_for(|at| at.flushed.is_some_and(|flushed| flushed.index >= index))
        .await
        .expect("open")
        .to_owned();
    let written = flushed.written.expect("written before it is flushed");
    assert!(written.index >= index);
    cluster.committed_everywhere(0, index).await;
    for status in cluster.statuses(0) {
        let term = status.term;
        assert_eq!(status.written, Some(LogPoint { term, index }));
        assert_eq!(status.flushed, status.written, "{}", status.member);
    }
    cluster.stop().await;
}

/// A group whose queue is full takes no timer check. Its checks are dropped
/// and counted, and the other groups on the broker still get theirs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_group_does_not_hold_up_the_rest() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    let stuck = cluster.runtime("a").group(&shard(0)).expect("group");

    // Park the group's core, then fill its queue, so a timer check waits.
    let (release, parked) = std::sync::mpsc::channel::<()>();
    stuck
        .raft()
        .external_request(move |_| {
            let _ = parked.recv();
        })
        .await
        .expect("park");
    // openraft's default queue length, which the runtime does not change.
    let queue = stuck.raft().config().api_channel_size.unwrap_or(65536);
    for _ in 0..queue {
        stuck.raft().trigger().tick().await.expect("fill");
    }
    assert_eq!(
        cluster
            .runtime("a")
            .status(&shard(0))
            .expect("status")
            .dropped_ticks,
        0
    );

    for runtime in cluster.runtimes() {
        runtime.start_ticker();
    }
    for n in 1..4 {
        cluster.form(n).await;
    }
    for n in 1..4 {
        cluster.elected(n).await;
        cluster.write(n, b"record").await;
    }
    let dropped = cluster
        .runtime("a")
        .status(&shard(0))
        .expect("status")
        .dropped_ticks;
    assert!(dropped > 0, "the stuck group's checks were not dropped");

    release.send(()).expect("release");
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_stops_every_group_and_refuses_new_ones() {
    let mut cluster = Cluster::new(true);
    for n in 0..4 {
        cluster.form(n).await;
    }
    cluster.elected(0).await;
    let runtime = cluster.runtime("a");
    let spare = open_log(cluster.root.path(), "a", 9, log_config());

    cluster.stop().await;

    assert_eq!(runtime.groups(), 0);
    assert_eq!(runtime.status(&shard(0)), None);
    assert!(!runtime.close(&shard(0)).await);
    assert!(matches!(
        runtime.open(group(9), 1, spare).await,
        Err(OpenError::Closed)
    ));
    // Every group's tasks are gone: nothing else still holds the transport.
    drop(runtime);
    let net = Arc::clone(&cluster.net);
    eventually(|| (Arc::strong_count(&net) == 2).then_some(())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_one_group_leaves_the_others() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    cluster.form(1).await;
    cluster.elected(0).await;

    let runtime = cluster.runtime("a");
    assert!(runtime.close(&shard(0)).await);
    assert!(!runtime.close(&shard(0)).await);
    assert_eq!(runtime.groups(), 1);
    assert_eq!(runtime.status(&shard(0)), None);

    // The other two members of group 0 still make a majority.
    cluster.write(0, b"without a").await;
    cluster.write(1, b"untouched").await;

    // The log is the caller's: close it, and the shard can be opened again.
    let log = cluster.logs.remove(&("a".to_string(), 0)).expect("log");
    log.close().await.expect("close");
    let log = open_log(cluster.root.path(), "a", 0, log_config());
    cluster.logs.insert(("a".into(), 0), log.clone());
    runtime.open(group(0), 1, log).await.expect("reopen");
    let last = cluster.write(0, b"with a again").await;
    eventually(|| (runtime.status(&shard(0))?.committed >= Some(last)).then_some(())).await;
    cluster.stop().await;
}

/// A restarted broker's members come back from their logs: the vote, the
/// entries and the membership. Nobody forms the group again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_brokers_resume_from_their_logs() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    cluster.write(0, b"before").await;
    let last = cluster.write(0, b"the restart").await;
    cluster.committed_everywhere(0, last).await;
    let term = cluster.statuses(0)[0].term;

    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;

    // What each member read back from its log. The old leader's vote is
    // still its own, so it may lead again at once and add an entry.
    for runtime in cluster.runtimes() {
        let status = eventually(|| {
            runtime
                .status(&shard(0))
                .filter(|status| status.last_index.is_some())
        })
        .await;
        assert!(status.last_index >= Some(last), "{status:?}");
        assert!(status.term >= term, "{status:?}");
    }
    cluster.elected(0).await;
    let next = cluster.write(0, b"after").await;
    assert!(next > last);
    // Forming it again is refused quietly: the members are past that.
    cluster
        .runtime("a")
        .initialize(&shard(0), members())
        .await
        .expect("initialize");
    assert!(cluster.statuses(0).iter().all(|status| status.term >= term));
    cluster.stop().await;
}

/// A snapshot holds the applied id and the membership and takes nothing
/// from the log. A member that restarts on one still has its entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_keeps_the_log() {
    use openraft::async_runtime::watch::WatchReceiver;

    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let last = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, last).await;

    for runtime in cluster.runtimes() {
        let member = runtime.group(&shard(0)).expect("group");
        // Asked again until it covers the record: the first may be taken
        // before the member has applied it.
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            member.raft().trigger().snapshot().await.expect("snapshot");
            tokio::time::sleep(Duration::from_millis(50)).await;
            let covered = member.raft().metrics().borrow_watched().snapshot;
            if covered.is_some_and(|covered| covered.index >= last) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "no snapshot");
        }
    }
    for log in cluster.logs.values() {
        assert_eq!(log.base_offset(), 0, "a snapshot purged the log");
    }

    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;
    cluster.elected(0).await;
    let next = cluster.write(0, b"after").await;
    assert!(next > last);
    for log in cluster.logs.values() {
        assert_eq!(log.base_offset(), 0);
    }
    cluster.stop().await;
}

/// The check is `RaftLog::new`'s. This pins that the runtime opens every
/// log through it, and that a refused open leaves the shard free.
#[tokio::test]
async fn a_log_that_does_not_fsync_on_commit_gets_no_group() {
    let cluster = Cluster::new(false);
    let runtime = cluster.runtime("a");
    let relaxed = LogConfig {
        fsync_mode: FsyncMode::Periodic {
            interval: Duration::from_millis(5),
        },
        ..log_config()
    };
    let log = open_log(cluster.root.path(), "a", 0, relaxed);
    let err = runtime
        .open(group(0), 1, log.clone())
        .await
        .expect_err("a relaxed log was opened");
    assert!(
        matches!(&err, OpenError::Log(err) if err.kind() == std::io::ErrorKind::InvalidData),
        "{err}"
    );
    assert_eq!(runtime.groups(), 0);

    log.close().await.expect("close");
    let log = open_log(cluster.root.path(), "a", 0, log_config());
    runtime.open(group(0), 1, log).await.expect("open");
    assert!(matches!(
        runtime
            .open(
                group(0),
                1,
                open_log(cluster.root.path(), "a", 1, log_config())
            )
            .await,
        Err(OpenError::AlreadyOpen)
    ));
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_member_forms_a_group_only_from_a_set_that_names_it() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    let runtime = cluster.runtime("a");
    let others: BTreeSet<_> = [MemberId::new("b", 1), MemberId::new("c", 1)].into();
    assert!(matches!(
        runtime.initialize(&shard(0), others).await,
        Err(InitializeError::NotAMember(member)) if member == MemberId::new("a", 1)
    ));
    assert!(matches!(
        runtime.initialize(&shard(7), members()).await,
        Err(InitializeError::NoSuchGroup)
    ));
    cluster.stop().await;
}

/// Another copy of the shard, or another epoch of its group, is another
/// member. A message for one never reaches the member that is here.
#[tokio::test]
async fn a_message_for_another_member_is_refused() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    let runtime = cluster.runtime("a");
    let vote = || {
        let candidate = LeaderIdOf::<ShardTypes>::new(5, MemberId::new("b", 1));
        let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(candidate, false);
        Request::Vote(VoteRequest::new(vote, None))
    };
    let request = |group: GroupId, to: MemberId| RaftRequest {
        group,
        to,
        beat: 0,
        body: vote(),
    };

    let wiped = request(group(0), MemberId::new("a", 2));
    let reformed = request(GroupId::new(shard(0), 1), MemberId::new("a", 1));
    let unopened = request(group(3), MemberId::new("a", 1));
    for other in [wiped, reformed, unopened] {
        let what = format!("{other:?}");
        assert!(
            matches!(runtime.handle(other).await, Err(Refused::NoSuchMember(_))),
            "{what}"
        );
    }
    assert_eq!(runtime.status(&shard(0)).expect("status").term, 0);

    runtime
        .handle(request(group(0), MemberId::new("a", 1)))
        .await
        .expect("handled");
    eventually(|| (runtime.status(&shard(0))?.term == 5).then_some(())).await;
    cluster.stop().await;
}

#[test]
fn the_timers_must_make_sense() {
    assert!(RuntimeConfig::default().raft().is_ok());
    let zero_tick = RuntimeConfig {
        tick: Duration::ZERO,
        ..RuntimeConfig::default()
    };
    assert!(zero_tick.raft().is_err());
    let heartbeat_past_election = RuntimeConfig {
        heartbeat: Duration::from_secs(3),
        ..RuntimeConfig::default()
    };
    assert!(heartbeat_past_election.raft().is_err());
}

#[test]
fn the_default_timers_are_the_designs() {
    let raft = RuntimeConfig::default().raft().expect("timers");
    assert_eq!(RuntimeConfig::default().tick, Duration::from_millis(50));
    assert_eq!(raft.heartbeat_interval, 200);
    assert_eq!(
        RuntimeConfig::default().idle_heartbeat,
        Duration::from_secs(2)
    );
    assert_eq!(RuntimeConfig::default().idle_after, Duration::from_secs(5));
    assert_eq!(raft.election_timeout_min, 1000);
    assert_eq!(raft.election_timeout_max, 2000);
    assert!(!raft.enable_tick);
    assert_eq!(raft.allow_log_reversion, Some(false));
}

#[test]
fn a_membership_round_trips_through_its_record() {
    let voter = |broker: &str| MemberId::new(broker, 1);
    let joint = vec![
        BTreeSet::from([voter("a"), voter("b"), voter("c")]),
        BTreeSet::from([voter("a"), voter("b"), voter("d")]),
    ];
    let everyone: BTreeSet<_> = ["a", "b", "c", "d", "e"].map(voter).into();
    let membership = Membership::new_with_defaults(joint, everyone);

    let payload = ShardCodec.encode_membership(&membership).expect("encode");
    assert_eq!(
        ShardCodec.decode_membership(&payload).expect("decode"),
        membership
    );
    let stored: StoredMembership = serde_json::from_slice(&payload).expect("json");
    assert_eq!(stored.learners, BTreeSet::from([voter("e")]));

    let newer = payload.to_vec();
    let newer = String::from_utf8(newer)
        .expect("utf8")
        .replace("\"version\":1", "\"version\":2");
    assert!(ShardCodec.decode_membership(newer.as_bytes()).is_err());
}

#[test]
fn a_member_id_round_trips_through_the_ballot() {
    for member in [MemberId::new("broker-1", 7), MemberId::new("odd#name", 0)] {
        let named = ShardCodec.encode_node_id(&member);
        assert_eq!(ShardCodec.decode_node_id(&named).expect("decode"), member);
    }
    assert!(ShardCodec.decode_node_id("broker-1").is_err());
    assert!(ShardCodec.decode_node_id("broker-1#x").is_err());
}

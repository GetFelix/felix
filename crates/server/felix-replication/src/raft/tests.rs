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
use felix_storage::raft_log::{EntryCodec, WrittenToken};
use openraft::raft::VoteRequest;
use openraft::type_config::alias::{LeaderIdOf, VoteOf};
use openraft::vote::{RaftLeaderId, RaftVote};
use tempfile::{TempDir, tempdir};

use super::codec::{ShardCodec, StoredMembership};
use super::network::Request;
use super::types::{Command, Membership, ShardTypes};
use super::*;
use crate::{ShardKey, ShardKind};

mod cache;
mod change;
mod commit;
mod deliver;
mod driver;
mod groups;
mod idempotent;
mod identity;
mod lease;
mod membership;
mod propose;
mod read;
mod root;
mod switch;
mod written;

/// Shards numbered from here are cache shards.
const CACHE: u32 = 1000;
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
        kind: match n >= CACHE {
            true => ShardKind::Cache,
            false => ShardKind::Stream,
        },
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
    /// The only broker they are lost on the way to, if one is named.
    lose_idle_to: parking_lot::Mutex<Option<String>>,
    lost_idle: std::sync::atomic::AtomicU32,
    /// Brokers nothing is heard from: what they send is lost.
    muted: parking_lot::Mutex<HashSet<String>>,
    /// While set, every append is lost. Votes still go through.
    lose_appends: std::sync::atomic::AtomicBool,
    /// While set, every vote request is lost. Pre-votes still go through.
    lose_votes: std::sync::atomic::AtomicBool,
    /// Cut appends as the peer transport does, and hold each for as long as
    /// its bytes take on a 1 Gbit/s link.
    gigabit: std::sync::atomic::AtomicBool,
    /// Appends to this broker are answered with this vote, as a member
    /// that has voted for a later candidate answers.
    higher_vote: parking_lot::Mutex<Option<(String, VoteOf<ShardTypes>)>>,
    /// While set, every transfer request is lost.
    lose_transfers: std::sync::atomic::AtomicBool,
    /// How long an append's answer takes to come back.
    answer_after: parking_lot::Mutex<Option<Duration>>,
    /// A cut broker comes back once this many appends that carry entries
    /// have been refused to it.
    heal_after: parking_lot::Mutex<HashMap<String, u32>>,
    /// When the first broker came back that way.
    healed: parking_lot::Mutex<Option<std::time::Instant>>,
    /// A member of shard `n` on a broker that is up but has not opened it
    /// yet: it refuses until this many appends that carry entries have
    /// been refused.
    closed: parking_lot::Mutex<HashMap<(String, u32), u32>>,
    /// When the first such member opened.
    opened: parking_lot::Mutex<Option<std::time::Instant>>,
    /// Every other call to this broker is answered after its deadline.
    flaky: parking_lot::Mutex<Option<String>>,
    flaky_calls: std::sync::atomic::AtomicU32,
    flaky_beats: std::sync::atomic::AtomicU32,
    flaky_rest: std::sync::atomic::AtomicU32,
    /// Appends carrying entries that were handed to a broker.
    appends: std::sync::atomic::AtomicU64,
    /// While set, an append is taken and its answer is lost.
    lose_answers: std::sync::atomic::AtomicBool,
    /// Calls to a cut broker.
    cut_calls: std::sync::atomic::AtomicU64,
    /// A cut broker that comes back just after the next heartbeat to it is
    /// refused, and when it did.
    heal_after_beat: parking_lot::Mutex<Option<String>>,
    healed_after_beat: parking_lot::Mutex<Option<std::time::Instant>>,
}

impl Net {
    fn refused_entries(&self, broker: &str) {
        let mut heal = self.heal_after.lock();
        let Some(left) = heal.get_mut(broker) else {
            return;
        };
        *left -= 1;
        if *left == 0 {
            heal.remove(broker);
            self.cut.lock().remove(broker);
            self.healed
                .lock()
                .get_or_insert_with(std::time::Instant::now);
        }
    }
}

/// The broker that sent `request`, when it is a leader's or a candidate's.
fn sender(request: &Request) -> Option<&str> {
    let vote = match request {
        Request::Append(rpc) => &rpc.vote,
        Request::Vote(rpc) | Request::PreVote(rpc) => &rpc.vote,
        Request::Snapshot { vote, .. } => vote,
        Request::TransferLeader(_) => return None,
        Request::Written { from, .. } => return Some(&from.broker),
    };
    Some(&vote.leader_id().node_id().broker)
}

#[async_trait]
impl RaftTransport for Net {
    async fn send(&self, mut request: RaftRequest) -> Result<RaftReply, Refused> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.gigabit.load(Relaxed) {
            super::wire::fit(&mut request);
            let bytes = super::wire::encode_request(&request).expect("encode").len();
            tokio::time::sleep(Duration::from_secs_f64(bytes as f64 / 125_000_000.0)).await;
        }
        let broker: &str = &request.to().broker;
        let loses = |only: &Option<String>| only.as_deref().is_none_or(|only| only == broker);
        if request.beat > timers().heartbeat.as_millis() as u64
            && loses(&self.lose_idle_to.lock())
            && self
                .lose_idle
                .try_update(Relaxed, Relaxed, |left| left.checked_sub(1))
                .is_ok()
        {
            self.lost_idle.fetch_add(1, Relaxed);
            return Err(Refused::Unreachable("lost".into()));
        }
        if sender(&request.body).is_some_and(|from| self.muted.lock().contains(from))
            || (matches!(request.body, Request::Append(_)) && self.lose_appends.load(Relaxed))
            || (matches!(request.body, Request::Vote(_)) && self.lose_votes.load(Relaxed))
            || (matches!(request.body, Request::TransferLeader(_))
                && self.lose_transfers.load(Relaxed))
        {
            return Err(Refused::Unreachable("lost".into()));
        }
        if matches!(request.body, Request::Append(_))
            && let Some((to, vote)) = &*self.higher_vote.lock()
            && to == broker
        {
            let higher = openraft::raft::AppendError::HigherVote(vote.clone());
            return Ok(RaftReply(super::network::Reply::Append(Err(higher))));
        }
        let cut = self.cut.lock().contains(broker);
        if cut {
            self.cut_calls.fetch_add(1, Relaxed);
        }
        if cut && matches!(&request.body, Request::Append(rpc) if !rpc.entries.is_empty()) {
            self.refused_entries(broker);
        }
        if cut
            && matches!(&request.body, Request::Append(rpc) if rpc.entries.is_empty())
            && self
                .heal_after_beat
                .lock()
                .take_if(|to| to == broker)
                .is_some()
        {
            self.cut.lock().remove(broker);
            *self.healed_after_beat.lock() = Some(std::time::Instant::now());
        }
        let key = (broker.to_string(), request.group.shard.shard);
        let closed = self.closed.lock().get(&key).copied();
        if !cut && let Some(left) = closed {
            if matches!(&request.body, Request::Append(rpc) if !rpc.entries.is_empty()) {
                if left == 1 {
                    self.closed.lock().remove(&key);
                    self.opened
                        .lock()
                        .get_or_insert_with(std::time::Instant::now);
                } else {
                    self.closed.lock().insert(key, left - 1);
                }
            }
            return Err(Refused::NoSuchMember(Box::new(super::network::Addressee {
                group: request.group.clone(),
                to: request.to().clone(),
            })));
        }
        // Heartbeats and the rest alternate apart, so neither kind is
        // always the one that fails.
        let beat = matches!(&request.body, Request::Append(rpc) if rpc.entries.is_empty());
        if self.flaky.lock().as_deref() == Some(broker) {
            self.flaky_calls.fetch_add(1, Relaxed);
            let kind = match beat {
                true => &self.flaky_beats,
                false => &self.flaky_rest,
            };
            if kind.fetch_add(1, Relaxed) % 2 == 0 {
                tokio::time::sleep(request.deadline + Duration::from_millis(10)).await;
                return Err(Refused::Unreachable("too late".into()));
            }
        }
        let runtime = match cut {
            true => None,
            false => self.brokers.lock().get(broker).cloned(),
        };
        if runtime.is_some()
            && matches!(&request.body, Request::Append(rpc) if !rpc.entries.is_empty())
        {
            self.appends.fetch_add(1, Relaxed);
        }
        let slow = match request.body {
            Request::Append(_) => *self.answer_after.lock(),
            _ => None,
        };
        let unheard = matches!(request.body, Request::Append(_)) && self.lose_answers.load(Relaxed);
        let answer = match runtime {
            Some(runtime) => runtime.handle(request).await,
            None => Err(Refused::Unreachable(format!("{broker} is down"))),
        };
        if unheard {
            return Err(Refused::Unreachable("the answer was lost".into()));
        }
        if let Some(slow) = slow {
            tokio::time::sleep(slow).await;
        }
        answer
    }
}

struct Cluster {
    root: TempDir,
    net: Arc<Net>,
    timers: RuntimeConfig,
    /// Brokers that run timers of their own instead of `timers`.
    own_timers: HashMap<&'static str, RuntimeConfig>,
    logs: HashMap<(String, u32), DiskLog>,
}

impl Cluster {
    /// Three brokers with their tickers running, or not.
    fn new(ticking: bool) -> Self {
        Self::with(timers(), ticking)
    }

    fn with(timers: RuntimeConfig, ticking: bool) -> Self {
        Self::with_own(timers, HashMap::new(), ticking)
    }

    fn with_own(
        timers: RuntimeConfig,
        own_timers: HashMap<&'static str, RuntimeConfig>,
        ticking: bool,
    ) -> Self {
        let cluster = Self {
            root: tempdir().expect("dir"),
            net: Arc::default(),
            timers,
            own_timers,
            logs: HashMap::new(),
        };
        cluster.start(ticking);
        cluster
    }

    fn start(&self, ticking: bool) {
        for broker in BROKERS {
            let transport: Arc<dyn RaftTransport> = self.net.clone();
            let timers = self.own_timers.get(broker).unwrap_or(&self.timers);
            let runtime =
                GroupRuntime::without_ticker(broker, timers.clone(), transport).expect("timers");
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
                producer: None,
                expected: None,
                trim: None,
                commit: false,
                group: false,
                publisher: None,
                headers: None,
                written: None,
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

    /// Commit at least `count` records through group `n`, many at a time
    /// so they share fsyncs. Returns the last one's index.
    async fn write_many(&self, n: u32, count: u64) -> u64 {
        let first = self.write(n, b"record").await;
        let leader = self.elected(n).await;
        let member = self
            .runtime(&leader.broker)
            .group(&shard(n))
            .expect("group");
        let mut last = first;
        for _ in 0..count.div_ceil(100) {
            let writes = (0..100).map(|_| {
                member.raft().client_write(Command {
                    timestamp_micros: 1_700_000_000,
                    payload: Bytes::from_static(b"record"),
                    producer: None,
                    expected: None,
                    trim: None,
                    commit: false,
                    group: false,
                    publisher: None,
                    headers: None,
                    written: None,
                })
            });
            for written in futures::future::join_all(writes)
                .await
                .into_iter()
                .flatten()
            {
                last = last.max(written.log_id.index);
            }
        }
        // Whatever a change of leader refused.
        while last < first + count {
            last = self.write(n, b"record").await;
        }
        last
    }

    /// Have `broker`'s member of group `n` take a snapshot that covers
    /// `index`, and give any purge that follows time to run.
    async fn snapshot(&self, broker: &str, n: u32, index: u64) {
        use openraft::async_runtime::watch::WatchReceiver;
        let member = self.runtime(broker).group(&shard(n)).expect("group");
        // Asked again until it covers the index: the first may be taken
        // before the member has applied it.
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            member.raft().trigger().snapshot().await.expect("snapshot");
            tokio::time::sleep(Duration::from_millis(50)).await;
            let covered = member.raft().metrics().borrow_watched().snapshot;
            if covered.is_some_and(|covered| covered.index >= index) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "no snapshot");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// The entry `broker`'s member of group `n` has purged its log up to.
    fn purged(&self, broker: &str, n: u32) -> Option<u64> {
        use openraft::async_runtime::watch::WatchReceiver;
        let member = self.runtime(broker).group(&shard(n)).expect("group");
        let purged = member.raft().metrics().borrow_watched().purged;
        purged.map(|purged| purged.index)
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

/// **The broker that forms a group hands its first leadership on.** The
/// lowest broker of a set forms every group of that set, so without this it
/// would lead them all. The target is asked only once it holds the leader's
/// log, so the transfer goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_broker_that_forms_a_group_hands_its_first_leadership_on() {
    let mut cluster = Cluster::new(true);
    cluster.open(0).await;
    let key = shard(0);
    let to = members()
        .into_iter()
        .find(|member| &*member.broker == "c")
        .expect("c is a member");
    // Asked straight after forming, as the broker does: the member may not
    // show the group formed yet.
    let a = cluster.runtime("a");
    a.initialize(&key, members()).await.expect("initialize");
    assert!(a.hand_over(&key, to.clone(), PATIENCE).await);
    eventually(|| (cluster.leader(0)? == to).then_some(())).await;
    cluster.write(0, b"record").await;
    // A member that no longer leads asks for nothing.
    assert!(!a.hand_over(&key, to.clone(), PATIENCE).await);
    assert_eq!(cluster.leader(0), Some(to));
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
        assert_eq!(status.term, term, "{status:?}");
        // The status names no leader once the member stops hearing it, but
        // openraft itself has noticed nothing.
        let group = cluster.runtime(broker).group(&shard(0)).expect("group");
        let named = {
            use openraft::async_runtime::watch::WatchReceiver;
            group
                .raft()
                .metrics()
                .borrow_watched()
                .current_leader
                .clone()
        };
        assert_eq!(named.as_ref(), Some(&first), "{status:?}");
    }

    for broker in ["b", "c"] {
        cluster.runtime(broker).start_ticker();
    }
    let second = cluster.elected(0).await;
    assert_ne!(second, first);
    cluster.stop().await;
}

/// The member that forms a group asks the others first. Only answers of
/// "no vote, no entry" from every one of them let it form; silence does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_blank_needs_a_blank_answer_from_every_other_member() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    let (a, key) = (cluster.runtime("a"), shard(0));
    let within = Duration::from_secs(5);
    a.peers_blank(&key, &members(), within)
        .await
        .expect("every member is blank");
    // The question is a pre-vote: it leaves the members asked as they were.
    for status in cluster.statuses(0) {
        assert_eq!(status.term, 0, "{status:?}");
    }

    cluster.net.cut.lock().insert("c".into());
    match a.peers_blank(&key, &members(), within).await {
        Err(NotBlank::Unanswered(member, _)) => assert_eq!(member, MemberId::new("c", 1)),
        other => panic!("a silent member counted as blank: {other:?}"),
    }
    cluster.net.cut.lock().clear();

    a.initialize(&key, members()).await.expect("initialize");
    cluster.elected(0).await;
    cluster.write(0, b"record").await;
    let b = cluster.runtime("b");
    match b.peers_blank(&key, &members(), within).await {
        Err(NotBlank::Formed(_)) => {}
        other => panic!("a formed group looked blank: {other:?}"),
    }
    cluster.stop().await;
}

/// A vote alone and an entry alone each mean a group reached the member.
/// A member can hold a vote and no entry after granting one in an election
/// that never got as far as an append.
#[test]
fn a_vote_or_an_entry_alone_is_not_blank() {
    use openraft::raft::VoteResponse;
    use openraft::type_config::alias::LogIdOf;
    let vote = |term: u64| {
        let leader = LeaderIdOf::<ShardTypes>::new(term, MemberId::new("a", 1));
        <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leader, false)
    };
    let answer = |term: u64, entry: bool| {
        let last = entry.then(|| LogIdOf::<ShardTypes>::new(1_u64.into(), 1));
        VoteResponse::<ShardTypes>::new(vote(term), last, false)
    };

    assert!(runtime::blank(&answer(0, false)));
    assert!(!runtime::blank(&answer(1, false)), "a vote alone");
    assert!(!runtime::blank(&answer(0, true)), "an entry alone");
    assert!(!runtime::blank(&answer(1, true)));
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
            // One per heartbeat, 12 here, and far from one per interval.
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
        lease: millis(config.lease),
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

    // A heartbeat c does not take: c may have restarted and forgotten the
    // gap, so the leader is fast again until c answers.
    let lost = pace.sending(&c, false);
    pace.unanswered(&c, &lost);
    assert_eq!(pace.leader_gap(timers.now()), short, "c may have forgotten");
    pace.answered(&c, &pace.sending(&c, false));
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

    // c never answers. Once it has been silent for 5 s it stops holding
    // the leader back.
    later(timers.election_min + 2 * timers.election_max).await;
    pace.answered(&b, &pace.sending(&b, false));
    assert_eq!(pace.leader_gap(timers.now()), idle);
}

/// A resend naming the short gap, then a heartbeat naming the idle one. The
/// follower answers the heartbeat first and stores the short gap last, once
/// the resend's fsync ends. The leader must stay at the short gap until the
/// follower has been told the idle one after that (#2595).
#[tokio::test(start_paused = true)]
async fn a_leader_stays_fast_while_a_short_gap_append_is_on_its_way() {
    let (leader, timers) = pace();
    let [b, c] = ["b", "c"].map(|broker| MemberId::new(broker, 1));
    let follower = super::group::Pace::new(timers, &group(0), &b);
    let (short, idle) = (timers.heartbeat, timers.idle_heartbeat);
    let later = |millis: u64| tokio::time::advance(Duration::from_millis(millis));
    // The follower stands only if the leader stays silent past its wait.
    let hears = || {
        let gap = leader.leader_gap(timers.now());
        let wait = follower.follower_wait();
        assert!(
            gap < wait,
            "leader silent for {gap} ms, follower waits {wait} ms"
        );
    };

    let entry = leader.sending(&b, true);
    follower.followed(Some(entry.gap));
    leader.answered(&b, &entry);
    leader.answered(&c, &leader.sending(&c, true));

    later(timers.idle_after - 1).await;
    let resend = leader.sending(&b, false);
    assert_eq!(resend.gap, short);
    later(1).await;
    let heartbeat = leader.sending(&b, false);
    assert_eq!(heartbeat.gap, idle);
    leader.answered(&c, &leader.sending(&c, false));

    follower.followed(Some(heartbeat.gap));
    leader.answered(&b, &heartbeat);
    hears();
    follower.followed(Some(resend.gap));
    leader.answered(&b, &resend);
    hears();
    assert_eq!(leader.leader_gap(timers.now()), short);

    // The next heartbeat is told after the resend: now both are idle.
    later(short).await;
    let heartbeat = leader.sending(&b, false);
    follower.followed(Some(heartbeat.gap));
    leader.answered(&b, &heartbeat);
    hears();
    assert_eq!(leader.leader_gap(timers.now()), idle);
}

/// A resend naming the short gap times out on the leader while the
/// follower's fsync for it runs, and the leader slows down on the next idle
/// heartbeat. The resend finishing after that must not leave the follower
/// at the short gap (#2595).
#[tokio::test(start_paused = true)]
async fn an_append_past_its_deadline_leaves_the_follower_patient() {
    let (leader, timers) = pace();
    let b = MemberId::new("b", 1);
    let follower = super::group::Pace::new(timers, &group(0), &b);
    let (short, idle) = (timers.heartbeat, timers.idle_heartbeat);
    let deadline = Duration::from_millis(short);
    let later = |millis: u64| tokio::time::advance(Duration::from_millis(millis));

    let entry = leader.sending(&b, true);
    follower.took(entry.gap, timers.now(), deadline);
    leader.answered(&b, &entry);

    later(timers.idle_after - 1).await;
    let resend = leader.sending(&b, false);
    assert_eq!(resend.gap, short);
    let arrived = timers.now();
    later(short).await;
    leader.unanswered(&b, &resend);

    let heartbeat = leader.sending(&b, false);
    assert_eq!(heartbeat.gap, idle);
    follower.took(heartbeat.gap, timers.now(), deadline);
    leader.answered(&b, &heartbeat);
    assert_eq!(leader.leader_gap(timers.now()), idle);

    // The resend's fsync ends after its deadline.
    later(short / 2).await;
    follower.took(resend.gap, arrived, deadline);
    let (gap, wait) = (leader.leader_gap(timers.now()), follower.follower_wait());
    assert!(
        gap < wait,
        "leader silent for {gap} ms, follower waits {wait} ms"
    );
}

/// A late append naming the idle gap is still stored: only a late short
/// gap can leave the follower impatient under a slow leader.
#[tokio::test(start_paused = true)]
async fn a_late_idle_gap_is_still_stored() {
    let (_, timers) = pace();
    let b = MemberId::new("b", 1);
    let follower = super::group::Pace::new(timers, &group(0), &b);
    let deadline = Duration::from_millis(timers.heartbeat);
    follower.took(timers.heartbeat, timers.now(), deadline);
    let short_wait = follower.follower_wait();
    let arrived = timers.now();
    tokio::time::advance(deadline).await;
    follower.took(timers.idle_heartbeat, arrived, deadline);
    assert!(
        follower.follower_wait() > short_wait,
        "a late idle gap was dropped: follower still waits {} ms",
        follower.follower_wait()
    );
}

/// A transport that never answers.
struct Stalls;

#[async_trait]
impl RaftTransport for Stalls {
    async fn send(&self, _: RaftRequest) -> Result<RaftReply, Refused> {
        std::future::pending().await
    }
}

/// openraft drops an append it stops waiting for. That counts as
/// unanswered, so the leader still slows down on the next idle heartbeat.
#[tokio::test(start_paused = true)]
async fn a_dropped_append_does_not_keep_its_leader_fast() {
    use openraft::network::RPCOption;
    use openraft::raft::AppendEntriesRequest;
    use openraft_multi::GroupRouter;
    let (pace, timers) = pace();
    let pace = Arc::new(pace);
    let router = super::network::Router::new(
        Arc::new(Stalls),
        Arc::clone(&pace),
        Arc::new(super::lease::Lease::new(Duration::from_secs(1))),
        Arc::new(super::written::Reports::new()),
        Arc::new(super::reach::Reach::new(
            tokio::time::Instant::now(),
            Duration::from_secs(1),
        )),
        Arc::new(super::reach::Refusals::new(group(0))),
        Arc::new(self::timers().raft(ShardKind::Stream).expect("timers")),
    );
    let b = MemberId::new("b", 1);
    pace.answered(&b, &pace.sending(&b, true));

    let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(
        LeaderIdOf::<ShardTypes>::new(2, MemberId::new("a", 1)),
        true,
    );
    let append = AppendEntriesRequest {
        vote,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    let call = router.append_entries(
        b.clone(),
        group(0),
        append,
        RPCOption::new(Duration::from_secs(1)),
    );
    tokio::time::timeout(Duration::from_millis(10), call)
        .await
        .expect_err("b never answers");

    tokio::time::advance(Duration::from_millis(timers.idle_after)).await;
    let heartbeat = pace.sending(&b, false);
    assert_eq!(heartbeat.gap, timers.idle_heartbeat);
    pace.answered(&b, &heartbeat);
    assert_eq!(pace.leader_gap(timers.now()), timers.idle_heartbeat);
}

/// Sending an entry again, to a member that did not take it or one catching
/// up, does not make an idle group busy. Only an entry no append has
/// carried before does (#2567).
#[tokio::test(start_paused = true)]
async fn only_an_entry_sent_for_the_first_time_makes_a_group_busy() {
    use openraft::type_config::alias::LogIdOf;
    let (pace, timers) = pace();
    let [b, c] = ["b", "c"].map(|broker| MemberId::new(broker, 1));
    let entry = |term: u64, index: u64| LogIdOf::<ShardTypes>::new(term.into(), index);
    assert!(pace.new_entries(&entry(1, 1)));
    let sent = pace.sending(&b, true);
    pace.answered(&b, &sent);
    assert!(!pace.new_entries(&entry(1, 1)), "the same entry to c");
    let lost = pace.sending(&c, false);
    pace.unanswered(&c, &lost);

    // Past the idle wait and c's silence, b answers an idle heartbeat.
    tokio::time::advance(Duration::from_millis(
        timers.election_min + 2 * timers.election_max,
    ))
    .await;
    pace.answered(&b, &pace.sending(&b, false));
    assert_eq!(pace.leader_gap(timers.now()), timers.idle_heartbeat);

    // The retry to c carries the same entry: still idle.
    assert!(!pace.new_entries(&entry(1, 1)));
    let retry = pace.sending(&c, false);
    assert_eq!(retry.gap, timers.idle_heartbeat);
    pace.unanswered(&c, &retry);
    assert_eq!(pace.leader_gap(timers.now()), timers.idle_heartbeat);

    // A later entry, or one at a lower index under a later term, is new.
    assert!(pace.new_entries(&entry(1, 2)));
    assert!(pace.new_entries(&entry(2, 2)));
    assert!(!pace.new_entries(&entry(1, 3)));
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

/// The gap comes from a peer. Whatever it says, a follower waits no longer
/// than it would for the idle gap it names itself.
#[tokio::test(start_paused = true)]
async fn a_gap_from_the_wire_is_held_to_the_idle_interval() {
    let (pace, timers) = pace();
    pace.followed(Some(timers.idle_heartbeat));
    let longest = pace.follower_wait();
    for huge in [timers.idle_heartbeat + 1, u64::MAX / 3 + 1, u64::MAX] {
        pace.followed(Some(huge));
        assert_eq!(pace.follower_wait(), longest, "a gap of {huge}");
    }
}

/// A follower that restarts forgets the idle gap and stands as soon as
/// openraft lets it. Its leader finds out from the first heartbeat the
/// follower does not take, and heartbeats fast again, so losing the next
/// ones costs the leader nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_follower_that_misses_idle_heartbeats_does_not_stand() {
    use std::sync::atomic::Ordering::Relaxed;
    // The follower may stand 3.8 s after it starts. Two lost heartbeats at
    // the idle interval would leave it unheard for almost 6 s.
    let idle = RuntimeConfig {
        idle_after: Duration::from_secs(1),
        election_min: Duration::from_millis(1800),
        ..timers()
    };
    let mut cluster = Cluster::with(idle, true);
    cluster.form(0).await;
    let last = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, last).await;
    let leader = cluster.elected(0).await;
    let term = cluster.statuses(0)[0].term;
    let follower = *BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");
    let checks = |cluster: &Cluster| -> u64 {
        let status = cluster.runtime(&leader.broker).status(&shard(0));
        status.expect("group").ticks
    };

    // Idle, and slow. Restart the follower just after a heartbeat.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let seen = checks(&cluster);
    while checks(&cluster) == seen {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let runtime = cluster.runtime(follower);
    assert!(runtime.close(&shard(0)).await);
    let log = cluster
        .logs
        .remove(&(follower.to_string(), 0))
        .expect("log");
    log.close().await.expect("close");
    let log = open_log(cluster.root.path(), follower, 0, log_config());
    cluster.logs.insert((follower.into(), 0), log.clone());
    *cluster.net.lose_idle_to.lock() = Some(follower.into());
    cluster.net.lose_idle.store(2, Relaxed);
    runtime.open(group(0), 1, log).await.expect("reopen");

    tokio::time::sleep(Duration::from_secs(8)).await;
    assert_eq!(cluster.net.lost_idle.load(Relaxed), 2);
    assert_eq!(cluster.leader(0), Some(leader), "the leader changed");
    for status in cluster.statuses(0) {
        assert_eq!(status.term, term, "{}: an election ran", status.member);
    }
    cluster.stop().await;
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
    // About 2 s apart (the idle heartbeat plus a tick), so 7 s holds 3 or
    // 4; the short interval would make about 35. The upper bound leaves room
    // for a beat or two made fast by a heartbeat that timed out under load.
    let window = Duration::from_secs(7);
    let before = checks(&cluster);
    tokio::time::sleep(window).await;
    let slow = checks(&cluster) - before;
    assert!((2..=8).contains(&slow), "{slow} checks while idle");

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
    // openraft counts a follower's entry toward its local commit once the
    // append is accepted, which can be before the log has written it.
    eventually(|| {
        cluster
            .statuses(0)
            .iter()
            .all(|status| status.flushed.is_some_and(|at| at.index >= index))
            .then_some(())
    })
    .await;
    for status in cluster.statuses(0) {
        let term = status.term;
        assert_eq!(status.written, Some(LogPoint { term, index }));
        assert_eq!(status.flushed, status.written, "{}", status.member);
    }
    cluster.stop().await;
}

/// A proposer's token comes back from the leader's own append with the
/// record's index and term, while the record cannot commit: both followers
/// are cut off, so `client_write` has nothing to answer with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proposed_record_is_reported_written_before_it_commits() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let before = cluster.write(0, b"committed").await;
    cluster.committed_everywhere(0, before).await;
    let leader = cluster.elected(0).await;
    let runtime = cluster.runtime(&leader.broker);
    let member = runtime.group(&shard(0)).expect("group");
    let term = runtime.status(&shard(0)).expect("status").term;

    for broker in BROKERS.iter().filter(|broker| **broker != &*leader.broker) {
        cluster.net.cut.lock().insert((*broker).into());
    }
    let (token, written) = WrittenToken::channel();
    let command = Command {
        expected: None,
        trim: None,
        commit: false,
        group: false,
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from_static(b"proposed"),
        producer: None,
        written: Some(token),
        headers: None,
        publisher: None,
    };
    let raft = member.raft().clone();
    let mut write = tokio::spawn(async move { raft.client_write(command).await });

    let at = tokio::time::timeout(PATIENCE, written)
        .await
        .expect("the token resolves")
        .expect("written");
    assert_eq!(
        at,
        LogPoint {
            term,
            index: before + 1
        }
    );
    let status = runtime.status(&shard(0)).expect("status");
    assert_eq!(status.committed, Some(before));
    assert!(
        status
            .written
            .is_some_and(|written| written.index >= at.index)
    );
    // A leader deposed meanwhile refuses the write. It never reports it done.
    assert!(
        !matches!(
            futures::FutureExt::now_or_never(&mut write),
            Some(Ok(Ok(_)))
        ),
        "the write was answered with no follower reachable"
    );
    cluster.net.cut.lock().clear();
    write.abort();
    cluster.stop().await;
}

/// The token is not part of what a peer is sent.
#[test]
fn a_commands_token_is_not_serialized() {
    let (token, _written) = WrittenToken::channel();
    let command = Command {
        expected: None,
        trim: None,
        commit: false,
        group: false,
        timestamp_micros: 7,
        payload: Bytes::from_static(b"p"),
        producer: None,
        written: Some(token),
        headers: None,
        publisher: None,
    };
    let sent = serde_json::to_value(&command).expect("json");
    assert_eq!(
        sent.as_object().expect("object").keys().collect::<Vec<_>>(),
        [
            "commit",
            "expected",
            "group",
            "headers",
            "payload",
            "producer",
            "publisher",
            "timestamp_micros",
            "trim"
        ]
    );
    let received: Command = serde_json::from_value(sent).expect("json");
    assert!(received.written.is_none());
    assert_eq!(received, command);
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
    // Wait on the ticker, not on how long the writes took: in a slow build
    // they can finish before its first deadline has passed.
    let stuck_dropped = || {
        let status = cluster.runtime("a").status(&shard(0)).expect("status");
        (status.dropped_ticks > 0).then_some(())
    };
    eventually(stuck_dropped).await;
    // And while it stays stuck, the ticker goes on to later intervals: each
    // other group's leader is checked again.
    let ticks = |n: u32| -> u64 {
        let statuses = cluster
            .runtimes()
            .into_iter()
            .map(|runtime| runtime.status(&shard(n)));
        statuses.map(|status| status.expect("status").ticks).sum()
    };
    let before: Vec<u64> = (1..4).map(ticks).collect();
    eventually(|| {
        (1..4)
            .zip(&before)
            .all(|(n, was)| ticks(n) > *was)
            .then_some(())
    })
    .await;

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

/// A close holds the shard until its member has stopped. An open that
/// races it is refused, so two members never run on one log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_that_races_a_close_is_refused_until_the_member_stops() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let runtime = cluster.runtime("a");
    let closing = runtime.group(&shard(0)).expect("group");

    // Park the member's core, so it cannot stop until released. Wait until
    // the core is in it: a shutdown sent before then stops the member first.
    let (release, parked) = std::sync::mpsc::channel::<()>();
    let (in_park, entered) = std::sync::mpsc::channel::<()>();
    closing
        .raft()
        .external_request(move |_| {
            let _ = in_park.send(());
            let _ = parked.recv();
        })
        .await
        .expect("park");
    eventually(|| entered.try_recv().ok()).await;
    let close = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.close(&shard(0)).await }
    });
    eventually(|| runtime.status(&shard(0)).is_none().then_some(())).await;

    let log = cluster.logs[&("a".to_string(), 0)].clone();
    for _ in 0..20 {
        let opened = runtime.open(group(0), 1, log.clone()).await;
        assert!(
            matches!(opened, Err(OpenError::Closing)),
            "a second member started while the first was still running: {opened:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!close.is_finished());
    assert_ne!(closing.status().role, Role::Stopped);

    // A caller that gave up on its close and tries again waits for the same
    // stop. Told "no group" now, it would close the log under a member that
    // can still write.
    let retried = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.close(&shard(0)).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !retried.is_finished(),
        "a retried close returned while the member was still running"
    );

    release.send(()).expect("release");
    assert!(retried.await.expect("retried close"));
    assert_eq!(closing.status().role, Role::Stopped);
    assert!(close.await.expect("close"));
    drop(closing);

    let log = cluster.logs.remove(&("a".to_string(), 0)).expect("log");
    log.close().await.expect("close");
    let log = open_log(cluster.root.path(), "a", 0, log_config());
    cluster.logs.insert(("a".into(), 0), log.clone());
    runtime.open(group(0), 1, log).await.expect("reopen");
    let last = cluster.write(0, b"with a again").await;
    eventually(|| (runtime.status(&shard(0))?.committed >= Some(last)).then_some(())).await;
    cluster.stop().await;
}

/// A ticker that panics is started again, and counted. Without one no group
/// on the broker would heartbeat or notice a timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ticker_that_panics_is_started_again() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let term = cluster.statuses(0)[0].term;
    let runtime = cluster.runtime(&leader.broker);
    let checks = || runtime.status(&shard(0)).expect("group").ticks;
    assert_eq!(runtime.ticker_restarts(), 0);

    runtime.trip_ticker();
    eventually(|| (runtime.ticker_restarts() == 1).then_some(())).await;
    let before = checks();
    eventually(|| (checks() >= before + 3).then_some(())).await;

    // The leader went on heartbeating soon enough to keep its followers.
    tokio::time::sleep(Duration::from_secs(5)).await;
    cluster.write(0, b"record").await;
    assert_eq!(cluster.leader(0), Some(leader), "the leader changed");
    for status in cluster.statuses(0) {
        assert_eq!(status.term, term, "{}: an election ran", status.member);
    }
    assert_eq!(runtime.ticker_restarts(), 1);
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

    // What each member read back from its log, before a leader elected
    // after the restart adds an entry.
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

/// **A restarted member's log still holds each record's publisher and
/// headers.** They live in the entry itself, so a member that reopens its log
/// serves them to a history read as it did before the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_member_reads_back_publisher_and_headers() {
    use felix_storage::log::ReadRange;

    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    let publisher = Bytes::from_static(b"svc-orders");
    let headers = Bytes::from_static(b"\x00\x01k\x00\x01v");
    let command = Command {
        timestamp_micros: 1_700_000_000,
        payload: Bytes::from_static(b"headered"),
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        group: false,
        publisher: Some(publisher.clone()),
        headers: Some(Box::new(headers.clone())),
        written: None,
    };
    let at = member
        .raft()
        .client_write(command)
        .await
        .expect("write")
        .log_id
        .index;
    cluster.committed_everywhere(0, at).await;

    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;
    cluster.elected(0).await;
    for ((broker, _), log) in &cluster.logs {
        let range = ReadRange {
            start: at,
            max_bytes: 1 << 20,
        };
        let page = log.read_range(range).await.expect("read");
        let record = &page.records[0];
        assert_eq!(record.offset, at, "{broker}");
        assert_eq!(record.publisher.as_ref(), Some(&publisher), "{broker}");
        assert_eq!(record.headers.as_deref(), Some(&headers), "{broker}");
    }
    cluster.stop().await;
}

/// A snapshot holds the applied id and the membership and takes nothing
/// from the log, however long the log is. A member that restarts on one
/// still has its entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_keeps_the_log() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    // More than openraft keeps under a snapshot unless told otherwise.
    let last = cluster.write_many(0, 1200).await;
    cluster.committed_everywhere(0, last).await;

    for broker in BROKERS {
        cluster.snapshot(broker, 0, last).await;
    }
    for broker in BROKERS {
        assert_eq!(cluster.purged(broker, 0), None, "{broker} purged its log");
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
    for broker in BROKERS {
        assert_eq!(cluster.purged(broker, 0), None, "{broker} purged its log");
    }
    for log in cluster.logs.values() {
        assert_eq!(log.base_offset(), 0);
    }
    cluster.stop().await;
}

/// A snapshot has no records in it, so a member that installed one from
/// ahead of its log would purge records it was never sent. A member far
/// behind refuses the snapshot and catches up from its leader's log, which
/// the leader's own snapshot left whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_behind_keeps_its_log_and_is_sent_every_record() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let first = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, first).await;
    let leader = cluster.elected(0).await;
    let behind = *BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");

    cluster.net.cut.lock().insert(behind.into());
    let last = cluster.write_many(0, 1200).await;
    let leader = cluster.elected(0).await;
    cluster.snapshot(&leader.broker, 0, last).await;
    let leading = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    let snapshot = leading.raft().get_snapshot().await.expect("read");
    let meta = snapshot.expect("snapshot").meta;
    assert!(meta.last_log_id.is_some_and(|id| id.index >= last));
    assert_eq!(cluster.purged(&leader.broker, 0), None, "the leader purged");

    let log = cluster.logs[&(behind.to_string(), 0)].clone();
    let held = log.tail_offset().await.expect("tail");
    let term = leading.status().term;
    let leads = LeaderIdOf::<ShardTypes>::new(term, leader.clone());
    let request = RaftRequest {
        group: group(0),
        to: MemberId::new(behind, 1),
        beat: 0,
        report: false,
        deadline: Duration::from_secs(1),
        body: Request::Snapshot {
            vote: <VoteOf<ShardTypes> as RaftVote>::from_leader_id(leads, true),
            meta,
            data_len: 0,
            offset: 0,
            data: Bytes::new(),
        },
    };
    let answer = cluster.runtime(behind).handle(request).await;
    assert!(matches!(answer, Err(Refused::NoSnapshots(_))));
    let tail = log.tail_offset().await.expect("tail");
    assert_eq!((log.base_offset(), tail), (0, held));

    cluster.net.cut.lock().clear();
    cluster.committed_everywhere(0, last).await;
    for broker in BROKERS {
        assert_eq!(cluster.purged(broker, 0), None, "{broker} purged its log");
    }
    for log in cluster.logs.values() {
        assert_eq!(log.base_offset(), 0, "a member is missing records");
    }
    cluster.stop().await;
}

/// A follower far behind must catch up over a 1 Gbit/s link. Each append
/// has one heartbeat interval before the leader gives up on it, and the
/// leader cuts the next the same way, so an append that cannot cross the
/// link and be fsynced in that time is never acknowledged. The log reader
/// already hands openraft about 4 MiB at a time; `wire::fit` is the bound
/// that holds whatever the reader does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_far_behind_catches_up_over_a_gigabit_link() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let first = cluster.write(0, b"record").await;
    cluster.committed_everywhere(0, first).await;
    let leader = cluster.elected(0).await;
    let behind = *BROKERS
        .iter()
        .find(|broker| ***broker != *leader.broker)
        .expect("a follower");

    cluster.net.cut.lock().insert(behind.into());
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    let record = Bytes::from(vec![7u8; 1024 * 1024]);
    let mut last = first;
    for _ in 0..48 {
        let command = Command {
            timestamp_micros: 1_700_000_000,
            payload: record.clone(),
            producer: None,
            expected: None,
            trim: None,
            commit: false,
            group: false,
            publisher: None,
            headers: None,
            written: None,
        };
        last = member
            .raft()
            .client_write(command)
            .await
            .expect("write")
            .log_id
            .index;
    }

    cluster
        .net
        .gigabit
        .store(true, std::sync::atomic::Ordering::Relaxed);
    cluster.net.cut.lock().clear();
    tokio::time::timeout(
        Duration::from_secs(60),
        cluster.committed_everywhere(0, last),
    )
    .await
    .expect("the follower did not catch up");
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

/// The ticker visits groups in shard order whatever order they opened and
/// closed in, so a simulation seed replays its run.
#[tokio::test]
async fn the_ticker_visits_groups_in_shard_order() {
    let cluster = Cluster::new(false);
    let runtime = cluster.runtime("a");
    for n in [7, 2, 9, 0, 5, 3, 8, 1] {
        let log = open_log(cluster.root.path(), "a", n, log_config());
        runtime.open(group(n), 1, log).await.expect("open");
    }
    assert!(runtime.close(&shard(5)).await);
    let want: Vec<_> = [0, 1, 2, 3, 7, 8, 9].map(shard).into();
    assert_eq!(runtime.ticked_shards(), want);
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
        report: false,
        deadline: Duration::from_secs(1),
        body: vote(),
    };

    let wiped = request(group(0), MemberId::new("a", 2));
    let reformed = request(GroupId::new(shard(0), 1), MemberId::new("a", 1));
    let unopened = request(group(3), MemberId::new("a", 1));
    // Another copy of the group is refused with the copy that is here.
    assert!(matches!(
        runtime.handle(wiped).await,
        Err(Refused::OtherCopy(_, 1))
    ));
    for other in [reformed, unopened] {
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

/// A vote the sender stopped waiting for is still granted once the member
/// takes it, and granting it pushes the member's election back (#2405).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vote_answered_past_the_deadline_still_restarts_the_timer() {
    let mut cluster = Cluster::new(false);
    cluster.open(0).await;
    let runtime = cluster.runtime("a");
    let member = runtime.group(&shard(0)).expect("group");
    assert_eq!(member.due_at(), 0);

    // Park the core so the vote cannot be answered before the deadline.
    let (release, parked) = std::sync::mpsc::channel::<()>();
    member
        .raft()
        .external_request(move |_| {
            let _ = parked.recv();
        })
        .await
        .expect("park");
    let candidate = LeaderIdOf::<ShardTypes>::new(5, MemberId::new("b", 1));
    let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(candidate, false);
    let request = RaftRequest {
        group: group(0),
        to: MemberId::new("a", 1),
        beat: 0,
        report: false,
        deadline: Duration::from_millis(50),
        body: Request::Vote(VoteRequest::new(vote, None)),
    };
    let answered = tokio::time::timeout(Duration::from_millis(50), runtime.handle(request)).await;
    assert!(answered.is_err(), "the parked member answered");

    release.send(()).expect("release");
    eventually(|| (runtime.status(&shard(0))?.term == 5).then_some(())).await;
    let election_min = cluster.timers.election_min.as_millis() as u64;
    eventually(|| (member.due_at() >= election_min).then_some(())).await;
    cluster.stop().await;
}

/// A follower back from a cut takes the entries it missed within about a
/// heartbeat. Each comes back just after its fourth refused append, when
/// openraft's default delays would make it wait a second more (#2543).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_healed_cut_commits_within_about_a_heartbeat() {
    // No follower stands while it is cut.
    let patient = RuntimeConfig {
        election_min: Duration::from_secs(10),
        election_max: Duration::from_secs(20),
        ..timers()
    };
    let mut cluster = Cluster::with(patient, true);
    cluster.form(0).await;
    let before = cluster.write(0, b"committed").await;
    cluster.committed_everywhere(0, before).await;
    let leader = cluster.elected(0).await;
    let member = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");

    for broker in BROKERS.iter().filter(|broker| **broker != &*leader.broker) {
        cluster.net.heal_after.lock().insert((*broker).into(), 4);
        cluster.net.cut.lock().insert((*broker).into());
    }
    let raft = member.raft().clone();
    let write = tokio::spawn(async move {
        raft.client_write(Command {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"after the cut"),
            producer: None,
            expected: None,
            trim: None,
            commit: false,
            group: false,
            publisher: None,
            headers: None,
            written: None,
        })
        .await
    });
    let written = tokio::time::timeout(PATIENCE, write)
        .await
        .expect("answered")
        .expect("task")
        .expect("committed");
    let healed = cluster.net.healed.lock().expect("a follower came back");
    let took = healed.elapsed();
    eprintln!("heal to first commit: {took:?}");
    assert_eq!(written.log_id.index, before + 1);
    assert_eq!(
        leader,
        cluster.elected(0).await,
        "the leader changed, so this measured an election"
    );
    assert!(
        took < Duration::from_millis(900),
        "the first commit after the cut took {took:?}"
    );
    cluster.stop().await;
}

/// The group a dead broker's ticker heartbeats at the short gap is the last
/// one whose append to it failed. A failed vote from a group that does not
/// lead must not take that place, or the heartbeat finds nothing to send.
#[tokio::test]
async fn a_failed_vote_does_not_replace_the_scout() {
    use openraft::network::RPCOption;
    use openraft::raft::AppendEntriesRequest;
    use openraft_multi::GroupRouter;
    let reach = Arc::new(super::reach::Reach::new(
        tokio::time::Instant::now(),
        Duration::from_secs(1),
    ));
    let config = Arc::new(timers().raft(ShardKind::Stream).expect("timers"));
    let router = |id: GroupId| {
        let (pace, _) = pace();
        super::network::Router::new(
            Arc::new(Net::default()),
            Arc::new(pace),
            Arc::new(super::lease::Lease::new(Duration::from_secs(1))),
            Arc::new(super::written::Reports::new()),
            Arc::clone(&reach),
            Arc::new(super::reach::Refusals::new(id)),
            Arc::clone(&config),
        )
    };
    let option = || RPCOption::new(Duration::from_secs(1));
    let (leading, standing) = (router(group(0)), router(group(1)));
    let b = MemberId::new("b", 1);

    let vote = <VoteOf<ShardTypes> as RaftVote>::from_leader_id(
        LeaderIdOf::<ShardTypes>::new(2, MemberId::new("a", 1)),
        false,
    );
    let append = AppendEntriesRequest {
        vote: vote.clone(),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    leading
        .append_entries(b.clone(), group(0), append, option())
        .await
        .expect_err("b is down");
    let peer = Arc::clone(&reach.peers()["b"]);
    assert_eq!(peer.scout(), Some(group(0)));

    standing
        .vote(
            b.clone(),
            group(1),
            VoteRequest::new(vote.clone(), None),
            option(),
        )
        .await
        .expect_err("b is down");
    standing
        .pre_vote(b, group(1), VoteRequest::new(vote, None), option())
        .await
        .expect_err("b is down");
    assert_eq!(peer.scout(), Some(group(0)), "a vote took the scout");
}

/// A broker that is down costs each group about an idle heartbeat's calls:
/// retrying an entry it missed neither keeps the group at the short
/// heartbeat nor goes on once a second. It still catches up as soon as it
/// is back (#2567).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_broker_costs_each_group_about_an_idle_heartbeat() {
    use std::sync::atomic::Ordering::Relaxed;
    const GROUPS: u32 = 6;
    let mut cluster = Cluster::new(true);
    for n in 0..GROUPS {
        cluster.form(n).await;
    }
    let dead = "c";
    // Muted too: its members stand while it is cut, and their vote requests
    // would depose leaders inside the window.
    cluster.net.muted.lock().insert(dead.into());
    cluster.net.cut.lock().insert(dead.into());
    let mut written = Vec::new();
    for n in 0..GROUPS {
        written.push(cluster.write(n, b"missed").await);
    }
    // Past the silence after which the dead broker no longer holds its
    // leaders at the short gap, and past openraft's first few delays.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let window = Duration::from_secs(10);
    let before = cluster.net.cut_calls.load(Relaxed);
    tokio::time::sleep(window).await;
    let calls = cluster.net.cut_calls.load(Relaxed) - before;
    // Less the one group's heartbeat each short gap that finds the broker
    // back, which costs the same whatever the number of groups.
    let scout = window.as_secs_f64() / timers().heartbeat.as_secs_f64();
    let per_group = (calls as f64 - scout) / window.as_secs_f64() / f64::from(GROUPS);
    eprintln!("{per_group:.2} calls a second per group to the dead broker");
    // An idle heartbeat is 0.5 a second and a retry every 10 s adds 0.1.
    // Without either half of the fix it is 1.5 or more, and about five
    // without both: a heartbeat every 200 ms and a retry every second.
    assert!(per_group < 0.9, "{per_group:.2} calls a second per group");

    cluster.net.cut.lock().remove(dead);
    cluster.net.muted.lock().remove(dead);
    let healed = tokio::time::Instant::now();
    let returned = cluster.runtime(dead);
    for (n, index) in (0..GROUPS).zip(written) {
        eventually(|| {
            let last = returned.status(&shard(n))?.last_index;
            (last >= Some(index)).then_some(())
        })
        .await;
    }
    // The first answer from the broker, within an idle heartbeat (2 s),
    // ends every group's wait, on real timers: `eventually` bounds it, and
    // the prompt answer has a test of its own below.
    eprintln!("caught up {:?} after the heal", healed.elapsed());
    cluster.stop().await;
}

/// A member that hears nobody, and is heard by nobody, keeps its term: its
/// pre-votes are lost, so it never stands. Back on the network, it follows
/// the leader it left instead of deposing it with a higher term (#2630).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_off_member_keeps_its_term_and_does_not_depose_the_leader_on_return() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    // Every member has heard the leader's term.
    let written = cluster.write(0, b"heard").await;
    cluster.committed_everywhere(0, written).await;
    let leader = cluster.elected(0).await;
    let term = cluster
        .runtime(&leader.broker)
        .status(&shard(0))
        .expect("status")
        .term;
    let alone = BROKERS
        .into_iter()
        .find(|broker| *broker != &*leader.broker)
        .expect("a follower");
    cluster.net.muted.lock().insert(alone.into());
    cluster.net.cut.lock().insert(alone.into());
    // Several election timeouts (1 to 2 s each).
    tokio::time::sleep(timers().election_max * 4).await;
    let status = cluster.runtime(alone).status(&shard(0)).expect("status");
    assert_eq!(status.term, term, "{alone}'s term while cut off");
    // openraft still names the leader at that term; the member does not
    // count on one it has not heard within its wait.
    eventually(|| {
        let status = cluster.runtime(alone).status(&shard(0)).expect("status");
        status.leader.is_none().then_some(())
    })
    .await;

    cluster.net.cut.lock().clear();
    cluster.net.muted.lock().clear();
    tokio::time::sleep(timers().election_max * 2).await;
    assert_eq!(cluster.elected(0).await, leader);
    let status = cluster
        .runtime(&leader.broker)
        .status(&shard(0))
        .expect("status");
    assert_eq!(status.term, term, "the leader was deposed");
    cluster.stop().await;
}

/// openraft never steps a leader down for a lost majority. Its status
/// names no leader once no majority answered it within its followers'
/// wait, and still hints at itself (#2630).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_no_majority_answers_names_no_leader() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let written = cluster.write(0, b"heard").await;
    cluster.committed_everywhere(0, written).await;
    let leader = cluster.elected(0).await;
    cluster.net.cut.lock().insert(leader.broker.to_string());
    cluster.net.muted.lock().insert(leader.broker.to_string());
    let status = eventually(|| {
        let status = cluster.runtime(&leader.broker).status(&shard(0))?;
        status.leader.is_none().then_some(status)
    })
    .await;
    assert_eq!(status.role, Role::Leader, "{status:?}");
    assert_eq!(status.leader_hint.as_ref(), Some(&leader), "{status:?}");
    cluster.stop().await;
}

/// openraft refuses a pre-vote only inside its 2 s lease, which ends just
/// before each idle heartbeat. A follower that heard its leader within its
/// own wait, and a leader a majority answered within its followers' wait,
/// refuse one past that lease too (#2630). Once a follower grants one, its
/// own timer is left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_group_refuses_a_pre_vote_past_the_openraft_lease() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let written = cluster.write(0, b"heard").await;
    cluster.committed_everywhere(0, written).await;
    let leader = cluster.elected(0).await;
    let leading = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    // Idle: every follower took a heartbeat naming the idle gap.
    eventually(|| (leading.leader_gap() > 1000).then_some(())).await;
    let [follower, candidate] = {
        let mut others = BROKERS
            .into_iter()
            .filter(|broker| *broker != &*leader.broker);
        [others.next().expect("b"), others.next().expect("c")]
    };
    // Neither hears the leader, and neither can stand.
    for broker in [follower, candidate] {
        cluster.net.cut.lock().insert(broker.into());
        cluster.net.muted.lock().insert(broker.into());
    }
    let following = cluster.runtime(follower).group(&shard(0)).expect("group");
    let acked = || {
        use openraft::Instant as _;
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = leading.raft().metrics();
        let acked = metrics.borrow_watched().last_quorum_acked;
        acked.expect("a majority answered").elapsed()
    };
    // Ages are read rather than slept for, so a slow runner shifts the
    // checks instead of breaking them. 3 s is past openraft's 2 s lease.
    let past_lease = Duration::from_secs(3);
    eventually(|| {
        (following.led_age().0 >= past_lease.as_millis() as u64 && acked() >= past_lease)
            .then_some(())
    })
    .await;

    let status = leading.status();
    let last = {
        use openraft::async_runtime::watch::WatchReceiver;
        leading.raft().metrics().borrow_watched().last_applied
    };
    let last = last.expect("an applied entry");
    let stands = LeaderIdOf::<ShardTypes>::new(status.term + 1, MemberId::new(candidate, 1));
    let pre_vote = |to: &str| RaftRequest {
        group: group(0),
        to: MemberId::new(to, 1),
        beat: 0,
        report: false,
        deadline: Duration::from_secs(1),
        body: Request::PreVote(VoteRequest::new(
            <VoteOf<ShardTypes> as RaftVote>::from_leader_id(stands.clone(), false),
            Some(last),
        )),
    };
    let refused = |answer: &Result<RaftReply, Refused>| {
        // An answer, not a drop: a dropped call counts the broker as down.
        matches!(
            answer,
            Ok(RaftReply(super::network::Reply::Vote(vote))) if !vote.vote_granted
        )
    };
    // Each refusal must hold while the age read after the answer is still
    // inside the wait, about 3 s after the checks start.
    let answer = cluster.runtime(follower).handle(pre_vote(follower)).await;
    let (age, wait) = following.led_age();
    assert!(
        age < wait,
        "the check came {age} ms after the take, past {wait} ms"
    );
    assert!(
        refused(&answer),
        "{follower} did not refuse a pre-vote {age} ms after its leader"
    );
    let answer = cluster
        .runtime(&leader.broker)
        .handle(pre_vote(&leader.broker))
        .await;
    let age = acked();
    assert!(
        age < Duration::from_secs(6),
        "the check came {age:?} after the ack"
    );
    assert!(
        refused(&answer),
        "the leader did not refuse a pre-vote {age:?} after its ack"
    );

    // Past the follower's wait it grants one, which promises nothing: its
    // own election timer runs on.
    let heard = following.heard();
    loop {
        let answer = cluster.runtime(follower).handle(pre_vote(follower)).await;
        if matches!(
            answer,
            Ok(RaftReply(super::network::Reply::Vote(ref vote))) if vote.vote_granted
        ) {
            break;
        }
        let (age, wait) = following.led_age();
        assert!(
            age < wait + 5000,
            "{follower} refused a pre-vote {age} ms after its leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(following.heard(), heard, "a pre-vote restarted the timer");
    cluster.stop().await;
}

/// A transfer to a target that never stands expires, and the old leader
/// stands. The other follower took the transfer, so it no longer counts on
/// that leader and grants the pre-vote at once, rather than refusing it
/// for its whole idle wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_group_elects_soon_after_a_transfer_to_a_dead_target() {
    let mut cluster = Cluster::new(true);
    cluster.form(0).await;
    let written = cluster.write(0, b"heard").await;
    cluster.committed_everywhere(0, written).await;
    let leader = cluster.elected(0).await;
    let leading = cluster
        .runtime(&leader.broker)
        .group(&shard(0))
        .expect("group");
    eventually(|| (leading.leader_gap() > 1000).then_some(())).await;
    let [target, other] = {
        let mut others = BROKERS
            .into_iter()
            .filter(|broker| *broker != &*leader.broker);
        [others.next().expect("b"), others.next().expect("c")]
    };
    cluster.net.cut.lock().insert(target.into());
    cluster.net.muted.lock().insert(target.into());
    let term = leading
        .transfer(MemberId::new(target, 1))
        .await
        .expect("it leads");

    // The old leader's pre-vote at expiry is granted by the other follower.
    let last = {
        use openraft::async_runtime::watch::WatchReceiver;
        leading.raft().metrics().borrow_watched().last_applied
    };
    let stands = LeaderIdOf::<ShardTypes>::new(term + 1, leader.clone());
    let pre_vote = || RaftRequest {
        group: group(0),
        to: MemberId::new(other, 1),
        beat: 0,
        report: false,
        deadline: Duration::from_secs(1),
        body: Request::PreVote(VoteRequest::new(
            <VoteOf<ShardTypes> as RaftVote>::from_leader_id(stands.clone(), false),
            last,
        )),
    };
    let asked = tokio::time::Instant::now();
    loop {
        let answer = cluster.runtime(other).handle(pre_vote()).await;
        if matches!(
            answer,
            Ok(RaftReply(super::network::Reply::Vote(ref vote))) if vote.vote_granted
        ) {
            break;
        }
        // Without the transfer forgetting the leader, the refusal lasts
        // the 6 s idle wait from a heartbeat at most 2 s old.
        assert!(
            asked.elapsed() < Duration::from_secs(2),
            "{other} refused the old leader's pre-vote after the transfer"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.stop().await;
}

/// A group whose entry waits on brokers that are down stays at the short
/// heartbeat, so the first of them back takes the entry within about one,
/// not within an idle heartbeat (#2567).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_waiting_on_dead_brokers_commits_within_about_a_heartbeat_of_one_back() {
    // b and c do not stand while they are cut.
    let patient = RuntimeConfig {
        election_min: Duration::from_secs(20),
        election_max: Duration::from_secs(40),
        ..timers()
    };
    let own = HashMap::from([("b", patient.clone()), ("c", patient)]);
    let mut cluster = Cluster::with_own(timers(), own, true);
    cluster.form(0).await;
    let before = cluster.write(0, b"committed").await;
    cluster.committed_everywhere(0, before).await;
    let leader = cluster.elected(0).await;
    assert_eq!(&*leader.broker, "a");
    let member = cluster.runtime("a").group(&shard(0)).expect("group");

    for broker in ["b", "c"] {
        cluster.net.cut.lock().insert(broker.into());
    }
    let raft = member.raft().clone();
    let write = tokio::spawn(async move {
        raft.client_write(Command {
            timestamp_micros: 1_700_000_000,
            payload: Bytes::from_static(b"while b and c are down"),
            producer: None,
            expected: None,
            trim: None,
            commit: false,
            group: false,
            publisher: None,
            headers: None,
            written: None,
        })
        .await
    });
    // Past the idle wait and the silence after which b and c no longer
    // hold the leader at the short gap.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(!write.is_finished(), "committed with b and c down");

    *cluster.net.heal_after_beat.lock() = Some("c".into());
    let written = tokio::time::timeout(PATIENCE, write)
        .await
        .expect("answered")
        .expect("task")
        .expect("committed");
    let healed = cluster.net.healed_after_beat.lock().expect("c came back");
    let took = healed.elapsed();
    eprintln!("c back to the commit: {took:?}");
    assert_eq!(written.log_id.index, before + 1);
    assert_eq!(
        leader,
        cluster.elected(0).await,
        "this measured an election"
    );
    // A heartbeat is 200 ms. At the idle one it took up to 2 s.
    assert!(
        took < Duration::from_millis(1500),
        "the commit took {took:?} after c was back"
    );
    cluster.stop().await;
}

/// A broker that was down gets what it missed within about a heartbeat of
/// coming back, even from a group with nothing left to commit, which
/// heartbeats only every idle gap: one group heartbeats to a broker that is
/// down at the short gap, and its first answer has every group retry it.
/// One group, so no other group's heartbeat finds the broker first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_group_catches_a_broker_up_within_about_a_heartbeat_of_it_coming_back() {
    // Neither b nor c stands while c is cut.
    let patient = RuntimeConfig {
        election_min: Duration::from_secs(20),
        election_max: Duration::from_secs(40),
        ..timers()
    };
    let own = HashMap::from([("b", patient.clone()), ("c", patient)]);
    let mut cluster = Cluster::with_own(timers(), own, true);
    cluster.form(0).await;
    assert_eq!(&*cluster.elected(0).await.broker, "a");
    cluster.net.cut.lock().insert("c".into());
    let missed = cluster.write(0, b"missed").await;
    // Past the idle wait and c's silence: the group heartbeats every 2 s.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(
        cluster
            .runtime("a")
            .group(&shard(0))
            .expect("group")
            .leader_gap()
            > 1000
    );

    *cluster.net.heal_after_beat.lock() = Some("c".into());
    let returned = cluster.runtime("c");
    eventually(|| {
        let last = returned.status(&shard(0))?.last_index;
        (last >= Some(missed)).then_some(())
    })
    .await;
    let healed = cluster.net.healed_after_beat.lock().expect("c came back");
    let took = healed.elapsed();
    eprintln!("c back to caught up: {took:?}");
    assert!(
        took < Duration::from_millis(1500),
        "c caught up {took:?} after it was back"
    );
    cluster.stop().await;
}

/// A broker back from a restart opens its groups in two waves. Its first
/// answers end the early wave's backoff, but the late wave's members are
/// not open yet and refuse, so each is retried on its own member's first
/// answer. Each opens just after its fourth refused append, when
/// openraft's delays alone would make it wait a second more (#2543).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_that_opens_late_after_a_restart_commits_within_about_a_heartbeat() {
    let patient = RuntimeConfig {
        election_min: Duration::from_secs(10),
        election_max: Duration::from_secs(20),
        ..timers()
    };
    let mut cluster = Cluster::with(patient, true);
    for n in [0, 1] {
        cluster.form(n).await;
        let index = cluster.write(n, b"committed").await;
        cluster.committed_everywhere(n, index).await;
    }
    let leader = cluster.elected(0).await;
    assert_eq!(leader.broker, cluster.elected(1).await.broker);

    for broker in BROKERS.iter().filter(|broker| **broker != &*leader.broker) {
        cluster.net.heal_after.lock().insert((*broker).into(), 4);
        cluster.net.closed.lock().insert(((*broker).into(), 1), 4);
        cluster.net.cut.lock().insert((*broker).into());
    }
    let writes: Vec<_> = [0, 1]
        .map(|n| {
            let raft = cluster
                .runtime(&leader.broker)
                .group(&shard(n))
                .expect("group")
                .raft()
                .clone();
            tokio::spawn(async move {
                raft.client_write(Command {
                    timestamp_micros: 1_700_000_000,
                    payload: Bytes::from_static(b"after the restart"),
                    producer: None,
                    expected: None,
                    trim: None,
                    commit: false,
                    group: false,
                    publisher: None,
                    headers: None,
                    written: None,
                })
                .await
            })
        })
        .into_iter()
        .collect();
    for write in writes {
        tokio::time::timeout(PATIENCE, write)
            .await
            .expect("answered")
            .expect("task")
            .expect("committed");
    }
    let opened = cluster.net.opened.lock().expect("a late member opened");
    let took = opened.elapsed();
    eprintln!("late open to first commit: {took:?}");
    assert_eq!(
        leader,
        cluster.elected(1).await,
        "this measured an election"
    );
    assert!(
        took < Duration::from_millis(900),
        "the late wave's first commit took {took:?}"
    );
    cluster.stop().await;
}

/// Calls to a follower that answers every other call after its deadline,
/// over a few seconds of steady writes, and the walks its leader started.
async fn calls_to_a_flaky_peer(wake: bool) -> (u32, usize) {
    use std::sync::atomic::Ordering::Relaxed;
    let patient = RuntimeConfig {
        election_min: Duration::from_secs(10),
        election_max: Duration::from_secs(20),
        ..timers()
    };
    let mut cluster = Cluster::with(patient, true);
    cluster.form(0).await;
    let leader = cluster.elected(0).await;
    let runtime = cluster.runtime(&leader.broker);
    runtime.reach().no_wake.store(!wake, Relaxed);
    let flaky = *BROKERS
        .iter()
        .find(|broker| **broker != &*leader.broker)
        .expect("a follower");
    *cluster.net.flaky.lock() = Some(flaky.into());
    let until = tokio::time::Instant::now() + Duration::from_secs(4);
    while tokio::time::Instant::now() < until {
        cluster.write(0, b"steady").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    *cluster.net.flaky.lock() = None;
    let calls = cluster.net.flaky_calls.load(Relaxed);
    let walks = runtime.reach().walks.load(Relaxed);
    assert_eq!(leader, cluster.elected(0).await, "the leader changed");
    cluster.stop().await;
    (calls, walks)
}

/// A peer that answers every other call too late is not retried ahead of
/// openraft's own backoff any more often than without the wake (#2543).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_timing_out_every_other_call_is_called_no_more_than_without_the_wake() {
    let (without, _) = calls_to_a_flaky_peer(false).await;
    let (with, walks) = calls_to_a_flaky_peer(true).await;
    eprintln!(
        "calls to the flaky peer: {without} without the wake, {with} with it ({walks} walks)"
    );
    assert!(without > 10, "the peer was called {without} times");
    assert!(
        with <= without + without / 5 + 4,
        "{with} calls with the wake, {without} without"
    );
}

#[test]
fn the_timers_must_make_sense() {
    assert!(RuntimeConfig::default().raft(ShardKind::Stream).is_ok());
    assert!(RuntimeConfig::default().raft(ShardKind::Cache).is_ok());
    let zero_tick = RuntimeConfig {
        tick: Duration::ZERO,
        ..RuntimeConfig::default()
    };
    assert!(zero_tick.raft(ShardKind::Stream).is_err());
    let heartbeat_past_election = RuntimeConfig {
        heartbeat: Duration::from_secs(3),
        ..RuntimeConfig::default()
    };
    assert!(heartbeat_past_election.raft(ShardKind::Stream).is_err());
}

#[test]
fn the_default_timers_are_the_designs() {
    let raft = RuntimeConfig::default()
        .raft(ShardKind::Stream)
        .expect("timers");
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
    let version = TargetVersion {
        incarnation: 1,
        counter: 7,
    };
    let membership = Membership::new(joint, [voter("e")].into(), version);

    let payload = ShardCodec.encode_membership(&membership).expect("encode");
    let read = ShardCodec.decode_membership(&payload).expect("decode");
    assert_eq!(read, membership);
    assert_eq!(read.version(), version);
    let stored: StoredMembership = serde_json::from_slice(&payload).expect("json");
    assert_eq!(stored.learners, BTreeSet::from([voter("e")]));

    let text = String::from_utf8(payload.to_vec()).expect("utf8");
    let newer = text.replace("\"version\":2", "\"version\":3");
    assert_ne!(newer, text);
    assert!(ShardCodec.decode_membership(newer.as_bytes()).is_err());
    // Layout 1 never held a version, so one that names a version is not
    // layout 1.
    let older = text.replace("\"version\":2", "\"version\":1");
    assert!(ShardCodec.decode_membership(older.as_bytes()).is_err());
    // Layout 2 always names its version. None is not the zero version.
    let mut bare: serde_json::Value = serde_json::from_slice(&payload).expect("json");
    let fields = bare.as_object_mut().expect("an object");
    fields.remove("target").expect("a target");
    let bare = bare.to_string();
    assert!(ShardCodec.decode_membership(bare.as_bytes()).is_err());
}

#[test]
fn a_membership_stored_before_versions_reads_as_the_zero_version() {
    let voter = |broker: &str| MemberId::new(broker, 1);
    let stored = r#"{"version":1,"voters":[[{"broker":"a","copy":1},{"broker":"b","copy":1}]],"learners":[{"broker":"c","copy":1}]}"#;
    let read = ShardCodec
        .decode_membership(stored.as_bytes())
        .expect("decode");
    let voters = BTreeSet::from([voter("a"), voter("b")]);
    let want = Membership::new(vec![voters], [voter("c")].into(), TargetVersion::default());
    assert_eq!(read, want);
}

/// A group write is stored with the log's group mark and reads back as one.
/// It holds no client record, and it is never stored with a producer, a
/// publisher or headers, or as a commit or a trim.
#[test]
fn a_group_write_round_trips_through_its_record() {
    let group = || Command {
        timestamp_micros: 9,
        payload: Bytes::from_static(b"an encoded group entry"),
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        group: true,
        publisher: None,
        headers: None,
        written: None,
    };
    let record = ShardCodec.encode_data(&group()).expect("encode");
    assert_eq!(record.mark, felix_storage::log::RecordMark::Group);
    assert!(!record.mark.is_data());
    let read = ShardCodec
        .decode_data(felix_storage::log::LogRecord {
            offset: 4,
            timestamp_micros: record.timestamp_micros,
            checksum: 0,
            payload: record.payload,
            mark: record.mark,
            publisher: None,
            headers: None,
            generation: Some(3),
        })
        .expect("decode");
    assert_eq!(read, group());
    assert!(!read.holds_record_at(4));
    let forged = felix_storage::log::LogRecord {
        offset: 4,
        timestamp_micros: 9,
        checksum: 0,
        payload: Bytes::from_static(b"x"),
        mark: felix_storage::log::RecordMark::Group,
        publisher: Some(Bytes::from_static(b"alice")),
        headers: None,
        generation: Some(3),
    };
    ShardCodec.decode_data(forged).expect_err("refused");
    let producer = types::ProducerMark {
        producer_id: 1,
        sequence: 1,
        position: 0,
        len: 1,
    };
    let refusals: [fn(&mut Command, types::ProducerMark); 6] = [
        |c, _| c.commit = true,
        |c, _| c.trim = Some(3),
        |c, p| c.producer = Some(p),
        |c, _| c.expected = Some(4),
        |c, _| c.publisher = Some(Bytes::from_static(b"alice")),
        |c, _| c.headers = Some(Box::new(Bytes::from_static(b"h"))),
    ];
    for refuse in refusals {
        let mut command = group();
        refuse(&mut command, producer);
        ShardCodec.encode_data(&command).expect_err("refused");
    }
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

/// A dropped timer check is counted on the group and in one counter for the
/// broker, with no label per group.
#[test]
fn a_dropped_tick_is_counted_for_the_broker() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = ::metrics::set_default_local_recorder(&recorder);
    let count = std::sync::atomic::AtomicU64::new(0);
    drop(super::group::Dropped(Some(&count)));
    drop(super::group::Dropped(Some(&count)));
    // A check the group took is not counted.
    drop(super::group::Dropped(None));
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 2);
    let rendered = handle.render();
    assert!(
        rendered.contains(&format!("{} 2", super::group::DROPPED_TICKS_TOTAL)),
        "{rendered}"
    );
}

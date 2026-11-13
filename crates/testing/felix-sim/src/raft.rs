//! Groups of the per-shard Raft engine on virtual time.
//!
//! No broker service routes a shard to Raft yet, so [`crate::run`] cannot
//! put one under test. This runs the engine as the service will hold it:
//! per member a `GroupRuntime`, a `RaftEngine` and a broker core whose
//! stream is bounded by the engine, with one in-memory log per shard and
//! simulated links in between. There is no control plane in the run.
//!
//! The member set is fixed and no member restarts. A seed fixes the member
//! count, the faults, the link delays and what the clients do. See
//! `docs/simulation.md`, "Raft groups".
//!
//! A run that is still going after [`RaftSimConfig::run_limit`] of virtual
//! time ends with a finding. One that holds the thread past
//! [`RaftSimConfig::real_time_limit`] ends the process, naming the seed.

mod net;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use felix_broker::{Broker, ConsistencyLevel, StreamHandle, StreamMetadata};
use felix_cluster::history::checker::{self, Report};
use felix_cluster::history::model::{
    Action, AppendOutcome, Consistency, Element, FaultEvent, History, ListSpec, Op,
};
use felix_cluster::history::rng::Rng;
use felix_replication::raft::{
    AckAt, GroupId, GroupRuntime, MemberId, RaftEngine, RaftReads, Role, RuntimeConfig,
};
use felix_replication::{ShardKey, ShardKind};
use felix_storage::log::{AppendOnlyLog, FsyncMode, LogConfig, ReadRange, RecordMark};
use felix_storage::{DiskLog, EphemeralCache};
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use self::net::{Links, Wire};
use crate::client::decode;
use crate::run::Deadline;

const TENANT: &str = "t";
const NAMESPACE: &str = "ns";
/// How long a publisher waits for its acknowledgement.
const ANSWER: Duration = Duration::from_secs(2);
/// How long after the heal a shard may take to serve again.
const RECOVERY: Duration = Duration::from_secs(30);

/// Whether the members' clocks agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clocks {
    Off,
    /// Each member's timers run at its own rate, 0.8x to 1.2x. The engine
    /// reads no clock but its timers, so this is what skew does to it.
    Bounded,
}

#[derive(Debug, Clone)]
pub struct RaftSimConfig {
    pub clocks: Clocks,
    pub shards: u32,
    /// How long faults are injected.
    pub nemesis: Duration,
    /// Virtual time the whole run may take. A healthy seed needs the
    /// nemesis and a few seconds more.
    pub run_limit: Duration,
    /// Real time the whole run may take.
    pub real_time_limit: Duration,
}

impl Default for RaftSimConfig {
    fn default() -> Self {
        Self {
            clocks: Clocks::Off,
            shards: 2,
            nemesis: Duration::from_secs(60),
            run_limit: Duration::from_secs(360),
            real_time_limit: Duration::from_secs(900),
        }
    }
}

/// What a seed's run found.
#[derive(Debug)]
pub struct RaftOutcome {
    pub seed: u64,
    pub members: usize,
    /// What the history checker found in what the clients saw.
    pub report: Report,
    /// What the group checks found.
    pub findings: Vec<String>,
    /// How many terms had a leader, over every shard, as sampled each
    /// second. More than the shard count means leadership moved.
    pub terms_led: usize,
    pub history: History,
    /// What each member's live subscribers were handed, in delivery order.
    pub deliveries: Vec<Delivery>,
}

/// A record a member's broker handed a live subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// When, on the run's clock.
    pub at: u64,
    pub member: usize,
    pub list: String,
    pub offset: u64,
    pub value: u64,
}

impl RaftOutcome {
    pub fn passed(&self) -> bool {
        self.report.violations.is_empty()
            && self.report.undecided.is_empty()
            && self.findings.is_empty()
    }

    /// The run as one line per event, for comparing two runs of a seed.
    pub fn trace(&self) -> Vec<String> {
        let history = &self.history;
        let mut lines = vec![format!(
            "members {}, terms led {}",
            self.members, self.terms_led
        )];
        lines.extend(history.faults.iter().map(|e| format!("fault {e:?}")));
        lines.extend(history.ops.iter().map(|op| format!("op {op:?}")));
        lines.extend(self.deliveries.iter().map(|d| format!("delivery {d:?}")));
        lines.extend(
            history
                .final_reads
                .iter()
                .map(|r| format!("final read {r:?}")),
        );
        let violations = self.report.violations.iter();
        lines.extend(violations.map(|v| format!("violation {v:?}")));
        lines.extend(self.findings.iter().map(|f| format!("finding {f:?}")));
        lines
    }

    /// [`Self::trace`] in one number (FNV-1a), the same in every build.
    pub fn trace_hash(&self) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for line in self.trace() {
            for byte in line.bytes().chain([b'\n']) {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        hash
    }

    /// The first thing that failed, in one line. Empty for a run that passed.
    pub fn first_failure(&self) -> String {
        if let Some(violation) = self.report.violations.first() {
            return format!("history: {violation}").replace('\n', " ");
        }
        if let Some(note) = self.report.undecided.first() {
            return format!("history: undecided: {note}").replace('\n', " ");
        }
        self.findings
            .first()
            .map_or(String::new(), |finding| format!("group: {finding}"))
    }
}

impl std::fmt::Display for RaftOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "raft seed {}: {} members", self.seed, self.members)?;
        writeln!(f, "{}", self.history.summary())?;
        writeln!(
            f,
            "{} terms led, {} live deliveries, trace {:016x}",
            self.terms_led,
            self.deliveries.len(),
            self.trace_hash()
        )?;
        for fault in &self.history.faults {
            writeln!(f, "  {:>8} ms  {}", fault.at / 1_000_000, fault.what)?;
        }
        for violation in &self.report.violations {
            writeln!(f, "history: {violation}")?;
        }
        for note in &self.report.undecided {
            writeln!(f, "history: undecided: {note}")?;
        }
        for finding in &self.findings {
            writeln!(f, "group: {finding}")?;
        }
        Ok(())
    }
}

/// Run `seed` on a paused clock and check it.
pub fn run_seed(seed: u64, config: RaftSimConfig) -> RaftOutcome {
    // The virtual bound in `run` needs the runtime to poll again. Work that
    // holds the thread would hang the test binary, so the process exits.
    let limit = config.real_time_limit;
    let _bound = Deadline::exits(
        limit,
        format!(
            "raft seed {seed} ran past {limit:?} of real time: something holds the simulation \
             thread and never returns to the runtime. To run it again:\n    \
             FELIX_SIM_SEED_START={seed} FELIX_SIM_SEEDS=1 cargo test -p felix-sim --test \
             raft_seeds -- --ignored --exact raft_seeds --nocapture"
        ),
    );
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all().start_paused(true);
    #[cfg(tokio_unstable)]
    builder.rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()));
    let runtime = builder.build().expect("build the simulation runtime");
    runtime.block_on(run(seed, config))
}

/// A member's data directory: a name on the in-memory filesystem.
struct DataDir(PathBuf);

impl DataDir {
    fn new(member: usize) -> Self {
        let base = std::env::temp_dir().join("felix-sim-raft");
        for attempt in 0.. {
            let path = base.join(format!("b{}-{attempt}", member + 1));
            if felix_storage::sim_fs::claim_root(&path) {
                return Self(path);
            }
        }
        unreachable!("some attempt finds a free path")
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        felix_storage::sim_fs::unregister_root(&self.0);
    }
}

struct Member {
    runtime: GroupRuntime,
    engine: RaftEngine,
    broker: Broker,
    /// Per shard: the broker's stream and the Raft log under it.
    streams: Vec<StreamHandle>,
    logs: Vec<DiskLog>,
    _dir: DataDir,
}

struct Sim {
    members: Vec<Member>,
    links: Arc<Links>,
    shards: u32,
    begun: Instant,
    history: Mutex<History>,
    findings: Mutex<Vec<String>>,
    deliveries: Mutex<Vec<Delivery>>,
    terms_led: AtomicUsize,
    /// What the run is doing, for the finding of a run that never ends.
    phase: Mutex<&'static str>,
    next_value: AtomicU64,
    next_process: AtomicUsize,
}

fn stream(shard: u32) -> String {
    format!("s{shard}")
}

fn list(shard: u32) -> String {
    format!("{TENANT}/{NAMESPACE}/{}", stream(shard))
}

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.into(),
        namespace: NAMESPACE.into(),
        stream: stream(shard),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn name(member: usize) -> String {
    format!("b{}", member + 1)
}

/// The shipped timers as a member whose clock runs at `rate_ppm` keeps them.
fn timers(rate_ppm: u64) -> RuntimeConfig {
    let at = |d: Duration| Duration::from_micros(d.as_micros() as u64 * 1_000_000 / rate_ppm);
    let shipped = RuntimeConfig::default();
    RuntimeConfig {
        tick: at(shipped.tick),
        heartbeat: at(shipped.heartbeat),
        idle_heartbeat: at(shipped.idle_heartbeat),
        idle_after: at(shipped.idle_after),
        election_min: at(shipped.election_min),
        election_max: at(shipped.election_max),
    }
}

impl Sim {
    async fn start(config: &RaftSimConfig, rng: &mut Rng) -> anyhow::Result<Arc<Self>> {
        let count = if rng.percent(50) { 3 } else { 5 };
        let links = Arc::new(Links::default());
        let voters: BTreeSet<MemberId> = (0..count).map(|m| MemberId::new(name(m), 1)).collect();
        let mut members = Vec::new();
        for member in 0..count {
            let rate = match config.clocks {
                Clocks::Off => 1_000_000,
                Clocks::Bounded => 800_000 + rng.below(400_001),
            };
            let wire = Arc::new(Wire::new(member, links.clone(), rng.fork()));
            let runtime = GroupRuntime::start(name(member), timers(rate), wire)
                .map_err(|err| anyhow::anyhow!("timers: {err:?}"))?;
            let engine = RaftEngine::new(runtime.clone());
            let broker = Broker::new(EphemeralCache::new().into());
            broker.set_read_bounds(Arc::new(RaftReads::new(engine.clone())));
            broker.register_tenant(TENANT).await?;
            broker.register_namespace(TENANT, NAMESPACE).await?;
            let dir = DataDir::new(member);
            let mut logs = Vec::new();
            for shard in 0..config.shards {
                let path = dir.0.join(stream(shard));
                std::fs::create_dir_all(&path)?;
                let log_config = LogConfig {
                    fsync_mode: FsyncMode::OnCommit,
                    preallocate_segments: false,
                    raft_log: true,
                    ..LogConfig::default()
                };
                let log = DiskLog::open(path, list(shard), log_config)?;
                runtime
                    .open(GroupId::new(key(shard), 1), 1, log.clone())
                    .await
                    .map_err(|err| anyhow::anyhow!("open: {err:?}"))?;
                logs.push(log);
            }
            members.push(Member {
                runtime,
                engine,
                broker,
                streams: Vec::new(),
                logs,
                _dir: dir,
            });
        }
        links.connect(members.iter().map(|m| m.runtime.clone()).collect());
        for member in &mut members {
            for shard in 0..config.shards {
                let metadata = StreamMetadata {
                    consistency: ConsistencyLevel::Leader,
                    ..Default::default()
                };
                let stream = stream(shard);
                let registered = member
                    .broker
                    .register_stream(TENANT, NAMESPACE, &stream, metadata);
                registered.await?;
                let handle = member
                    .broker
                    .resolve_stream_handle(TENANT, NAMESPACE, &stream, 0)
                    .await?;
                handle.bound_every_level();
                member.engine.follow_commits(&key(shard), handle.clone());
                member.streams.push(handle);
                member
                    .runtime
                    .initialize(&key(shard), voters.clone())
                    .await
                    .map_err(|err| anyhow::anyhow!("initialize: {err:?}"))?;
            }
        }
        let mut history = History::default();
        for shard in 0..config.shards {
            // The stream's level is `Leader`, but a Raft shard acknowledges
            // at commit whatever the level. Checked as `Leader`, a lost
            // acknowledged write and a rewritten tail would pass.
            let spec = ListSpec {
                consistency: Consistency::Quorum,
                base: 0,
            };
            history.lists.insert(list(shard), spec);
        }
        Ok(Arc::new(Self {
            members,
            links,
            shards: config.shards,
            begun: Instant::now(),
            history: Mutex::new(history),
            findings: Mutex::new(Vec::new()),
            deliveries: Mutex::new(Vec::new()),
            terms_led: AtomicUsize::new(0),
            phase: Mutex::new("starting"),
            next_value: AtomicU64::new(1),
            next_process: AtomicUsize::new(0),
        }))
    }

    fn now(&self) -> u64 {
        self.begun.elapsed().as_nanos() as u64
    }

    fn finding(&self, text: String) {
        let mut findings = self.findings.lock();
        if !findings.contains(&text) {
            findings.push(text);
        }
    }

    fn fault(&self, what: String) {
        let at = self.now();
        self.history.lock().faults.push(FaultEvent { at, what });
    }

    /// The member that says it leads `shard` and has opened it.
    fn leader(&self, shard: u32) -> Option<usize> {
        let key = key(shard);
        (0..self.members.len()).find(|member| {
            let member = &self.members[*member];
            let leads = member
                .runtime
                .status(&key)
                .is_some_and(|status| status.role == Role::Leader);
            leads && member.engine.open_term(&key).is_some()
        })
    }

    /// Publish a fresh value to `shard` on `member` and record how it ended.
    /// Every error is recorded as unknown: a refused batch may still commit.
    async fn append(&self, process: usize, member: usize, shard: u32) -> bool {
        let value = self.next_value.fetch_add(1, Ordering::Relaxed);
        let at = &self.members[member];
        let invoke = self.now();
        let payload = Bytes::from(value.to_le_bytes().to_vec());
        // 4e: a Periodic shard publishes with the written-majority ack here.
        let proposed = at.engine.publish(
            &at.broker,
            &at.streams[shard as usize],
            &key(shard),
            vec![payload],
            AckAt::Flushed,
            ANSWER,
        );
        let outcome = match proposed {
            Ok(proposed) => match proposed.complete().await {
                Ok(done) => {
                    let offset = done.offsets.map(|(first, _)| first);
                    self.acknowledged(member, shard, offset);
                    AppendOutcome::Ok { offset }
                }
                Err(_) => AppendOutcome::Info,
            },
            Err(_) => AppendOutcome::Info,
        };
        self.history.lock().ops.push(Op {
            process,
            invoke,
            complete: self.now(),
            action: Action::Append {
                list: list(shard),
                value,
                outcome,
            },
        });
        matches!(outcome, AppendOutcome::Ok { .. })
    }

    /// An acknowledged record is committed, so the member that acknowledged
    /// it has it below its read bound, on a majority's disks.
    fn acknowledged(&self, member: usize, shard: u32, offset: Option<u64>) {
        let Some(offset) = offset else {
            self.finding(format!(
                "{}: {} acknowledged a write without an offset",
                list(shard),
                name(member)
            ));
            return;
        };
        let key = key(shard);
        let bound = self.members[member].engine.read_bound(&key).unwrap_or(0);
        if bound <= offset {
            self.finding(format!(
                "{}: {} acknowledged offset {offset} with its read bound at {bound}",
                list(shard),
                name(member)
            ));
        }
        let holders = self.flushed_past(shard, offset);
        if holders < self.majority() {
            self.finding(format!(
                "{}: {} acknowledged offset {offset}, which only {holders} of {} members have \
                 flushed",
                list(shard),
                name(member),
                self.members.len()
            ));
        }
    }

    fn majority(&self) -> usize {
        self.members.len() / 2 + 1
    }

    /// How many members have `offset` of `shard` on disk.
    fn flushed_past(&self, shard: u32, offset: u64) -> usize {
        let key = key(shard);
        self.members
            .iter()
            .filter_map(|member| member.runtime.status(&key))
            .filter(|status| status.flushed.is_some_and(|point| point.index >= offset))
            .count()
    }

    /// A live subscriber on `member`'s broker. Only the member that leads
    /// has publishers, so this sees what it fans out while it leads. A
    /// record it is handed is committed: below the member's read bound then,
    /// and in the log for good (checked against the final read).
    async fn subscriber(self: Arc<Self>, member: usize, shard: u32, stop: CancellationToken) {
        let at = &self.members[member];
        let stream = stream(shard);
        let subscribed = at.broker.subscribe(TENANT, NAMESPACE, &stream, 0);
        let Ok(subscription) = subscribed.await else {
            self.finding(format!(
                "{}: {} refused a subscriber",
                list(shard),
                name(member)
            ));
            return;
        };
        let (mut receiver, _guard) = subscription.into_parts();
        let key = key(shard);
        let mut next = 0;
        loop {
            let envelope = tokio::select! {
                biased;
                envelope = receiver.recv() => envelope,
                () = stop.cancelled() => return,
            };
            let Some(envelope) = envelope else { return };
            let Some(base) = envelope.base_offset() else {
                self.finding(format!(
                    "{}: {} delivered a batch without an offset",
                    list(shard),
                    name(member)
                ));
                continue;
            };
            let bound = at.engine.read_bound(&key).unwrap_or(0);
            for (offset, payload) in (base..).zip(envelope.payloads()) {
                if offset < next {
                    self.finding(format!(
                        "{}: {} delivered offset {offset} after offset {}",
                        list(shard),
                        name(member),
                        next - 1
                    ));
                }
                next = offset + 1;
                if bound <= offset {
                    self.finding(format!(
                        "{}: {} delivered offset {offset} with its read bound at {bound}",
                        list(shard),
                        name(member)
                    ));
                }
                self.deliveries.lock().push(Delivery {
                    at: self.now(),
                    member,
                    list: list(shard),
                    offset,
                    value: decode(payload),
                });
            }
        }
    }

    /// Every delivered record is in the final log at its offset. Offsets
    /// past the final read are left alone: a batch whose publisher timed out
    /// can commit, and be delivered, after the read was taken.
    fn check_deliveries(&self) {
        let history = self.history.lock();
        for delivery in self.deliveries.lock().iter() {
            let Some(log) = history.final_reads.get(&delivery.list) else {
                continue;
            };
            if log.last().is_none_or(|last| delivery.offset > last.offset) {
                continue;
            }
            let held = log.binary_search_by_key(&delivery.offset, |element| element.offset);
            let held = held.ok().map(|at| log[at].value);
            if held != Some(delivery.value) {
                let held = held.map_or("no record".to_string(), |value| format!("value {value}"));
                self.finding(format!(
                    "{}: {} delivered value {} at offset {}, where the log holds {held}",
                    delivery.list,
                    name(delivery.member),
                    delivery.value,
                    delivery.offset
                ));
            }
        }
    }

    /// What `member` serves of `shard`: its log below its read bound. `None`
    /// when it refuses reads.
    async fn read(&self, member: usize, shard: u32) -> Option<Vec<Element>> {
        let at = &self.members[member];
        let bound = at.engine.read_bound(&key(shard)).ok()?;
        let log = &at.logs[shard as usize];
        let (mut elements, mut skipped, mut start) = (Vec::new(), 0, 0);
        while start < bound {
            let range = ReadRange {
                start,
                max_bytes: 1 << 20,
            };
            let page = log.read_range(range).await.ok()?;
            if page.records.is_empty() {
                break;
            }
            for record in page.records {
                start = record.offset + 1;
                if record.offset >= bound {
                    return Some(elements);
                }
                if matches!(record.mark, RecordMark::None) {
                    elements.push(Element {
                        offset: record.offset,
                        value: decode(&record.payload),
                        skipped_before: skipped,
                    });
                    skipped = 0;
                } else {
                    // A blank or membership entry: an offset no read returns.
                    skipped += 1;
                }
            }
        }
        Some(elements)
    }

    async fn writer(self: Arc<Self>, shard: u32, mut rng: Rng, stop: CancellationToken) {
        let process = self.next_process.fetch_add(1, Ordering::Relaxed);
        while !stop.is_cancelled() {
            let pause = Duration::from_millis(5)..Duration::from_millis(60);
            tokio::time::sleep(rng.between(pause.start, pause.end)).await;
            // Mostly the leader, sometimes whoever: a member that does not
            // lead must refuse, not write.
            let member = match self.leader(shard) {
                Some(leader) if !rng.percent(15) => leader,
                _ => rng.below(self.members.len() as u64) as usize,
            };
            self.append(process, member, shard).await;
        }
    }

    async fn reader(self: Arc<Self>, shard: u32, mut rng: Rng, stop: CancellationToken) {
        let process = self.next_process.fetch_add(1, Ordering::Relaxed);
        while !stop.is_cancelled() {
            tokio::time::sleep(rng.between(Duration::from_millis(50), Duration::from_millis(400)))
                .await;
            let member = rng.below(self.members.len() as u64) as usize;
            let invoke = self.now();
            if let Some(observed) = self.read(member, shard).await {
                self.history.lock().ops.push(Op {
                    process,
                    invoke,
                    complete: self.now(),
                    action: Action::Read {
                        list: list(shard),
                        observed,
                    },
                });
            }
        }
    }

    /// Faults until `length` has passed, each healed before the next.
    async fn nemesis(&self, mut rng: Rng, length: Duration) {
        let end = Instant::now() + length;
        let count = self.members.len();
        while Instant::now() < end {
            tokio::time::sleep(rng.between(Duration::from_secs(1), Duration::from_secs(4))).await;
            let pick = rng.below(count as u64) as usize;
            match rng.below(4) {
                0 => {
                    self.links.isolate(pick, count);
                    self.fault(format!("isolate {}", name(pick)));
                }
                1 => {
                    // A minority, cut from the rest and joined to each other.
                    let minority: Vec<usize> = (0..count / 2).map(|n| (pick + n) % count).collect();
                    for inside in &minority {
                        for outside in (0..count).filter(|m| !minority.contains(m)) {
                            self.links.cut(*inside, outside);
                            self.links.cut(outside, *inside);
                        }
                    }
                    let names: Vec<_> = minority.iter().map(|m| name(*m)).collect();
                    self.fault(format!("split off {}", names.join(" ")));
                }
                2 => {
                    let other = (pick + 1 + rng.below(count as u64 - 1) as usize) % count;
                    self.links.cut(pick, other);
                    self.fault(format!(
                        "one way: {} cannot reach {}",
                        name(pick),
                        name(other)
                    ));
                }
                _ => {
                    let shard = rng.below(u64::from(self.shards)) as u32;
                    let leader = self.leader(shard).unwrap_or(pick);
                    self.links.isolate(leader, count);
                    self.fault(format!(
                        "isolate {}, leader of {}",
                        name(leader),
                        list(shard)
                    ));
                }
            }
            tokio::time::sleep(rng.between(Duration::from_secs(2), Duration::from_secs(10))).await;
            self.links.heal();
            self.fault("heal".into());
        }
    }

    /// Checked every second of the run, faults or not.
    async fn watch(self: Arc<Self>, stop: CancellationToken) {
        let mut leaders: BTreeMap<(u32, u64), usize> = BTreeMap::new();
        let mut bounds: BTreeMap<(u32, usize), u64> = BTreeMap::new();
        let majority = self.majority();
        while !stop.is_cancelled() {
            for shard in 0..self.shards {
                let key = key(shard);
                let statuses: Vec<_> = self
                    .members
                    .iter()
                    .map(|member| member.runtime.status(&key))
                    .collect();
                for (member, status) in statuses.iter().enumerate() {
                    let Some(status) = status else { continue };
                    if status.role == Role::Leader {
                        let first = *leaders.entry((shard, status.term)).or_insert(member);
                        self.terms_led.store(leaders.len(), Ordering::Relaxed);
                        if first != member {
                            self.finding(format!(
                                "{}: {} and {} both led term {}",
                                list(shard),
                                name(first),
                                name(member),
                                status.term
                            ));
                        }
                    }
                    let Ok(bound) = self.members[member].engine.read_bound(&key) else {
                        continue;
                    };
                    let before = bounds.insert((shard, member), bound).unwrap_or(0);
                    if bound < before {
                        self.finding(format!(
                            "{}: {}'s read bound went back from {before} to {bound}",
                            list(shard),
                            name(member)
                        ));
                    }
                    // A record below the bound is committed, so a majority
                    // has it on disk.
                    let holders = match bound.checked_sub(1) {
                        Some(last) => self.flushed_past(shard, last),
                        None => majority,
                    };
                    if holders < majority {
                        self.finding(format!(
                            "{}: {} serves up to {bound}, which only {holders} of {} members have \
                             flushed",
                            list(shard),
                            name(member),
                            self.members.len()
                        ));
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// After the heal every shard gets an open leader that acknowledges a
    /// write.
    async fn recover(&self) {
        let process = self.next_process.fetch_add(1, Ordering::Relaxed);
        for shard in 0..self.shards {
            let deadline = Instant::now() + RECOVERY;
            let mut written = false;
            while !written && Instant::now() < deadline {
                if let Some(leader) = self.leader(shard) {
                    written = self.append(process, leader, shard).await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            if !written {
                self.finding(format!(
                    "{}: no leader acknowledged a write within {RECOVERY:?} of the heal",
                    list(shard)
                ));
            }
        }
    }

    /// With the writers stopped: no member refuses a read and every member
    /// serves the same records. The leader's read is the list's final read.
    async fn final_reads(&self) {
        for shard in 0..self.shards {
            let deadline = Instant::now() + RECOVERY;
            let mut reads = Vec::new();
            loop {
                reads.clear();
                for member in 0..self.members.len() {
                    reads.push(self.read(member, shard).await);
                }
                let same = reads.iter().all(|read| read.is_some() && *read == reads[0]);
                if same || Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            for (member, read) in reads.iter().enumerate() {
                match read {
                    None => self.finding(format!(
                        "{}: {} still refuses reads {RECOVERY:?} after the heal",
                        list(shard),
                        name(member)
                    )),
                    Some(read) if Some(read) != reads[0].as_ref() => self.finding(format!(
                        "{}: {} serves {} records and {} serves {}, or not the same ones",
                        list(shard),
                        name(member),
                        read.len(),
                        name(0),
                        reads[0].as_ref().map_or(0, Vec::len)
                    )),
                    Some(_) => {}
                }
            }
            let mut history = self.history.lock();
            match self.leader(shard).and_then(|leader| reads[leader].clone()) {
                Some(read) => {
                    history.final_reads.insert(list(shard), read);
                }
                None => {
                    history.unread_lists.insert(list(shard));
                    drop(history);
                    self.finding(format!(
                        "{}: no leader to take the final read from",
                        list(shard)
                    ));
                }
            }
        }
    }
}

async fn run(seed: u64, config: RaftSimConfig) -> RaftOutcome {
    let mut rng = Rng::new(seed);
    let sim = match Sim::start(&config, &mut rng).await {
        Ok(sim) => sim,
        Err(err) => {
            return RaftOutcome {
                seed,
                members: 0,
                report: checker::check(&History::default()),
                findings: vec![format!("the groups did not start: {err:#}")],
                terms_led: 0,
                history: History::default(),
                deliveries: Vec::new(),
            };
        }
    };
    let (stop, watching) = (CancellationToken::new(), CancellationToken::new());
    let driven = drive(&sim, &config, rng, stop.clone(), watching.clone());
    if tokio::time::timeout(config.run_limit, driven)
        .await
        .is_err()
    {
        stop.cancel();
        watching.cancel();
        sim.finding(format!(
            "stalled: the run was still {} after {:?} of virtual time",
            *sim.phase.lock(),
            config.run_limit
        ));
        let mut history = sim.history.lock();
        for shard in 0..config.shards {
            if !history.final_reads.contains_key(&list(shard)) {
                history.unread_lists.insert(list(shard));
            }
        }
    }
    sim.check_deliveries();
    let history = sim.history.lock().clone();
    RaftOutcome {
        seed,
        members: sim.members.len(),
        report: checker::check(&history),
        findings: sim.findings.lock().clone(),
        terms_led: sim.terms_led.load(Ordering::Relaxed),
        history,
        deliveries: sim.deliveries.lock().clone(),
    }
}

async fn drive(
    sim: &Arc<Sim>,
    config: &RaftSimConfig,
    mut rng: Rng,
    stop: CancellationToken,
    watching: CancellationToken,
) {
    let phase = |now| *sim.phase.lock() = now;
    let mut tasks = vec![tokio::spawn(sim.clone().watch(watching.clone()))];
    for shard in 0..config.shards {
        for member in 0..sim.members.len() {
            let subscriber = sim.clone().subscriber(member, shard, watching.clone());
            tasks.push(tokio::spawn(subscriber));
        }
    }
    let watchers = tasks.len();
    for shard in 0..config.shards {
        for _ in 0..2 {
            tasks.push(tokio::spawn(sim.clone().writer(
                shard,
                rng.fork(),
                stop.clone(),
            )));
        }
        tasks.push(tokio::spawn(sim.clone().reader(
            shard,
            rng.fork(),
            stop.clone(),
        )));
    }
    phase("injecting faults");
    sim.nemesis(rng.fork(), config.nemesis).await;
    sim.links.heal();
    let clients = tasks.split_off(watchers);
    phase("waiting for each shard to acknowledge a write after the heal");
    sim.recover().await;
    // Nothing is written while the final reads are taken, so a record
    // acknowledged and missing from them is lost.
    phase("waiting for the clients to finish");
    stop.cancel();
    for task in clients {
        let _ = task.await;
    }
    phase("taking the final reads");
    sim.final_reads().await;
    phase("stopping the watch and the subscribers");
    watching.cancel();
    for task in tasks {
        let _ = task.await;
    }
    phase("stopping the members");
    for member in &sim.members {
        member.runtime.shutdown().await;
    }
}

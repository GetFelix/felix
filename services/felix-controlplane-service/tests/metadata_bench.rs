//! The metadata group's benchmark: what the control plane's Raft group costs
//! on its store today, so a later store can be held to "no cell worse".
//!
//! Ignored by default; it measures, it does not assert. Run it with
//! `scripts/ci/metadata_bench.sh`, or by hand:
//!
//! ```text
//! FELIX_METADATA_BENCH_OUT=bench.json FELIX_METADATA_BENCH_SCALE=quick \
//!   cargo test -p felix-controlplane-service --release --test metadata_bench -- --ignored --nocapture
//! ```
//!
//! Members run in this process, each on its own redb directory and its own
//! peer listener, driven through the store traits the HTTP API uses. The
//! output is a JSON map of cell name to metrics, read by
//! `scripts/ci/metadata_bench.py`. The cells are listed in
//! `docs/metadata-raft-design.md`, "Perf gate".
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use felix_controlplane_service::model::{
    ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity, NodeLifecycle, NodeSpec,
    NodeStatus, ReplicaReport, RetentionPolicy, ShardKey, Stream, StreamKind, Tenant,
};
use felix_controlplane_service::raft::{
    AppStateMachine, NodeId, PeerSecurity, RaftHandle, RaftSettings,
};
use felix_controlplane_service::store::memory::InMemoryStore;
use felix_controlplane_service::store::raft::RaftStore;
use felix_controlplane_service::store::raft::state_machine::MetadataStateMachine;
use felix_controlplane_service::store::{ControlPlaneStore, StoreConfig};
use serde_json::{Value, json};

const TENANT: &str = "bench";
const NAMESPACE: &str = "ns";

/// Measurement windows and sizes. `quick` proves the harness works in a few
/// minutes; `full` is what the gate compares.
struct Scale {
    window: Duration,
    idle: Duration,
    failover_rounds: usize,
    shards: u32,
    snapshot_sizes: Vec<(&'static str, usize)>,
}

impl Scale {
    fn from_env() -> Self {
        match std::env::var("FELIX_METADATA_BENCH_SCALE").as_deref() {
            Ok("full") => Self {
                window: Duration::from_secs(10),
                idle: Duration::from_secs(30),
                failover_rounds: 5,
                shards: 1_500,
                snapshot_sizes: vec![("1MiB", 1 << 20), ("32MiB", 32 << 20)],
            },
            _ => Self {
                window: Duration::from_secs(2),
                idle: Duration::from_secs(5),
                failover_rounds: 1,
                shards: 64,
                snapshot_sizes: vec![("1MiB", 1 << 20)],
            },
        }
    }
}

fn security() -> PeerSecurity {
    PeerSecurity {
        cluster_id: "bench-cluster".to_string(),
        token: Some("0123456789abcdef0123456789abcdef".to_string()),
        tls: None,
    }
}

/// Counts what the metadata state machine does, around the real one.
struct Probe {
    inner: Arc<MetadataStateMachine>,
    snapshots: AtomicU64,
    last_snapshot_nanos: AtomicU64,
}

#[async_trait::async_trait]
impl AppStateMachine for Probe {
    async fn apply(&self, command: &[u8]) -> Vec<u8> {
        self.inner.apply(command).await
    }
    async fn snapshot(&self) -> Vec<u8> {
        let started = Instant::now();
        let bytes = self.inner.snapshot().await;
        self.last_snapshot_nanos
            .store(started.elapsed().as_nanos() as u64, Ordering::SeqCst);
        self.snapshots.fetch_add(1, Ordering::SeqCst);
        bytes
    }
    async fn restore(&self, snapshot: &[u8]) {
        self.inner.restore(snapshot).await
    }
    fn restamp(&self, command: &[u8], now_millis: u64) -> Option<Vec<u8>> {
        AppStateMachine::restamp(&*self.inner, command, now_millis)
    }
    fn version(&self) -> u16 {
        self.inner.version()
    }
    fn join_floor(&self) -> u16 {
        self.inner.join_floor()
    }
}

struct Member {
    handle: RaftHandle,
    store: Arc<RaftStore>,
    probe: Arc<Probe>,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
    dir: tempfile::TempDir,
}

fn settings(id: NodeId, dir: &std::path::Path, snapshot_every: u64, kept: u64) -> RaftSettings {
    let mut settings = RaftSettings::new(id, dir.into(), security());
    settings.snapshot_logs_since_last = snapshot_every;
    settings.logs_kept_behind_snapshot = kept;
    settings
}

/// Open a member on `dir`: a new one on an empty directory, a restart on a used one.
async fn open_member(id: NodeId, dir: tempfile::TempDir, snapshot_every: u64) -> Member {
    match try_open_member(id, dir, snapshot_every).await {
        Ok(member) => member,
        Err((err, _)) => panic!("start member: {err:#}"),
    }
}

async fn try_open_member(
    id: NodeId,
    dir: tempfile::TempDir,
    snapshot_every: u64,
) -> Result<Member, (anyhow::Error, tempfile::TempDir)> {
    let inner = Arc::new(MetadataStateMachine::new(Arc::new(InMemoryStore::new(
        StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        },
    ))));
    let probe = Arc::new(Probe {
        inner: Arc::clone(&inner),
        snapshots: AtomicU64::new(0),
        last_snapshot_nanos: AtomicU64::new(0),
    });
    let handle = match RaftHandle::start(
        settings(id, dir.path(), snapshot_every, 0),
        Arc::clone(&probe) as Arc<dyn AppStateMachine>,
    )
    .await
    {
        Ok(handle) => handle,
        Err(err) => return Err((err, dir)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = handle.rpc_router();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok(Member {
        store: Arc::new(RaftStore::new(handle.clone(), inner)),
        handle,
        probe,
        addr,
        server,
        dir,
    })
}

/// A formed group with a leader and the bench tenant and namespace in it.
struct Group {
    members: Vec<Member>,
}

impl Group {
    async fn start(size: usize, snapshot_every: u64) -> Self {
        let mut members = Vec::new();
        for i in 0..size {
            let dir = tempfile::tempdir().expect("tempdir");
            members.push(open_member((i + 1) as NodeId, dir, snapshot_every).await);
        }
        let addrs: BTreeMap<NodeId, String> = members
            .iter()
            .map(|m| (m.handle.status().id, m.addr.to_string()))
            .collect();
        members[0]
            .handle
            .initialize(addrs)
            .await
            .expect("initialize");
        let group = Self { members };
        group.wait_for_leader().await;
        let store = &group.members[0].store;
        store
            .create_tenant(Tenant {
                tenant_id: TENANT.to_string(),
                display_name: TENANT.to_string(),
            })
            .await
            .expect("tenant");
        store
            .create_namespace(namespace("ns", 0))
            .await
            .expect("namespace");
        group
    }

    async fn wait_for_leader(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self
            .members
            .iter()
            .all(|m| m.handle.status().leader.is_some())
        {
            assert!(Instant::now() < deadline, "no leader elected");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn leader_index(&self) -> usize {
        let leader = self
            .members
            .iter()
            .find_map(|m| m.handle.status().leader)
            .expect("leader");
        self.members
            .iter()
            .position(|m| m.handle.status().id == leader)
            .expect("leader is a member")
    }

    /// Writes go to a follower when there is one, as the API behind a load
    /// balancer does, so the forwarding hop is in the number.
    fn writer(&self) -> Arc<RaftStore> {
        let leader = self.leader_index();
        let index = if self.members.len() > 1 {
            (leader + 1) % self.members.len()
        } else {
            leader
        };
        Arc::clone(&self.members[index].store)
    }

    async fn stop(self) {
        for member in &self.members {
            let _ = member.handle.shutdown().await;
            member.server.abort();
        }
    }
}

fn namespace(name: &str, payload: usize) -> Namespace {
    Namespace {
        tenant_id: TENANT.to_string(),
        namespace: name.to_string(),
        display_name: "x".repeat(payload),
    }
}

fn percentile(sorted: &[Duration], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[index].as_secs_f64() * 1e3
}

/// Closed-loop load: `proposers` tasks each run `op` back to back for
/// `window`. Returns throughput and latency quantiles of the acknowledged
/// operations, and how many failed.
async fn run_load<F, Fut>(proposers: usize, window: Duration, op: F) -> Value
where
    F: Fn(usize, u64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = bool> + Send + 'static,
{
    let op = Arc::new(op);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for proposer in 0..proposers {
        let op = Arc::clone(&op);
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut failed = 0u64;
            let mut i = 0u64;
            while started.elapsed() < window {
                let begun = Instant::now();
                if op(proposer, i).await {
                    latencies.push(begun.elapsed());
                } else {
                    failed += 1;
                }
                i += 1;
            }
            (latencies, failed)
        }));
    }
    let mut all = Vec::new();
    let mut failed = 0;
    for task in tasks {
        let (latencies, f) = task.await.expect("proposer");
        all.extend(latencies);
        failed += f;
    }
    let elapsed = started.elapsed().as_secs_f64();
    all.sort();
    json!({
        "ops_per_s": all.len() as f64 / elapsed,
        "p50_ms": percentile(&all, 0.50),
        "p99_ms": percentile(&all, 0.99),
        "failed": failed,
    })
}

/// Linux accounting for this process; zeros elsewhere, so the cells that use
/// it read as "not measured" rather than failing a laptop run.
#[derive(Clone, Copy, Default)]
struct Usage {
    cpu_secs: f64,
    rss_bytes: u64,
    written_bytes: u64,
}

fn usage() -> Usage {
    let mut usage = Usage::default();
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        // Fields after the parenthesised command name; utime and stime are
        // the 12th and 13th of those.
        if let Some(rest) = stat.rsplit_once(')').map(|(_, rest)| rest) {
            let fields: Vec<&str> = rest.split_whitespace().collect();
            let ticks = |i: usize| fields.get(i).and_then(|f| f.parse::<f64>().ok());
            if let (Some(user), Some(system)) = (ticks(11), ticks(12)) {
                usage.cpu_secs = (user + system) / 100.0;
            }
        }
    }
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm")
        && let Some(pages) = statm
            .split_whitespace()
            .nth(1)
            .and_then(|p| p.parse::<u64>().ok())
    {
        usage.rss_bytes = pages * 4096;
    }
    if let Ok(io) = std::fs::read_to_string("/proc/self/io") {
        for line in io.lines() {
            if let Some(value) = line.strip_prefix("write_bytes: ") {
                usage.written_bytes = value.trim().parse().unwrap_or(0);
            }
        }
    }
    usage
}

async fn propose_cells(scale: &Scale, cells: &mut BTreeMap<String, Value>) {
    for members in [1usize, 3] {
        for (label, payload) in [("256B", 256usize), ("16KiB", 16 << 10)] {
            for proposers in [1usize, 16, 128] {
                let group = Group::start(members, 1_000_000).await;
                let store = group.writer();
                let before = usage();
                let result = run_load(proposers, scale.window, move |p, i| {
                    let store = Arc::clone(&store);
                    async move {
                        store
                            .create_namespace(namespace(&format!("n-{p}-{i}"), payload))
                            .await
                            .is_ok()
                    }
                })
                .await;
                let after = usage();
                let mut result = result;
                let ops = result["ops_per_s"].as_f64().unwrap_or(0.0) * scale.window.as_secs_f64();
                if ops > 0.0 && after.written_bytes > before.written_bytes {
                    // The whole process writes: every member's log, so this is
                    // per committed entry across the group.
                    result["bytes_written_per_entry"] =
                        json!((after.written_bytes - before.written_bytes) as f64 / ops);
                }
                cells.insert(format!("propose/m{members}/{label}/p{proposers}"), result);
                group.stop().await;
            }
        }
    }
}

fn node_named(node_id: &str) -> Node {
    Node {
        node_id: node_id.to_string(),
        spec: NodeSpec {
            advertise_addr: "10.0.0.4:7000".to_string(),
            client_addr: None,
            kafka_addr: None,
            region: "r1".to_string(),
            zone: None,
            labels: Default::default(),
            capacity: NodeCapacity::default(),
        },
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: 0,
            registered_at_millis: 1,
            incarnation: 0,
            features: Default::default(),
            storage_id: None,
        },
    }
}

fn bench_stream(shards: u32) -> Stream {
    Stream {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: "s".to_string(),
        kind: StreamKind::Stream,
        shards,
        replication_factor: 1,
        retention: RetentionPolicy {
            max_age_seconds: None,
            max_size_bytes: None,
        },
        consistency: ConsistencyLevel::Leader,
        delivery: DeliveryGuarantee::AtMostOnce,
        durable: true,
        region: None,
        routing: Default::default(),
        incarnation: 0,
        mount: None,
    }
}

fn report_for(key: &ShardKey, generation: u64) -> ReplicaReport {
    ReplicaReport {
        inherited_storage: Default::default(),
        given_up: false,
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        key: key.clone(),
        generation,
        caught_up: Default::default(),
        offsets: Default::default(),
        reported_at_millis: 1,
        drained: false,
        leader_offset: None,
        halted: Default::default(),
        leader: None,
        node_incarnations: Default::default(),
        follower_storage: Default::default(),
        joint_start_acked: None,
        refused: None,
        holds_start: Default::default(),
    }
}

/// Stream creation, placement updates and the replica-report stream: the
/// writes the control plane's log carries in steady state.
async fn placement_cells(scale: &Scale, cells: &mut BTreeMap<String, Value>) {
    let group = Group::start(3, 1_000_000).await;
    let store = group.writer();
    store.register_node(node_named("n1")).await.expect("node");

    // Stream creates, one after another: the latency of one create.
    let creates = run_load(1, scale.window, {
        let store = Arc::clone(&store);
        move |_, i| {
            let store = Arc::clone(&store);
            async move {
                let mut stream = bench_stream(4);
                stream.stream = format!("create-{i}");
                store.create_stream(stream).await.is_ok()
            }
        }
    })
    .await;
    cells.insert("stream_create".to_string(), creates);

    let created = store
        .create_stream(bench_stream(scale.shards))
        .await
        .expect("stream with shards");
    let mut keys = Vec::new();
    for shard in 0..created.shards {
        let key = ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: "s".to_string(),
            shard,
            kind: Default::default(),
        };
        let assignment =
            felix_controlplane_service::cluster::placement::assignment_for(&key, "n1", Vec::new());
        let put = store
            .put_shard_assignment(assignment)
            .await
            .expect("assignment");
        keys.push((key, put.generation));
    }
    let keys = Arc::new(keys);

    // Placement updates: rewrite assignments the way a move or a repair does.
    let updates = run_load(4, scale.window, {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        move |p, i| {
            let store = Arc::clone(&store);
            let keys = Arc::clone(&keys);
            async move {
                let (key, _) = &keys[(p * 7919 + i as usize) % keys.len()];
                let assignment = felix_controlplane_service::cluster::placement::assignment_for(
                    key,
                    "n1",
                    Vec::new(),
                );
                store.put_shard_assignment(assignment).await.is_ok()
            }
        }
    })
    .await;
    cells.insert("placement_update".to_string(), updates);

    // The report stream. Generations moved on above, so read them back.
    let current = store.list_shard_assignments().await.expect("assignments");
    let generations: BTreeMap<(String, u32), u64> = current
        .into_iter()
        .map(|a| ((a.key.stream.clone(), a.key.shard), a.generation))
        .collect();
    let generations = Arc::new(generations);
    let reports = run_load(16, scale.window, {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        move |p, i| {
            let store = Arc::clone(&store);
            let keys = Arc::clone(&keys);
            let generations = Arc::clone(&generations);
            async move {
                let (key, _) = &keys[(p * 104_729 + i as usize) % keys.len()];
                let generation = generations
                    .get(&(key.stream.clone(), key.shard))
                    .copied()
                    .unwrap_or(1);
                store
                    .record_replica_report(report_for(key, generation), "n1")
                    .await
                    .is_ok()
            }
        }
    })
    .await;
    cells.insert("replica_report".to_string(), reports);
    group.stop().await;
}

/// Kill the leader and time the next acknowledged write.
async fn failover_cells(scale: &Scale, cells: &mut BTreeMap<String, Value>) {
    let mut millis = Vec::new();
    for round in 0..scale.failover_rounds {
        let group = Group::start(3, 1_000_000).await;
        let leader = group.leader_index();
        let survivor = Arc::clone(&group.members[(leader + 1) % 3].store);
        let began = Instant::now();
        let _ = group.members[leader].handle.shutdown().await;
        group.members[leader].server.abort();
        let written = survivor
            .create_namespace(namespace(&format!("after-{round}"), 256))
            .await;
        if written.is_ok() {
            millis.push(began.elapsed().as_secs_f64() * 1e3);
        }
        group.stop().await;
    }
    millis.sort_by(f64::total_cmp);
    let median = millis.get(millis.len() / 2).copied().unwrap_or(0.0);
    let worst = millis.last().copied().unwrap_or(0.0);
    cells.insert(
        "failover".to_string(),
        json!({ "p50_ms": median, "max_ms": worst, "rounds": millis.len() }),
    );
}

/// Fill one member to about `bytes` of metadata, in 16 KiB namespaces.
async fn fill(store: &Arc<RaftStore>, bytes: usize) {
    const ENTRY: usize = 16 << 10;
    let count = bytes / ENTRY;
    let mut tasks = Vec::new();
    for lane in 0..32 {
        let store = Arc::clone(store);
        tasks.push(tokio::spawn(async move {
            let mut i = lane;
            while i < count {
                store
                    .create_namespace(namespace(&format!("fill-{i}"), ENTRY))
                    .await
                    .expect("fill");
                i += 32;
            }
        }));
    }
    for task in tasks {
        task.await.expect("fill lane");
    }
}

/// Snapshot build, the write stall it causes, restart to ready behind it, and
/// an empty member catching up from it.
async fn snapshot_cells(scale: &Scale, cells: &mut BTreeMap<String, Value>) {
    for (label, bytes) in &scale.snapshot_sizes {
        let group = Group::start(1, 1_000_000).await;
        fill(&group.members[0].store, *bytes).await;

        // A writer runs through the snapshot; its slowest write is the stall.
        let writer = Arc::clone(&group.members[0].store);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worst = Arc::new(AtomicU64::new(0));
        let probe = Arc::clone(&group.members[0].probe);
        let background = tokio::spawn({
            let stop = Arc::clone(&stop);
            let worst = Arc::clone(&worst);
            async move {
                let mut i = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    let begun = Instant::now();
                    let _ = writer
                        .create_namespace(namespace(&format!("during-{i}"), 256))
                        .await;
                    worst.fetch_max(begun.elapsed().as_nanos() as u64, Ordering::SeqCst);
                    i += 1;
                }
            }
        });
        let before = probe.snapshots.load(Ordering::SeqCst);
        group.members[0]
            .handle
            .trigger_snapshot()
            .await
            .expect("snapshot");
        let deadline = Instant::now() + Duration::from_secs(120);
        while probe.snapshots.load(Ordering::SeqCst) == before {
            assert!(Instant::now() < deadline, "snapshot never built");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Let the snapshot reach the store before the stall is read.
        tokio::time::sleep(Duration::from_secs(2)).await;
        stop.store(true, Ordering::SeqCst);
        background.await.expect("writer");
        cells.insert(
            format!("snapshot_build/{label}"),
            json!({
                "build_ms": probe.last_snapshot_nanos.load(Ordering::SeqCst) as f64 / 1e6,
                "longest_write_ms": worst.load(Ordering::SeqCst) as f64 / 1e6,
            }),
        );

        // Restart to ready: stop the member and start it on the same directory.
        let mut group = group;
        let Member {
            handle,
            store,
            server,
            dir,
            ..
        } = group.members.remove(0);
        let _ = handle.shutdown().await;
        server.abort();
        let _ = server.await;
        drop((handle, store));
        // The database lock goes when the last clone of the handle does, which
        // a listener task that is still winding down may hold a moment longer.
        // That wait is not part of the measurement.
        let mut dir = dir;
        let (restarted, began) = loop {
            let began = Instant::now();
            match try_open_member(1, dir, 1_000_000).await {
                Ok(member) => break (member, began),
                Err((_, back)) => {
                    dir = back;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let deadline = Instant::now() + Duration::from_secs(120);
        while !restarted.handle.confirm_leadership().await {
            assert!(Instant::now() < deadline, "member never became ready");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        cells.insert(
            format!("restart_to_ready/{label}"),
            json!({ "ready_ms": began.elapsed().as_secs_f64() * 1e3 }),
        );
        let _ = restarted.handle.shutdown().await;
        restarted.server.abort();
        group.stop().await;

        install_cell(label, *bytes, cells).await;
    }
}

/// A two-member group with its log purged behind a snapshot, then an empty
/// member added: the time is the snapshot install and apply.
async fn install_cell(label: &str, bytes: usize, cells: &mut BTreeMap<String, Value>) {
    let group = Group::start(1, 1_000_000).await;
    fill(&group.members[0].store, bytes).await;
    let before = group.members[0].probe.snapshots.load(Ordering::SeqCst);
    group.members[0]
        .handle
        .trigger_snapshot()
        .await
        .expect("snapshot");
    while group.members[0].probe.snapshots.load(Ordering::SeqCst) == before {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let joiner = open_member(2, dir, 1_000_000).await;
    let began = Instant::now();
    let joined = group.members[0]
        .handle
        .add_learner(2, joiner.addr.to_string())
        .await;
    let elapsed = began.elapsed().as_secs_f64() * 1e3;
    cells.insert(
        format!("snapshot_install/{label}"),
        json!({ "install_ms": elapsed, "ok": joined.is_ok() }),
    );
    let _ = joiner.handle.shutdown().await;
    joiner.server.abort();
    group.stop().await;
}

/// An idle three-member group: CPU and memory of the whole process.
async fn idle_cell(scale: &Scale, cells: &mut BTreeMap<String, Value>) {
    let group = Group::start(3, 1_000_000).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let before = usage();
    tokio::time::sleep(scale.idle).await;
    let after = usage();
    cells.insert(
        "idle/m3".to_string(),
        json!({
            "cpu_percent": (after.cpu_secs - before.cpu_secs) / scale.idle.as_secs_f64() * 100.0,
            "rss_mib": after.rss_bytes as f64 / (1 << 20) as f64,
        }),
    );
    group.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a benchmark; run it with scripts/ci/metadata_bench.sh"]
async fn metadata_bench() {
    let scale = Scale::from_env();
    let mut cells = BTreeMap::new();
    propose_cells(&scale, &mut cells).await;
    placement_cells(&scale, &mut cells).await;
    failover_cells(&scale, &mut cells).await;
    snapshot_cells(&scale, &mut cells).await;
    idle_cell(&scale, &mut cells).await;

    let report = json!({ "scale": std::env::var("FELIX_METADATA_BENCH_SCALE").unwrap_or_else(|_| "quick".into()), "cells": cells });
    let text = serde_json::to_string_pretty(&report).expect("encode");
    println!("{text}");
    if let Ok(path) = std::env::var("FELIX_METADATA_BENCH_OUT") {
        std::fs::write(path, text).expect("write results");
    }
}

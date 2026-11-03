//! A lease cache keeps its renewals on time while shards are created in bulk
//! and a steady stream of appends lands on some of them.
//!
//! The failure this guards against is quiet: every broker stays up, no leader
//! changes, nothing is logged at ERROR, and the lease cache's writes still
//! wait longer than its TTL, so its leases lapse together. felixd hit it at
//! 1000 workloads (#1760). The causes were the replication driver handing its
//! pass slots to the same shards (#1794), a control-plane read awaited on the
//! peer request path (#1791), and read-round fences queued behind promotion
//! fences (#1796). `docs/cluster-harness.md` ("Shard starvation") says how to
//! read a failure.
//!
//! `docs/cluster-harness.md` has the commands to run each size.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use felix_client::{CacheCondition, Client};
use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

/// The canary: one small replicated cache, as felixd keeps its leases.
const CACHE: &str = "leases";
const KEYS: usize = 4;
/// A renewal applied later than `TTL - RENEW_EVERY` after the last one finds
/// the entry gone. felixd runs 10 s and a third of it; shorter here so a run
/// sees more renewals.
const TTL: Duration = Duration::from_secs(6);
const RENEW_EVERY: Duration = Duration::from_secs(2);
/// How long the canary runs on a quiet cluster first. The idle samples are
/// reported next to the loaded ones but not judged.
const IDLE: Duration = Duration::from_secs(12);
const APPEND_EVERY: Duration = Duration::from_millis(50);
const CREATES_IN_FLIGHT: usize = 16;
/// What felixd's cluster finalizes, so the canary's writes and reads take the
/// same path they take there: follower acks, and reads confirmed by a round of
/// fences.
const FEATURES: [&str; 4] = [
    "generation_start",
    "majority_ack",
    "lease_free_reads",
    "fenced_caches",
];

/// The slowest a renewal under load may be: one that takes longer lands after
/// the lease it renews has run out. A starved shard's renewals take 5 to 6 s.
/// A debug build saturates the per-PR runner's CPU, which slows every quorum
/// write, and healthy renewals there reached 2.2 s.
const RENEWAL_BOUND: Duration = TTL.saturating_sub(RENEW_EVERY);
/// The slowest a read under load may be. Reads stay under 0.4 s on a saturated
/// runner, and reads queued behind other shards' fences took 2.5 to 6 s.
/// `docs/cluster-harness.md` has the runs.
const READ_BOUND: Duration = Duration::from_secs(2);

/// How much load a run applies.
#[derive(Clone, Copy, Debug)]
struct Load {
    /// Single-shard `Quorum` streams with three replicas, created after the
    /// baseline. Each broker leads about a third, many times the replication
    /// driver's 16 pass slots.
    streams: usize,
    /// How many of those take an append every [`APPEND_EVERY`] from the
    /// moment they exist. Starvation needs more shards with work on a leader
    /// than its 16 pass slots, so this must be well over 48: at 32 the
    /// canary stays on time even without the #1794 fix.
    appended: usize,
    /// How long the appends go on after the last stream is created.
    hold: Duration,
}

impl Load {
    /// `default`, with any of `FELIX_SCALE_STREAMS`, `FELIX_SCALE_APPENDED`
    /// and `FELIX_SCALE_HOLD_SECS` overriding it, so a run can be sized
    /// without a rebuild.
    fn from_env(default: Load) -> Load {
        let read = |name: &str| {
            std::env::var(name).ok().map(|value| {
                value
                    .trim()
                    .parse::<usize>()
                    .unwrap_or_else(|_| panic!("{name} must be a number, not {value:?}"))
            })
        };
        Load {
            streams: read("FELIX_SCALE_STREAMS").unwrap_or(default.streams),
            appended: read("FELIX_SCALE_APPENDED").unwrap_or(default.appended),
            hold: read("FELIX_SCALE_HOLD_SECS")
                .map_or(default.hold, |secs| Duration::from_secs(secs as u64)),
        }
    }
}

/// **Renewals stay on time while every broker leads hundreds of busy shards.**
/// Sized for the per-PR cluster job.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_cache_renews_on_time_while_shards_are_created_in_bulk() {
    run(Load::from_env(Load {
        streams: 600,
        appended: 96,
        hold: Duration::from_secs(30),
    }))
    .await;
}

/// The same at felixd's scale (about 1500 streams), where #1760 was found.
/// Nightly (`scale-nightly.yml`).
#[serial]
#[ignore = "nightly: wants a release build and 1500 streams"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_cache_renews_on_time_at_felixd_scale() {
    run(Load::from_env(Load {
        streams: 1_500,
        appended: 96,
        hold: Duration::from_secs(60),
    }))
    .await;
}

async fn run(load: Load) {
    assert!(load.appended <= load.streams, "{load:?}");
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        caches: vec![CacheSpec::quorum(CACHE, 1, 3)],
        streams: vec![StreamSpec::quorum("probe", 1, 3)],
        // A deployment's sync interval: the brokers' share of the work.
        sync_interval_ms: 1_000,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            &FEATURES,
            felix_cluster::wait::budget(Duration::from_secs(30)),
        )
        .await
        .expect("every broker enables felixd's fleet features");
    // Placement on the control plane's own schedule, as in a deployment. New
    // shards are placed as they are created; this is what moves them after.
    cluster.run_placement(Duration::from_secs(5));

    let addrs = cluster.broker_addrs();
    let token = cluster.client_token();
    // Two clients, so the canary never waits behind the appends in a pool of
    // its own client's.
    let canary_client = felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &token)
        .await
        .expect("canary client");
    let append_client = Arc::new(
        felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &token)
            .await
            .expect("append client"),
    );

    let canary = Arc::new(Canary::new(
        canary_client,
        cluster.tenant_id.clone(),
        cluster.namespace.clone(),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    // The baseline starts once every key is claimed, so a slow first leader
    // election does not eat into it.
    let claimed = Arc::new(Barrier::new(KEYS + 1));
    let mut renewers = JoinSet::new();
    for key in 0..KEYS {
        let canary = Arc::clone(&canary);
        let stop = Arc::clone(&stop);
        let claimed = Arc::clone(&claimed);
        renewers.spawn(async move { canary.renew(format!("lease-{key}"), &claimed, &stop).await });
    }
    // Bounded: a renewer whose claim panicked never reaches the barrier.
    tokio::time::timeout(
        felix_cluster::wait::budget(Duration::from_secs(90)),
        claimed.wait(),
    )
    .await
    .expect("every lease key claimed");

    tokio::time::sleep(IDLE).await;
    canary.loaded.store(true, Ordering::Release);
    let started = Instant::now();

    let appends = Arc::new(Appends::default());
    let mut appenders = JoinSet::new();
    create_streams(&cluster, load, |index, name| {
        if index < load.appended {
            appenders.spawn(append(
                Arc::clone(&append_client),
                cluster.tenant_id.clone(),
                cluster.namespace.clone(),
                name,
                Arc::clone(&appends),
                Arc::clone(&stop),
            ));
        }
    })
    .await;
    let created_in = started.elapsed();
    tokio::time::sleep(load.hold).await;

    stop.store(true, Ordering::Release);
    // A renewer that panicked stopped measuring its key; that fails the run.
    while let Some(done) = renewers.join_next().await {
        done.expect("renewer");
    }
    while let Some(done) = appenders.join_next().await {
        done.expect("appender");
    }

    let report = canary.report();
    let summary = format!(
        "{load:?}: created in {created_in:.1?}; appends {} ok, {} failed; {report}",
        appends.ok.load(Ordering::Relaxed),
        appends.failed.load(Ordering::Relaxed),
    );
    // Printed either way, so a green nightly still records the margin.
    eprintln!("lease canary: {summary}");
    let mut problems = report.problems();
    // Without the load the canary passes for nothing.
    let loaded_streams = appends.streams.load(Ordering::Relaxed) as usize;
    if loaded_streams * 2 < load.appended {
        problems.push(format!(
            "only {loaded_streams} of {} appended streams took an append, so the load never applied",
            load.appended
        ));
    }
    if !problems.is_empty() {
        panic!(
            "the lease canary failed: {}\n{summary}\n{}",
            problems.join("; "),
            quorum_timeouts(&cluster),
        );
    }
}

/// Create `load.streams` streams through the control plane's API, as an
/// operator or felixd would, and call `created` with each one's index and
/// name as it exists.
async fn create_streams(cluster: &Cluster, load: Load, mut created: impl FnMut(usize, String)) {
    // Bounded, so a hung control plane fails the test instead of the job.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    let url = format!(
        "{}/v1/tenants/{}/namespaces/{}/streams",
        cluster.control_plane_url(),
        cluster.tenant_id,
        cluster.namespace
    );
    let token = cluster.admin_token();
    let mut in_flight = JoinSet::new();
    let mut next = 0;
    loop {
        while next < load.streams && in_flight.len() < CREATES_IN_FLIGHT {
            let (http, url, token) = (http.clone(), url.clone(), token.clone());
            let index = next;
            in_flight.spawn(async move {
                let name = format!("bulk-{index:05}");
                create_stream(&http, &url, &token, &name).await;
                (index, name)
            });
            next += 1;
        }
        match in_flight.join_next().await {
            Some(done) => {
                let (index, name) = done.expect("create task");
                created(index, name);
            }
            None => break,
        }
    }
}

async fn create_stream(http: &reqwest::Client, url: &str, token: &str, name: &str) {
    let body = serde_json::json!({
        "stream": name,
        "kind": "Stream",
        "shards": 1,
        "replication_factor": 3,
        "retention": { "max_age_seconds": null, "max_size_bytes": null },
        "consistency": "Quorum",
        "delivery": "AtLeastOnce",
        "durable": true,
    });
    // `busy` wrote nothing, so it is safe to send again.
    let mut waits_ms = [100, 250, 500, 1_000, 2_000, 4_000].into_iter();
    loop {
        let response = http
            .post(url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|err| panic!("create {name}: {err}"));
        let status = response.status();
        if status.is_success() || status == reqwest::StatusCode::CONFLICT {
            return;
        }
        let text = response.text().await.unwrap_or_default();
        let busy = status == reqwest::StatusCode::SERVICE_UNAVAILABLE && text.contains("busy");
        assert!(busy, "create {name}: {status}: {text}");
        let Some(wait_ms) = waits_ms.next() else {
            panic!("create {name}: the control plane stayed busy");
        };
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }
}

#[derive(Default)]
struct Appends {
    ok: AtomicU64,
    failed: AtomicU64,
    /// Streams that took at least one append.
    streams: AtomicU64,
}

/// Append to `stream` every [`APPEND_EVERY`] until `stop`. Failures are
/// counted, not fatal: a stream that is still being placed refuses for a
/// while. The run only checks that most streams took some appends.
async fn append(
    client: Arc<Client>,
    tenant_id: String,
    namespace: String,
    stream: String,
    appends: Arc<Appends>,
    stop: Arc<AtomicBool>,
) {
    let mut tick = tokio::time::interval(APPEND_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut took_one = false;
    while !stop.load(Ordering::Acquire) {
        tick.tick().await;
        let publish = client.publish(
            &tenant_id,
            &namespace,
            &stream,
            b"steady".to_vec(),
            AckMode::PerMessage,
        );
        match tokio::time::timeout(Duration::from_secs(10), publish).await {
            Ok(Ok(_)) => {
                if !took_one {
                    took_one = true;
                    appends.streams.fetch_add(1, Ordering::Relaxed);
                }
                appends.ok.fetch_add(1, Ordering::Relaxed)
            }
            _ => appends.failed.fetch_add(1, Ordering::Relaxed),
        };
    }
}

enum Read {
    Live(u64),
    Absent,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Renew,
    Read,
}

struct Sample {
    op: Op,
    loaded: bool,
    took: Duration,
}

/// Renews [`KEYS`] leases the way felixd does: a conditional put on the
/// version last written, with a TTL, every [`RENEW_EVERY`]. After each renewal
/// it reads the key back, which under `lease_free_reads` is a round of fences
/// to the followers.
struct Canary {
    client: Client,
    tenant_id: String,
    namespace: String,
    /// Set once the baseline is taken; each sample records it as it starts.
    loaded: AtomicBool,
    samples: Mutex<Vec<Sample>>,
    /// Renewals refused because the entry had expired.
    lapses: Mutex<Vec<String>>,
}

impl Canary {
    fn new(client: Client, tenant_id: String, namespace: String) -> Self {
        Self {
            client,
            tenant_id,
            namespace,
            loaded: AtomicBool::new(false),
            samples: Mutex::new(Vec::new()),
            lapses: Mutex::new(Vec::new()),
        }
    }

    async fn renew(&self, key: String, claimed: &Barrier, stop: &AtomicBool) {
        let mut version = self.claim(&key, 0).await;
        claimed.wait().await;
        let mut tick = tokio::time::interval(RENEW_EVERY);
        // A late renewal is followed by the next one at once, as a lease
        // holder's would be, rather than by a burst making up the missed ones.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let mut seq = 1u64;
        while !stop.load(Ordering::Acquire) {
            tick.tick().await;
            seq += 1;
            let loaded = self.loaded.load(Ordering::Acquire);
            let started = Instant::now();
            let put = self.client.cache_put_if(
                &self.tenant_id,
                &self.namespace,
                CACHE,
                &key,
                Bytes::from(format!("{key}/{seq}")),
                Some(TTL.as_millis() as u64),
                CacheCondition::Version(version),
            );
            let answer = tokio::time::timeout(TTL, put).await;
            self.record(Op::Renew, loaded, started.elapsed());
            match answer {
                Ok(Ok(result)) if result.applied => {
                    version = result.version.expect("an applied put's version");
                }
                // Only this task writes the key, so a live entry at another
                // version is an earlier renewal whose answer was lost.
                Ok(Ok(result)) if result.version.is_some() => {
                    version = result.version.expect("checked");
                    continue;
                }
                // Refused with no live entry: it expired before the renewal
                // was applied.
                Ok(Ok(_)) => {
                    self.lapse(&key, seq, "refused, the entry had expired".to_string());
                    version = self.claim(&key, seq).await;
                    continue;
                }
                // Unanswered, and maybe applied: the read below says.
                Ok(Err(_)) | Err(_) => {}
            }
            match self.read(&key, loaded).await {
                Read::Live(at) => version = at,
                Read::Absent => {
                    self.lapse(&key, seq, "gone on the read after it".to_string());
                    version = self.claim(&key, seq).await;
                }
                // Keep the version: the next renewal finds out.
                Read::Failed => {}
            }
        }
    }

    /// Take `key` while it is absent, retrying until that works. Not timed:
    /// a claim follows a lapse, which is already a failure.
    async fn claim(&self, key: &str, seq: u64) -> u64 {
        let deadline = Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
        loop {
            let put = self.client.cache_put_if(
                &self.tenant_id,
                &self.namespace,
                CACHE,
                key,
                Bytes::from(format!("{key}/{seq}")),
                Some(TTL.as_millis() as u64),
                CacheCondition::Absent,
            );
            if let Ok(Ok(result)) = tokio::time::timeout(TTL, put).await
                && let (true, Some(version)) = (result.applied, result.version)
            {
                return version;
            }
            if let Read::Live(version) = self.read(key, false).await {
                // Our own claim, applied though its answer was lost.
                return version;
            }
            assert!(Instant::now() < deadline, "could not claim {key}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Read `key` back, timed.
    async fn read(&self, key: &str, loaded: bool) -> Read {
        let started = Instant::now();
        let get = self
            .client
            .cache_get_versioned(&self.tenant_id, &self.namespace, CACHE, key);
        let answer = tokio::time::timeout(TTL, get).await;
        self.record(Op::Read, loaded, started.elapsed());
        match answer {
            Ok(Ok(Some(value))) => Read::Live(value.version),
            Ok(Ok(None)) => Read::Absent,
            Ok(Err(_)) | Err(_) => Read::Failed,
        }
    }

    fn record(&self, op: Op, loaded: bool, took: Duration) {
        self.samples
            .lock()
            .expect("samples")
            .push(Sample { op, loaded, took });
    }

    fn lapse(&self, key: &str, seq: u64, how: String) {
        self.lapses
            .lock()
            .expect("lapses")
            .push(format!("{key} renewal {seq} ({how})"));
    }

    fn report(&self) -> Report {
        let samples = self.samples.lock().expect("samples");
        let latencies = |op: Op, loaded: bool| {
            let mut took: Vec<Duration> = samples
                .iter()
                .filter(|sample| sample.op == op && sample.loaded == loaded)
                .map(|sample| sample.took)
                .collect();
            took.sort();
            took
        };
        Report {
            renew_idle: latencies(Op::Renew, false),
            renew_loaded: latencies(Op::Renew, true),
            read_idle: latencies(Op::Read, false),
            read_loaded: latencies(Op::Read, true),
            lapses: self.lapses.lock().expect("lapses").clone(),
        }
    }
}

/// What the canary saw, sorted fastest first.
struct Report {
    renew_idle: Vec<Duration>,
    renew_loaded: Vec<Duration>,
    read_idle: Vec<Duration>,
    read_loaded: Vec<Duration>,
    lapses: Vec<String>,
}

impl Report {
    /// Each way the run failed. Empty when it passed.
    fn problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if !self.lapses.is_empty() {
            problems.push(format!(
                "{} lease(s) lapsed: {}",
                self.lapses.len(),
                self.lapses.join(", ")
            ));
        }
        for (what, idle, loaded, bound) in [
            (
                "renewal",
                &self.renew_idle,
                &self.renew_loaded,
                RENEWAL_BOUND,
            ),
            ("read", &self.read_idle, &self.read_loaded, READ_BOUND),
        ] {
            assert!(
                !idle.is_empty() && !loaded.is_empty(),
                "the canary took no {what} samples"
            );
            let slow = loaded.iter().filter(|took| **took > bound).count();
            if slow > 0 {
                problems.push(format!(
                    "{slow} of {} {what}s under load took over {bound:.1?}, the slowest {:.1?}",
                    loaded.len(),
                    loaded.last().copied().unwrap_or_default()
                ));
            }
        }
        problems
    }
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let line = |took: &[Duration]| {
            format!(
                "n={} p50={:.1?} p99={:.1?} max={:.1?}",
                took.len(),
                percentile(took, 50),
                p99(took),
                took.last().copied().unwrap_or_default()
            )
        };
        write!(
            f,
            "renewals idle [{}] loaded [{}] bound {RENEWAL_BOUND:.1?}; reads idle [{}] loaded [{}] bound {READ_BOUND:.1?}; {} lapse(s)",
            line(&self.renew_idle),
            line(&self.renew_loaded),
            line(&self.read_idle),
            line(&self.read_loaded),
            self.lapses.len(),
        )
    }
}

fn p99(sorted: &[Duration]) -> Duration {
    percentile(sorted, 99)
}

/// Nearest rank.
fn percentile(sorted: &[Duration], pct: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = (sorted.len() * pct).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Each broker's `quorum wait timed out` warnings, which name how many driver
/// scans a shard went without a pass (`driver_scan` against
/// `shard_last_scan`). The first few, and any for the canary's cache.
fn quorum_timeouts(cluster: &Cluster) -> String {
    let mut out = String::from("quorum waits that timed out, per broker:");
    for node in &cluster.nodes {
        let log = std::fs::read_to_string(node.data_dir.join("broker.log")).unwrap_or_default();
        let timeouts: Vec<&str> = log
            .lines()
            .filter(|line| line.contains("quorum wait timed out"))
            .collect();
        out.push_str(&format!("\n[{}] {}", node.node_id, timeouts.len()));
        let canary = timeouts.iter().filter(|line| line.contains(CACHE));
        for line in timeouts.iter().take(3).chain(canary.take(3)) {
            out.push_str(&format!("\n  {line}"));
        }
    }
    out
}

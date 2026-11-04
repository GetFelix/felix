//! Every shard a killed broker led serves again: on a new leader while it is
//! down, and still once it is back.
//!
//! felixd's scale run found both broken with many shards on one leader (#1805,
//! #1914): the shards the control plane re-placed never served during the
//! outage, and with `replica_election` they did not heal after the restart
//! either. Each test kills the broker leading a third of the streams while a
//! client keeps publishing to every one of them, then restarts it.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use felix_client::Client;
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;
use tokio::task::JoinSet;

const APPEND_EVERY: Duration = Duration::from_millis(200);
/// How long a shard may go without a publish after its leader is killed. An
/// election takes about S + 3 s at worst (`docs/replication-design.md`,
/// "Failover time"), placement about as long; the rest is margin for a busy
/// runner.
const OUTAGE_BOUND: Duration = Duration::from_secs(45);
/// How long after the restart every shard must be serving.
const RESTART_BOUND: Duration = Duration::from_secs(45);

/// **With `replica_election`, every shard the killed broker led serves again,
/// during the outage and after the restart.**
#[serial]
#[ignore = "dispatch: scale-nightly.yml, variant failover"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_election_every_shard_of_a_killed_leader_serves_again() {
    run(true).await;
}

/// **Without election, the control plane's placement alone does the same.**
#[serial]
#[ignore = "dispatch: scale-nightly.yml, variant failover"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_election_every_shard_of_a_killed_leader_serves_again() {
    run(false).await;
}

fn stream_count() -> usize {
    std::env::var("FELIX_SCALE_STREAMS")
        .ok()
        .map(|value| value.trim().parse().expect("FELIX_SCALE_STREAMS"))
        .unwrap_or(150)
}

async fn run(election: bool) {
    let streams: Vec<String> = (0..stream_count()).map(|i| format!("wl-{i:04}")).collect();
    let mut broker_env = Vec::new();
    if election {
        broker_env.push(("FELIX_INTERNAL_ELECTION".to_string(), "true".to_string()));
    }
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: streams
            .iter()
            .map(|name| StreamSpec::quorum(name.clone(), 1, 3))
            .collect(),
        sync_interval_ms: 1_000,
        broker_env,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let mut features = vec!["majority_ack", "lease_free_reads"];
    if election {
        features.push("replica_election");
    }
    cluster
        .finalize_fleet_features(
            &features,
            felix_cluster::wait::budget(Duration::from_secs(30)),
        )
        .await
        .expect("finalize fleet features");
    cluster.run_placement(Duration::from_secs(1));

    let client = Arc::new(
        felix_cluster::client::connect_cluster(
            &cluster.broker_addrs(),
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("client"),
    );
    let progress = Arc::new(Progress::default());
    let started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let mut appenders = JoinSet::new();
    for name in &streams {
        appenders.spawn(append(
            Arc::clone(&client),
            cluster.tenant_id.clone(),
            cluster.namespace.clone(),
            name.clone(),
            Arc::clone(&progress),
            Arc::clone(&stop),
        ));
    }
    // Every stream takes a publish before the kill, so a later silence is the
    // failover's.
    felix_cluster::wait::until(
        Duration::from_secs(120),
        "every stream to take a publish",
        || async { progress.served_since(started).len() == streams.len() },
    )
    .await
    .expect("every stream serves before the kill");

    let owners = cluster.shard_assignments().await.expect("assignments");
    let mut led: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in &streams {
        let key = format!("{}/{}/{name}/0", cluster.tenant_id, cluster.namespace);
        let owner = owners
            .get(&key)
            .map(|a| a.leader.clone())
            .expect("assigned");
        led.entry(owner).or_default().push(name.clone());
    }
    let (victim, victims) = led
        .iter()
        .max_by_key(|(_, names)| names.len())
        .map(|(id, names)| (id.clone(), names.clone()))
        .expect("a leader");
    eprintln!(
        "failover heal: {} streams, {victim} leads {}; election {election}",
        streams.len(),
        victims.len()
    );

    let killed = Instant::now();
    cluster.kill_node(&victim).expect("kill");
    let outage_bound = felix_cluster::wait::budget(OUTAGE_BOUND);
    let outage = wait_served(&progress, &victims, killed, outage_bound).await;
    let outage_report = report(&cluster, &progress, &victims, killed, "outage").await;

    cluster.restart_node(&victim).await.expect("restart");
    let restarted = Instant::now();
    let restart_bound = felix_cluster::wait::budget(RESTART_BOUND);
    let after = wait_served(&progress, &streams, restarted, restart_bound).await;
    let restart_report = report(&cluster, &progress, &streams, restarted, "restart").await;

    stop.store(true, Ordering::Release);
    while let Some(done) = appenders.join_next().await {
        done.expect("appender");
    }
    eprintln!("failover heal: {outage_report}\nfailover heal: {restart_report}");
    let mut problems = Vec::new();
    if !outage.is_empty() {
        problems.push(format!(
            "{} of {} shards {victim} led did not serve within {outage_bound:?} of the kill",
            outage.len(),
            victims.len()
        ));
    }
    if !after.is_empty() {
        problems.push(format!(
            "{} of {} shards did not serve within {restart_bound:?} of {victim}'s restart",
            after.len(),
            streams.len()
        ));
    }
    cluster.shutdown().await;
    assert!(problems.is_empty(), "{}", problems.join("; "));
}

/// The streams in `names` that took no publish within `bound` of `since`.
async fn wait_served(
    progress: &Progress,
    names: &[String],
    since: Instant,
    bound: Duration,
) -> BTreeSet<String> {
    loop {
        let served = progress.served_since(since);
        let missing: BTreeSet<String> = names
            .iter()
            .filter(|name| !served.contains(*name))
            .cloned()
            .collect();
        if missing.is_empty() || since.elapsed() >= bound {
            return missing;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// When each stream first served after `since`, and what the control plane
/// and the last refusal say about the ones that did not.
async fn report(
    cluster: &Cluster,
    progress: &Progress,
    names: &[String],
    since: Instant,
    phase: &str,
) -> String {
    let assignments = cluster.shard_assignments().await.unwrap_or_default();
    let state = progress.state.lock().expect("progress");
    let mut times: Vec<Duration> = Vec::new();
    let mut stuck = Vec::new();
    for name in names {
        let entry = state.get(name);
        match entry.and_then(|entry| entry.first_after(since)) {
            Some(at) => times.push(at - since),
            None => {
                let key = format!("{}/{}/{name}/0", cluster.tenant_id, cluster.namespace);
                let assigned = assignments
                    .get(&key)
                    .map(|a| format!("{}@{} {:?}", a.leader, a.generation, a.replicas))
                    .unwrap_or_default();
                let error = entry
                    .and_then(|entry| entry.last_error.clone())
                    .unwrap_or_default();
                stuck.push(format!("{name} [{assigned}] {error}"));
            }
        }
    }
    times.sort();
    let at = |q: usize| times.get(times.len().saturating_sub(1) * q / 100).copied();
    format!(
        "{phase}: {} of {} served, p50 {:?} p99 {:?} max {:?}; {} silent{}",
        times.len(),
        names.len(),
        at(50),
        at(99),
        times.last(),
        stuck.len(),
        stuck
            .iter()
            .take(20)
            .map(|line| format!("\n  {line}"))
            .collect::<String>(),
    )
}

#[derive(Default)]
struct Progress {
    state: Mutex<BTreeMap<String, StreamProgress>>,
}

#[derive(Default)]
struct StreamProgress {
    served: Vec<Instant>,
    last_error: Option<String>,
}

impl StreamProgress {
    fn first_after(&self, since: Instant) -> Option<Instant> {
        self.served.iter().copied().find(|at| *at >= since)
    }
}

impl Progress {
    fn served_since(&self, since: Instant) -> BTreeSet<String> {
        let state = self.state.lock().expect("progress");
        state
            .iter()
            .filter(|(_, entry)| entry.served.last().is_some_and(|at| *at >= since))
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn record(&self, stream: &str, outcome: Result<(), String>) {
        let mut state = self.state.lock().expect("progress");
        let entry = state.entry(stream.to_string()).or_default();
        match outcome {
            Ok(()) => entry.served.push(Instant::now()),
            Err(error) => entry.last_error = Some(error),
        }
    }
}

/// Publish to `stream` every [`APPEND_EVERY`] until `stop`, as a felixd agent
/// writes its output, recording each success and the last refusal.
async fn append(
    client: Arc<Client>,
    tenant_id: String,
    namespace: String,
    stream: String,
    progress: Arc<Progress>,
    stop: Arc<AtomicBool>,
) {
    let mut tick = tokio::time::interval(APPEND_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !stop.load(Ordering::Acquire) {
        tick.tick().await;
        let publish = client.publish(
            &tenant_id,
            &namespace,
            &stream,
            b"out".to_vec(),
            AckMode::PerMessage,
        );
        let outcome = match tokio::time::timeout(Duration::from_secs(5), publish).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => Err(format!("{err:#}")),
            Err(_) => Err("timed out".to_string()),
        };
        progress.record(&stream, outcome);
    }
}

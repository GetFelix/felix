//! A Kafka client reading a stream whose shards are led by different
//! brokers, and following a shard when its leader changes.
//!
//! kcat runs from the `edenhill/kcat:1.7.1` image, so this needs Docker;
//! without it the test says so and returns.
//!
//! Run with `cargo test -p felix-cluster --test routing kafka_leaders::`.
use std::collections::{BTreeMap, BTreeSet};
use std::process::Output;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec, wait};
use serial_test::serial;
use tokio::process::Command;

const STREAM: &str = "orders";
const SHARDS: u32 = 3;
const PER_SHARD: usize = 3;
const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";
/// Each kcat is a container start plus a consume; a hang should fail the
/// test rather than stall the suite.
const KCAT_BOUND: Duration = Duration::from_secs(90);
/// kcat expands the `\n` itself.
const FORMAT: &str = "%p:%o:%s\\n";

/// **kcat reads every partition from whichever broker leads it, and follows
/// a partition to its new leader.** Bootstrapped at one broker, a consume
/// from the beginning returns every record of every shard at the offsets
/// Felix gave them. Then a shard moves while a kcat is parked on its old
/// leader; the old leader answers NOT_LEADER_OR_FOLLOWER, and kcat finds the
/// new leader through Metadata and reads what was published there.
#[serial]
#[tokio::test]
async fn kcat_reads_every_shard_and_follows_a_moved_one() {
    if !kcat_available().await {
        return;
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, SHARDS)],
        kafka: true,
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let topic = format!("{}.{STREAM}", cluster.namespace);

    // Placement may put every shard on one broker; then there is no leader
    // for kcat to find, so spread them.
    let mut owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    if owners.values().collect::<BTreeSet<_>>().len() < 2 {
        let elsewhere = other_node(&cluster, &owners[&0]);
        move_shard(&cluster, 1, &elsewhere).await;
        owners = cluster.shard_owners_for(STREAM).await.expect("owners");
    }
    assert_eq!(owners.len(), SHARDS as usize, "{owners:?}");
    assert!(
        owners.values().collect::<BTreeSet<_>>().len() >= 2,
        "{owners:?}"
    );

    let mut expected = Vec::new();
    for shard in 0..SHARDS {
        let ours: Vec<String> = (0..PER_SHARD).map(|i| format!("s{shard}-r{i}")).collect();
        publish(&cluster, &owners[&shard], shard, &ours).await;
        expected.extend(logged(&cluster, &owners[&shard], shard, &ours).await);
    }
    expected.sort();

    let bootstrap = kafka_addr(&cluster, "broker-0");
    let output = kcat(sasl(&cluster, &bootstrap, &["-L", "-t", &topic])).await;
    let listing = stdout(&output);
    assert!(output.status.success(), "{listing}\n{}", stderr(&output));
    assert!(
        listing.contains(&format!("topic \"{topic}\" with {SHARDS} partitions")),
        "{listing}"
    );
    for node in &cluster.nodes {
        let addr = node.kafka_addr.as_deref().expect("kafka listener");
        assert!(
            listing.contains(addr),
            "{} is not listed: {listing}",
            node.node_id
        );
    }
    // Kafka broker ids are Felix node ids hashed, so check the grouping: two
    // partitions share a leader id exactly when they share a Felix owner.
    let leaders = leaders(&listing);
    assert_eq!(leaders.len(), SHARDS as usize, "{listing}");
    assert!(
        leaders.values().collect::<BTreeSet<_>>().len() >= 2,
        "{listing}"
    );
    for (a, b) in (0..SHARDS).flat_map(|a| (0..SHARDS).map(move |b| (a, b))) {
        assert_eq!(
            leaders[&a] == leaders[&b],
            owners[&a] == owners[&b],
            "partitions {a} and {b}: owners {owners:?}, listing {listing}"
        );
    }

    let consume_all = [
        "-C",
        "-t",
        &topic,
        "-o",
        "beginning",
        "-e",
        "-q",
        "-f",
        FORMAT,
    ];
    let output = kcat(sasl(&cluster, &bootstrap, &consume_all)).await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), expected);

    // Park a kcat at the tail of one shard on its leader, then move the shard.
    let moved = 2;
    let old = owners[&moved].clone();
    let new = other_node(&cluster, &old);
    let end = expected
        .iter()
        .filter_map(|line| line.strip_prefix(&format!("{moved}:")))
        .filter_map(|rest| rest.split(':').next()?.parse::<u64>().ok())
        .max()
        .expect("the moved shard has records")
        + 1;
    let old_bootstrap = kafka_addr(&cluster, &old);
    // The kcat above can exit with a fetch still parked on the old leader,
    // which counts its wait only when it returns. Read the baseline once its
    // connections are gone, or that late wait passes for the follower's.
    wait::until(
        Duration::from_secs(30),
        "the last kcat to disconnect",
        || {
            let cluster = &cluster;
            let old = old.clone();
            async move { kafka_connections(cluster, &old).await == 0.0 }
        },
    )
    .await
    .expect("the old leader kept a kafka connection open");
    let waits_before = fetch_waits(&cluster, &old).await;
    let follower = {
        let (partition, offset, count) =
            (moved.to_string(), end.to_string(), PER_SHARD.to_string());
        // No -e: kcat keeps fetching until it has -c records.
        let args = sasl(
            &cluster,
            &old_bootstrap,
            &[
                "-C", "-t", &topic, "-p", &partition, "-o", &offset, "-c", &count, "-q", "-f",
                FORMAT,
            ],
        );
        tokio::spawn(kcat(args))
    };
    // A fetch that waited at the tail is kcat reading from the old leader;
    // moving before that would let it find the new leader on its first
    // Metadata and prove nothing.
    wait::until(Duration::from_secs(60), "kcat to wait at the tail", || {
        let cluster = &cluster;
        let old = old.clone();
        async move { fetch_waits(cluster, &old).await > waits_before }
    })
    .await
    .expect("kcat never fetched from the old leader");

    move_shard(&cluster, moved, &new).await;
    let after: Vec<String> = (0..PER_SHARD)
        .map(|i| format!("s{moved}-after-{i}"))
        .collect();
    publish(&cluster, &new, moved, &after).await;
    let mut owed: Vec<String> = after
        .iter()
        .zip(end..)
        .map(|(payload, offset)| format!("{moved}:{offset}:{payload}"))
        .collect();
    owed.sort();

    let output = follower.await.expect("kcat task");
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(records(&output), owed);
    assert!(
        kafka_requests(&cluster, &old, "Fetch", "not_leader_or_follower").await > 0.0,
        "the old leader never refused a fetch, so kcat did not follow the move"
    );

    // And a fresh consumer bootstrapped at the old leader reads everything,
    // the moved shard from its new leader.
    let output = kcat(sasl(&cluster, &old_bootstrap, &consume_all)).await;
    assert!(output.status.success(), "{}", stderr(&output));
    let mut everything = expected;
    everything.extend(owed);
    everything.sort();
    assert_eq!(records(&output), everything);
    cluster.shutdown().await;
}

fn other_node(cluster: &Cluster, not: &str) -> String {
    cluster
        .node_ids()
        .into_iter()
        .find(|id| id != not)
        .expect("more than one broker")
}

fn kafka_addr(cluster: &Cluster, node: &str) -> String {
    cluster
        .node(node)
        .and_then(|node| node.kafka_addr.clone())
        .expect("kafka listener")
}

/// Move one shard with the operator API and step placement until it has
/// cut over.
async fn move_shard(cluster: &Cluster, shard: u32, destination: &str) {
    cluster
        .start_move(STREAM, shard, destination)
        .await
        .expect("start the move");
    wait::until(Duration::from_secs(60), "the move to cut over", || async {
        cluster.place_shards().await;
        cluster
            .shard_owner_of("stream", STREAM, shard)
            .await
            .is_ok_and(|owner| owner == destination)
    })
    .await
    .expect("move");
}

/// A routing key the broker hashes to `shard`.
fn key_for(shard: u32) -> String {
    (0..)
        .map(|i| format!("key-{i}"))
        .find(|key| felix_wire::routing::shard_for(SHARDS, Some(key.as_bytes())) == shard)
        .expect("some key lands on every shard")
}

async fn publish(cluster: &Cluster, via: &str, shard: u32, payloads: &[String]) {
    let key = key_for(shard);
    for payload in payloads {
        cluster
            .publish_keyed_via_settled(
                via,
                STREAM,
                key.as_bytes(),
                payload.clone().into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish");
    }
}

/// `shard:offset:payload` for everything in the shard up to and including
/// `ours`, read over QUIC from its owner: what kcat should see, the harness's
/// own probe records included.
async fn logged(cluster: &Cluster, owner: &str, shard: u32, ours: &[String]) -> Vec<String> {
    let (_client, mut subscription) = cluster
        .replay_shard(owner, STREAM, shard)
        .await
        .expect("replay");
    let mut lines = Vec::new();
    let mut seen = 0;
    while seen < ours.len() {
        let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .expect("replay stalled")
            .expect("replay")
            .expect("replay ended");
        let payload = String::from_utf8_lossy(&event.payload).into_owned();
        seen += usize::from(ours.contains(&payload));
        let offset = event
            .offset
            .expect("a durable stream's events carry offsets");
        lines.push(format!("{shard}:{offset}:{payload}"));
    }
    lines
}

/// Partition to leader id, from `kcat -L`.
fn leaders(listing: &str) -> BTreeMap<u32, String> {
    listing
        .lines()
        .filter_map(|line| {
            let rest = line.trim().strip_prefix("partition ")?;
            let (partition, rest) = rest.split_once(", leader ")?;
            let leader = rest.split(',').next()?.trim().to_string();
            Some((partition.parse().ok()?, leader))
        })
        .collect()
}

async fn fetch_waits(cluster: &Cluster, node: &str) -> f64 {
    cluster
        .metric(node, "felix_kafka_fetch_waits_total")
        .await
        .expect("scrape")
        .unwrap_or(0.0)
}

async fn kafka_connections(cluster: &Cluster, node: &str) -> f64 {
    cluster
        .metric(node, "felix_kafka_connections")
        .await
        .expect("scrape")
        .unwrap_or(0.0)
}

/// `felix_kafka_requests_total` for one API and outcome. `Cluster::metric`
/// sums over labels, and the outcome is the label that matters here.
async fn kafka_requests(cluster: &Cluster, node: &str, api: &str, error: &str) -> f64 {
    let addr = cluster.node(node).expect("node").metrics_addr;
    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .expect("scrape")
        .text()
        .await
        .expect("metrics body");
    let (api, error) = (format!("api=\"{api}\""), format!("error=\"{error}\""));
    body.lines()
        .filter(|line| line.starts_with("felix_kafka_requests_total{"))
        .filter(|line| line.contains(&api) && line.contains(&error))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// kcat arguments for SASL/PLAIN: the tenant as the user, the same token a
/// QUIC client presents as the password.
fn sasl(cluster: &Cluster, bootstrap: &str, args: &[&str]) -> Vec<String> {
    let mut all: Vec<String> = [
        "-b",
        bootstrap,
        "-X",
        "security.protocol=SASL_PLAINTEXT",
        "-X",
        "sasl.mechanisms=PLAIN",
    ]
    .map(str::to_string)
    .into();
    all.extend([
        "-X".to_string(),
        format!("sasl.username={}", cluster.tenant_id),
        "-X".to_string(),
        format!("sasl.password={}", cluster.client_token()),
    ]);
    all.extend(args.iter().map(|arg| arg.to_string()));
    all
}

/// Whether Docker can run the kcat image.
async fn kcat_available() -> bool {
    let ok = Command::new("docker")
        .args(["run", "--rm", KCAT_IMAGE, "-V"])
        .output()
        .await
        .is_ok_and(|out| out.status.success());
    if !ok {
        eprintln!("skipping: docker cannot run {KCAT_IMAGE}");
    }
    ok
}

async fn kcat(args: Vec<String>) -> Output {
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let name = format!(
        "felix-cluster-kcat-{}-{}",
        std::process::id(),
        RUNS.fetch_add(1, Ordering::Relaxed)
    );
    let mut command = Command::new("docker");
    command.args(["run", "--rm", "--name", &name]);
    // Docker Desktop reaches the host through host.docker.internal, which is
    // what the brokers advertise; on Linux the container shares loopback.
    if cfg!(target_os = "linux") {
        command.args(["--network", "host"]);
    }
    command.arg(KCAT_IMAGE).args(&args).kill_on_drop(true);
    match tokio::time::timeout(KCAT_BOUND, command.output()).await {
        Ok(output) => output.expect("run docker"),
        Err(_) => {
            // Killing the docker CLI leaves the container running.
            let _ = Command::new("docker")
                .args(["rm", "-f", &name])
                .output()
                .await;
            panic!("kcat {args:?} did not finish within {KCAT_BOUND:?}");
        }
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// `partition:offset:value` lines, sorted.
fn records(output: &Output) -> Vec<String> {
    let mut lines: Vec<String> = stdout(output)
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

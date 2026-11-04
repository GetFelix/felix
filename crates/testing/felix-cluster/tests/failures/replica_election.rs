//! With `replica_election` finalized (brokers offering `ELECTION` through
//! `FELIX_INTERNAL_ELECTION`, and the features reported through
//! `FELIX_TEST_FLEET_FEATURES`), a shard whose leader dies while the control
//! plane is down elects a new leader among its followers, and the control
//! plane adopts it when it returns. See "Electing without the control plane"
//! in `docs/replication-design.md`.
//!
//! Every test checks the model's `OneLeaderPerGeneration` from the brokers'
//! logs: no two brokers open the shard for writes at one generation.
//!
//! Run with `cargo test -p felix-cluster --test failures replica_election::`.
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use felix_common::fleet;
use felix_controlplane_service::store::ControlPlaneStore;
use felix_controlplane_service::store::memory::InMemoryStore;
use serial_test::serial;

const STREAM: &str = "orders";

/// How long a follower takes to suspect a silent leader. Short, so a test
/// spends its time on the election rather than on the detector.
const SUSPECT_AFTER_MS: u64 = 1000;

/// From the leader's death to a new leader serving: the suspect window, a
/// pre-vote, a place delay or two, the ballot and one fence, with room for a
/// split round's backoff.
const ELECTED_WITHIN: Duration = Duration::from_secs(20);

/// What the driver logs once a majority took a leader's fence, with the
/// generation it opened at.
const OPENED: &str = "a majority took the fence; opening the shard for writes";
/// What a candidate logs once its ballot is on disk.
const STOOD: &str = "standing for election";
/// What a candidate logs while `FELIX_TEST_PRE_VOTE_GATE` holds its pre-vote.
const HELD: &str = "holding the pre-vote at the test gate";

/// `nodes` brokers, one `Quorum` stream at `rf`, and every feature
/// `replica_election` needs finalized. The set is then written again so its
/// leader fences and keeps it: the first placement ran before the features
/// were on, and a set nobody keeps is one nobody can stand on.
async fn start(nodes: usize, rf: u32, proxy_links: bool) -> Cluster {
    start_with(nodes, rf, proxy_links, Vec::new()).await
}

/// [`start`], with more environment for every broker.
async fn start_with(
    nodes: usize,
    rf: u32,
    proxy_links: bool,
    env: Vec<(String, String)>,
) -> Cluster {
    let reported = fleet::IMPLEMENTED
        .iter()
        .chain(&[
            fleet::JOINT_FENCE,
            fleet::START_HOLDERS,
            fleet::PEER_ELECTION,
            fleet::REPLICA_ELECTION,
        ])
        .map(|feature| feature.name())
        .collect::<Vec<_>>()
        .join(",");
    let cluster = Cluster::start(ClusterConfig {
        nodes,
        streams: vec![StreamSpec::quorum(STREAM, 1, rf)],
        broker_env: [
            ("FELIX_INTERNAL_ELECTION".to_string(), "true".to_string()),
            ("FELIX_TEST_FLEET_FEATURES".to_string(), reported),
            (
                "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
                "2000".to_string(),
            ),
            (
                "FELIX_LEADER_SUSPECT_AFTER_MS".to_string(),
                SUSPECT_AFTER_MS.to_string(),
            ),
        ]
        .into_iter()
        .chain(env)
        .collect(),
        proxy_links,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            &[
                fleet::GENERATION_START.name(),
                fleet::MAJORITY_ACK.name(),
                fleet::LEASE_FREE_READS.name(),
                fleet::JOINT_FENCE.name(),
                fleet::REPLICA_ELECTION.name(),
            ],
            Duration::from_secs(20),
        )
        .await
        .expect("every broker enables replica_election");

    let placed = assignment(&cluster).await;
    let same = stored_assignment(&cluster).await;
    store(&cluster)
        .put_shard_assignment(same)
        .await
        .expect("rewrite the set");
    let kept = assignment(&cluster).await;
    assert!(kept.generation > placed.generation, "{kept:?}");
    level_at(&cluster, &kept.leader, &kept.replicas, kept.generation).await;
    cluster
}

fn store(cluster: &Cluster) -> &InMemoryStore {
    &cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
}

async fn stored_assignment(
    cluster: &Cluster,
) -> felix_controlplane_service::model::ShardAssignment {
    store(cluster)
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .find(|assignment| assignment.key.stream == STREAM)
        .expect("the stream's assignment")
}

async fn assignment(cluster: &Cluster) -> felix_cluster::Assignment {
    let key = format!("{}/{}/{STREAM}/0", cluster.tenant_id, cluster.namespace);
    cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .remove(&key)
        .expect("the stream is placed")
}

/// Publish through `leader` until a report at `generation` names every one of
/// `followers` level with it.
async fn level_at(cluster: &Cluster, leader: &str, followers: &[String], generation: u64) {
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "a level report at the generation",
        || async {
            if cluster
                .publish_via(leader, STREAM, b"level".to_vec())
                .await
                .is_err()
            {
                return false;
            }
            cluster
                .replica_report(STREAM, 0)
                .await
                .ok()
                .flatten()
                .is_some_and(|report| {
                    report.generation == generation
                        && followers.iter().all(|id| {
                            report.caught_up.contains(id)
                                && report.offsets.get(id).copied() == report.leader_offset
                        })
                })
        },
    )
    .await
    .expect("the set holds the leader's writes");
}

/// Publish `count` numbered records through `via`, each acknowledged.
async fn publish_acked(cluster: &Cluster, via: &str, tag: &str, count: usize) -> Vec<String> {
    let mut acked = Vec::new();
    for i in 0..count {
        let payload = format!("{tag}-{i}");
        cluster
            .publish_via(via, STREAM, payload.clone().into_bytes())
            .await
            .unwrap_or_else(|err| panic!("publish {payload} via {via}: {err:#}"));
        acked.push(payload);
    }
    acked
}

/// Generation -> the brokers that logged `message` for the stream at it,
/// read from every broker's log, and the one a restart kept, into `seen`.
fn read_logged(cluster: &Cluster, message: &str, seen: &mut BTreeMap<u64, BTreeSet<String>>) {
    for id in cluster.node_ids() {
        let dir = &cluster.node(&id).expect("node").data_dir;
        let log = ["broker.log", "broker.log.previous"]
            .iter()
            .map(|name| std::fs::read_to_string(dir.join(name)).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        for line in log.lines().map(strip_ansi) {
            if line.contains(message)
                && line.contains(&format!("stream={STREAM}"))
                && let Some(generation) = field(&line, "generation=")
            {
                seen.entry(generation).or_default().insert(id.clone());
            }
        }
    }
}

/// The brokers whose log has `message` for the stream.
fn logged_by(cluster: &Cluster, message: &str) -> BTreeSet<String> {
    cluster
        .node_ids()
        .into_iter()
        .filter(|id| {
            let dir = &cluster.node(id).expect("node").data_dir;
            std::fs::read_to_string(dir.join("broker.log"))
                .unwrap_or_default()
                .lines()
                .map(strip_ansi)
                .any(|line| line.contains(message) && line.contains(&format!("stream={STREAM}")))
        })
        .collect()
}

fn field(line: &str, name: &str) -> Option<u64> {
    let rest = &line[line.find(name)? + name.len()..];
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// The broker log goes to a file with the formatter's colours on.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `OneLeaderPerGeneration`: no generation above `above` was opened for
/// writes by two brokers.
fn assert_one_leader_per_generation(opened: &BTreeMap<u64, BTreeSet<String>>, above: u64) {
    for (generation, brokers) in opened.range(above + 1..) {
        assert!(
            brokers.len() == 1,
            "two leaders opened the shard at generation {generation}: {brokers:?} \
             (every open above {above}: {opened:?})"
        );
    }
}

/// The broker that opened the highest generation above `above`, once one has
/// and it takes a write. Waits up to [`ELECTED_WITHIN`].
async fn elected_above(
    cluster: &Cluster,
    above: u64,
    opened: &mut BTreeMap<u64, BTreeSet<String>>,
) -> (String, u64) {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a follower to be elected and serve", || {
        read_logged(cluster, OPENED, opened);
        let newest = opened
            .range(above + 1..)
            .next_back()
            .and_then(|(generation, brokers)| Some((brokers.iter().next()?.clone(), *generation)));
        async move {
            let (leader, generation) = newest?;
            cluster
                .publish_via(&leader, STREAM, b"probe".to_vec())
                .await
                .ok()?;
            Some((leader, generation))
        }
    })
    .await
    .expect("a follower is elected while the control plane is down")
}

/// What one replay of the stream from the start came to.
enum ReadBack {
    /// The broker answered and the replay ran to the end of what it holds.
    Held(BTreeSet<String>),
    /// The broker refused, is still opening, or went quiet or dropped the
    /// replay before the end. Nothing can be said about what it holds.
    Refused(String),
}

/// What `read` says about `wanted`. A refusal is never a missing record.
enum Verdict {
    Complete,
    Unreadable(String),
    Missing(Vec<String>),
}

fn judge(wanted: &[String], read: ReadBack) -> Verdict {
    match read {
        ReadBack::Refused(why) => Verdict::Unreadable(why),
        ReadBack::Held(held) => {
            let missing: Vec<String> = wanted
                .iter()
                .filter(|record| !held.contains(*record))
                .cloned()
                .collect();
            if missing.is_empty() {
                Verdict::Complete
            } else {
                Verdict::Missing(missing)
            }
        }
    }
}

/// Replay the stream from the start on `node` until it has nothing more to
/// send. Only an idle replay counts as a read: a refusal, an error or an
/// early end leaves the broker's contents unknown.
async fn read_back(cluster: &Cluster, node: &str) -> ReadBack {
    let (_client, mut replay) = match cluster.replay_on(node, STREAM).await {
        Ok(opened) => opened,
        Err(err) => return ReadBack::Refused(format!("{err:#}")),
    };
    let mut held = BTreeSet::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await {
            Ok(Ok(Some(event))) => {
                held.insert(String::from_utf8_lossy(&event.payload).into_owned());
            }
            Ok(Ok(None)) => return ReadBack::Refused("the broker ended the replay".to_string()),
            Ok(Err(err)) => return ReadBack::Refused(format!("delivery error: {err:#}")),
            Err(_) => return ReadBack::Held(held),
        }
    }
}

/// Every record of `wanted` that `node` does not hold, replaying the stream
/// from the start. Waits a while for a follower still catching up, and for a
/// broker that cannot be read yet. A broker that stays unreadable fails as
/// not serving, not as having lost records.
async fn missing_on(cluster: &Cluster, node: &str, wanted: &[String]) -> Vec<String> {
    let within = felix_cluster::wait::budget(Duration::from_secs(20));
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match judge(wanted, read_back(cluster, node).await) {
            Verdict::Complete => return Vec::new(),
            Verdict::Unreadable(why) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "no broker serving {STREAM} within {}s; last refusal from {node}: {why}",
                        within.as_secs()
                    );
                }
            }
            Verdict::Missing(missing) => {
                if tokio::time::Instant::now() >= deadline {
                    return missing;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The leader's report names every one of `followers` caught up at
/// `generation` or later. A follower does not serve reads, so this is how
/// its copy is checked.
async fn caught_up(cluster: &Cluster, followers: &[String], generation: u64) {
    let last = Mutex::new(None);
    let done = felix_cluster::wait::until(
        Duration::from_secs(30),
        "the followers to catch up with the elected leader",
        || async {
            let report = cluster.replica_report(STREAM, 0).await.ok().flatten();
            let done = report.as_ref().is_some_and(|report| {
                report.generation >= generation
                    && followers.iter().all(|id| report.caught_up.contains(id))
            });
            *last.lock().expect("lock") = report;
            done
        },
    )
    .await;
    assert!(
        done.is_ok(),
        "{followers:?} are not all caught up at {generation}: the last report is {:?}",
        last.lock().expect("lock")
    );
}

/// The control plane names `leader` at `generation`, by adopting it.
async fn adopted(cluster: &Cluster, leader: &str, generation: u64) {
    let last = Mutex::new(None);
    let found = felix_cluster::wait::until(
        Duration::from_secs(30),
        "the control plane to adopt the elected leader",
        || async {
            let now = cluster.shard_assignments().await.ok().and_then(|mut all| {
                all.remove(&format!(
                    "{}/{}/{STREAM}/0",
                    cluster.tenant_id, cluster.namespace
                ))
            });
            let done = now
                .as_ref()
                .is_some_and(|now| now.leader == leader && now.generation == generation);
            *last.lock().expect("lock") = now;
            done
        },
    )
    .await;
    assert!(
        found.is_ok(),
        "the control plane did not adopt {leader} at {generation}: it names {:?}",
        last.lock().expect("lock")
    );
}

/// **A follower is elected while the control plane is down.** Three brokers,
/// RF 3. With the control plane stopped, the leader is killed: a follower
/// opens within the bound, every acknowledged record reads back from it, it
/// acknowledges new writes, and the control plane adopts it on its return.
#[serial]
#[tokio::test]
async fn a_follower_is_elected_while_the_control_plane_is_down() {
    let mut cluster = start(3, 3, false).await;
    let before = assignment(&cluster).await;
    let mut acked = publish_acked(&cluster, &before.leader, "before", 20).await;

    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    cluster.kill_node(&before.leader).expect("kill the leader");
    let mut opened = BTreeMap::new();
    let (elected, generation) = elected_above(&cluster, before.generation, &mut opened).await;
    assert!(
        before.replicas.contains(&elected),
        "{elected} is not a follower of {before:?}"
    );
    acked.extend(publish_acked(&cluster, &elected, "elected", 20).await);
    let missing = missing_on(&cluster, &elected, &acked).await;
    assert!(
        missing.is_empty(),
        "{elected} lost acknowledged records: {missing:?}"
    );

    cluster
        .recover_control_plane()
        .await
        .expect("recover the control plane");
    adopted(&cluster, &elected, generation).await;
    acked.extend(publish_acked(&cluster, &elected, "adopted", 5).await);
    let survivor: Vec<String> = before
        .replicas
        .iter()
        .filter(|id| **id != elected)
        .cloned()
        .collect();
    caught_up(&cluster, &survivor, generation).await;
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);
    cluster.shutdown().await;
}

/// **Two followers stand at once and exactly one opens.** Five brokers,
/// RF 5. The two lowest-id followers cannot reach each other, so neither
/// counts the other ahead of it, and the other two cannot ask for votes at
/// all. A test gate holds both candidates' pre-votes until each is waiting,
/// then lets them go together, and what the two send the others arrives
/// late, so each one's pre-vote lands before the other's fence: both put a
/// ballot for the same generation on disk and fence the same two voters.
/// Those voters' ballots let only one of them open there. Without the
/// ballot check both would.
#[serial]
#[tokio::test]
async fn two_followers_stand_at_once_and_one_opens() {
    let gate_dir = tempfile::tempdir().expect("a directory for the gate");
    let gate = gate_dir.path().join("pre-vote-gate");
    let mut cluster = start_with(
        5,
        5,
        true,
        vec![(
            "FELIX_TEST_PRE_VOTE_GATE".to_string(),
            gate.display().to_string(),
        )],
    )
    .await;
    let before = assignment(&cluster).await;
    let mut acked = publish_acked(&cluster, &before.leader, "before", 10).await;
    let mut followers = before.replicas.clone();
    followers.sort();
    assert_eq!(followers.len(), 4, "RF 5: {before:?}");
    let (candidates, voters) = followers.split_at(2);

    // After the crash: a voter cut off from its leader suspects it at once.
    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    for (node, other) in [
        (&candidates[0], &candidates[1]),
        (&candidates[1], &candidates[0]),
    ] {
        cluster
            .inject(&Fault::Refuse {
                node: node.clone(),
                peers: vec![other.clone()],
            })
            .await
            .expect("cut the candidates off from each other");
    }
    for voter in voters {
        cluster
            .inject(&Fault::Refuse {
                node: voter.clone(),
                peers: cluster.node_ids(),
            })
            .await
            .expect("keep a voter from standing");
    }
    // Under the one-second pre-vote timeout, so the votes still count.
    let late = Duration::from_millis(700);
    for candidate in candidates {
        for voter in voters {
            cluster
                .inject(&Fault::Delay {
                    from: Endpoint::node(candidate.clone()),
                    to: Endpoint::node(voter.clone()),
                    by: late,
                })
                .await
                .expect("delay a candidate's requests");
        }
    }

    std::fs::write(&gate, b"").expect("close the gate");
    cluster.kill_node(&before.leader).expect("kill the leader");
    let both: BTreeSet<String> = candidates.iter().cloned().collect();
    let held = felix_cluster::wait::until(ELECTED_WITHIN, "both candidates at the gate", || {
        let held = logged_by(&cluster, HELD);
        let all = held.is_superset(&both);
        async move { all }
    })
    .await;
    assert!(
        held.is_ok(),
        "both candidates must wait at the gate; only {:?} did",
        logged_by(&cluster, HELD)
    );
    // Long enough for each candidate's dial to its voters to finish over the
    // late links, so both requests go out the moment the gate opens.
    tokio::time::sleep(Duration::from_secs(3)).await;
    std::fs::remove_file(&gate).expect("open the gate");
    let mut opened = BTreeMap::new();
    let (elected, _) = elected_above(&cluster, before.generation, &mut opened).await;
    let mut stood = BTreeMap::new();
    read_logged(&cluster, STOOD, &mut stood);
    assert!(
        stood
            .range(before.generation + 1..)
            .any(|(_, brokers)| brokers.len() > 1),
        "the candidates never stood at the same generation, so nothing was tested: {stood:?}"
    );
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);

    cluster.heal_all().await.expect("heal");
    acked.extend(publish_acked(&cluster, &elected, "elected", 10).await);
    let missing = missing_on(&cluster, &elected, &acked).await;
    assert!(
        missing.is_empty(),
        "{elected} lost acknowledged records: {missing:?}"
    );
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);
    cluster.shutdown().await;
}

/// **The control plane returns during an election and names someone else.**
/// Three brokers, RF 3. With the control plane stopped and the leader
/// killed, a follower stands. The control plane comes back and promotes the
/// other follower at the next generation it issues, which is likely the one
/// the candidate took. One side wins: at most one broker opens each
/// generation, the shard serves again under the leader the control plane
/// ends up naming, and nothing acknowledged is lost.
#[serial]
#[tokio::test]
async fn the_control_plane_returning_mid_election_leaves_one_leader() {
    let mut cluster = start(3, 3, false).await;
    let before = assignment(&cluster).await;
    let mut acked = publish_acked(&cluster, &before.leader, "before", 20).await;

    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    cluster.kill_node(&before.leader).expect("kill the leader");
    let mut stood = BTreeMap::new();
    felix_cluster::wait::until(ELECTED_WITHIN, "a follower to stand", || {
        read_logged(&cluster, STOOD, &mut stood);
        let any = stood.range(before.generation + 1..).next().is_some();
        async move { any }
    })
    .await
    .expect("a follower stands");
    let candidate = stood
        .range(before.generation + 1..)
        .next()
        .and_then(|(_, brokers)| brokers.iter().next().cloned())
        .expect("a candidate");
    let other = before
        .replicas
        .iter()
        .find(|id| **id != candidate)
        .expect("the other follower")
        .clone();

    cluster
        .recover_control_plane()
        .await
        .expect("recover the control plane");
    let mut promoted = stored_assignment(&cluster).await;
    promoted.leader = other.clone();
    promoted.replicas = vec![candidate.clone(), before.leader.clone()];
    // The write may lose to an adoption that landed first; the control plane
    // then promotes above it, which is the same race one step later.
    let _ = store(&cluster).put_shard_assignment(promoted).await;
    cluster.run_placement(Duration::from_millis(500));

    let served = felix_cluster::wait::until_some(
        Duration::from_secs(60),
        "the leader the control plane names to serve",
        || async {
            let now = cluster.owner(STREAM).await.ok()?;
            let ok = cluster
                .publish_via(&now, STREAM, b"probe".to_vec())
                .await
                .is_ok()
                && cluster.owner(STREAM).await.is_ok_and(|again| again == now);
            ok.then_some(now)
        },
    )
    .await;
    let mut opened = BTreeMap::new();
    read_logged(&cluster, OPENED, &mut opened);
    assert!(
        served.is_ok(),
        "no leader serves after the control plane returned; opened {opened:?}, stood {stood:?}"
    );
    let leader = served.expect("checked above");
    acked.extend(publish_acked(&cluster, &leader, "after", 10).await);
    let missing = missing_on(&cluster, &leader, &acked).await;
    assert!(
        missing.is_empty(),
        "{leader} lost acknowledged records: {missing:?}"
    );
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);
    cluster.shutdown().await;
}

/// Per broker: publishes acknowledged, failed, and the last failure.
type Tally = BTreeMap<SocketAddr, (usize, usize, Option<String>)>;

/// Publishes numbered records through the given brokers in turn until
/// stopped, keeping the ones acknowledged. Holds no borrow of the cluster, so
/// the test can kill and restart brokers under it.
struct Writer {
    acked: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    /// Brokers left out of the rotation: a killed one would cost every
    /// round its full timeout.
    skipped: Arc<Mutex<BTreeSet<SocketAddr>>>,
    tally: Arc<Mutex<Tally>>,
    task: tokio::task::JoinHandle<()>,
}

impl Writer {
    fn start(cluster: &Cluster) -> Self {
        let addrs: Vec<SocketAddr> = cluster
            .node_ids()
            .iter()
            .map(|id| cluster.node(id).expect("node").client_addr)
            .collect();
        let tenant = cluster.tenant_id.clone();
        let namespace = cluster.namespace.clone();
        let token = cluster.client_token();
        let acked = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let skipped = Arc::new(Mutex::new(BTreeSet::new()));
        let tally = Arc::new(Mutex::new(BTreeMap::new()));
        let task = tokio::spawn({
            let acked = Arc::clone(&acked);
            let stop = Arc::clone(&stop);
            let skipped = Arc::clone(&skipped);
            let tally = Arc::clone(&tally);
            async move {
                let mut i = 0usize;
                let mut turn = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let addr = addrs[turn % addrs.len()];
                    turn += 1;
                    let skip = skipped.lock().expect("skip lock").contains(&addr);
                    if skip {
                        continue;
                    }
                    let payload = format!("w-{i}");
                    let sent = tokio::time::timeout(Duration::from_secs(3), async {
                        let client = felix_cluster::client::connect(addr, &tenant, &token).await?;
                        let publisher = client.publisher().await?;
                        publisher
                            .publish(
                                &tenant,
                                &namespace,
                                STREAM,
                                payload.clone().into_bytes(),
                                felix_wire::AckMode::PerMessage,
                            )
                            .await?;
                        anyhow::Ok(())
                    })
                    .await;
                    let failure = match sent {
                        Ok(Ok(())) => {
                            acked.lock().expect("acked lock").push(payload);
                            None
                        }
                        Ok(Err(err)) => Some(format!("{err:#}")),
                        Err(_) => Some("no answer within 3s".to_string()),
                    };
                    {
                        let mut tally = tally.lock().expect("tally lock");
                        let entry = tally.entry(addr).or_insert((0, 0, None));
                        match failure {
                            None => entry.0 += 1,
                            Some(why) => {
                                entry.1 += 1;
                                entry.2 = Some(why);
                            }
                        }
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        });
        Self {
            acked,
            stop,
            skipped,
            tally,
            task,
        }
    }

    /// Leave `node` out of the rotation until [`Self::resume`].
    fn skip(&self, cluster: &Cluster, node: &str) {
        let addr = cluster.node(node).expect("node").client_addr;
        self.skipped.lock().expect("skip lock").insert(addr);
    }

    /// Put every skipped broker back in the rotation.
    fn resume(&self) {
        self.skipped.lock().expect("skip lock").clear();
    }

    /// Per broker: acknowledged, failed, and the last failure.
    fn tally(&self) -> Tally {
        self.tally.lock().expect("tally lock").clone()
    }

    fn acked(&self) -> usize {
        self.acked.lock().expect("acked lock").len()
    }

    async fn stop(self) -> Vec<String> {
        self.stop.store(true, Ordering::Relaxed);
        self.task.await.expect("writer task");
        Arc::try_unwrap(self.acked)
            .expect("the writer is done")
            .into_inner()
            .expect("acked lock")
    }
}

/// **The control plane stays down across a leader kill under load.** Three
/// brokers, RF 3, a writer publishing throughout. The control plane crashes,
/// the leader is killed, and a follower is elected and takes the writer's
/// records, all before the control plane is back. It then adopts the elected
/// leader, and the old leader restarts and catches up. Every acknowledged
/// record reads back from the elected leader, every other broker is caught
/// up with it, and no generation has two leaders.
#[serial]
#[tokio::test]
async fn the_control_plane_stays_down_across_a_leader_kill_under_load() {
    let mut cluster = start(3, 3, false).await;
    let before = assignment(&cluster).await;
    let writer = Writer::start(&cluster);
    felix_cluster::wait::until(Duration::from_secs(10), "the writer to get going", || {
        let acked = writer.acked();
        async move { acked >= 20 }
    })
    .await
    .expect("the writer's records are acknowledged");

    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    writer.skip(&cluster, &before.leader);
    cluster.kill_node(&before.leader).expect("kill the leader");
    let mut opened = BTreeMap::new();
    let (elected, generation) = elected_above(&cluster, before.generation, &mut opened).await;
    let at_election = writer.acked();
    felix_cluster::wait::until(
        Duration::from_secs(10),
        "the elected leader to take the writer's records",
        || {
            let acked = writer.acked();
            async move { acked >= at_election + 20 }
        },
    )
    .await
    .unwrap_or_else(|err| {
        panic!(
            "the writer's records are acknowledged after the election: {err:#}; \
             {elected} was elected; per broker (acked, failed, last failure): {:?}",
            writer.tally()
        )
    });

    cluster
        .recover_control_plane()
        .await
        .expect("recover the control plane");
    adopted(&cluster, &elected, generation).await;
    // A broker starts only once the control plane answers.
    cluster
        .restart_node(&before.leader)
        .await
        .expect("restart the old leader");
    writer.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let acked = writer.stop().await;

    let missing = missing_on(&cluster, &elected, &acked).await;
    assert!(
        missing.is_empty(),
        "{elected} lacks {} of {} acknowledged records, first {:?}",
        missing.len(),
        acked.len(),
        missing.first()
    );
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != elected)
        .collect();
    caught_up(&cluster, &followers, generation).await;
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);
    cluster.shutdown().await;
}

/// **A broker restarted while the control plane is down does not vote.** Three
/// brokers, RF 3. A broker starts only once the control plane answers, so a
/// follower restarted during the outage neither votes nor stands, and with
/// the leader also dead only one copy is serving: no majority, no new
/// generation until the control plane returns (#1196).
#[serial]
#[tokio::test]
async fn a_broker_restarted_while_the_control_plane_is_down_does_not_vote() {
    let mut cluster = start(3, 3, false).await;
    let before = assignment(&cluster).await;
    let mut acked = publish_acked(&cluster, &before.leader, "before", 20).await;
    let restarted = before.replicas.first().expect("a follower").clone();

    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    cluster.kill_node(&restarted).expect("kill a follower");
    cluster
        .start_node_unready(&restarted)
        .expect("start the follower again without waiting for it");
    cluster.kill_node(&before.leader).expect("kill the leader");

    // Three times the election bound: long enough for a vote or a candidacy
    // to show up in the logs if the restarted broker could take part.
    let mut opened = BTreeMap::new();
    let mut stood = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + ELECTED_WITHIN * 3;
    while tokio::time::Instant::now() < deadline {
        read_logged(&cluster, OPENED, &mut opened);
        read_logged(&cluster, STOOD, &mut stood);
        assert!(
            opened.range(before.generation + 1..).next().is_none()
                && stood.range(before.generation + 1..).next().is_none(),
            "a shard with one serving copy elected while the control plane was down: \
             opened {opened:?}, stood {stood:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    cluster
        .recover_control_plane()
        .await
        .expect("recover the control plane");
    cluster
        .await_node_ready(&restarted)
        .await
        .expect("the restarted follower comes up once the control plane answers");
    let last = Mutex::new(None);
    let (leader, generation) =
        felix_cluster::wait::until_some(ELECTED_WITHIN * 3, "the shard to serve again", || {
            let last = &last;
            let cluster = &cluster;
            let before = &before;
            async move {
                let now = cluster.shard_assignments().await.ok().and_then(|mut all| {
                    all.remove(&format!(
                        "{}/{}/{STREAM}/0",
                        cluster.tenant_id, cluster.namespace
                    ))
                });
                *last.lock().expect("lock") = now.clone();
                let now = now?;
                if now.generation <= before.generation || now.leader == before.leader {
                    return None;
                }
                cluster
                    .publish_via(&now.leader, STREAM, b"probe".to_vec())
                    .await
                    .ok()?;
                Some((now.leader, now.generation))
            }
        })
        .await
        .unwrap_or_else(|err| {
            panic!(
                "no leader serves above generation {} after the control plane returned: \
                 {err:#}; the control plane names {:?}",
                before.generation,
                last.lock().expect("lock")
            )
        });
    assert!(generation > before.generation, "{generation}");
    acked.extend(publish_acked(&cluster, &leader, "after", 5).await);
    let missing = missing_on(&cluster, &leader, &acked).await;
    assert!(
        missing.is_empty(),
        "{leader} lost acknowledged records: {missing:?}"
    );
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);
    cluster.shutdown().await;
}

#[test]
fn a_refused_read_is_not_a_lost_record() {
    let wanted = vec!["a".to_string(), "b".to_string()];
    let refused = ReadBack::Refused("still opening the shard".to_string());
    assert!(matches!(judge(&wanted, refused), Verdict::Unreadable(_)));
}

#[test]
fn a_read_that_succeeds_without_a_record_is_a_loss() {
    let wanted = vec!["a".to_string(), "b".to_string()];
    let held = ReadBack::Held(BTreeSet::from(["a".to_string()]));
    match judge(&wanted, held) {
        Verdict::Missing(missing) => assert_eq!(missing, vec!["b".to_string()]),
        _ => panic!("an absent record on a successful read must be reported as missing"),
    }
    let all = ReadBack::Held(wanted.iter().cloned().collect());
    assert!(matches!(judge(&wanted, all), Verdict::Complete));
}

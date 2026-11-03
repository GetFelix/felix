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

/// `nodes` brokers, one `Quorum` stream at `rf`, and every feature
/// `replica_election` needs finalized. The set is then written again so its
/// leader fences and keeps it: the first placement ran before the features
/// were on, and a set nobody keeps is one nobody can stand on.
async fn start(nodes: usize, rf: u32, proxy_links: bool) -> Cluster {
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
        broker_env: vec![
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
        ],
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

/// Every record of `wanted` that `node` does not hold, replaying the stream
/// from the start. Waits a while for a follower still catching up.
async fn missing_on(cluster: &Cluster, node: &str, wanted: &[String]) -> Vec<String> {
    let deadline =
        tokio::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(20));
    loop {
        let mut held = BTreeSet::new();
        if let Ok((_client, mut replay)) = cluster.replay_on(node, STREAM).await {
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(2), replay.next_event()).await
            {
                held.insert(String::from_utf8_lossy(&event.payload).into_owned());
            }
        }
        let missing: Vec<String> = wanted
            .iter()
            .filter(|record| !held.contains(*record))
            .cloned()
            .collect();
        if missing.is_empty() || tokio::time::Instant::now() >= deadline {
            return missing;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
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
    let survivor = before
        .replicas
        .iter()
        .find(|id| **id != elected)
        .expect("the other follower");
    let missing = missing_on(&cluster, survivor, &acked).await;
    assert!(
        missing.is_empty(),
        "the other follower {survivor} lacks acknowledged records: {missing:?}"
    );
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);
    cluster.shutdown().await;
}

/// **Two followers stand at once and exactly one opens.** Five brokers,
/// RF 5. The two lowest-id followers cannot reach each other, so neither
/// counts the other ahead of it, and the other two cannot ask for votes at
/// all. What the two send the others arrives late, so each one's pre-vote
/// lands before the other's fence: both put a ballot for the same generation
/// on disk and fence the same two voters. Those voters' ballots let only one
/// of them open there. Without the ballot check both would.
#[serial]
#[tokio::test]
async fn two_followers_stand_at_once_and_one_opens() {
    let mut cluster = start(5, 5, true).await;
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

    cluster.kill_node(&before.leader).expect("kill the leader");
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

/// Publishes numbered records through the given brokers in turn until
/// stopped, keeping the ones acknowledged. Holds no borrow of the cluster, so
/// the test can kill and restart brokers under it.
struct Writer {
    acked: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
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
        let task = tokio::spawn({
            let acked = Arc::clone(&acked);
            let stop = Arc::clone(&stop);
            async move {
                let mut i = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let payload = format!("w-{i}");
                    let addr = addrs[i % addrs.len()];
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
                    if matches!(sent, Ok(Ok(()))) {
                        acked.lock().expect("acked lock").push(payload);
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        });
        Self { acked, stop, task }
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
/// record survives on every broker, and no generation has two leaders.
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
    .expect("the writer's records are acknowledged after the election");

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
    tokio::time::sleep(Duration::from_secs(3)).await;
    let acked = writer.stop().await;

    for id in cluster.node_ids() {
        let missing = missing_on(&cluster, &id, &acked).await;
        assert!(
            missing.is_empty(),
            "{id} lacks {} of {} acknowledged records, first {:?}",
            missing.len(),
            acked.len(),
            missing.first()
        );
    }
    read_logged(&cluster, OPENED, &mut opened);
    assert_one_leader_per_generation(&opened, before.generation);
    cluster.shutdown().await;
}

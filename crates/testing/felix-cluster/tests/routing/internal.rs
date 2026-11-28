//! A broker started with `FELIX_NODE_ROLE=internal` holds no tenant shard,
//! so on Raft it never holds a seat in a group: not at formation, not when
//! a drain moves copies, and not when a lost voter must be replaced and it
//! is the only spare. The lost seat waits for an ordinary broker instead.
//! Every placement pass is checked, not only the end state, since a seat
//! given and then taken back still made the internal broker a voter.
//!
//! Run with `cargo test -p felix-cluster --test routing internal::`.
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_controlplane_service::cluster::placement::MovePolicy;
use felix_controlplane_service::model::ShardAssignment;
use serial_test::serial;

const STREAM: &str = "orders";
const SHARDS: usize = 4;
const INTERNAL: &str = "broker-4";

fn config() -> ClusterConfig {
    let mut node_env = vec![Vec::new(); 4];
    node_env.push(vec![(
        "FELIX_NODE_ROLE".to_string(),
        "internal".to_string(),
    )]);
    ClusterConfig {
        nodes: 5,
        node_env,
        streams: vec![StreamSpec::replicated(STREAM, SHARDS as u32, 3)],
        ..Default::default()
    }
}

/// A broker down a second is lost, so its seat is given away.
fn refilling() -> MovePolicy {
    MovePolicy {
        restore_after_millis: Some(1_000),
        max_concurrent: 4,
        ..MovePolicy::default()
    }
}

async fn records(cluster: &Cluster) -> Vec<ShardAssignment> {
    let records: Vec<ShardAssignment> = cluster
        .control_plane
        .as_ref()
        .expect("control plane running")
        .store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .filter(|a| a.key.stream == STREAM)
        .collect();
    assert_eq!(records.len(), SHARDS, "{records:?}");
    records
}

fn set(record: &ShardAssignment) -> BTreeSet<String> {
    record.nodes().cloned().collect()
}

fn voters(record: &ShardAssignment) -> BTreeSet<String> {
    record
        .raft_voters
        .as_ref()
        .map(|voters| voters.members.keys().cloned().collect())
        .unwrap_or_default()
}

/// Every broker the record gives a seat, a vote, or a place in the target
/// the group is told to reach.
fn seats(record: &ShardAssignment) -> BTreeSet<String> {
    let target = record
        .raft_target
        .iter()
        .flat_map(|target| target.members.keys().cloned());
    set(record)
        .into_iter()
        .chain(voters(record))
        .chain(target)
        .chain(record.successor.clone())
        .chain(record.joining.clone())
        .collect()
}

/// Reads every record, fails if any seats the internal broker, and returns
/// them.
async fn checked(cluster: &Cluster, when: &str) -> Vec<ShardAssignment> {
    let records = records(cluster).await;
    for record in &records {
        assert!(
            !seats(record).contains(INTERNAL),
            "{when}: shard {} seats the internal {INTERNAL}: {record:?}",
            record.key.shard
        );
    }
    records
}

/// Steps placement under `policy` until `done` holds of every record,
/// checking the internal broker after each pass.
async fn place_until(
    cluster: &Cluster,
    policy: &MovePolicy,
    when: &str,
    done: impl Fn(&ShardAssignment) -> bool,
) {
    let deadline = Instant::now() + felix_cluster::wait::budget(Duration::from_secs(180));
    loop {
        let outcome = cluster
            .control_plane
            .as_ref()
            .expect("control plane running")
            .place_shards_with(policy.clone())
            .await;
        let now = checked(cluster, when).await;
        if now.iter().all(&done) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{when}: never settled: {now:?}; last pass: {outcome:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn full_and_voting(record: &ShardAssignment) -> bool {
    set(record).len() == 3 && voters(record) == set(record)
}

#[serial]
#[tokio::test]
async fn on_raft_an_internal_broker_never_holds_a_voter_seat() {
    if !felix_cluster::suite_runs_on_raft() {
        eprintln!("skipped: the seats checked here are a Raft group's");
        return;
    }
    let mut cluster = Cluster::start(config()).await.expect("start cluster");

    place_until(
        &cluster,
        &MovePolicy::default(),
        "formation",
        full_and_voting,
    )
    .await;

    let drained = "broker-0";
    cluster.drain_node(drained).await.expect("drain");
    place_until(&cluster, &refilling(), "the drain", |record| {
        full_and_voting(record) && !set(record).contains(drained)
    })
    .await;

    // Every set is now broker-1, broker-2 and broker-3. With broker-1 lost
    // the only live broker outside the sets is the internal one, so the
    // seat waits.
    let lost = "broker-1";
    cluster.kill_node(lost).expect("kill");
    let held_until = Instant::now() + Duration::from_secs(10);
    while Instant::now() < held_until {
        cluster
            .control_plane
            .as_ref()
            .expect("control plane running")
            .place_shards_with(refilling())
            .await;
        for record in checked(&cluster, "with only the internal broker spare").await {
            assert!(set(&record).contains(lost), "{record:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let joined = cluster.add_node().await.expect("add a broker");
    place_until(&cluster, &refilling(), "the replacement", |record| {
        full_and_voting(record) && set(record).contains(&joined) && !set(record).contains(lost)
    })
    .await;
    cluster.shutdown().await;
}

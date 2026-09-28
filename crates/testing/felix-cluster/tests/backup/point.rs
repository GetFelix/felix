//! A backup point taken under load holds every acknowledged record and no
//! uncommitted one.
//!
//! Run with `cargo test -p felix-cluster --test backup point::`.
use std::collections::HashMap;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Endpoint, Fault, StreamSpec};
use serial_test::serial;

use super::{LOAD_WARMUP, Load, committed_on, point_shard, take_point};

const QUORUM: &str = "orders";
const QUORUM_SHARDS: u32 = 4;
const LEADER: &str = "events";
const LEADER_SHARDS: u32 = 2;
/// A one-shard `Quorum` stream the second half stalls on purpose.
const HELD: &str = "ledger";

/// **Every shard is at a committed prefix and nothing acknowledged before
/// the barrier is missing.** Publishers run through every broker, on a
/// `Quorum` stream and a replicated `Leader` one, while the point is taken.
/// Then a `Quorum` shard whose leader can reach no follower is given writes
/// it can append and never commit, and a second point must not count them.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_point_under_load_is_committed_and_misses_nothing_acknowledged_before_it() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![
            StreamSpec::quorum(QUORUM, QUORUM_SHARDS, 3),
            StreamSpec::replicated(LEADER, LEADER_SHARDS, 3),
            StreamSpec::quorum(HELD, 1, 3),
        ],
        proxy_links: true,
        broker_env: vec![
            // A `Leader` acknowledgement means committed only with this on;
            // otherwise it is sent when the publish is queued.
            ("FELIX_ACK_ON_COMMIT".to_string(), "true".to_string()),
            (
                "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
                "1500".to_string(),
            ),
        ],
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start");

    let load = Load::start(&cluster, &[QUORUM, LEADER]).await;
    tokio::time::sleep(super::LOAD_WARMUP).await;
    let point = take_point(&cluster, "under-load").await;
    tokio::time::sleep(LOAD_WARMUP / 2).await;
    let acked = load.stop().await;

    let assignments = cluster.shard_assignments().await.expect("assignments");
    assert_eq!(
        point.shards.len(),
        assignments.len(),
        "the point has an entry for every assigned shard"
    );

    // Read after the load stopped, so everything acknowledged is below these.
    let mut later = HashMap::new();
    for node in cluster.node_ids() {
        later.extend(committed_on(&cluster, &node).await);
    }
    let mut offsets = HashMap::new();
    for (stream, shards) in [(QUORUM, QUORUM_SHARDS), (LEADER, LEADER_SHARDS)] {
        for shard in 0..shards {
            let entry = point_shard(&point, stream, shard);
            let committed = later[&(stream.to_string(), shard)];
            assert!(
                entry.logs.records <= committed,
                "{stream}/{shard}: the point is at {} but only {committed} is committed",
                entry.logs.records
            );
            offsets.extend(read_shard(&cluster, &entry.leader, stream, shard, committed).await);
        }
    }

    let before: Vec<_> = acked
        .iter()
        .filter(|ack| ack.at_millis < point.taken_at_millis)
        .collect();
    assert!(
        before.len() > 50,
        "too few acknowledgements before the barrier to show anything: {}",
        before.len()
    );
    for ack in before {
        let Some(&(shard, offset)) = offsets.get(&ack.payload) else {
            panic!(
                "{} was acknowledged before the barrier and is not in the log",
                ack.payload
            );
        };
        let at = point_shard(&point, ack.stream, shard).logs.records;
        assert!(
            offset < at,
            "{} was acknowledged before the barrier at {}/{shard} offset {offset}, \
             outside the point, which ends at {at}",
            ack.payload,
            ack.stream
        );
    }

    // Now a gap on purpose: the held stream's leader loses every follower, so
    // what it appends stays past its mark.
    let owner = cluster.owner(HELD).await.expect("owner");
    let committed_before = committed_on(&cluster, &owner).await[&(HELD.to_string(), 0)];
    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|node| *node != owner)
        .collect();
    let cut: Vec<Fault> = followers
        .iter()
        .map(|follower| Fault::Drop {
            from: Endpoint::node(&owner),
            to: Endpoint::node(follower),
        })
        .collect();
    for fault in &cut {
        cluster.inject(fault).await.expect("drop a link");
    }
    let client = cluster.client_on(&owner).await.expect("connect");
    let publisher = client.publisher().await.expect("publisher");
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let stalled = tokio::spawn(async move {
        for seq in 0..5u32 {
            let _ = publisher
                .publish(
                    &tenant,
                    &namespace,
                    HELD,
                    format!("unacknowledged-{seq}").into_bytes(),
                    felix_wire::AckMode::PerMessage,
                )
                .await;
        }
    });
    // Each publish is on the leader's disk well before its quorum wait ends.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let gapped = take_point(&cluster, "with-a-gap").await;
    // Still waiting on a majority, so the leader holds a record past its mark
    // and the check below has something to exclude.
    assert!(
        !stalled.is_finished(),
        "the publishes to {HELD} were answered while its leader reached no follower"
    );
    let committed_now = committed_on(&cluster, &owner).await[&(HELD.to_string(), 0)];
    let held = point_shard(&gapped, HELD, 0).logs.records;
    assert_eq!(
        committed_now, committed_before,
        "nothing can commit on a leader that reaches no follower"
    );
    assert!(
        held <= committed_now,
        "the point put {HELD}/0 at {held}, past its committed {committed_now}: \
         it holds a record no majority has"
    );

    cluster.heal_all().await.expect("heal");
    stalled.await.expect("stalled publishes");
    drop(client);
    cluster.shutdown().await;
}

/// Every record of one shard below `until`, as payload -> (shard, offset).
async fn read_shard(
    cluster: &Cluster,
    node: &str,
    stream: &str,
    shard: u32,
    until: u64,
) -> HashMap<String, (u32, u64)> {
    let mut records = HashMap::new();
    if until == 0 {
        return records;
    }
    let (_client, mut subscription) = cluster
        .replay_shard(node, stream, shard)
        .await
        .expect("replay");
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
            .await
            .unwrap_or_else(|_| panic!("{stream}/{shard} stopped short of {until}"))
            .expect("event")
            .expect("subscription open");
        let offset = event.offset.expect("durable events carry offsets");
        records.insert(
            String::from_utf8(event.payload.to_vec()).expect("utf8"),
            (shard, offset),
        );
        if offset + 1 >= until {
            return records;
        }
    }
}

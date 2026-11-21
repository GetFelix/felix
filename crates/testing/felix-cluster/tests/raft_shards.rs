//! Durable stream shards on Raft, across real broker processes.
//!
//! Every broker here runs with `FELIX_SHARD_ENGINE=raft`, which needs a
//! broker built with the `raft` feature. The harness runs whatever
//! `felix-broker` sits beside the test binary, so these are opt-in: without
//! `FELIX_TEST_RAFT_SHARDS` set they pass without starting anything.
//!
//! ```text
//! cargo build -p felix-broker-service --bin felix-broker --features raft
//! FELIX_TEST_RAFT_SHARDS=1 cargo test -p felix-cluster --test raft_shards
//! ```
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_wire::AckMode;
use serial_test::serial;

const STREAM: &str = "orders";
const OPT_IN: &str = "FELIX_TEST_RAFT_SHARDS";
const ELECTED_WITHIN: Duration = Duration::from_secs(60);

/// Whether this run asked for the Raft tests. Says so when it did not, so a
/// pass in the default suite does not read as coverage.
fn opted_in() -> bool {
    let asked = std::env::var_os(OPT_IN).is_some();
    if !asked {
        eprintln!("skipped: set {OPT_IN}=1 and build felix-broker with --features raft");
    }
    asked
}

fn engine(name: &str) -> (String, String) {
    ("FELIX_SHARD_ENGINE".to_string(), name.to_string())
}

/// Three brokers, one shard, each broker a member of its group.
fn raft_cluster() -> ClusterConfig {
    raft_cluster_with(StreamSpec::quorum(STREAM, 1, 3))
}

fn raft_cluster_with(stream: StreamSpec) -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        streams: vec![stream],
        broker_env: vec![engine("raft")],
        ..Default::default()
    }
}

/// How many times `held` has `payload`.
fn copies(held: &[Vec<u8>], payload: &[u8]) -> usize {
    held.iter().filter(|have| *have == payload).count()
}

/// The broker that leads the shard: the one that reports serving its Raft
/// term. A follower accepts subscribes too, so a subscribe cannot tell them
/// apart. Waits until exactly one broker in `among` says so, since a deposed
/// leader's gauge trails by one feed pass, and until it still says so a
/// little later: the broker that forms a group leads it only until it hands
/// the first leadership to placement's leader. Only brokers in `among` are
/// asked.
async fn leader_among(cluster: &Cluster, among: &[String]) -> String {
    loop {
        let first = one_leader(cluster, among).await;
        tokio::time::sleep(LEADER_SETTLES).await;
        if one_leader(cluster, among).await == first {
            return first;
        }
    }
}

/// How long a leader must keep leading before [`leader_among`] names it.
const LEADER_SETTLES: Duration = Duration::from_secs(2);

async fn one_leader(cluster: &Cluster, among: &[String]) -> String {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a broker to lead the shard", || async {
        let mut leading = Vec::new();
        for node in among {
            let led = cluster
                .metric(node, "felix_broker_raft_shards_led")
                .await
                .ok()
                .flatten();
            if led.is_some_and(|led| led >= 1.0) {
                leading.push(node.clone());
            }
        }
        (leading.len() == 1).then(|| leading.remove(0))
    })
    .await
    .expect("a leader")
}

/// Publish through `node` until the broker acknowledges, and return the
/// offset. A shard between leaders answers "not ready", which a client
/// retries.
async fn publish_acked(cluster: &Cluster, node: &str, payload: &[u8]) -> u64 {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a publish to be acknowledged", || async {
        cluster
            .publish_via_at(node, STREAM, payload.to_vec())
            .await
            .ok()
            .flatten()
    })
    .await
    .unwrap_or_else(|err| panic!("publish through {node}: {err:#}"))
}

/// Finalizes `raft_follower_read`, without which a follower sends a read
/// that could start below a trim floor to the leader.
async fn allow_follower_reads(cluster: &Cluster) {
    cluster
        .finalize_fleet_features(&["raft_follower_read"], Duration::from_secs(30))
        .await
        .expect("finalize raft_follower_read");
}

/// Everything `node` replays from the start of the stream, read until every
/// payload in `owed` has arrived.
async fn replay_holding(cluster: &Cluster, node: &str, owed: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let (_client, mut subscription) = cluster.replay_on(node, STREAM).await.expect("replay");
    let mut payloads = Vec::new();
    while !owed.iter().all(|payload| payloads.contains(payload)) {
        match tokio::time::timeout(Duration::from_secs(10), subscription.next_event()).await {
            Ok(Ok(Some(event))) => payloads.push(event.payload.to_vec()),
            other => panic!(
                "{node} stopped at {} records with {} owed: {:?}",
                payloads.len(),
                owed.len(),
                other.map(|event| event.map(|event| event.is_some()))
            ),
        }
    }
    payloads
}

/// **A record published through any broker reaches a subscriber, whether
/// or not its publisher asked for an answer.** The subscriber connects to a
/// follower, which serves it from its own log.
#[tokio::test]
#[serial]
async fn a_publish_through_any_broker_reaches_a_subscriber() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    allow_follower_reads(&cluster).await;
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("follower");

    let reader = std::sync::Arc::new(
        felix_cluster::client::connect_cluster(
            &[cluster.node(follower).expect("node").client_addr],
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("reader"),
    );
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe through a follower");

    let mut owed = Vec::new();
    let mut acked = Vec::new();
    for node in &nodes {
        let payload = format!("through-{node}").into_bytes();
        acked.push(publish_acked(&cluster, node, &payload).await);
        owed.push(payload);
    }
    assert!(
        acked.windows(2).all(|pair| pair[0] < pair[1]),
        "acknowledged offsets are not in publish order: {acked:?}"
    );

    // A batch nobody waits on is committed and delivered all the same.
    let writer = felix_cluster::client::connect(
        cluster.node(&leader).expect("node").client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("writer");
    let unacked: Vec<Vec<u8>> = (0..16)
        .map(|i| format!("unacked-{i}").into_bytes())
        .collect();
    writer
        .publisher()
        .await
        .expect("publisher")
        .publish_batch(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            unacked.clone(),
            felix_wire::AckMode::None,
        )
        .await
        .expect("an unacknowledged batch");
    owed.extend(unacked);

    let mut seen = Vec::new();
    let mut offsets = Vec::new();
    while !owed.iter().all(|payload| seen.contains(payload)) {
        let event = tokio::time::timeout(Duration::from_secs(30), subscription.next_event())
            .await
            .expect("an event within 30s")
            .expect("delivery")
            .expect("the subscription stays open");
        offsets.push(event.offset.expect("a durable stream carries offsets"));
        seen.push(event.payload.to_vec());
    }
    assert!(
        offsets.windows(2).all(|pair| pair[0] < pair[1]),
        "delivered out of order: {offsets:?}"
    );
}

/// A key and one header naming `trace`.
/// **A shard that started on Raft gets its target from its brokers' copy
/// ids, and its group forms only on that target.** Each broker reports the
/// copy its member votes under once the copy is synced, and the control
/// plane writes the target only when every broker in the set has. No broker
/// leads the group before the target is stored, and the group serves under
/// it.
#[tokio::test]
#[serial]
async fn a_shard_started_on_raft_forms_its_group_on_the_target_from_its_copies() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let mut nodes = cluster.node_ids();
    nodes.sort();
    let target =
        felix_cluster::wait::until_some(ELECTED_WITHIN, "the shard's Raft target", || async {
            // Leaders read before the target: a leader seen while the target
            // still reads as absent led a group formed without one.
            let mut leading = Vec::new();
            for node in &nodes {
                if metric(&cluster, node, "felix_broker_raft_shards_led").await >= 1.0 {
                    leading.push(node.clone());
                }
            }
            let target = cluster.raft_target(STREAM, 0).await.ok().flatten();
            assert!(
                target.is_some() || leading.is_empty(),
                "{leading:?} led the shard's group before the control plane stored a target"
            );
            target
        })
        .await
        .expect("a target");
    assert_eq!(target.keys().cloned().collect::<Vec<_>>(), nodes);
    assert!(target.values().all(|copy| *copy != 0), "{target:?}");
    let leader = leader_among(&cluster, &nodes).await;
    publish_acked(&cluster, &leader, b"under the target").await;
    cluster.shutdown().await;
}

fn headers(trace: &str) -> felix_client::RecordHeaders {
    felix_client::RecordHeaders {
        key: Some(bytes::Bytes::from(format!("key-{trace}"))),
        headers: vec![("trace".to_string(), bytes::Bytes::from(trace.to_string()))],
    }
}

/// Publish one headered record through `node` until it is acknowledged. A
/// shard between leaders, or a broker the finalize has not reached yet,
/// refuses, and a client retries.
async fn publish_headered(cluster: &Cluster, node: &str, trace: &str) -> u64 {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a headered publish", || async {
        let client = cluster.client_on(node).await.ok()?;
        let publisher = client.publisher().await.ok()?;
        publisher
            .publish_with_headers(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                headers(trace),
                trace.as_bytes().to_vec(),
                AckMode::PerMessage,
            )
            .await
            .ok()
            .flatten()
    })
    .await
    .unwrap_or_else(|err| panic!("publish {trace} through {node}: {err:#}"))
}

/// **A record's key and headers on a Raft shard reach every member.**
/// Published through the leader and through followers, which forward to it,
/// each record replays with its headers from every broker's own log.
#[tokio::test]
#[serial]
async fn record_headers_on_a_raft_shard_replay_from_every_member() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(
            &[
                "publisher_principal",
                "record_headers",
                "raft_record_headers",
                "raft_follower_read",
            ],
            Duration::from_secs(30),
        )
        .await
        .expect("finalize the headers features");
    let nodes = cluster.node_ids();
    let mut published = Vec::new();
    for node in &nodes {
        let trace = format!("through-{node}");
        let offset = publish_headered(&cluster, node, &trace).await;
        published.push((offset, trace));
    }
    for node in &nodes {
        let addr = cluster.node(node).expect("node").client_addr;
        let client = felix_cluster::client::connect_with(
            addr,
            &cluster.tenant_id,
            &cluster.client_token(),
            |config| config.publishers = true,
        )
        .await
        .expect("connect");
        let mut subscription = client
            .subscribe_from(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .expect("replay");
        let mut held = Vec::new();
        while held.len() < published.len() {
            let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
                .await
                .unwrap_or_else(|_| panic!("{node} stopped at {} records", held.len()))
                .expect("delivery")
                .expect("the subscription stays open");
            let Some(offset) = event.offset else { continue };
            let Some((_, trace)) = published.iter().find(|(at, _)| *at == offset) else {
                continue;
            };
            assert_eq!(
                event.payload.as_ref(),
                trace.as_bytes(),
                "{node} at {offset}"
            );
            assert_eq!(event.headers, Some(headers(trace)), "{node} at {offset}");
            let publisher = event.publisher.as_deref().unwrap_or_default();
            assert!(
                publisher.contains("harness-client"),
                "{node} at {offset}: {publisher:?}"
            );
            held.push(offset);
        }
    }
}

/// Whether every acknowledged offset has reached the subscriber. A publish
/// retried after an unknown outcome can leave a record the test never saw
/// acknowledged, so the counts are not compared.
fn arrived(acked: &[(u64, Vec<u8>)], seen: &[(u64, Vec<u8>)]) -> bool {
    acked
        .iter()
        .all(|(offset, _)| seen.iter().any(|(got, _)| got == offset))
}

/// **A follower serves a subscriber, and the subscriber keeps every
/// acknowledged record across the leader's death.** The reader connects only
/// to a follower, reads from offset 0, and sees each record at the offset the
/// leader acknowledged. When the leader is killed and a survivor takes over,
/// the subscription resumes (or is resumed by offset) with no record missing
/// and none twice.
#[tokio::test]
#[serial]
async fn a_followers_subscriber_keeps_every_acknowledged_record_when_the_leader_dies() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    allow_follower_reads(&cluster).await;
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("follower")
        .clone();

    // One connection to the follower alone: it does not follow a redirect, so
    // a follower that sent the reader to the leader fails the subscribe.
    let reader = felix_cluster::client::connect(
        cluster.node(&follower).expect("node").client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("reader");
    let mut subscription = reader
        .subscribe_from(
            &cluster.tenant_id,
            &cluster.namespace,
            STREAM,
            Some(felix_client::StartPosition::Offset(0)),
        )
        .await
        .expect("subscribe on a follower");

    let mut acked: Vec<(u64, Vec<u8>)> = Vec::new();
    for i in 0..10 {
        let payload = format!("before-{i}").into_bytes();
        acked.push((publish_acked(&cluster, &leader, &payload).await, payload));
    }
    let mut seen: Vec<(u64, Vec<u8>)> = Vec::new();
    while !arrived(&acked, &seen) {
        let event = tokio::time::timeout(Duration::from_secs(30), subscription.next_event())
            .await
            .expect("an event within 30s")
            .expect("delivery")
            .expect("the subscription stays open while the leader lives");
        seen.push((event.offset.expect("offset"), event.payload.to_vec()));
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    leader_among(&cluster, &survivors).await;
    for i in 0..10 {
        let payload = format!("after-{i}").into_bytes();
        acked.push((publish_acked(&cluster, &follower, &payload).await, payload));
    }

    while !arrived(&acked, &seen) {
        let next = tokio::time::timeout(Duration::from_secs(30), subscription.next_event())
            .await
            .expect("an event within 30s");
        match next {
            Ok(Some(event)) => seen.push((event.offset.expect("offset"), event.payload.to_vec())),
            // The subscription ended with its member's term: resume by offset.
            Ok(None) | Err(_) => {
                let from = seen.last().map_or(0, |(offset, _)| offset + 1);
                subscription = felix_cluster::wait::until_some(
                    ELECTED_WITHIN,
                    "a resubscribe to be accepted",
                    || async {
                        reader
                            .subscribe_from(
                                &cluster.tenant_id,
                                &cluster.namespace,
                                STREAM,
                                Some(felix_client::StartPosition::Offset(from)),
                            )
                            .await
                            .ok()
                    },
                )
                .await
                .expect("resubscribe");
            }
        }
    }

    assert!(
        seen.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "a record repeated or arrived out of order: {:?}",
        seen.iter().map(|(offset, _)| offset).collect::<Vec<_>>()
    );
    for (offset, payload) in &acked {
        assert!(
            seen.iter()
                .any(|(got, body)| got == offset && body == payload),
            "acknowledged record {:?} at offset {offset} never arrived",
            String::from_utf8_lossy(payload)
        );
    }
}

/// **The leader dies while the control plane is down, and nothing
/// acknowledged is lost.** The group elects among the survivors on its own:
/// no placement, no lease and no promotion from the control plane. Publishes
/// continue, and the new leader holds every acknowledged record.
#[tokio::test]
#[serial]
async fn a_leader_lost_with_the_control_plane_down_loses_nothing_acknowledged() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let mut owed = Vec::new();
    let mut last = None;
    for i in 0..20 {
        let payload = format!("before-{i}").into_bytes();
        last = Some(publish_acked(&cluster, &leader, &payload).await);
        owed.push(payload);
    }

    cluster
        .crash_control_plane()
        .await
        .expect("crash the control plane");
    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;

    // Through the survivor that does not lead, so the forward is covered too.
    let other = survivors
        .iter()
        .find(|node| **node != elected)
        .expect("the other survivor");
    for i in 0..20 {
        let payload = format!("after-{i}").into_bytes();
        let offset = publish_acked(&cluster, other, &payload).await;
        assert!(
            Some(offset) > last,
            "{elected} acknowledged offset {offset} at or below {last:?}"
        );
        last = Some(offset);
        owed.push(payload);
    }

    let held = replay_holding(&cluster, &elected, &owed).await;
    for payload in &owed {
        assert_eq!(
            held.iter().filter(|have| *have == payload).count(),
            1,
            "{elected} holds {:?} other than once",
            String::from_utf8_lossy(payload)
        );
    }
}

/// **A `Leader` stream keeps acknowledging across a lost leader, and loses
/// at most the tail its old leader acknowledged alone.** A publisher keeps
/// publishing while the leader is cut off from its followers, so what the
/// leader answers on its lease from then on is on its log alone and goes
/// with it. What the next leader keeps of the acknowledged sequence is a
/// prefix of it, once and in order, and the next leader's acknowledgements
/// are all in its log.
#[tokio::test]
#[serial]
async fn a_leader_stream_lost_with_its_leader_keeps_a_prefix_of_what_was_acknowledged() {
    if !opted_in() {
        return;
    }
    let stream = StreamSpec::replicated(STREAM, 1, 3);
    let config = raft_cluster_with(stream);
    let mut cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    publish_acked(&cluster, &leader, b"first").await;

    // One connection, publishing in order until a publish is not answered:
    // the first one left waiting for a commit that cannot come.
    let client = cluster.client_on(&leader).await.expect("client");
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let publishing = tokio::spawn(async move {
        let publisher = client.publisher().await.expect("publisher");
        let mut acked = vec![b"first".to_vec()];
        for i in 0..100_000 {
            let payload = format!("run-{i}").into_bytes();
            let publish = publisher.publish(
                &tenant,
                &namespace,
                STREAM,
                payload.clone(),
                AckMode::PerMessage,
            );
            match tokio::time::timeout(Duration::from_secs(1), publish).await {
                Ok(Ok(_)) => acked.push(payload),
                _ => break,
            }
        }
        acked
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.partition_node(&leader).expect("partition");
    let acked = tokio::time::timeout(Duration::from_secs(30), publishing)
        .await
        .expect("the publisher stopped being answered")
        .expect("publisher task");

    cluster.kill_node(&leader).expect("kill the leader");
    cluster.heal_partitions().expect("heal");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;
    let mut after = Vec::new();
    for i in 0..20 {
        let payload = format!("after-{i}").into_bytes();
        publish_acked(&cluster, &elected, &payload).await;
        after.push(payload);
    }

    let held = replay_holding(&cluster, &elected, &after).await;
    for payload in &after {
        assert_eq!(copies(&held, payload), 1, "{elected} acknowledged it");
    }
    let kept: Vec<&Vec<u8>> = held.iter().filter(|have| acked.contains(have)).collect();
    assert!(
        kept.len() < acked.len(),
        "{elected} kept all {} acknowledged records: nothing was answered on the lease alone",
        acked.len()
    );
    assert!(
        kept.first().is_some_and(|have| have.as_slice() == b"first"),
        "{elected} lost `first`, acknowledged before the partition with its followers reachable"
    );
    assert_eq!(
        kept,
        acked[..kept.len()].iter().collect::<Vec<_>>(),
        "what {elected} kept is not a prefix of what was acknowledged"
    );
}

/// **A leader of a `Leader` stream that its followers cannot reach
/// acknowledges nothing the group loses once its lease is over.** The
/// broker keeps running and still says it leads, while the others elect.
/// Whatever is acknowledged from then on, by either side, is in the log the
/// group agrees on after the partition heals. This late the group takes no
/// write from the old leader, so this is the whole path and not the lease
/// check alone, which felix-replication's `raft/tests/deliver.rs` covers.
#[tokio::test]
#[serial]
async fn a_cut_off_leader_acknowledges_nothing_the_group_loses_after_its_lease() {
    if !opted_in() {
        return;
    }
    let stream = StreamSpec::replicated(STREAM, 1, 3);
    let config = raft_cluster_with(stream);
    let cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let mut owed = vec![b"before".to_vec()];
    publish_acked(&cluster, &leader, &owed[0]).await;

    cluster.partition_node(&leader).expect("partition");
    // Longer than the lease (1.5 s) and the longest election timeout (2 s).
    tokio::time::sleep(Duration::from_secs(3)).await;
    for i in 0..3 {
        let payload = format!("cut-off-{i}").into_bytes();
        let publish = cluster.publish_via_at(&leader, STREAM, payload.clone());
        let answered = tokio::time::timeout(Duration::from_secs(3), publish).await;
        if matches!(answered, Ok(Ok(_))) {
            owed.push(payload);
        }
    }
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;
    for i in 0..5 {
        let payload = format!("theirs-{i}").into_bytes();
        publish_acked(&cluster, &elected, &payload).await;
        owed.push(payload);
    }

    cluster.heal_partitions().expect("heal");
    let healed = b"healed".to_vec();
    publish_acked(&cluster, &leader, &healed).await;
    owed.push(healed);
    let held = replay_holding(&cluster, &elected, &owed).await;
    for payload in &owed {
        let text = String::from_utf8_lossy(payload);
        assert_eq!(copies(&held, payload), 1, "{elected} holds {text:?}");
    }
}

/// **A shard placed on fewer brokers than its stream asks for forms no
/// group and acknowledges nothing.** The stream asks for three brokers and
/// the cluster has one, so placement names that one alone. A group of one
/// would acknowledge on its own, and brokers placed later would form the
/// full set again on empty logs and elect each other.
#[tokio::test]
#[serial]
async fn a_short_placement_acknowledges_nothing() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: Vec::new(),
        broker_env: vec![engine("raft")],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let only = cluster.node_ids().remove(0);
    cluster
        .create_stream(&StreamSpec::quorum(STREAM, 1, 3))
        .await
        .expect("create the stream");
    felix_cluster::wait::until(ELECTED_WITHIN, "the shard to be placed", || async {
        cluster.place_shards().await;
        cluster
            .shard_owners_for(STREAM)
            .await
            .is_ok_and(|owners| owners.get(&0) == Some(&only))
    })
    .await
    .expect("placed on the one broker");

    let mut acked = 0;
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < until {
        cluster.place_shards().await;
        let publish = cluster.publish_via_at(&only, STREAM, b"alone".to_vec());
        if let Ok(Ok(Some(_))) = tokio::time::timeout(Duration::from_secs(2), publish).await {
            acked += 1;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        acked, 0,
        "one broker of three acknowledged records on its own"
    );
    // The control plane stores no target for a short set, so the broker
    // forms no group to lead.
    assert_eq!(cluster.raft_target(STREAM, 0).await.expect("record"), None);
    assert_eq!(
        metric(&cluster, &only, "felix_broker_raft_shards_led").await,
        0.0,
        "a group formed with no stored target"
    );
}

/// **Until the fleet finalizes `raft_consumer_groups`, a group on a Raft
/// shard is refused by name** by the leader, which alone serves groups. The
/// refusal names the fleet feature so an operator knows what to finalize; a
/// follower redirects to the leader.
#[tokio::test]
#[serial]
async fn a_consumer_group_on_a_raft_shard_is_refused_until_the_fleet_finalizes_it() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    publish_acked(&cluster, &leader, b"work").await;

    let err = cluster
        .group_poll_via(&leader, STREAM, 0, "workers", 10)
        .await
        .expect_err("a group was served before the fleet finalized the feature");
    assert!(
        format!("{err:#}").contains("raft_consumer_groups"),
        "the leader's refusal does not name the fleet feature: {err:#}"
    );
    for node in nodes.iter().filter(|node| **node != leader) {
        let err = cluster
            .group_poll_via(node, STREAM, 0, "workers", 10)
            .await
            .expect_err("a follower served a group");
        assert!(
            format!("{err:#}").contains(&format!("owned by {leader}")),
            "{node} does not redirect to the leader {leader}: {err:#}"
        );
    }
}

/// **A consumer group on a Raft shard works once the fleet finalizes
/// `raft_consumer_groups`.** Records are claimed and acknowledged through
/// the leader, an acknowledged record is not handed out again, and a
/// follower redirects to the leader.
#[tokio::test]
#[serial]
async fn a_consumer_group_on_a_raft_shard_serves_once_the_fleet_finalizes_it() {
    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(&["raft_consumer_groups"], Duration::from_secs(30))
        .await
        .expect("finalize the group feature");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    for payload in [b"one".as_slice(), b"two", b"three"] {
        publish_acked(&cluster, &leader, payload).await;
    }

    let first = cluster
        .group_poll_records_via(&leader, STREAM, 0, "workers", 1)
        .await
        .expect("the leader serves the group");
    assert_eq!(first.len(), 1, "one record asked for: {first:?}");
    cluster
        .group_ack_via(&leader, STREAM, 0, "workers", first[0].offset)
        .await
        .expect("ack through the leader");

    let rest = cluster
        .group_poll_records_via(&leader, STREAM, 0, "workers", 10)
        .await
        .expect("the leader serves the rest");
    assert!(
        rest.iter().all(|record| record.offset != first[0].offset),
        "an acknowledged record was handed out again: {rest:?}"
    );
    // The stream may hold records written while the cluster started, so check
    // that ours each came out once rather than counting the whole stream. A
    // record is its offset: the startup probe retries after an unanswered
    // publish, so the stream can hold its payload twice.
    let mut offsets: Vec<u64> = first
        .iter()
        .chain(rest.iter())
        .map(|record| record.offset)
        .collect();
    let handed_out = offsets.len();
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(
        offsets.len(),
        handed_out,
        "a record came out twice: {first:?} {rest:?}"
    );
    for payload in [b"one".as_slice(), b"two", b"three"] {
        let times = first
            .iter()
            .chain(rest.iter())
            .filter(|record| record.payload.as_ref() == payload)
            .count();
        assert_eq!(
            times,
            1,
            "{} was handed out {times} times: {first:?} {rest:?}",
            String::from_utf8_lossy(payload)
        );
    }
    for record in &rest {
        cluster
            .group_ack_via(&leader, STREAM, 0, "workers", record.offset)
            .await
            .expect("ack through the leader");
    }

    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("a follower");
    let err = cluster
        .group_poll_records_via(follower, STREAM, 0, "workers", 10)
        .await
        .expect_err("a follower served the group");
    assert!(
        format!("{err:#}").contains(&format!("owned by {leader}")),
        "{follower} does not redirect to the leader {leader}: {err:#}"
    );
    let drained = cluster
        .group_poll_records_via(&leader, STREAM, 0, "workers", 10)
        .await
        .expect("poll after everything is acknowledged");
    assert!(drained.is_empty(), "records left after acks: {drained:?}");
}

/// **A consumer's claims survive its Raft leader dying.** Claims are group
/// entries in the shard's log, so the member elected next hands a claimed
/// record to nobody else, the holder can still extend it, and the holder
/// reconnecting with `reclaim` takes it back at the same attempt.
#[tokio::test]
#[serial]
async fn a_group_claim_survives_its_raft_leader_dying() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(&["raft_consumer_groups"], Duration::from_secs(30))
        .await
        .expect("finalize the group feature");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    for payload in [b"one".as_slice(), b"two", b"three"] {
        publish_acked(&cluster, &leader, payload).await;
    }
    // Drain whatever the stream held before ours, so the claims below are on
    // our records.
    let mut held = Vec::new();
    while held.len() < 2 {
        let records = cluster
            .group_poll_as_via(&leader, STREAM, 0, "workers", "w", false, 1)
            .await
            .expect("poll as a member");
        assert!(!records.is_empty(), "ran out of records: {held:?}");
        for record in records {
            if [b"one".as_slice(), b"two"].contains(&record.payload.as_ref()) {
                held.push(record);
            } else {
                cluster
                    .group_ack_via(&leader, STREAM, 0, "workers", record.offset)
                    .await
                    .expect("ack");
            }
        }
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;

    cluster
        .group_extend_via(
            &elected,
            STREAM,
            0,
            "workers",
            &held[0],
            Duration::from_secs(60),
        )
        .await
        .expect("the holder extends its claim on the new leader");
    let others = cluster
        .group_poll_as_via(&elected, STREAM, 0, "workers", "other", false, 10)
        .await
        .expect("another member polls the new leader");
    for record in &others {
        assert!(
            held.iter().all(|h| h.offset != record.offset),
            "{elected} handed a claimed record to another member: {record:?}"
        );
    }
    let back = cluster
        .group_poll_as_via(&elected, STREAM, 0, "workers", "w", true, 10)
        .await
        .expect("the holder reclaims on the new leader");
    for h in &held {
        let again = back
            .iter()
            .find(|record| record.offset == h.offset)
            .unwrap_or_else(|| panic!("{} was not reclaimed: {back:?}", h.offset));
        assert_eq!(again.attempts, h.attempts, "reclaiming spent an attempt");
    }
}

/// **An idempotent producer keeps its sequence when its Raft leader dies.**
/// The producer's id and sequences are in the entries, so the member
/// elected next answers a re-send of the last batch from its own log
/// without writing it, and takes the producer's next batch as the next in
/// sequence. Kept in the dead leader's memory, the re-send would be written
/// again and the producer told it is unknown.
#[tokio::test]
#[serial]
async fn a_producer_keeps_its_sequence_when_its_raft_leader_dies() {
    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let addrs: Vec<std::net::SocketAddr> = survivors
        .iter()
        .map(|node| cluster.node(node).expect("node").client_addr)
        .collect();
    let client =
        felix_cluster::client::connect_cluster(&addrs, &cluster.tenant_id, &cluster.client_token())
            .await
            .expect("connect");
    let producer = std::sync::Arc::new(client)
        .idempotent_producer()
        .await
        .expect("a producer id");
    let record = |i: u32| format!("record-{i}").into_bytes();
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let publish = |i: u32| {
        let (producer, payload) = (&producer, record(i));
        let (tenant, namespace) = (&tenant, &namespace);
        async move { producer.publish(tenant, namespace, STREAM, payload).await }
    };
    // Acknowledged at the commit, so a majority holds each batch.
    for i in 0..5 {
        publish(i).await.expect("published before the failover");
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let elected = leader_among(&cluster, &survivors).await;

    // What a producer does when the answer to its last batch never came:
    // the same batch under the same sequence, to whoever leads now.
    let node = cluster.node(&elected).expect("node");
    let direct = felix_cluster::client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect to the new leader");
    let publisher = direct.publisher().await.expect("publisher");
    felix_cluster::wait::until(
        ELECTED_WITHIN,
        "the new leader to take the re-send",
        || async {
            publisher
                .publish_idempotent_batch(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    vec![record(4)],
                    producer.producer_id(),
                    4,
                )
                .await
                .is_ok()
        },
    )
    .await
    .expect("the re-send is answered");
    for i in 5..8 {
        publish(i).await.expect("published after the failover");
    }

    // A batch of several records is written once, and its re-send is
    // answered without writing it again.
    for _ in 0..2 {
        publisher
            .publish_idempotent_batch(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                vec![record(8), record(9)],
                producer.producer_id(),
                8,
            )
            .await
            .expect("a batch of several is taken");
    }

    let owed: Vec<Vec<u8>> = (0..10).map(record).collect();
    let mut held = replay_holding(&cluster, &elected, &owed).await;
    // The harness's own readiness probe may be on the stream too.
    held.retain(|payload| payload != b"harness-probe");
    assert_eq!(held, owed, "a batch landed twice, or out of order");
    cluster.shutdown().await;
}

/// The sum of `node`'s samples of the metric whose line starts with
/// `series`, read from its own metrics.
async fn metric(cluster: &Cluster, node: &str, series: &str) -> f64 {
    let addr = cluster.node(node).expect("node").metrics_addr;
    let body = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .expect("scrape metrics")
        .text()
        .await
        .expect("metrics body");
    body.lines()
        .filter(|line| line.starts_with(series))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// **A broker on another engine is refused at start.** One of three Raft
/// brokers comes back on the hybrid engine with an empty disk, as a
/// replaced broker would. The control plane's identity names Raft, so the
/// broker exits before it opens anything and says why, and the Raft pair
/// keeps serving the shard. (Brokers that got past that check, under a
/// control plane with no identity route, refuse each other instead:
/// `shards/engine_match.rs`.)
#[tokio::test]
#[serial]
async fn a_broker_on_another_engine_is_refused_at_start() {
    if !opted_in() {
        return;
    }
    let hybrid = "broker-2".to_string();
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    let nodes = cluster.node_ids();
    publish_acked(&cluster, &leader_among(&cluster, &nodes).await, b"agreed").await;

    cluster
        .set_node_env(&hybrid, "FELIX_SHARD_ENGINE", "hybrid")
        .expect("set engine");
    cluster.terminate_node(&hybrid).expect("stop");
    cluster
        .wait_for_exit(&hybrid, ELECTED_WITHIN)
        .await
        .expect("exit");
    let node = cluster.node(&hybrid).expect("node");
    std::fs::remove_dir_all(node.storage_dir()).expect("empty the disk");
    let log = node.data_dir.join("broker.log");
    cluster
        .restart_node(&hybrid)
        .await
        .expect_err("a hybrid broker started under a Raft control plane");
    let said = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        said.contains("runs shard engine hybrid but the cluster was formed with raft"),
        "the refused broker does not say why: {said}"
    );

    let pair: Vec<String> = nodes.into_iter().filter(|node| *node != hybrid).collect();
    publish_acked(
        &cluster,
        &leader_among(&cluster, &pair).await,
        b"still served",
    )
    .await;
}

/// Publish latency on three brokers, one publisher, one record at a time:
/// a report for the Raft reports workflow, which sets the level and the
/// lease. `RAFT_LATENCY_LEASE_MS=0` turns the lease off, so a `Leader`
/// publish waits for its commit as a `Quorum` one does.
#[tokio::test]
#[serial]
#[ignore = "a measurement; run by .github/workflows/raft-idle.yml"]
async fn report_publish_latency() {
    if !opted_in() {
        return;
    }
    let var = |name: &str| std::env::var(name).ok();
    let level = var("RAFT_LATENCY_LEVEL").unwrap_or_else(|| "Leader".into());
    let lease = var("RAFT_LATENCY_LEASE_MS");
    let publishes = var("RAFT_LATENCY_PUBLISHES").map_or(2000, |n| n.parse().expect("a count"));
    let stream = match level.as_str() {
        "Leader" => StreamSpec::replicated(STREAM, 1, 3),
        "Quorum" => StreamSpec::quorum(STREAM, 1, 3),
        other => panic!("RAFT_LATENCY_LEVEL is Leader or Quorum, not {other}"),
    };
    let mut config = raft_cluster_with(stream);
    if let Some(lease) = &lease {
        let lease = ("FELIX_SHARD_RAFT_LEASE_MS".to_string(), lease.clone());
        config.broker_env.push(lease);
    }
    let (tenant, namespace) = (config.tenant_id.clone(), config.namespace.clone());
    let cluster = Cluster::start(config).await.expect("start cluster");
    let leader = leader_among(&cluster, &cluster.node_ids()).await;
    publish_acked(&cluster, &leader, b"first").await;

    let client = cluster.client_on(&leader).await.expect("client");
    let publisher = client.publisher().await.expect("publisher");
    let mut micros = Vec::with_capacity(publishes);
    // The first tenth warms the connection and the group and is not kept.
    for i in 0..publishes + publishes / 10 {
        let payload = vec![0u8; 256];
        let sent = std::time::Instant::now();
        publisher
            .publish(&tenant, &namespace, STREAM, payload, AckMode::PerMessage)
            .await
            .expect("acknowledged");
        if i >= publishes / 10 {
            micros.push(sent.elapsed().as_micros());
        }
    }
    micros.sort_unstable();
    let at = |percent: usize| micros[(micros.len() * percent / 100).min(micros.len() - 1)];
    let lease = lease.as_deref().unwrap_or("default");
    println!(
        "raft publish latency: level={level} lease_ms={lease} publishes={publishes} p50_us={} p99_us={}",
        at(50),
        at(99)
    );
}

/// **An atomic commit on a Raft shard is applied whole on every member.**
/// Committed through the leader, it is read back by `state_get` at its own
/// offset, a `commit_if` off its offset writes nothing of it, and every
/// broker replays the event, not the commit record, from its own log.
#[tokio::test]
#[serial]
async fn an_atomic_commit_on_a_raft_shard_reaches_every_member() {
    use felix_client::{CommitOp, ConditionalWrite};

    if !opted_in() {
        return;
    }
    let cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["atomic_commit", "raft_atomic_commit", "raft_follower_read"],
            Duration::from_secs(30),
        )
        .await
        .expect("finalize the commit features");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let addr = cluster.node(&leader).expect("node").client_addr;
    let client = felix_cluster::client::connect_with(
        addr,
        &cluster.tenant_id,
        &cluster.client_token(),
        |_| {},
    )
    .await
    .expect("connect");
    let ops = |value: &'static str| {
        vec![
            CommitOp::publish(STREAM, "placed"),
            CommitOp::put(STREAM, "status", value),
        ]
    };
    let (tenant, namespace) = (&cluster.tenant_id, &cluster.namespace);
    let last_error = std::sync::Mutex::new(None);
    let receipt = felix_cluster::wait::until_some(ELECTED_WITHIN, "the commit", || async {
        match client
            .commit(tenant, namespace, b"order-42", ops("placed"))
            .await
        {
            Ok(receipt) => Some(receipt),
            Err(err) => {
                *last_error.lock().unwrap() = Some(err.to_string());
                None
            }
        }
    })
    .await
    .unwrap_or_else(|err| panic!("{err}; last refusal: {:?}", last_error.lock().unwrap()));
    let state = client
        .state_get(tenant, namespace, STREAM, b"order-42", "status")
        .await
        .expect("state read");
    assert_eq!(state.value.as_deref(), Some(&b"placed"[..]));
    assert_eq!(state.version, Some(receipt.offset));
    let stale = client
        .commit_if(tenant, namespace, b"order-42", ops("stale"), receipt.offset)
        .await
        .expect("answered");
    assert!(
        matches!(stale, ConditionalWrite::Refused { .. }),
        "{stale:?}"
    );
    let state = client
        .state_get(tenant, namespace, STREAM, b"order-42", "status")
        .await
        .expect("state read");
    assert_eq!(
        state.version,
        Some(receipt.offset),
        "a refused commit moved state"
    );

    for node in &nodes {
        let addr = cluster.node(node).expect("node").client_addr;
        let reader = felix_cluster::client::connect_with(
            addr,
            &cluster.tenant_id,
            &cluster.client_token(),
            |_| {},
        )
        .await
        .expect("connect");
        let mut subscription = reader
            .subscribe_from(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                Some(felix_client::StartPosition::Earliest),
            )
            .await
            .expect("replay");
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), subscription.next_event())
                .await
                .unwrap_or_else(|_| panic!("{node} never replayed the commit"))
                .expect("delivery")
                .expect("the subscription stays open");
            if event.offset == Some(receipt.offset) {
                assert_eq!(event.payload.as_ref(), b"placed", "{node}");
                break;
            }
        }
    }
}

/// Every broker's sample of `name`, or `None` while one has none.
async fn samples(cluster: &Cluster, nodes: &[String], name: &str) -> Option<Vec<f64>> {
    let mut samples = Vec::new();
    for node in nodes {
        samples.push(cluster.metric(node, name).await.ok().flatten()?);
    }
    Some(samples)
}

/// Whether every shard is led by the broker placement names for it.
async fn led_where_placed(cluster: &Cluster, nodes: &[String], shards: u32) -> bool {
    let led = samples(cluster, nodes, "felix_broker_raft_shards_led").await;
    let off = samples(cluster, nodes, "felix_broker_raft_shards_led_off_placement").await;
    let (Some(led), Some(off)) = (led, off) else {
        return false;
    };
    led.iter().sum::<f64>() == f64::from(shards) && off.iter().all(|off| *off == 0.0)
}

/// **Raft shard leaders follow placement.** Each shard's leader is the
/// broker placement names, so leaders spread as placement spreads them.
/// When a broker dies the survivors lead its shards, and once it is back
/// they hand them to it again.
#[tokio::test]
#[serial]
async fn raft_shard_leaders_return_to_placement_after_a_restart() {
    if !opted_in() {
        return;
    }
    const SHARDS: u32 = 6;
    let returned_within = Duration::from_secs(120);
    let mut cluster = Cluster::start(raft_cluster_with(StreamSpec::quorum(STREAM, SHARDS, 3)))
        .await
        .expect("start cluster");
    let nodes = cluster.node_ids();
    felix_cluster::wait::until(returned_within, "leaders where placed", || {
        led_where_placed(&cluster, &nodes, SHARDS)
    })
    .await
    .expect("leaders follow placement once formed");
    let led = samples(&cluster, &nodes, "felix_broker_raft_shards_led")
        .await
        .expect("led");
    assert!(led.iter().all(|led| *led < f64::from(SHARDS)), "{led:?}");

    let restarted = "broker-2".to_string();
    let survivors: Vec<String> = nodes.iter().filter(|n| **n != restarted).cloned().collect();
    cluster.kill_node(&restarted).expect("kill");
    felix_cluster::wait::until(ELECTED_WITHIN * 2, "survivors lead every shard", || async {
        samples(&cluster, &survivors, "felix_broker_raft_shards_led")
            .await
            .is_some_and(|led| led.iter().sum::<f64>() == f64::from(SHARDS))
    })
    .await
    .expect("survivors take over");

    cluster.restart_node(&restarted).await.expect("restart");
    felix_cluster::wait::until(returned_within, "leaders back where placed", || {
        led_where_placed(&cluster, &nodes, SHARDS)
    })
    .await
    .expect("leadership returns to the restarted broker");
    cluster.shutdown().await;
}

const CACHE: &str = "sessions";

/// Put `key` through `node` until a broker acknowledges it.
async fn cache_put_acked(cluster: &Cluster, node: &str, key: &str, value: &[u8]) {
    felix_cluster::wait::until_some(ELECTED_WITHIN, "a cache put to be acknowledged", || async {
        cluster.cache_put_via(node, CACHE, key, value).await.ok()
    })
    .await
    .unwrap_or_else(|err| panic!("cache put through {node}: {err:#}"));
}

/// Add one to `key` through `node` until acknowledged, returning the sum and
/// how many adds failed first: each of those may have counted anyway.
async fn counter_add_acked(cluster: &Cluster, node: &str, key: &str) -> (i64, i64) {
    let mut failed = 0;
    let deadline = tokio::time::Instant::now() + ELECTED_WITHIN;
    loop {
        match cluster.counter_add_via(node, CACHE, key, 1).await {
            Ok(sum) => return (sum, failed),
            Err(err) if tokio::time::Instant::now() > deadline => {
                panic!("counter add through {node}: {err:#}")
            }
            Err(_) => {
                failed += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// **A `Quorum` cache on Raft keeps every acknowledged value and counter add
/// when its leader dies.** Writes go through a broker that is not the
/// leader, so the forward is covered, and the survivors elect a leader that
/// serves them and takes new writes.
#[tokio::test]
#[serial]
async fn a_raft_cache_keeps_what_it_acknowledged_when_its_leader_dies() {
    if !opted_in() {
        return;
    }
    // No stream: the default one is a group of one on some broker, which
    // would count as a second leader in `leader_among`.
    let config = ClusterConfig {
        nodes: 3,
        streams: Vec::new(),
        caches: vec![felix_cluster::CacheSpec::quorum(CACHE, 1, 3)],
        broker_env: vec![engine("raft")],
        ..Default::default()
    };
    let mut cluster = Cluster::start(config).await.expect("start cluster");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let other = nodes
        .iter()
        .find(|node| **node != leader)
        .expect("a follower")
        .clone();
    let mut sum = 0;
    let mut unknown = 0;
    for i in 0..10 {
        let key = format!("k{i}");
        cache_put_acked(&cluster, &other, &key, key.as_bytes()).await;
        let (acked, failed) = counter_add_acked(&cluster, &other, "hits").await;
        (sum, unknown) = (acked, unknown + failed);
    }

    cluster.kill_node(&leader).expect("kill the leader");
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let elected = leader_among(&cluster, &survivors).await;
    let reader = survivors
        .iter()
        .find(|node| **node != elected)
        .expect("the other survivor")
        .clone();

    for i in 0..10 {
        let key = format!("k{i}");
        let found = felix_cluster::wait::until_some(ELECTED_WITHIN, "a cache read", || async {
            cluster.cache_get_via(&reader, CACHE, &key).await.ok()
        })
        .await
        .expect("read");
        assert_eq!(found.as_deref(), Some(key.as_bytes()), "{key} was lost");
    }
    let counted = felix_cluster::wait::until_some(ELECTED_WITHIN, "a counter read", || async {
        cluster.counter_get_via(&reader, CACHE, "hits").await.ok()
    })
    .await
    .expect("read")
    .expect("the counter exists");
    assert!(
        (sum..=sum + unknown).contains(&counted),
        "the counter reads {counted}, acknowledged {sum} with {unknown} unknown adds"
    );

    cache_put_acked(&cluster, &reader, "after", b"after").await;
    let (after, _) = counter_add_acked(&cluster, &reader, "hits").await;
    assert!(
        after > counted,
        "an add after the failover answered {after}"
    );
}

/// Bytes of segment files under `node`'s storage: what its logs hold on disk.
fn segment_bytes(cluster: &Cluster, node: &str) -> u64 {
    fn walk(dir: &std::path::Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                let path = entry.path();
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => walk(&path),
                    Ok(_) if path.extension().is_some_and(|ext| ext == "log") => {
                        entry.metadata().map_or(0, |meta| meta.len())
                    }
                    _ => 0,
                }
            })
            .sum()
    }
    walk(&cluster.node(node).expect("node").storage_dir())
}

/// **Retention frees disk on every member of a Raft shard.** The brokers
/// keep 16 KiB of 4 KiB segments. Once the fleet finalizes
/// `raft_stream_purge`, the leader's head trims hide what that drops and
/// every member's purge deletes the segments below the floor, so each
/// broker's segment files shrink to about the bound while the stream keeps
/// taking publishes.
#[tokio::test]
#[serial]
async fn retention_frees_the_disk_of_every_raft_member() {
    if !opted_in() {
        return;
    }
    const BOUND: u64 = 16 * 1024;
    let mut config = raft_cluster();
    config.broker_env.extend(
        [
            ("FELIX_DURABLE_SEGMENT_BYTES", "4096"),
            ("FELIX_DURABLE_RETENTION_BYTES", "16384"),
            ("FELIX_DURABLE_RETENTION_INTERVAL_SECONDS", "1"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    let cluster = Cluster::start(config).await.expect("start cluster");
    cluster
        .finalize_fleet_features(&["raft_stream_purge"], Duration::from_secs(30))
        .await
        .expect("finalize raft_stream_purge");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let payload = vec![7u8; 512];
    for _ in 0..512 {
        publish_acked(&cluster, &leader, &payload).await;
    }
    // 256 KiB was written; the bound, the segment the purge point falls in
    // and the open one are kept, plus a margin for the other logs.
    let kept = BOUND + 4 * 4096 + 16 * 1024;
    for node in &nodes {
        felix_cluster::wait::until(Duration::from_secs(60), "the member's disk freed", || {
            let held = segment_bytes(&cluster, node);
            async move { held <= kept }
        })
        .await
        .unwrap_or_else(|err| {
            panic!(
                "{node} holds {} bytes of segments: {err:#}",
                segment_bytes(&cluster, node)
            )
        });
    }
    // Still taking publishes after the purge.
    publish_acked(&cluster, &leader, b"after").await;
    cluster.shutdown().await;
}

/// **A conditional publish of several records on a Raft shard is written
/// whole at its expected offset and refused whole off it, and a leader that
/// dies as a batch lands leaves all of it or none.** Each record is a piece
/// kept only at the expected offset plus its place, and readers on every
/// member skip a batch the next leader cut short.
#[tokio::test]
#[serial]
async fn a_conditional_batch_on_a_raft_shard_is_read_whole_or_not_at_all() {
    use felix_client::ConditionalWrite;

    if !opted_in() {
        return;
    }
    let mut cluster = Cluster::start(raft_cluster()).await.expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["raft_conditional_batches", "raft_follower_read"],
            Duration::from_secs(30),
        )
        .await
        .expect("finalize raft_conditional_batches");
    let nodes = cluster.node_ids();
    let leader = leader_among(&cluster, &nodes).await;
    let survivors: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let direct = std::sync::Arc::new(
        felix_cluster::client::connect(
            cluster.node(&leader).expect("node").client_addr,
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect to the leader"),
    );
    let (tenant, namespace) = (cluster.tenant_id.clone(), cluster.namespace.clone());
    let batch = |prefix: &str, n: usize| -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| format!("{prefix}{i}").into_bytes())
            .collect()
    };
    let publish_if = |payloads: Vec<Vec<u8>>, expected: u64| {
        let (direct, tenant, namespace) = (direct.clone(), tenant.clone(), namespace.clone());
        async move {
            direct
                .publish_if(&tenant, &namespace, STREAM, None, payloads, expected)
                .await
        }
    };

    let tail = felix_cluster::wait::until_some(ELECTED_WITHIN, "the shard's tail", || async {
        match publish_if(batch("far", 2), 1 << 40).await {
            Ok(ConditionalWrite::Refused { tail }) => Some(tail),
            _ => None,
        }
    })
    .await
    .expect("a refusal with the tail");
    let written = publish_if(batch("kept", 3), tail).await.expect("answered");
    assert_eq!(written, ConditionalWrite::Written { offset: tail });
    let refused = publish_if(batch("stale", 2), tail).await.expect("answered");
    assert_eq!(refused, ConditionalWrite::Refused { tail: tail + 5 });

    // The leader dies while a batch is in flight: it may commit whole, be
    // cut short by the next leader, or never be appended.
    let in_flight = tokio::spawn(publish_if(batch("flight", 4), tail + 5));
    cluster.kill_node(&leader).expect("kill the leader");
    let answered = in_flight.await.expect("joined");
    let elected = leader_among(&cluster, &survivors).await;
    publish_acked(&cluster, &elected, b"after").await;

    let mut owed = batch("kept", 3);
    owed.push(b"after".to_vec());
    for node in &survivors {
        let held = replay_holding(&cluster, node, &owed).await;
        let kept = held.iter().position(|p| p == b"kept0").expect("kept0");
        assert_eq!(&held[kept..kept + 3], &batch("kept", 3)[..], "{node}");
        assert!(
            !held
                .iter()
                .any(|p| p.starts_with(b"stale") || p.starts_with(b"far")),
            "{node} read part of a refused batch: {held:?}"
        );
        let flight: Vec<usize> = held
            .iter()
            .enumerate()
            .filter(|(_, p)| p.starts_with(b"flight"))
            .map(|(at, _)| at)
            .collect();
        match flight.first() {
            None => assert!(
                !matches!(answered, Ok(ConditionalWrite::Written { .. })),
                "{node} lost an acknowledged batch"
            ),
            Some(&first) => {
                assert_eq!(flight.len(), 4, "{node} read part of a batch: {held:?}");
                assert_eq!(
                    held.get(first..first + 4),
                    Some(&batch("flight", 4)[..]),
                    "{node}"
                );
            }
        }
    }
    cluster.shutdown().await;
}

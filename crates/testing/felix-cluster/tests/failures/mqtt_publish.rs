//! MQTT QoS 1 publishes through a broker that does not own the shard, across
//! a leader failover.
//!
//! Run with `cargo test -p felix-cluster --test failures mqtt_publish::`. The
//! harness runs a prebuilt `target/<profile>/felix-broker`; build it first with
//! `cargo build -p felix-broker-service --bin felix-broker`.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Packet, PubAckReason};
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use serial_test::serial;

const STREAM: &str = "plant";

/// **Every acknowledged QoS 1 message survives the leader, and a message
/// acknowledged on its first try is stored once.** Messages are published
/// through a broker that does not own the shard, which forwards each to the
/// owner. The owner is killed partway. A message whose `PUBACK` does not come
/// back as success is published again, as an application would; only those
/// may be stored twice. Inside one write the forwarding broker sends a batch
/// again under the same producer sequence until an owner answers, so a lost
/// forward answer is not a second copy.
#[serial]
#[tokio::test]
async fn qos1_through_a_non_owner_survives_a_failover_without_duplicates() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3).mounted_at("plant/")],
        // Ports the harness picks per spawn, so a lost race restarts the
        // broker on another one.
        tcp_listeners: vec![(
            "FELIX_MQTT_LISTEN".to_string(),
            "127.0.0.1:{port}".to_string(),
        )],
        broker_env: vec![("FELIX_MQTT_TLS".to_string(), "false".to_string())],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(&["record_headers"], Duration::from_secs(30))
        .await
        .expect("finalize record_headers");
    let port_of: HashMap<String, u16> = cluster
        .node_ids()
        .into_iter()
        .map(|id| {
            let port = cluster
                .node(&id)
                .and_then(|node| node.tcp_port("FELIX_MQTT_LISTEN"))
                .expect("an MQTT port");
            (id, port)
        })
        .collect();

    let (owner, ingress) = cluster.owner_and_non_owner(STREAM).await.expect("owner");
    let mut retried = Vec::new();
    for i in 0..5 {
        if publish_until_acked(&cluster, port_of[&ingress], i).await > 1 {
            retried.push(i);
        }
    }

    // Published without a pause from before the kill until after a new owner
    // serves, so forwards are in flight when the owner dies. Killed with no
    // warning, so a forward to it can lose its answer.
    let acked = Arc::new(AtomicU32::new(5));
    let stop = Arc::new(AtomicBool::new(false));
    let publishing = {
        let tenant = cluster.tenant_id.clone();
        let token = cluster.client_token();
        let port = port_of[&ingress];
        let (acked, stop) = (Arc::clone(&acked), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut retried = Vec::new();
            let mut i = 5;
            while !stop.load(Ordering::Acquire) {
                if publish_with(&tenant, &token, port, i).await > 1 {
                    retried.push(i);
                }
                i += 1;
                acked.store(i, Ordering::Release);
            }
            retried
        })
    };
    felix_cluster::wait::until(Duration::from_secs(30), "publishing under way", || {
        let acked = Arc::clone(&acked);
        async move { acked.load(Ordering::Acquire) >= 8 }
    })
    .await
    .expect("publishing");
    cluster.stop_node(&owner).await.expect("kill the owner");
    let new_owner = new_owner(&cluster, &owner).await;
    let served = acked.load(Ordering::Acquire);
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "publishing after the failover",
        || {
            let acked = Arc::clone(&acked);
            async move { acked.load(Ordering::Acquire) >= served + 3 }
        },
    )
    .await
    .expect("publishing after the failover");
    stop.store(true, Ordering::Release);
    retried.extend(publishing.await.expect("publisher"));

    // Through whichever live broker does not own the shard now. Its routes
    // may still name the dead owner for a moment, so a try may be refused.
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|node| node != &owner && node != &new_owner)
        .expect("a live non-owner");
    let last = acked.load(Ordering::Acquire);
    for i in last..last + 3 {
        if publish_until_acked(&cluster, port_of[&other], i).await > 1 {
            retried.push(i);
        }
    }

    let stored = read_back(&cluster, &new_owner).await;
    for i in 0..last + 3 {
        let copies = stored.iter().filter(|p| **p == format!("m{i}")).count();
        assert!(copies >= 1, "acknowledged m{i} was lost: {stored:?}");
        if !retried.contains(&i) {
            assert_eq!(
                copies, 1,
                "m{i} was acknowledged once and stored {copies} times"
            );
        }
    }
    cluster.shutdown().await;
}

/// **On a Raft shard, a QoS 1 message is never refused for the election a
/// leader's death starts, and one acknowledged is stored once.** Messages go
/// out one try each, through a follower (which forwards to the leader) and
/// through the leader, and keep going through a follower while the leader
/// is killed. A message caught by the kill may get no answer, but none is
/// refused: the broker waits for the next leader as the default engine does.
/// None is sent twice, so none may be stored twice.
#[serial]
#[tokio::test]
async fn qos1_on_a_raft_shard_survives_its_leader_without_duplicates() {
    if !super::raft::opted_in() {
        return;
    }
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3).mounted_at("plant/")],
        tcp_listeners: vec![(
            "FELIX_MQTT_LISTEN".to_string(),
            "127.0.0.1:{port}".to_string(),
        )],
        broker_env: vec![
            ("FELIX_MQTT_TLS".to_string(), "false".to_string()),
            super::raft::engine(),
        ],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            &["record_headers", "raft_record_headers"],
            Duration::from_secs(30),
        )
        .await
        .expect("finalize the headers features");
    let port_of: HashMap<String, u16> = cluster
        .node_ids()
        .into_iter()
        .map(|id| {
            let port = cluster
                .node(&id)
                .and_then(|node| node.tcp_port("FELIX_MQTT_LISTEN"))
                .expect("an MQTT port");
            (id, port)
        })
        .collect();
    let nodes = cluster.node_ids();
    let leader = super::raft::leader_among(&cluster, &nodes).await;
    let survivors: Vec<String> = nodes
        .iter()
        .filter(|node| **node != leader)
        .cloned()
        .collect();
    let follower = survivors[0].clone();
    let (tenant, token) = (cluster.tenant_id.clone(), cluster.client_token());

    for (i, node) in [(0, &follower), (1, &follower), (2, &leader), (3, &leader)] {
        let answer = publish_once(&tenant, &token, port_of[node], i).await;
        assert_eq!(answer, Answer::Acked, "m{i} through {node}");
    }

    // One try each, without a pause, from before the kill until well after,
    // through both survivors in turn: the one elected is placed as owner
    // before it serves its term, the other forwards to it.
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU32::new(4));
    let publishing = {
        let (tenant, token) = (tenant.clone(), token.clone());
        let ports: Vec<u16> = survivors.iter().map(|node| port_of[node]).collect();
        let (stop, sent) = (Arc::clone(&stop), Arc::clone(&sent));
        tokio::spawn(async move {
            let mut answers = Vec::new();
            let mut i = 4;
            while !stop.load(Ordering::Acquire) {
                let port = ports[i as usize % ports.len()];
                answers.push((i, publish_once(&tenant, &token, port, i).await));
                i += 1;
                sent.store(i, Ordering::Release);
            }
            answers
        })
    };
    felix_cluster::wait::until(Duration::from_secs(30), "publishing under way", || {
        let sent = Arc::clone(&sent);
        async move { sent.load(Ordering::Acquire) >= 7 }
    })
    .await
    .expect("publishing");
    cluster.stop_node(&leader).await.expect("kill the leader");
    let at_kill = sent.load(Ordering::Acquire);
    felix_cluster::wait::until(Duration::from_secs(60), "publishing after the kill", || {
        let sent = Arc::clone(&sent);
        async move { sent.load(Ordering::Acquire) >= at_kill + 5 }
    })
    .await
    .expect("publishing after the kill");
    stop.store(true, Ordering::Release);
    let answers = publishing.await.expect("publisher");

    let refused: Vec<_> = answers
        .iter()
        .filter(|(_, answer)| matches!(answer, Answer::Refused(_)))
        .collect();
    assert!(
        refused.is_empty(),
        "refused during the election: {refused:?}"
    );
    assert!(
        answers
            .iter()
            .rev()
            .take(3)
            .all(|(_, answer)| *answer == Answer::Acked),
        "not served after the election: {answers:?}"
    );

    let live: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let next = super::raft::leader_among(&cluster, &live).await;
    let stored = read_back(&cluster, &next).await;
    let acked = (0..4).chain(
        answers
            .iter()
            .filter(|(_, answer)| *answer == Answer::Acked)
            .map(|(i, _)| *i),
    );
    for i in acked {
        let copies = stored.iter().filter(|p| **p == format!("m{i}")).count();
        assert_eq!(copies, 1, "acknowledged m{i} is stored {copies} times");
    }
    for (i, _) in &answers {
        let copies = stored.iter().filter(|p| **p == format!("m{i}")).count();
        assert!(copies <= 1, "m{i}, sent once, is stored {copies} times");
    }
    cluster.shutdown().await;
}

/// How a single try at a QoS 1 publish ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    Acked,
    /// A `PUBACK` that is not success: nothing was written.
    Refused(String),
    /// No `PUBACK`: the connection closed or the answer did not come.
    Unanswered,
}

/// Publish `m{i}` at QoS 1 once, on a fresh connection.
async fn publish_once(tenant: &str, token: &str, port: u16, i: u32) -> Answer {
    let mut options = MqttOptions::new(format!("device-{i}-{port}"), "127.0.0.1", port);
    options.set_credentials(tenant, token);
    let (client, mut events) = AsyncClient::new(options, 10);
    if client
        .publish(
            format!("plant/line/{i}"),
            QoS::AtLeastOnce,
            false,
            format!("m{i}"),
        )
        .await
        .is_err()
    {
        return Answer::Unanswered;
    }
    let answer = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match events.poll().await {
                Ok(Event::Incoming(Packet::PubAck(ack))) if ack.reason == PubAckReason::Success => {
                    return Answer::Acked;
                }
                Ok(Event::Incoming(Packet::PubAck(ack))) => {
                    let reason = ack
                        .properties
                        .and_then(|properties| properties.reason_string)
                        .unwrap_or_default();
                    return Answer::Refused(format!("{:?} {reason}", ack.reason));
                }
                Ok(_) => {}
                Err(_) => return Answer::Unanswered,
            }
        }
    })
    .await
    .unwrap_or(Answer::Unanswered);
    let _ = client.disconnect().await;
    answer
}

async fn publish_until_acked(cluster: &Cluster, port: u16, i: u32) -> u32 {
    publish_with(&cluster.tenant_id, &cluster.client_token(), port, i).await
}

/// Publish `m{i}` at QoS 1 on a fresh connection until a `PUBACK` says
/// success, and return how many tries it took. A refusal or a closed
/// connection is tried again, as an application whose message was not
/// acknowledged would.
async fn publish_with(tenant: &str, token: &str, port: u16, i: u32) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut tries = 0;
    loop {
        tries += 1;
        if try_publish(tenant, token, port, i).await {
            return tries;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "m{i} was never acknowledged"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn try_publish(tenant: &str, token: &str, port: u16, i: u32) -> bool {
    let mut options = MqttOptions::new(format!("device-{i}-{port}"), "127.0.0.1", port);
    options.set_credentials(tenant, token);
    let (client, mut events) = AsyncClient::new(options, 10);
    if client
        .publish(
            format!("plant/line/{i}"),
            QoS::AtLeastOnce,
            false,
            format!("m{i}"),
        )
        .await
        .is_err()
    {
        return false;
    }
    let acked = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match events.poll().await {
                Ok(Event::Incoming(Packet::PubAck(ack))) => {
                    return ack.reason == PubAckReason::Success;
                }
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    })
    .await
    .unwrap_or(false);
    let _ = client.disconnect().await;
    acked
}

/// Step placement until a broker other than `gone` owns the shard.
async fn new_owner(cluster: &Cluster, gone: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        cluster.place_shards().await;
        if let Ok(owner) = cluster.owner(STREAM).await
            && owner != gone
        {
            return owner;
        }
        assert!(tokio::time::Instant::now() < deadline, "no new leader");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The payloads of the test's messages, replayed over QUIC from `owner`.
async fn read_back(cluster: &Cluster, owner: &str) -> Vec<String> {
    let (_client, mut subscription) = cluster
        .replay_shard(owner, STREAM, 0)
        .await
        .expect("replay");
    let mut read = Vec::new();
    while let Ok(Ok(Some(event))) =
        tokio::time::timeout(Duration::from_secs(3), subscription.next_event()).await
    {
        let payload = String::from_utf8_lossy(&event.payload).into_owned();
        if payload.starts_with('m') {
            read.push(payload);
        }
    }
    read
}

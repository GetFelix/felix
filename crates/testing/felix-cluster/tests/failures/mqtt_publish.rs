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
use rumqttc::Outgoing;
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

/// **On a Raft shard, a connection with many QoS 1 messages in flight keeps
/// their order and stores each once across its leader's death.** One
/// connection publishes through the leader, whose MQTT listener proposes a
/// connection's queued messages without waiting for each commit, and one
/// through a follower, which forwards them. Both publish as fast as they
/// are answered while the leader is killed. Neither client sends anything
/// twice: the first error ends its connection. So each acknowledged
/// message is stored exactly once, no message more than once, and each
/// connection's messages in the order it sent them.
#[serial]
#[tokio::test]
async fn pipelined_qos1_on_a_raft_shard_survives_its_leader_in_order() {
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
    let nodes = cluster.node_ids();
    let leader = super::raft::leader_among(&cluster, &nodes).await;
    let follower = nodes
        .iter()
        .find(|node| **node != leader)
        .cloned()
        .expect("a follower");
    let port = |node: &str| {
        cluster
            .node(node)
            .and_then(|node| node.tcp_port("FELIX_MQTT_LISTEN"))
            .expect("an MQTT port")
    };
    let (tenant, token) = (cluster.tenant_id.clone(), cluster.client_token());
    let stop = Arc::new(AtomicBool::new(false));
    let publish = |label: char, port: u16| {
        let (tenant, token, stop) = (tenant.clone(), token.clone(), Arc::clone(&stop));
        let acked = Arc::new(AtomicU32::new(0));
        let progress = Arc::clone(&acked);
        let task = tokio::spawn(async move {
            publish_pipelined(&tenant, &token, port, label, stop, progress).await
        });
        (task, acked)
    };
    let (through_leader, leader_acked) = publish('L', port(&leader));
    let (through_follower, follower_acked) = publish('F', port(&follower));

    felix_cluster::wait::until(Duration::from_secs(30), "publishing under way", || {
        let (l, f) = (Arc::clone(&leader_acked), Arc::clone(&follower_acked));
        async move { l.load(Ordering::Acquire) >= 200 && f.load(Ordering::Acquire) >= 20 }
    })
    .await
    .expect("publishing");
    cluster.stop_node(&leader).await.expect("kill the leader");
    let at_kill = follower_acked.load(Ordering::Acquire);
    // The follower's connection may be closed by a message whose outcome
    // the election left unknown, so this is not required.
    let _ =
        felix_cluster::wait::until(Duration::from_secs(30), "publishing after the kill", || {
            let f = Arc::clone(&follower_acked);
            async move { f.load(Ordering::Acquire) >= at_kill + 20 }
        })
        .await;
    stop.store(true, Ordering::Release);
    let through_leader = through_leader.await.expect("publisher through the leader");
    let through_follower = through_follower
        .await
        .expect("publisher through the follower");
    assert!(
        through_leader.most_in_flight > 1,
        "never more than one message in flight through the leader"
    );

    let live: Vec<String> = nodes.into_iter().filter(|node| *node != leader).collect();
    let next = super::raft::leader_among(&cluster, &live).await;
    let stored = read_back(&cluster, &next).await;
    for (label, published) in [('L', &through_leader), ('F', &through_follower)] {
        assert!(
            published.refused.is_empty(),
            "{label}: refused {:?}",
            published.refused
        );
        for i in &published.acked {
            let copies = stored
                .iter()
                .filter(|p| **p == format!("m{label}{i}"))
                .count();
            assert_eq!(
                copies, 1,
                "acknowledged m{label}{i} is stored {copies} times"
            );
        }
        let order: Vec<u32> = stored
            .iter()
            .filter_map(|p| p.strip_prefix(&format!("m{label}"))?.parse().ok())
            .collect();
        assert!(
            order.windows(2).all(|pair| pair[0] < pair[1]),
            "m{label} stored out of order or twice: {order:?}"
        );
        assert!(
            order.iter().all(|i| *i < published.sent),
            "m{label} stored a message never sent"
        );
    }
    cluster.shutdown().await;
}

/// What one pipelined connection sent and was told.
struct Pipelined {
    /// Messages handed to the socket, `0..sent`.
    sent: u32,
    acked: Vec<u32>,
    refused: Vec<u32>,
    most_in_flight: usize,
}

/// Publish `m{label}0`, `m{label}1`, ... at QoS 1 on one connection with up
/// to 64 in flight, until `stop` and every message is answered, or until
/// the connection ends. Never reconnects, so nothing is sent twice.
async fn publish_pipelined(
    tenant: &str,
    token: &str,
    port: u16,
    label: char,
    stop: Arc<AtomicBool>,
    progress: Arc<AtomicU32>,
) -> Pipelined {
    let mut options = MqttOptions::new(format!("pipelined-{label}-{port}"), "127.0.0.1", port);
    options.set_credentials(tenant, token);
    options.set_outgoing_inflight_upper_limit(64);
    let (client, mut events) = AsyncClient::new(options, 64);
    let fed = Arc::new(AtomicU32::new(0));
    let feeding = {
        let (fed, stop) = (Arc::clone(&fed), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut i = 0u32;
            while !stop.load(Ordering::Acquire) {
                let topic = format!("plant/line/{label}{i}");
                let sent = client.publish(topic, QoS::AtLeastOnce, false, format!("m{label}{i}"));
                if sent.await.is_err() {
                    break;
                }
                i += 1;
                fed.store(i, Ordering::Release);
            }
            client
        })
    };
    let mut published = Pipelined {
        sent: 0,
        acked: Vec::new(),
        refused: Vec::new(),
        most_in_flight: 0,
    };
    let mut in_flight: HashMap<u16, u32> = HashMap::new();
    loop {
        if feeding.is_finished()
            && published.sent == fed.load(Ordering::Acquire)
            && in_flight.is_empty()
        {
            break;
        }
        match tokio::time::timeout(Duration::from_secs(20), events.poll()).await {
            Ok(Ok(Event::Outgoing(Outgoing::Publish(pkid)))) => {
                in_flight.insert(pkid, published.sent);
                published.sent += 1;
                published.most_in_flight = published.most_in_flight.max(in_flight.len());
            }
            Ok(Ok(Event::Incoming(Packet::PubAck(ack)))) => {
                if let Some(i) = in_flight.remove(&ack.pkid) {
                    if ack.reason == PubAckReason::Success {
                        published.acked.push(i);
                        progress.fetch_add(1, Ordering::AcqRel);
                    } else {
                        published.refused.push(i);
                    }
                }
            }
            Ok(Ok(_)) => {}
            // Polling again would reconnect and send the unanswered ones
            // again.
            Ok(Err(_)) | Err(_) => break,
        }
    }
    feeding.abort();
    published
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

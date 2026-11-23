//! An MQTT subscription on a multi-broker `Quorum` stream is sent only what
//! the shard has committed.
//!
//! Run with `cargo test -p felix-cluster --test failures mqtt_subscribe::`. The
//! harness runs a prebuilt `target/<profile>/felix-broker`; build it first with
//! `cargo build -p felix-broker-service --bin felix-broker`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{Packet, PubAckReason};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use serial_test::serial;

const STREAM: &str = "plant";

/// **A subscriber is never sent a record above the commit mark.** The leader's
/// followers are cut off, so a publish is written on the leader and not
/// committed, and the publisher is refused. The subscriber on the leader gets
/// nothing for it while the cut lasts, since a failover could take it back.
/// Once the followers are back and the mark covers it, it arrives.
#[serial]
#[tokio::test]
async fn a_subscriber_is_not_sent_a_record_the_shard_has_not_committed() {
    withheld_until_committed(false).await;
}

/// The same on a Raft shard. The cut leader is deposed once the followers
/// elect among themselves, and an entry only it held can then be truncated,
/// so the record may never arrive. What holds is that it is not sent while
/// the cut lasts and that a new subscriber is served after the heal.
#[serial]
#[tokio::test]
async fn a_subscriber_on_a_raft_shard_is_not_sent_a_record_the_shard_has_not_committed() {
    withheld_until_committed(true).await;
}

async fn withheld_until_committed(raft: bool) {
    if raft && !super::raft::opted_in() {
        return;
    }
    let mut broker_env = vec![
        ("FELIX_MQTT_TLS".to_string(), "false".to_string()),
        // A refused publish comes back quickly; the record stays on the
        // leader.
        (
            "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
            "3000".to_string(),
        ),
    ];
    if raft {
        broker_env.push(super::raft::engine());
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3).mounted_at("plant/")],
        tcp_listeners: vec![(
            "FELIX_MQTT_LISTEN".to_string(),
            "127.0.0.1:{port}".to_string(),
        )],
        broker_env,
        proxy_links: true,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(
            if raft {
                &["record_headers", "raft_record_headers"][..]
            } else {
                &["record_headers"][..]
            },
            Duration::from_secs(30),
        )
        .await
        .expect("finalize the headers features");
    let leader = if raft {
        super::raft::leader_among(&cluster, &cluster.node_ids()).await
    } else {
        cluster.owner(STREAM).await.expect("owner")
    };
    let port = cluster
        .node(&leader)
        .and_then(|node| node.tcp_port("FELIX_MQTT_LISTEN"))
        .expect("an MQTT port");

    let mut options = MqttOptions::new("watcher", "127.0.0.1", port);
    options.set_credentials(cluster.tenant_id.clone(), cluster.client_token());
    let (client, mut events) = AsyncClient::new(options, 10);
    client
        .subscribe("plant/#", QoS::AtLeastOnce)
        .await
        .expect("subscribe");
    wait_for_suback(&mut events).await;

    let (tenant, token) = (cluster.tenant_id.clone(), cluster.client_token());
    publish_qos1(&tenant, &token, port, "plant/line/1", "committed")
        .await
        .expect("publish while whole");
    assert_eq!(
        next_payload(&mut events, Duration::from_secs(20)).await,
        Some("committed".to_string()),
        "a committed record reaches the subscriber"
    );

    let followers: Vec<String> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let refuse = Fault::Refuse {
        node: leader.clone(),
        peers: followers,
    };
    cluster
        .inject(&refuse)
        .await
        .expect("cut the followers off");
    publish_qos1(&tenant, &token, port, "plant/line/2", "uncommitted")
        .await
        .expect_err("a write only the leader holds was acknowledged");

    // Well past the read's wait, so a read that ignored the mark would have
    // delivered it by now.
    assert_eq!(
        next_payload(&mut events, Duration::from_secs(6)).await,
        None,
        "the subscriber was sent a record above the commit mark"
    );

    cluster.heal(&refuse).await.expect("heal the peers");
    if raft {
        let _ = client.disconnect().await;
        let nodes = cluster.node_ids();
        let leader = super::raft::leader_among(&cluster, &nodes).await;
        let port = cluster
            .node(&leader)
            .and_then(|node| node.tcp_port("FELIX_MQTT_LISTEN"))
            .expect("an MQTT port");
        let mut options = MqttOptions::new("watcher-after", "127.0.0.1", port);
        options.set_credentials(cluster.tenant_id.clone(), cluster.client_token());
        let (client, mut events) = AsyncClient::new(options, 10);
        client
            .subscribe("plant/#", QoS::AtLeastOnce)
            .await
            .expect("subscribe");
        wait_for_suback(&mut events).await;
        let mut sent = Err("never tried".to_string());
        for _ in 0..30 {
            sent = publish_qos1(&tenant, &token, port, "plant/line/3", "after").await;
            if sent.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        sent.expect("a publish after the heal");
        // The cut record may arrive first, if the leader kept it.
        let mut seen = Vec::new();
        while let Some(payload) = next_payload(&mut events, Duration::from_secs(30)).await {
            let done = payload == "after";
            seen.push(payload);
            if done {
                break;
            }
        }
        assert!(
            seen.iter().any(|payload| payload == "after"),
            "the new subscriber never got the record published after the heal: {seen:?}"
        );
        let _ = client.disconnect().await;
        cluster.shutdown().await;
        return;
    }
    assert_eq!(
        next_payload(&mut events, Duration::from_secs(60)).await,
        Some("uncommitted".to_string()),
        "the record arrives once the shard commits it"
    );
    let _ = client.disconnect().await;
    cluster.shutdown().await;
}

/// Publish one QoS 1 message on a fresh connection. `Err` when the broker
/// refuses it or never answers.
async fn publish_qos1(
    tenant: &str,
    token: &str,
    port: u16,
    topic: &str,
    payload: &str,
) -> Result<(), String> {
    let mut options = MqttOptions::new(format!("publisher-{payload}"), "127.0.0.1", port);
    options.set_credentials(tenant, token);
    let (client, mut events) = AsyncClient::new(options, 10);
    client
        .publish(topic, QoS::AtLeastOnce, false, payload.to_string())
        .await
        .map_err(|err| err.to_string())?;
    let answer = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match events.poll().await {
                Ok(Event::Incoming(Packet::PubAck(ack))) => {
                    return match ack.reason {
                        PubAckReason::Success => Ok(()),
                        other => Err(format!("{other:?}")),
                    };
                }
                Ok(_) => {}
                Err(err) => return Err(err.to_string()),
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err("no answer".to_string()));
    let _ = client.disconnect().await;
    answer
}

async fn wait_for_suback(events: &mut EventLoop) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match events.poll().await {
                Ok(Event::Incoming(Packet::SubAck(_))) => return,
                Ok(_) => {}
                Err(err) => panic!("connection failed before the SUBACK: {err}"),
            }
        }
    })
    .await
    .expect("a SUBACK");
}

/// The payload of the next message delivered within `wait`, or `None`.
async fn next_payload(events: &mut EventLoop, wait: Duration) -> Option<String> {
    tokio::time::timeout(wait, async {
        loop {
            match events.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    return Some(String::from_utf8_lossy(&publish.payload).into_owned());
                }
                Ok(_) => {}
                Err(_) => return None,
            }
        }
    })
    .await
    .unwrap_or(None)
}

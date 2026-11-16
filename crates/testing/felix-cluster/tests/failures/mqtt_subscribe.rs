//! An MQTT subscription on a multi-broker `Quorum` stream is sent only what
//! the shard has committed.
//!
//! Run with `cargo test -p felix-cluster --test failures mqtt_subscribe::`. The
//! harness runs a prebuilt `target/<profile>/felix-broker`; build it first with
//! `cargo build -p felix-broker-service --bin felix-broker`.
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, Fault, StreamSpec};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
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
    let cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3).mounted_at("plant/")],
        tcp_listeners: vec![(
            "FELIX_MQTT_LISTEN".to_string(),
            "127.0.0.1:{port}".to_string(),
        )],
        broker_env: vec![
            ("FELIX_MQTT_TLS".to_string(), "false".to_string()),
            // A refused publish comes back quickly; the record stays on the
            // leader.
            (
                "FELIX_PUBLISH_QUORUM_TIMEOUT_MS".to_string(),
                "3000".to_string(),
            ),
        ],
        proxy_links: true,
        inherit_output: std::env::var("FELIX_TEST_BROKER_OUTPUT").is_ok(),
        ..Default::default()
    })
    .await
    .expect("start cluster");
    cluster
        .finalize_fleet_features(&["record_headers"], Duration::from_secs(30))
        .await
        .expect("finalize record_headers");
    let leader = cluster.owner(STREAM).await.expect("owner");
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

    cluster
        .publish_via(&leader, STREAM, b"committed".to_vec())
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
    cluster
        .publish_via(&leader, STREAM, b"uncommitted".to_vec())
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
    assert_eq!(
        next_payload(&mut events, Duration::from_secs(60)).await,
        Some("uncommitted".to_string()),
        "the record arrives once the shard commits it"
    );
    let _ = client.disconnect().await;
    cluster.shutdown().await;
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

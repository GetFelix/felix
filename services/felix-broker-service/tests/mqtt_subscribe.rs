//! MQTT subscribing against real MQTT clients: `rumqttc` (3.1.1 and 5.0) and,
//! when it is installed, `mosquitto_sub`.
//!
//! One broker with a durable stream mounted at `plant/`. Each test subscribes
//! through the listener, publishes through it from a second client, and
//! checks what the subscriber is sent. Without `mosquitto_sub` on the path its
//! test says so and returns; CI installs it.
//!
//! Run with `cargo test -p felix-broker-service --test mqtt_subscribe`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_broker_service::config::MqttListenerConfig;
use felix_broker_service::serving::auth::demo::{DemoAuth, demo_auth_for_tenant};
use felix_broker_service::serving::mqtt::{
    BrokerMounts, BrokerMqttAuth, BrokerReads, BrokerWrites, MqttListener,
};
use felix_mqtt::Settings;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const TENANT: &str = "t1";
const WAIT: Duration = Duration::from_secs(10);

struct Running {
    demo: DemoAuth,
    port: u16,
    shutdown: CancellationToken,
    _data: tempfile::TempDir,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn start(headers: bool) -> Result<Running> {
    let data = tempfile::tempdir()?;
    let storage = DurableStorage::open(
        data.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )?;
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    if headers {
        broker.record_headers_when(|| true);
    }
    broker.register_tenant(TENANT).await?;
    broker.register_namespace(TENANT, "ns").await?;
    broker
        .register_stream(
            TENANT,
            "ns",
            "plant",
            StreamMetadata {
                durable: true,
                mount: Some("plant/".to_string()),
                ..StreamMetadata::default()
            },
        )
        .await?;

    let demo = demo_auth_for_tenant(TENANT)?;
    let listener = MqttListener::bind(
        &MqttListenerConfig {
            listen: "127.0.0.1:0".parse()?,
            tls: false,
            worker_threads: 2,
            max_connections: 64,
            max_connections_per_ip: 0,
            connect_rate: 0,
            settings: Settings::default(),
            ws: None,
        },
        None,
        Arc::new(BrokerMqttAuth::new(Arc::clone(&demo.auth))),
        Arc::new(BrokerMounts::new(Arc::clone(&broker), None)),
        Arc::new(BrokerWrites::new(
            Arc::clone(&broker),
            Arc::new(felix_broker_service::shards::routing::StreamEngine::default()),
            Duration::from_secs(5),
        )),
    )?
    .with_reads(Arc::new(BrokerReads::new(Arc::clone(&broker), None)));
    let port = listener.local_addr()?.port();
    let shutdown = CancellationToken::new();
    drop(listener.spawn(shutdown.clone(), TaskTracker::new(), None));
    Ok(Running {
        demo,
        port,
        shutdown,
        _data: data,
    })
}

/// Wait for the broker to say the subscription is in place.
macro_rules! subscribed {
    ($events:expr, $pattern:pat) => {
        tokio::time::timeout(WAIT, async {
            loop {
                if let $pattern = $events.poll().await? {
                    return Ok::<_, rumqttc::ConnectionError>(());
                }
            }
        })
        .await
        .context("no SUBACK")??
    };
}

#[tokio::test]
async fn rumqttc_3_1_1_subscribes_with_a_wildcard_at_qos_1() -> Result<()> {
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

    let felix = start(true).await?;
    let mut options = MqttOptions::new("sub-1", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (subscriber, mut events) = AsyncClient::new(options, 10);
    subscriber
        .subscribe("plant/+/temp", QoS::AtLeastOnce)
        .await?;
    subscribed!(events, Event::Incoming(Packet::SubAck(_)));

    let mut options = MqttOptions::new("pub-1", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (publisher, mut publisher_events) = AsyncClient::new(options, 10);
    tokio::spawn(async move { while publisher_events.poll().await.is_ok() {} });
    publisher
        .publish("plant/7/temp", QoS::AtLeastOnce, false, "21.5")
        .await?;
    publisher
        .publish("plant/7/humidity", QoS::AtLeastOnce, false, "40")
        .await?;
    publisher
        .publish("plant/8/temp", QoS::AtMostOnce, false, "19")
        .await?;

    let mut got = Vec::new();
    tokio::time::timeout(WAIT, async {
        while got.len() < 2 {
            if let Event::Incoming(Packet::Publish(p)) = events.poll().await? {
                got.push((p.topic.clone(), p.payload.clone(), p.qos));
                // A QoS 1 delivery is acknowledged by the client library.
            }
        }
        Ok::<_, rumqttc::ConnectionError>(())
    })
    .await
    .context("deliveries did not arrive")??;
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got[0].0, "plant/7/temp");
    assert_eq!(got[0].1, "21.5");
    assert_eq!(got[0].2, QoS::AtLeastOnce);
    assert_eq!(got[1].0, "plant/8/temp");
    // Published at QoS 0, so delivered at QoS 0 whatever was granted.
    assert_eq!(got[1].2, QoS::AtMostOnce);
    Ok(())
}

#[tokio::test]
async fn rumqttc_5_gets_properties_and_a_refusal_per_filter() -> Result<()> {
    use rumqttc::v5::mqttbytes::QoS;
    use rumqttc::v5::mqttbytes::v5::{Filter, Packet, PublishProperties, SubscribeReasonCode};
    use rumqttc::v5::{AsyncClient, Event, MqttOptions};

    let felix = start(true).await?;
    let mut options = MqttOptions::new("sub-5", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (subscriber, mut events) = AsyncClient::new(options, 10);
    subscriber
        .subscribe_many(vec![
            Filter::new("plant/#", QoS::AtLeastOnce),
            Filter::new("yard/#", QoS::AtLeastOnce),
        ])
        .await?;
    let ack = tokio::time::timeout(WAIT, async {
        loop {
            if let Event::Incoming(Packet::SubAck(ack)) = events.poll().await? {
                return Ok::<_, rumqttc::v5::ConnectionError>(ack);
            }
        }
    })
    .await
    .context("no SUBACK")??;
    assert!(matches!(
        ack.return_codes[0],
        SubscribeReasonCode::Success(QoS::AtLeastOnce)
    ));
    assert!(matches!(
        ack.return_codes[1],
        SubscribeReasonCode::TopicFilterInvalid
    ));

    let mut options = MqttOptions::new("pub-5", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (publisher, mut publisher_events) = AsyncClient::new(options, 10);
    tokio::spawn(async move { while publisher_events.poll().await.is_ok() {} });
    let properties = PublishProperties {
        content_type: Some("text/plain".to_string()),
        user_properties: vec![("site".to_string(), "north".to_string())],
        ..PublishProperties::default()
    };
    publisher
        .publish_with_properties("plant/7/temp", QoS::AtLeastOnce, false, "21.5", properties)
        .await?;
    let delivered = tokio::time::timeout(WAIT, async {
        loop {
            if let Event::Incoming(Packet::Publish(p)) = events.poll().await? {
                return Ok::<_, rumqttc::v5::ConnectionError>(p);
            }
        }
    })
    .await
    .context("no delivery")??;
    assert_eq!(delivered.payload, "21.5");
    let props = delivered.properties.context("properties")?;
    assert_eq!(props.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        props.user_properties,
        vec![("site".to_string(), "north".to_string())]
    );
    Ok(())
}

#[tokio::test]
async fn mosquitto_sub_receives_a_published_message() -> Result<()> {
    use rumqttc::{AsyncClient, MqttOptions, QoS};

    let felix = start(true).await?;
    let child = tokio::process::Command::new("mosquitto_sub")
        .args(["-h", "127.0.0.1", "-p"])
        .arg(felix.port.to_string())
        .args(["-u", TENANT, "-P"])
        .arg(&felix.demo.token)
        .args(["-t", "plant/#", "-q", "1", "-C", "1", "-W", "10", "-v"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn();
    let child = match child {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping: mosquitto_sub is not installed");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let mut options = MqttOptions::new("pub-m", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (publisher, mut events) = AsyncClient::new(options, 10);
    tokio::spawn(async move { while events.poll().await.is_ok() {} });
    // mosquitto_sub has no way to say it is subscribed, so publish until it
    // has taken one and exited.
    let output = tokio::spawn(child.wait_with_output());
    let deadline = tokio::time::Instant::now() + WAIT;
    while !output.is_finished() && tokio::time::Instant::now() < deadline {
        publisher
            .publish("plant/7/line/3/temp", QoS::AtLeastOnce, false, "21.5")
            .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let output = output.await??;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "plant/7/line/3/temp 21.5"
    );
    Ok(())
}

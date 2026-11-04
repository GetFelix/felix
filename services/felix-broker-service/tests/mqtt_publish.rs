//! MQTT publishing against real MQTT clients: `rumqttc` (3.1.1 and 5.0) and,
//! when it is installed, `mosquitto_pub`.
//!
//! One broker with a durable stream mounted at `plant/`. Each test publishes
//! through the listener and reads the records back from the stream, checking
//! the key (the topic), the headers and the payload. Without `mosquitto_pub`
//! on the path its test says so and returns; CI installs it.
//!
//! Run with `cargo test -p felix-broker-service --test mqtt_publish`.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_broker_service::config::MqttListenerConfig;
use felix_broker_service::serving::auth::demo::{DemoAuth, demo_auth_for_tenant};
use felix_broker_service::serving::mqtt::{
    BrokerMounts, BrokerMqttAuth, BrokerWrites, MqttListener,
};
use felix_mqtt::Settings;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::RecordHeaders;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const TENANT: &str = "t1";
const WAIT: Duration = Duration::from_secs(10);

struct Running {
    broker: Arc<Broker>,
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
            None,
            None,
            Duration::from_secs(5),
        )),
    )?;
    let port = listener.local_addr()?.port();
    let shutdown = CancellationToken::new();
    drop(listener.spawn(shutdown.clone(), TaskTracker::new(), None));
    Ok(Running {
        broker,
        demo,
        port,
        shutdown,
        _data: data,
    })
}

struct Stored {
    payload: Bytes,
    key: Bytes,
    headers: Vec<(String, Bytes)>,
}

impl Stored {
    fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_ref())
    }
}

/// The stream's records, waiting up to `WAIT` for `count` of them.
async fn stored(broker: &Broker, count: usize) -> Result<Vec<Stored>> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let records = broker
            .read_durable(TENANT, "ns", "plant", 0, 0, 1 << 20)
            .await?;
        if records.len() >= count {
            return records
                .into_iter()
                .map(|record| {
                    let block = record.headers.context("a record without headers")?;
                    let block = RecordHeaders::decode((*block).clone())?;
                    Ok(Stored {
                        payload: record.payload,
                        key: block.key.context("a record without a key")?,
                        headers: block.headers,
                    })
                })
                .collect();
        }
        if tokio::time::Instant::now() > deadline {
            bail!("{} of {count} records arrived", records.len());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn rumqttc_3_1_1_publishes_at_qos_0_and_1() -> Result<()> {
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

    let felix = start(true).await?;
    let mut options = MqttOptions::new("dev-1", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (client, mut events) = AsyncClient::new(options, 10);

    client
        .publish("plant/7/temp", QoS::AtLeastOnce, false, "21.5")
        .await?;
    client
        .publish("plant/7/temp", QoS::AtMostOnce, false, "21.6")
        .await?;
    // The PUBACK for the QoS 1 message arrives only once it is written.
    let mut acked = false;
    tokio::time::timeout(WAIT, async {
        while !acked {
            if let Event::Incoming(Packet::PubAck(_)) = events.poll().await? {
                acked = true;
            }
        }
        Ok::<_, rumqttc::ConnectionError>(())
    })
    .await
    .context("no PUBACK")??;

    let records = stored(&felix.broker, 2).await?;
    let qos1 = records
        .iter()
        .find(|r| r.payload == "21.5")
        .context("the QoS 1 message")?;
    assert_eq!(qos1.key, Bytes::from_static(b"plant/7/temp"));
    assert_eq!(qos1.header("felix.mqtt.qos"), Some(&b"1"[..]));
    let qos0 = records
        .iter()
        .find(|r| r.payload == "21.6")
        .context("the QoS 0 message")?;
    assert_eq!(qos0.key, Bytes::from_static(b"plant/7/temp"));
    assert_eq!(qos0.header("felix.mqtt.qos"), Some(&b"0"[..]));
    Ok(())
}

#[tokio::test]
async fn rumqttc_5_carries_properties_as_headers() -> Result<()> {
    use rumqttc::v5::mqttbytes::QoS;
    use rumqttc::v5::mqttbytes::v5::{Packet, PublishProperties};
    use rumqttc::v5::{AsyncClient, Event, MqttOptions};

    let felix = start(true).await?;
    let mut options = MqttOptions::new("dev-5", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (client, mut events) = AsyncClient::new(options, 10);

    let properties = PublishProperties {
        content_type: Some("text/plain".to_string()),
        user_properties: vec![("site".to_string(), "north".to_string())],
        ..PublishProperties::default()
    };
    client
        .publish_with_properties("plant/7/temp", QoS::AtLeastOnce, false, "21.5", properties)
        .await?;
    tokio::time::timeout(WAIT, async {
        loop {
            match events.poll().await? {
                Event::Incoming(Packet::PubAck(ack)) => return Ok(ack),
                Event::Incoming(Packet::Disconnect(d)) => {
                    bail!("disconnected: {:?}", d.reason_code)
                }
                _ => {}
            }
        }
    })
    .await
    .context("no PUBACK")??;

    let records = stored(&felix.broker, 1).await?;
    let record = &records[0];
    assert_eq!(record.payload, "21.5");
    assert_eq!(record.key, Bytes::from_static(b"plant/7/temp"));
    assert_eq!(
        record.header("felix.mqtt.content_type"),
        Some(&b"text/plain"[..])
    );
    assert_eq!(record.header("site"), Some(&b"north"[..]));
    Ok(())
}

#[tokio::test]
async fn a_publish_outside_every_mount_is_not_written() -> Result<()> {
    use rumqttc::{AsyncClient, Event, MqttOptions, QoS};

    let felix = start(true).await?;
    let mut options = MqttOptions::new("dev-2", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (client, mut events) = AsyncClient::new(options, 10);
    client
        .publish("yard/gate", QoS::AtLeastOnce, false, "open")
        .await?;
    // A 3.1.1 client cannot be told no for one message: the connection ends.
    let ended = tokio::time::timeout(WAIT, async {
        loop {
            match events.poll().await {
                Err(err) => return err,
                Ok(Event::Incoming(rumqttc::Packet::PubAck(_))) => {
                    panic!("an unmounted publish was acknowledged")
                }
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the connection should have closed");
    assert!(
        felix
            .broker
            .read_durable(TENANT, "ns", "plant", 0, 0, 1 << 20)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn a_broker_without_record_headers_writes_nothing() -> Result<()> {
    use rumqttc::{AsyncClient, MqttOptions, QoS};

    let felix = start(false).await?;
    let mut options = MqttOptions::new("dev-3", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.demo.token.clone());
    let (client, mut events) = AsyncClient::new(options, 10);
    client
        .publish("plant/7/temp", QoS::AtLeastOnce, false, "21.5")
        .await?;
    let ended = tokio::time::timeout(WAIT, async {
        loop {
            if events.poll().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "3.1.1 is refused by closing the connection");
    assert!(
        felix
            .broker
            .read_durable(TENANT, "ns", "plant", 0, 0, 1 << 20)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn mosquitto_pub_publishes_at_qos_1() -> Result<()> {
    let felix = start(true).await?;
    let run = tokio::process::Command::new("mosquitto_pub")
        .args(["-h", "127.0.0.1", "-p"])
        .arg(felix.port.to_string())
        .args(["-u", TENANT, "-P"])
        .arg(&felix.demo.token)
        .args(["-t", "plant/7/line/3/temp", "-q", "1", "-m", "21.5"])
        .output()
        .await;
    let output = match run {
        Ok(output) => output,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping: mosquitto_pub is not installed");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    assert!(
        output.status.success(),
        "mosquitto_pub failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = stored(&felix.broker, 1).await?;
    assert_eq!(records[0].payload, "21.5");
    assert_eq!(records[0].key, Bytes::from_static(b"plant/7/line/3/temp"));
    assert_eq!(records[0].header("felix.mqtt.qos"), Some(&b"1"[..]));
    Ok(())
}

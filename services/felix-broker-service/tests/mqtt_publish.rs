//! MQTT publishing against real MQTT clients: `rumqttc` (3.1.1 and 5.0) and,
//! when it is installed, `mosquitto_pub`.
//!
//! One broker with a durable stream mounted at `plant/`. Each test publishes
//! through the listener and reads the records back from the stream, checking
//! the key (the topic), the headers and the payload. Without `mosquitto_pub`
//! on the path its test says so and returns; CI installs it.
//!
//! Run with `cargo test -p felix-broker-service --test mqtt_publish`.
#[path = "support/mqtt_fixture.rs"]
mod mqtt_fixture;
#[path = "support/raft_shard.rs"]
mod raft_shard;

use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use mqtt_fixture::{Felix, Fixture, Options, TENANT, on_both};

const WAIT: Duration = Duration::from_secs(10);

async fn start(fixture: Fixture, headers: bool) -> Result<Felix> {
    Felix::start(
        fixture,
        Options {
            headers,
            reads: false,
        },
    )
    .await
}

on_both!(
    rumqttc_3_1_1_publishes_at_qos_0_and_1,
    rumqttc_5_carries_properties_as_headers,
    a_publish_outside_every_mount_is_not_written,
    a_broker_without_record_headers_writes_nothing,
    mosquitto_pub_publishes_at_qos_1,
);

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
async fn stored(felix: &Felix, count: usize) -> Result<Vec<Stored>> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let records = felix.records().await?;
        if records.len() >= count {
            return records
                .into_iter()
                .map(|(payload, headers)| {
                    let block = headers.context("a record without headers")?;
                    Ok(Stored {
                        payload,
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

async fn rumqttc_3_1_1_publishes_at_qos_0_and_1(fixture: Fixture) -> Result<()> {
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

    let felix = start(fixture, true).await?;
    let mut options = MqttOptions::new("dev-1", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.token.clone());
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

    let records = stored(&felix, 2).await?;
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

async fn rumqttc_5_carries_properties_as_headers(fixture: Fixture) -> Result<()> {
    use rumqttc::v5::mqttbytes::QoS;
    use rumqttc::v5::mqttbytes::v5::{Packet, PublishProperties};
    use rumqttc::v5::{AsyncClient, Event, MqttOptions};

    let felix = start(fixture, true).await?;
    let mut options = MqttOptions::new("dev-5", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.token.clone());
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

    let records = stored(&felix, 1).await?;
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

async fn a_publish_outside_every_mount_is_not_written(fixture: Fixture) -> Result<()> {
    use rumqttc::{AsyncClient, Event, MqttOptions, QoS};

    let felix = start(fixture, true).await?;
    let mut options = MqttOptions::new("dev-2", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.token.clone());
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
    assert!(felix.records().await?.is_empty());
    Ok(())
}

async fn a_broker_without_record_headers_writes_nothing(fixture: Fixture) -> Result<()> {
    use rumqttc::{AsyncClient, MqttOptions, QoS};

    let felix = start(fixture, false).await?;
    let mut options = MqttOptions::new("dev-3", "127.0.0.1", felix.port);
    options.set_credentials(TENANT, felix.token.clone());
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
    assert!(felix.records().await?.is_empty());
    Ok(())
}

async fn mosquitto_pub_publishes_at_qos_1(fixture: Fixture) -> Result<()> {
    let felix = start(fixture, true).await?;
    let run = tokio::process::Command::new("mosquitto_pub")
        .args(["-h", "127.0.0.1", "-p"])
        .arg(felix.port.to_string())
        .args(["-u", TENANT, "-P"])
        .arg(&felix.token)
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
    let records = stored(&felix, 1).await?;
    assert_eq!(records[0].payload, "21.5");
    assert_eq!(records[0].key, Bytes::from_static(b"plant/7/line/3/temp"));
    assert_eq!(records[0].header("felix.mqtt.qos"), Some(&b"1"[..]));
    Ok(())
}

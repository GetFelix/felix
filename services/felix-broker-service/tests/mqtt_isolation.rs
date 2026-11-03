//! A device fleet cannot slow QUIC clients down.
//!
//! The MQTT listener runs on a runtime of its own. Here a flood of connections,
//! each making the broker check a token, hits it from many tasks while a QUIC
//! client publishes; every publish must still be acknowledged promptly. The
//! flood has one worker thread to spend, so it saturates that thread and
//! nothing else.
//!
//! Run with `cargo test -p felix-broker-service --test mqtt_isolation`.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use felix_broker::{Broker, StreamMetadata};
use felix_broker_service::config::{BrokerConfig, MqttListenerConfig};
use felix_broker_service::serving::auth::demo::demo_auth_for_tenant;
use felix_broker_service::serving::mqtt::{BrokerMqttAuth, MqttListener};
use felix_broker_service::serving::quic::serve;
use felix_client::{ClientConfig, Connection};
use felix_mqtt::Settings;
use felix_mqtt_wire::{Connect, Packet, Properties, Utf8, Version};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const FLOOD_TASKS: usize = 48;
const PUBLISHES: usize = 200;
/// Far above a publish's normal cost on a loaded machine, far below the
/// seconds a starved runtime would take.
const SLOWEST_PUBLISH: Duration = Duration::from_secs(2);

fn connect_with_bad_token() -> Vec<u8> {
    Packet::Connect(Connect {
        version: Version::V311,
        clean_start: true,
        keep_alive: 30,
        client_id: Utf8::new("flood").expect("id"),
        will: None,
        username: Some(Utf8::new("t1").expect("tenant")),
        password: Some(bytes::Bytes::from_static(b"not-a-token")),
        properties: Properties::default(),
    })
    .to_bytes(Version::V311)
    .expect("encode")
    .to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_flood_does_not_slow_quic_publishes() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "orders", StreamMetadata::default())
        .await?;
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let quic_addr = server.local_addr()?;
    let demo = demo_auth_for_tenant("t1")?;
    let quic_task = tokio::spawn(serve(
        server,
        broker,
        BrokerConfig::default(),
        Arc::clone(&demo.auth),
    ));

    let mqtt = MqttListener::bind(
        &MqttListenerConfig {
            listen: "127.0.0.1:0".parse()?,
            tls: false,
            worker_threads: 1,
            max_connections: 1024,
            max_connections_per_ip: 0,
            connect_rate: 0,
            settings: Settings::default(),
            ws: None,
        },
        None,
        Arc::new(BrokerMqttAuth::new(Arc::clone(&demo.auth))),
        Arc::new(felix_mqtt::NoMounts),
    )?;
    let mqtt_addr = mqtt.local_addr()?;
    let shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    drop(mqtt.spawn(shutdown.clone(), connections.clone(), None));

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let mut config = ClientConfig::from_env_or_yaml(
        quinn::ClientConfig::with_root_certificates(Arc::new(roots))?,
        None,
    )?;
    config.auth_tenant_id = Some(demo.tenant_id.clone());
    config.auth_token = Some(demo.token.clone());
    let client = Connection::connect(quic_addr, "localhost", config).await?;
    let publisher = client.publisher().await?;

    let stop = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(AtomicU64::new(0));
    let flood: Vec<_> = (0..FLOOD_TASKS)
        .map(|_| {
            let (stop, attempts) = (Arc::clone(&stop), Arc::clone(&attempts));
            tokio::spawn(async move {
                let packet = connect_with_bad_token();
                while !stop.load(Ordering::Relaxed) {
                    let Ok(mut socket) = TcpStream::connect(mqtt_addr).await else {
                        continue;
                    };
                    if socket.write_all(&packet).await.is_ok() {
                        let mut reply = [0u8; 16];
                        let _ = socket.read(&mut reply).await;
                    }
                    attempts.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    let mut slowest = Duration::ZERO;
    for i in 0..PUBLISHES {
        let started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(10),
            publisher.publish(
                "t1",
                "default",
                "orders",
                format!("m{i}").into_bytes(),
                AckMode::PerMessage,
            ),
        )
        .await
        .context("publish acknowledged")??;
        slowest = slowest.max(started.elapsed());
    }

    stop.store(true, Ordering::Relaxed);
    for task in flood {
        let _ = task.await;
    }
    shutdown.cancel();
    connections.close();
    quic_task.abort();

    assert!(
        attempts.load(Ordering::Relaxed) > 100,
        "the flood made only {} attempts, so it proved nothing",
        attempts.load(Ordering::Relaxed)
    );
    assert!(
        slowest < SLOWEST_PUBLISH,
        "a publish took {slowest:?} during the flood"
    );
    Ok(())
}

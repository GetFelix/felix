//! The broker an MQTT client test runs against: a local `Broker` with its
//! listener in this process, or a one-node cluster whose stream is on a Raft
//! shard. Both serve one durable stream, `plant`, mounted at `plant/`.

// Compiled into each MQTT test binary, and each uses only part of it.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_broker_service::config::MqttListenerConfig;
use felix_broker_service::serving::auth::demo::{DemoAuth, demo_auth_for_tenant};
use felix_broker_service::serving::mqtt::{
    BrokerMounts, BrokerMqttAuth, BrokerReads, BrokerWrites, MqttListener,
};
use felix_cluster::{ClusterConfig, StreamSpec};
use felix_mqtt::Settings;
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::RecordHeaders;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::raft_shard::RaftShard;

pub(crate) const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "plant";
const MOUNT: &str = "plant/";
const LISTEN: &str = "FELIX_MQTT_LISTEN";

/// Which broker a test runs against.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fixture {
    Local,
    RaftShard,
}

/// Run each test body against both fixtures: `<name>::on_a_local_broker` and
/// `<name>::on_a_raft_shard`. The Raft runs each start a cluster, so they
/// take turns.
macro_rules! on_both {
    ($($name:ident),* $(,)?) => {$(
        mod $name {
            use super::*;

            #[tokio::test]
            async fn on_a_local_broker() -> Result<()> {
                super::$name(Fixture::Local).await
            }

            #[serial_test::serial(raft_fixture)]
            #[tokio::test]
            async fn on_a_raft_shard() -> Result<()> {
                super::$name(Fixture::RaftShard).await
            }
        }
    )*};
}
pub(crate) use on_both;

/// What a broker asks of its MQTT listener.
#[derive(Clone, Copy)]
pub(crate) struct Options {
    /// Store MQTT topics and properties as keys and headers, without which
    /// a publish is refused.
    pub(crate) headers: bool,
    /// Serve SUBSCRIBE.
    pub(crate) reads: bool,
}

pub(crate) struct Felix {
    /// The MQTT listener's port on 127.0.0.1.
    pub(crate) port: u16,
    /// The password a client connects with, as the tenant.
    pub(crate) token: String,
    backend: Backend,
}

enum Backend {
    Local {
        broker: Arc<Broker>,
        shutdown: CancellationToken,
        _demo: DemoAuth,
        _data: tempfile::TempDir,
    },
    Raft(Box<RaftShard>),
}

impl Drop for Felix {
    fn drop(&mut self) {
        if let Backend::Local { shutdown, .. } = &self.backend {
            shutdown.cancel();
        }
    }
}

impl Felix {
    pub(crate) async fn start(fixture: Fixture, options: Options) -> Result<Self> {
        match fixture {
            Fixture::Local => start_local(options).await,
            Fixture::RaftShard => Ok(start_raft(options).await),
        }
    }

    /// Every record of the stream, as a payload and its key and headers:
    /// from the broker's log, or on a cluster from a replay through a Felix
    /// client.
    pub(crate) async fn records(&self) -> Result<Vec<(Bytes, Option<RecordHeaders>)>> {
        match &self.backend {
            Backend::Local { broker, .. } => broker
                .read_durable(TENANT, NAMESPACE, STREAM, 0, 0, 1 << 20)
                .await?
                .into_iter()
                .map(|record| {
                    let headers = record
                        .headers
                        .map(|block| RecordHeaders::decode((*block).clone()))
                        .transpose()?;
                    Ok((record.payload, headers))
                })
                .collect(),
            Backend::Raft(shard) => Ok(shard
                .replay(0)
                .await
                .into_iter()
                .map(|event| (event.payload, event.headers))
                .collect()),
        }
    }
}

async fn start_raft(options: Options) -> Felix {
    // A cluster broker always serves SUBSCRIBE; `reads` only matters to the
    // local listener, which a test builds with exactly what it uses.
    let shard = RaftShard::start(
        ClusterConfig {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            tcp_listeners: vec![(LISTEN.to_string(), "127.0.0.1:{port}".to_string())],
            broker_env: vec![("FELIX_MQTT_TLS".to_string(), "false".to_string())],
            ..Default::default()
        },
        StreamSpec::new(STREAM, 1).mounted_at(MOUNT),
        options.headers,
    )
    .await;
    let port = shard
        .cluster
        .node(&shard.node)
        .and_then(|node| node.tcp_port(LISTEN))
        .expect("an MQTT port");
    Felix {
        port,
        token: shard.cluster.client_token(),
        backend: Backend::Raft(Box::new(shard)),
    }
}

async fn start_local(options: Options) -> Result<Felix> {
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
    if options.headers {
        broker.record_headers_when(|| true);
    }
    broker.register_tenant(TENANT).await?;
    broker.register_namespace(TENANT, NAMESPACE).await?;
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                mount: Some(MOUNT.to_string()),
                ..StreamMetadata::default()
            },
        )
        .await?;

    let demo = demo_auth_for_tenant(TENANT)?;
    let mut listener = MqttListener::bind(
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
    )?;
    if options.reads {
        listener = listener.with_reads(Arc::new(BrokerReads::new(Arc::clone(&broker), None)));
    }
    let port = listener.local_addr()?.port();
    let shutdown = CancellationToken::new();
    drop(listener.spawn(shutdown.clone(), TaskTracker::new(), None));
    Ok(Felix {
        port,
        token: demo.token.clone(),
        backend: Backend::Local {
            broker,
            shutdown,
            _demo: demo,
            _data: data,
        },
    })
}

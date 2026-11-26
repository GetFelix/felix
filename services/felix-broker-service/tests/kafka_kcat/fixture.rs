//! The broker a kcat test runs against: a local `Broker` with its listeners in
//! this process, or a one-node cluster whose stream is on a Raft shard. One
//! test body runs against both, through what [`Felix`] offers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use felix_broker::{Broker, DurableStorage, StreamMetadata};
use felix_broker_service::config::KafkaListenerConfig;
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_broker_service::serving::kafka::{BrokerCluster, KafkaListener, tls_config};
use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig, RecordMark};
use jsonwebtoken::Algorithm;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::{SHARDS, TENANT, container_host};

const NAMESPACE: &str = "orders";
const STREAM: &str = "created";

/// Which broker a test runs against.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fixture {
    Local,
    RaftShard,
}

/// The Kafka listeners a test can dial.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Listener {
    /// SASL/PLAIN over plaintext.
    Plain,
    /// SASL/PLAIN over TLS.
    Tls,
    /// No SASL: the development switch, as the tenant.
    Anonymous,
}

/// One record as Felix stored it.
pub(crate) struct Stored {
    pub(crate) offset: u64,
    pub(crate) payload: Bytes,
}

pub(crate) struct Felix {
    pub(crate) token: String,
    /// `host:port` a kcat container dials, by listener.
    addrs: HashMap<Listener, String>,
    /// Holds `ca.pem`, the certificate the TLS listener serves.
    certs: TempDir,
    backend: Backend,
}

enum Backend {
    Local {
        broker: Arc<Broker>,
        _data: TempDir,
        shutdown: CancellationToken,
    },
    /// A one-node cluster. Its broker runs one Kafka listener, so a test that
    /// asks for another restarts it with that one; the shard's log stays.
    Raft {
        cluster: Box<Cluster>,
        node: String,
        listener: Listener,
        record_headers: bool,
    },
}

impl Drop for Felix {
    fn drop(&mut self) {
        if let Backend::Local { shutdown, .. } = &self.backend {
            shutdown.cancel();
        }
    }
}

impl Felix {
    pub(crate) async fn start(fixture: Fixture) -> Self {
        Self::start_with(fixture, false).await
    }

    /// `record_headers` opts the listener in to storing keys and headers, and
    /// makes the broker accept them.
    pub(crate) async fn start_with(fixture: Fixture, record_headers: bool) -> Self {
        match fixture {
            Fixture::Local => start_local(record_headers).await,
            Fixture::RaftShard => start_raft(record_headers).await,
        }
    }

    /// The plaintext SASL listener, the one most tests dial.
    pub(crate) fn plain(&self) -> &str {
        self.addrs
            .get(&Listener::Plain)
            .expect("the plaintext listener is up")
    }

    /// `listener`'s address, once it is up.
    pub(crate) async fn listener(&mut self, listener: Listener) -> String {
        if let Backend::Raft {
            cluster,
            node,
            listener: current,
            record_headers,
        } = &mut self.backend
            && *current != listener
        {
            cluster.stop_node(node).await.expect("stop the broker");
            cluster.set_kafka_listener(felix_cluster::KafkaListener {
                tls: listener == Listener::Tls,
                anonymous: listener == Listener::Anonymous,
                record_headers: *record_headers,
            });
            cluster
                .restart_node(node)
                .await
                .expect("restart the broker");
            await_shards(cluster, node).await;
            let broker = cluster.node(node).expect("the broker");
            // A new certificate on every start.
            std::fs::copy(
                broker.client_certificate(),
                self.certs.path().join("ca.pem"),
            )
            .expect("copy the broker's certificate");
            self.addrs = HashMap::from([(listener, kafka_addr(cluster, node))]);
            *current = listener;
        }
        self.addrs
            .get(&listener)
            .cloned()
            .unwrap_or_else(|| panic!("no {listener:?} listener"))
    }

    pub(crate) fn certs(&self) -> &Path {
        self.certs.path()
    }

    /// Publish `values` to `shard` through Felix itself, not Kafka.
    pub(crate) async fn publish(&self, shard: u32, values: &[&str]) {
        match &self.backend {
            Backend::Local { broker, .. } => {
                let payloads: Vec<Bytes> = values
                    .iter()
                    .map(|v| Bytes::copy_from_slice(v.as_bytes()))
                    .collect();
                broker
                    .publish_batch(TENANT, NAMESPACE, STREAM, shard, &payloads)
                    .await
                    .expect("publish");
            }
            Backend::Raft { cluster, node, .. } => {
                let client = cluster.client_on(node).await.expect("connect");
                let publisher = client.publisher().await.expect("publisher");
                publisher
                    .publish_batch_on_shard(
                        TENANT,
                        NAMESPACE,
                        STREAM,
                        shard,
                        values.iter().map(|v| v.as_bytes().to_vec()).collect(),
                        felix_wire::AckMode::PerMessage,
                    )
                    .await
                    .expect("publish");
            }
        }
    }

    /// Every record of one shard: from the broker's log, or on a cluster
    /// from a replay of the shard through a Felix client.
    pub(crate) async fn stored(&self, shard: u32) -> Vec<Stored> {
        match &self.backend {
            Backend::Local { .. } => self
                .local_log(shard)
                .await
                .into_iter()
                .map(|record| Stored {
                    offset: record.offset,
                    payload: record.payload,
                })
                .collect(),
            Backend::Raft { cluster, node, .. } => replay(cluster, node, shard)
                .await
                .into_iter()
                .map(|event| Stored {
                    offset: event.offset.expect("a durable stream's offsets"),
                    payload: event.payload,
                })
                .collect(),
        }
    }

    /// Each stored record's `(producer id, sequence)`, from the broker's own
    /// log. `None` on a cluster: no client is shown producer marks.
    pub(crate) async fn producer_marks(&self, shard: u32) -> Option<Vec<Option<(u64, u64)>>> {
        let Backend::Local { .. } = &self.backend else {
            return None;
        };
        let marks = self
            .local_log(shard)
            .await
            .into_iter()
            .map(|record| match record.mark {
                RecordMark::Opens(batch) => Some((batch.producer_id, batch.sequence)),
                _ => None,
            })
            .collect();
        Some(marks)
    }

    async fn local_log(&self, shard: u32) -> Vec<felix_storage::log::LogRecord> {
        let Backend::Local { broker, .. } = &self.backend else {
            unreachable!("only a local broker's log is read directly");
        };
        broker
            .resolve_stream_handle(TENANT, NAMESPACE, STREAM, shard)
            .await
            .expect("handle")
            .log()
            .expect("durable")
            .read_from(0, usize::MAX)
            .await
            .expect("read")
    }
}

fn kafka_addr(cluster: &Cluster, node: &str) -> String {
    cluster
        .node(node)
        .and_then(|node| node.kafka_addr.clone())
        .expect("a Kafka listener")
}

/// A one-node cluster on Raft with the test's stream created after it starts,
/// so the harness's readiness probes write to another stream and every shard
/// of this one starts empty.
async fn start_raft(record_headers: bool) -> Felix {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        streams: vec![StreamSpec::new("ready", 1)],
        kafka: true,
        kafka_listener: felix_cluster::KafkaListener {
            record_headers,
            ..Default::default()
        },
        // Named, so the fixture is Raft whatever engine a suite defaults to.
        broker_env: vec![("FELIX_SHARD_ENGINE".to_string(), "raft".to_string())],
        ..Default::default()
    })
    .await
    .expect("start a one-node Raft cluster");
    // A new Raft cluster has every Raft feature finalized; a test that names
    // its engine finalizes them itself.
    let mut features: Vec<&str> = felix_common::fleet::IMPLEMENTED
        .iter()
        .map(|feature| feature.name())
        .filter(|name| name.starts_with("raft_"))
        .collect();
    if record_headers {
        features.push(felix_common::fleet::RECORD_HEADERS.name());
    }
    cluster
        .finalize_fleet_features(&features, Duration::from_secs(30))
        .await
        .expect("finalize the Raft features");
    cluster
        .create_stream(&StreamSpec::new(STREAM, SHARDS))
        .await
        .expect("create the stream");
    let node = cluster.node_ids().remove(0);
    await_shards(&cluster, &node).await;
    let certs = tempfile::tempdir().expect("certs");
    let addrs = HashMap::from([(Listener::Plain, kafka_addr(&cluster, &node))]);
    Felix {
        token: cluster.client_token(),
        addrs,
        certs,
        backend: Backend::Raft {
            cluster: Box::new(cluster),
            node,
            listener: Listener::Plain,
            record_headers,
        },
    }
}

/// Wait until every shard of the stream has a leader that serves a read,
/// without writing to it.
async fn await_shards(cluster: &Cluster, node: &str) {
    cluster
        .await_raft_leaders_placed(Duration::from_secs(60))
        .await
        .expect("every shard led");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        "the broker to route the stream at its full width",
        || async move {
            cluster
                .stream_shards_via(node, STREAM)
                .await
                .is_ok_and(|width| width == SHARDS)
        },
    )
    .await
    .expect("stream width");
    for shard in 0..SHARDS {
        replay(cluster, node, shard).await;
    }
}

async fn replay(cluster: &Cluster, node: &str, shard: u32) -> Vec<felix_client::Event> {
    cluster
        .whole_replay_of_shard(
            node,
            STREAM,
            shard,
            Duration::from_secs(1),
            Duration::from_secs(30),
        )
        .await
        .expect("replay the shard")
}

async fn start_local(record_headers: bool) -> Felix {
    let data = tempfile::tempdir().expect("dir");
    let storage = DurableStorage::open(
        data.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
    if record_headers {
        broker.record_headers_when(|| true);
        broker.read_record_headers_when(|| true);
    }
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("ns");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            StreamMetadata {
                durable: true,
                shards: SHARDS,
                ..Default::default()
            },
        )
        .await
        .expect("stream");

    let (auth, token) = auth_and_token();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("cert");
    let certs = tempfile::tempdir().expect("certs");
    std::fs::write(certs.path().join("ca.pem"), cert.cert.pem()).expect("write cert");
    let tls = tls_config(
        cert.cert.der().clone(),
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())),
    )
    .expect("tls");

    let shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    let mut addrs = HashMap::new();
    for (kind, tls_on, anonymous) in [
        (Listener::Plain, false, None),
        (Listener::Tls, true, None),
        (Listener::Anonymous, false, Some(TENANT)),
    ] {
        // The advertised address has to name the port before the bind, so the
        // port comes from a probe socket that is closed again. Another test can
        // take it in between; pick a fresh one when that happens.
        let mut attempt = 0;
        let (listener, advertise) = loop {
            attempt += 1;
            let port = free_port();
            let advertise = format!("{}:{port}", container_host());
            let config = KafkaListenerConfig {
                listen: SocketAddr::from(([0, 0, 0, 0], port)),
                advertise: advertise.clone(),
                tls: tls_on,
                anonymous_tenant: anonymous.map(str::to_string),
                default_namespace: None,
                max_connections: 64,
                max_connections_per_ip: 64,
                auth_timeout_ms: 10_000,
                record_headers,
            };
            let cluster = BrokerCluster::new(
                Arc::clone(&auth),
                Arc::new(felix_broker_service::shards::routing::StreamEngine::default()),
                None,
                "felix",
                &advertise,
            )
            .expect("cluster");
            match KafkaListener::bind(
                &config,
                Some(Arc::clone(&tls)),
                Arc::clone(&broker),
                Arc::new(cluster),
                "felix-test".to_string(),
            )
            .await
            {
                Ok(listener) => break (listener, advertise),
                Err(err) if attempt < 5 && is_addr_in_use(&err) => continue,
                Err(err) => panic!("bind: {err:?}"),
            }
        };
        tokio::spawn(listener.serve(shutdown.clone(), connections.clone()));
        addrs.insert(kind, advertise);
    }
    Felix {
        token,
        addrs,
        certs,
        backend: Backend::Local {
            broker,
            _data: data,
            shutdown,
        },
    }
}

fn is_addr_in_use(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::AddrInUse)
    })
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("0.0.0.0:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// A broker auth that knows one tenant key, and a token signed by it that may
/// read and write the tenant's streams.
fn auth_and_token() -> (Arc<BrokerAuth>, String) {
    let private_key = [9u8; 32];
    let public_key = SigningKey::from_bytes(&private_key)
        .verifying_key()
        .to_bytes();
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(URL_SAFE_NO_PAD.encode(public_key)),
        }],
    };
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(TENANT), jwks.clone());
    let material = TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key,
        public_key,
        jwks,
    };
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(HashMap::from([(TENANT.to_string(), material)])),
    );
    let token = issuer
        .mint(
            &TenantId::new(TENANT),
            "p:kafka-test",
            vec![
                "stream.subscribe:stream:t1/orders/*".to_string(),
                "stream.publish:stream:t1/orders/*".to_string(),
            ],
        )
        .expect("mint");
    (Arc::new(BrokerAuth::with_key_store(key_store)), token)
}

//! A Kafka client in miniature, against an in-process broker.
//!
//! The client speaks the real wire format through `kafka-protocol`'s client
//! half, so these tests check the bytes a consumer would see. Placement and
//! credentials come from a [`FakeCluster`] the test controls.

mod fetch;
mod groups;
mod list_offsets;
mod metadata;
mod parse;
mod produce;
mod producer_id;
mod sasl;
mod versions;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use felix_authz::PermissionMatcher;
use felix_broker::{Broker, DurableStorage, PublishOutcome, StreamHandle, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_util::sync::CancellationToken;

use crate::cluster::{Cluster, Endpoint, Placement, Principal, ShardRef, WriteError, WritePermit};
use crate::service::{KafkaService, Settings};

pub(super) const TENANT: &str = "t1";
pub(super) const TOKEN: &str = "good-token";
pub(super) const LOCAL: &str = "node-a";
pub(super) const REMOTE: &str = "node-b";

/// Placement and credentials, as the test says.
pub(super) struct FakeCluster {
    placements: Mutex<HashMap<(String, u32), Placement>>,
    brokers: Mutex<Vec<Endpoint>>,
    permissions: Vec<String>,
    /// Writes that waited on their stream's consistency.
    pub(super) consistency_waits: AtomicUsize,
    /// What the next consistency wait answers, when not success.
    pub(super) consistency_error: Mutex<Option<WriteError>>,
    /// Commit points by `(stream, shard)`; a shard without one is unbounded.
    committed: Mutex<HashMap<(String, u32), u64>>,
}

#[async_trait]
impl Cluster for FakeCluster {
    async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<Principal, String> {
        if tenant_id == TENANT && token == TOKEN {
            let matcher = PermissionMatcher::from_strings(&self.permissions).expect("perms");
            Ok(Principal::with_permissions(tenant_id, matcher))
        } else {
            Err("token rejected".to_string())
        }
    }

    fn brokers(&self) -> Vec<Endpoint> {
        self.brokers.lock().expect("lock").clone()
    }

    fn local_node_id(&self) -> String {
        LOCAL.to_string()
    }

    fn placement(&self, shard: &ShardRef<'_>) -> Placement {
        self.placements
            .lock()
            .expect("lock")
            .get(&(shard.stream.to_string(), shard.shard))
            .cloned()
            .unwrap_or(Placement::Local {
                replicas: Vec::new(),
            })
    }

    async fn admit_write(&self, shard: &ShardRef<'_>) -> Result<WritePermit, WriteError> {
        match self.placement(shard) {
            Placement::Local { .. } => Ok(WritePermit::default()),
            _ => Err(WriteError::NotLeader),
        }
    }

    async fn await_consistency(
        &self,
        _shard: &ShardRef<'_>,
        _handle: &StreamHandle,
        _outcome: &PublishOutcome,
    ) -> Result<(), WriteError> {
        self.consistency_waits.fetch_add(1, Ordering::SeqCst);
        match self.consistency_error.lock().expect("lock").take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn committed_until(&self, shard: &ShardRef<'_>, _handle: &StreamHandle) -> Option<u64> {
        self.committed
            .lock()
            .expect("lock")
            .get(&(shard.stream.to_string(), shard.shard))
            .copied()
    }
}

impl FakeCluster {
    pub(super) fn place(&self, stream: &str, shard: u32, placement: Placement) {
        self.placements
            .lock()
            .expect("lock")
            .insert((stream.to_string(), shard), placement);
    }

    /// Set a shard's commit point, as a `Quorum` stream's quorum mark would.
    pub(super) fn commit_until(&self, stream: &str, shard: u32, offset: u64) {
        self.committed
            .lock()
            .expect("lock")
            .insert((stream.to_string(), shard), offset);
    }

    pub(super) fn add_broker(&self, node_id: &str, host: &str, port: u16) {
        self.brokers.lock().expect("lock").push(Endpoint {
            node_id: node_id.to_string(),
            host: host.to_string(),
            port,
        });
    }
}

pub(super) struct Fixture {
    pub(super) broker: Arc<Broker>,
    pub(super) cluster: Arc<FakeCluster>,
    service: KafkaService,
    _dir: TempDir,
}

impl Fixture {
    /// Anonymous access to `TENANT`, the simplest thing to test reads with.
    pub(super) async fn anonymous() -> Self {
        Self::with(Settings {
            anonymous_tenant: Some(TENANT.to_string()),
            default_namespace: None,
            cluster_id: "test-cluster".to_string(),
        })
        .await
    }

    /// No anonymous access: a connection must authenticate, and then reads
    /// what `permissions` allow.
    pub(super) async fn secured(permissions: &[&str]) -> Self {
        Self::build(
            Settings {
                anonymous_tenant: None,
                default_namespace: None,
                cluster_id: "test-cluster".to_string(),
            },
            permissions.iter().map(|p| p.to_string()).collect(),
        )
        .await
    }

    pub(super) async fn with(settings: Settings) -> Self {
        Self::build(settings, Vec::new()).await
    }

    async fn build(settings: Settings, permissions: Vec<String>) -> Self {
        let dir = tempfile::tempdir().expect("dir");
        let storage = DurableStorage::open(
            dir.path(),
            LogConfig {
                fsync_mode: FsyncMode::None,
                preallocate_segments: false,
                ..LogConfig::default()
            },
        )
        .expect("storage");
        let broker =
            Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
        broker.register_tenant(TENANT).await.expect("tenant");
        let cluster = Arc::new(FakeCluster {
            placements: Mutex::new(HashMap::new()),
            brokers: Mutex::new(Vec::new()),
            permissions,
            consistency_waits: AtomicUsize::new(0),
            consistency_error: Mutex::new(None),
            committed: Mutex::new(HashMap::new()),
        });
        cluster.add_broker(LOCAL, "kafka-a.test", 9092);
        let service = KafkaService::new(Arc::clone(&broker), cluster.clone(), settings);
        Self {
            broker,
            cluster,
            service,
            _dir: dir,
        }
    }

    /// Register a stream, creating its namespace.
    pub(super) async fn stream(&self, namespace: &str, stream: &str, shards: u32, durable: bool) {
        self.broker
            .register_namespace(TENANT, namespace)
            .await
            .expect("namespace");
        self.broker
            .register_stream(
                TENANT,
                namespace,
                stream,
                StreamMetadata {
                    durable,
                    shards,
                    ..Default::default()
                },
            )
            .await
            .expect("stream");
    }

    pub(super) async fn publish(&self, namespace: &str, stream: &str, shard: u32, values: &[&str]) {
        let payloads: Vec<Bytes> = values
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect();
        self.broker
            .publish_batch(TENANT, namespace, stream, shard, &payloads)
            .await
            .expect("publish");
    }

    pub(super) fn connect(&self) -> Client {
        self.connect_until(CancellationToken::new())
    }

    /// A connection served until `shutdown` is cancelled.
    pub(super) fn connect_until(&self, shutdown: CancellationToken) -> Client {
        let (client, server) = tokio::io::duplex(1 << 20);
        let service = self.service.clone();
        tokio::spawn(async move {
            service.serve_connection(server, shutdown).await;
        });
        Client {
            stream: client,
            next_correlation: 1,
        }
    }
}

pub(super) struct Client {
    stream: DuplexStream,
    next_correlation: i32,
}

impl Client {
    /// Send one request and read its response.
    pub(super) async fn call<R>(&mut self, request: &R, version: i16) -> R::Response
    where
        R: Request,
    {
        let correlation_id = self.send(request, version).await;
        let mut frame = self.read_frame().await.expect("a response");
        assert_eq!(frame.get_i32(), correlation_id, "correlation id");
        if R::Response::header_version(version) >= 1 {
            assert_eq!(frame.get_u8(), 0, "no tagged header fields");
        }
        let response = R::Response::decode(&mut frame, version).expect("decode response");
        assert!(!frame.has_remaining(), "response fully consumed");
        response
    }

    pub(super) async fn send<R: Request>(&mut self, request: &R, version: i16) -> i32 {
        let correlation_id = self.next_correlation;
        self.next_correlation += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("felix-test")));
        let mut body = BytesMut::new();
        header
            .encode(&mut body, R::header_version(version))
            .expect("encode header");
        request.encode(&mut body, version).expect("encode request");
        self.send_raw(&body).await;
        correlation_id
    }

    pub(super) async fn send_raw(&mut self, body: &[u8]) {
        let mut frame = BytesMut::with_capacity(body.len() + 4);
        frame.put_i32(body.len() as i32);
        frame.put_slice(body);
        self.stream.write_all(&frame).await.expect("write");
    }

    /// The next response frame, or `None` once the server has closed.
    pub(super) async fn read_frame(&mut self) -> Option<Bytes> {
        let mut size = [0u8; 4];
        self.stream.read_exact(&mut size).await.ok()?;
        let mut frame = vec![0u8; i32::from_be_bytes(size) as usize];
        self.stream.read_exact(&mut frame).await.ok()?;
        Some(Bytes::from(frame))
    }

    /// Authenticate with SASL/PLAIN and return the `SaslAuthenticate` error.
    pub(super) async fn login(&mut self, tenant: &str, token: &str) -> i16 {
        use kafka_protocol::messages::{SaslAuthenticateRequest, SaslHandshakeRequest};
        let handshake = self
            .call(
                &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
                1,
            )
            .await;
        assert_eq!(handshake.error_code, 0);
        self.call(
            &SaslAuthenticateRequest::default()
                .with_auth_bytes(Bytes::from(format!("\0{tenant}\0{token}"))),
            2,
        )
        .await
        .error_code
    }
}

/// Every topic answer as `(name, error)`.
pub(super) fn topic_errors(
    response: &kafka_protocol::messages::MetadataResponse,
) -> Vec<(String, i16)> {
    response
        .topics
        .iter()
        .map(|topic| {
            (
                topic
                    .name
                    .as_ref()
                    .map(|name| name.as_str().to_string())
                    .unwrap_or_default(),
                topic.error_code,
            )
        })
        .collect()
}

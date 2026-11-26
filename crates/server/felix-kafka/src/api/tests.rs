//! A Kafka client in miniature, against an in-process broker.
//!
//! The client speaks the real wire format through `kafka-protocol`'s client
//! half, so these tests check the bytes a consumer would see. Placement and
//! credentials come from a [`FakeCluster`] the test controls.

mod fetch;
mod groups;
mod headers;
mod limits;
mod list_offsets;
mod metadata;
mod parse;
mod pipeline;
mod produce;
mod producer_id;
mod sasl;
mod versions;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

use crate::cluster::{
    Cluster, Endpoint, Placement, Principal, ShardRef, Unreadable, WriteError, WritePermit,
};
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
    /// What `admit_write` answers, when not by placement.
    pub(super) admit_error: Mutex<Option<WriteError>>,
    /// What `refuses_record_headers` answers.
    pub(super) headers_refused: Mutex<Option<&'static str>>,
    /// Commit points by `(stream, shard)`; a shard without one is unbounded.
    committed: Mutex<HashMap<(String, u32), Result<u64, Unreadable>>>,
    /// What `admit_produce` answers.
    pub(super) produce_hold: Mutex<std::time::Duration>,
    /// Every `admit_produce` call, as `(tenant, records, bytes)`.
    pub(super) produce_charges: Mutex<Vec<(String, u64, u64)>>,
    /// The certificate chain each `authenticate_peer` call was handed.
    pub(super) peer_certs_seen: Mutex<Vec<Vec<Vec<u8>>>>,
    /// Set, `produce` claims each batch on the broker and holds its commit
    /// until the test releases it, as a Raft group's commit lags its
    /// proposal.
    pub(super) gated: Mutex<Option<Gated>>,
    /// Fence permits `admit_write` handed out and not yet dropped.
    pub(super) live_permits: Arc<AtomicUsize>,
    /// Set to end every credential `authenticate` handed out, and every
    /// anonymous connection that presented a certificate.
    pub(super) credential_ended: tokio::sync::watch::Sender<bool>,
    /// Set, the client certificate of every anonymous connection has passed
    /// its end, whether or not `credential_ended` has fired yet.
    pub(super) certificate_expired: Arc<AtomicBool>,
}

/// Counts itself in [`FakeCluster::live_permits`] while alive.
struct PermitGuard(Arc<AtomicUsize>);

impl Drop for PermitGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Commits held by the test, in claim order.
pub(super) struct Gated {
    broker: Arc<Broker>,
    /// Set, a batch reaches the log only when released, and its producer's
    /// turn is held until then, as a Raft group holds it until the proposal
    /// is written.
    write_on_release: bool,
    pub(super) releases: Vec<Option<tokio::sync::oneshot::Sender<Result<(), WriteError>>>>,
}

impl FakeCluster {
    /// How many batches `produce` has claimed while gated.
    pub(super) fn claimed(&self) -> usize {
        self.gated
            .lock()
            .expect("lock")
            .as_ref()
            .map_or(0, |gated| gated.releases.len())
    }

    /// Commit claimed batch `at`, or fail it with `error`.
    pub(super) fn release(&self, at: usize, error: Option<WriteError>) {
        let release = self
            .gated
            .lock()
            .expect("lock")
            .as_mut()
            .expect("gated")
            .releases[at]
            .take()
            .expect("released once");
        let _ = release.send(error.map_or(Ok(()), Err));
    }
}

#[async_trait]
impl Cluster for FakeCluster {
    async fn authenticate(&self, tenant_id: &str, token: &str) -> Result<Principal, String> {
        if tenant_id == TENANT && token == TOKEN {
            let matcher = PermissionMatcher::from_strings(&self.permissions).expect("perms");
            let ended = self.credential_ended.clone();
            Ok(
                Principal::with_permissions(tenant_id, matcher).with_end(move || {
                    let mut ended = ended.subscribe();
                    Box::pin(async move {
                        let _ = ended.wait_for(|ended| *ended).await;
                    })
                }),
            )
        } else {
            Err("token rejected".to_string())
        }
    }

    async fn authenticate_peer(
        &self,
        tenant_id: &str,
        token: &str,
        peer_certs: &[Vec<u8>],
    ) -> Result<Principal, String> {
        self.peer_certs_seen
            .lock()
            .expect("lock")
            .push(peer_certs.to_vec());
        self.authenticate(tenant_id, token).await
    }

    fn anonymous(&self, tenant_id: &str, peer_certs: &[Vec<u8>]) -> Result<Principal, String> {
        let principal = Principal::anonymous(tenant_id);
        if peer_certs.is_empty() {
            return Ok(principal);
        }
        let expired = Arc::clone(&self.certificate_expired);
        let ended = self.credential_ended.clone();
        Ok(principal
            .with_revocation(move || expired.load(Ordering::SeqCst))
            .with_end(move || {
                let mut ended = ended.subscribe();
                Box::pin(async move {
                    let _ = ended.wait_for(|ended| *ended).await;
                })
            }))
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
        if let Some(error) = self.admit_error.lock().expect("lock").clone() {
            return Err(error);
        }
        match self.placement(shard) {
            Placement::Local { .. } => {
                self.live_permits.fetch_add(1, Ordering::SeqCst);
                let guard = PermitGuard(Arc::clone(&self.live_permits));
                Ok(WritePermit::holding(guard, 0))
            }
            _ => Err(WriteError::NotLeader),
        }
    }

    fn refuses_record_headers(
        &self,
        _broker: &felix_broker::Broker,
        _shard: &ShardRef<'_>,
    ) -> Option<&'static str> {
        *self.headers_refused.lock().expect("lock")
    }

    async fn await_consistency(
        &self,
        _shard: &ShardRef<'_>,
        _handle: &StreamHandle,
        _outcome: &PublishOutcome,
        _generation: u64,
    ) -> Result<(), WriteError> {
        self.consistency_waits.fetch_add(1, Ordering::SeqCst);
        match self.consistency_error.lock().expect("lock").take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn committed_until(
        &self,
        shard: &ShardRef<'_>,
        _handle: &StreamHandle,
    ) -> Result<Option<u64>, Unreadable> {
        self.committed
            .lock()
            .expect("lock")
            .get(&(shard.stream.to_string(), shard.shard))
            .copied()
            .transpose()
    }

    async fn produce(
        &self,
        broker: &Broker,
        _shard: &ShardRef<'_>,
        handle: &StreamHandle,
        _permit: &WritePermit,
        produce: crate::cluster::Produce<'_>,
    ) -> Option<Result<crate::cluster::Proposed, crate::cluster::ProduceError>> {
        use crate::cluster::ProduceError;
        let (arc, write_on_release) = {
            let gated = self.gated.lock().expect("lock");
            let gated = gated.as_ref()?;
            (Arc::clone(&gated.broker), gated.write_on_release)
        };
        let (id, sequence) = produce.producer.expect("gated tests produce idempotently");
        let (release, released) = tokio::sync::oneshot::channel::<Result<(), WriteError>>();
        let handle = handle.clone();
        let proposed: crate::cluster::Proposed = if write_on_release {
            // As a Raft group does, the turn is let go once the batch is
            // written, whether or not anyone waits on its commit. The claim
            // takes the turn itself, so it is let go just before.
            let turn = broker.producer_turn(&handle, id).await;
            let payloads = produce.payloads.to_vec();
            let (done, outcome) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let result = match released.await {
                    Ok(Ok(())) => {
                        drop(turn);
                        match arc
                            .claim_records_idempotent(&handle, id, sequence, &payloads, None)
                            .await
                        {
                            Ok(claim) => arc
                                .complete_records_idempotent(&handle, claim)
                                .await
                                .map_err(ProduceError::Broker),
                            Err(err) => Err(ProduceError::Broker(err)),
                        }
                    }
                    Ok(Err(error)) => Err(ProduceError::Write(error)),
                    Err(_) => Err(ProduceError::Write(WriteError::LeadershipLost)),
                };
                let _ = done.send(result);
            });
            Box::pin(async move {
                outcome
                    .await
                    .unwrap_or(Err(ProduceError::Write(WriteError::LeadershipLost)))
            })
        } else {
            let claim = match broker
                .claim_records_idempotent(&handle, id, sequence, produce.payloads, None)
                .await
            {
                Ok(claim) => claim,
                Err(err) => return Some(Err(ProduceError::Broker(err))),
            };
            Box::pin(async move {
                match released.await {
                    Ok(Ok(())) => arc
                        .complete_records_idempotent(&handle, claim)
                        .await
                        .map_err(ProduceError::Broker),
                    Ok(Err(error)) => Err(ProduceError::Write(error)),
                    Err(_) => Err(ProduceError::Write(WriteError::LeadershipLost)),
                }
            })
        };
        self.gated
            .lock()
            .expect("lock")
            .as_mut()
            .expect("gated")
            .releases
            .push(Some(release));
        Some(Ok(proposed))
    }

    fn admit_produce(&self, tenant_id: &str, records: u64, bytes: u64) -> std::time::Duration {
        self.produce_charges
            .lock()
            .expect("lock")
            .push((tenant_id.to_string(), records, bytes));
        *self.produce_hold.lock().expect("lock")
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
            .insert((stream.to_string(), shard), Ok(offset));
    }

    /// Stop serving a shard's commit point, as a `Quorum` shard that moved
    /// away after its read was placed here.
    pub(super) fn stop_serving(&self, stream: &str, shard: u32) {
        self.committed
            .lock()
            .expect("lock")
            .insert((stream.to_string(), shard), Err(Unreadable::NotServed));
    }

    /// Lead a shard with no mark yet, as a broker just after a failover.
    pub(super) fn settle(&self, stream: &str, shard: u32) {
        self.committed
            .lock()
            .expect("lock")
            .insert((stream.to_string(), shard), Err(Unreadable::Settling));
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
    /// What the broker's record-headers gate answers.
    headers_accepted: Arc<AtomicBool>,
    _dir: TempDir,
}

impl Fixture {
    /// Anonymous access to `TENANT`, the simplest thing to test reads with.
    pub(super) async fn anonymous() -> Self {
        Self::with(Settings {
            anonymous_tenant: Some(TENANT.to_string()),
            default_namespace: None,
            cluster_id: "test-cluster".to_string(),
            ..Settings::default()
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
                ..Settings::default()
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
        broker.allow_head_trims_when(|| true);
        let headers_accepted = Arc::new(AtomicBool::new(false));
        let gate = Arc::clone(&headers_accepted);
        broker.record_headers_when(move || gate.load(Ordering::SeqCst));
        broker.register_tenant(TENANT).await.expect("tenant");
        let cluster = Arc::new(FakeCluster {
            placements: Mutex::new(HashMap::new()),
            brokers: Mutex::new(Vec::new()),
            permissions,
            consistency_waits: AtomicUsize::new(0),
            consistency_error: Mutex::new(None),
            admit_error: Mutex::new(None),
            headers_refused: Mutex::new(None),
            committed: Mutex::new(HashMap::new()),
            produce_hold: Mutex::new(std::time::Duration::ZERO),
            produce_charges: Mutex::new(Vec::new()),
            peer_certs_seen: Mutex::new(Vec::new()),
            gated: Mutex::new(None),
            live_permits: Arc::new(AtomicUsize::new(0)),
            credential_ended: tokio::sync::watch::channel(false).0,
            certificate_expired: Arc::new(AtomicBool::new(false)),
        });
        cluster.add_broker(LOCAL, "kafka-a.test", 9092);
        let service = KafkaService::new(Arc::clone(&broker), cluster.clone(), settings);
        Self {
            broker,
            cluster,
            service,
            headers_accepted,
            _dir: dir,
        }
    }

    /// Hold every produce's commit until the test releases it.
    pub(super) fn gate_commits(&self) {
        self.gate(false);
    }

    /// [`Self::gate_commits`], with each batch also reaching the log only
    /// once released.
    pub(super) fn gate_writes(&self) {
        self.gate(true);
    }

    fn gate(&self, write_on_release: bool) {
        *self.cluster.gated.lock().expect("lock") = Some(Gated {
            broker: Arc::clone(&self.broker),
            write_on_release,
            releases: Vec::new(),
        });
    }

    /// Make the broker accept record headers, as a finalized fleet does.
    pub(super) fn accept_headers(&self) {
        self.headers_accepted.store(true, Ordering::SeqCst);
    }

    /// Publish as a Felix client does, with a key and headers block per record.
    pub(super) async fn publish_with_blocks(
        &self,
        namespace: &str,
        stream: &str,
        shard: u32,
        records: &[(&str, Option<felix_wire::RecordHeaders>)],
    ) {
        let handle = self
            .broker
            .resolve_stream_handle(TENANT, namespace, stream, shard)
            .await
            .expect("handle");
        let payloads: Vec<Bytes> = records
            .iter()
            .map(|(value, _)| Bytes::copy_from_slice(value.as_bytes()))
            .collect();
        let blocks: Vec<Option<Bytes>> = records
            .iter()
            .map(|(_, block)| block.as_ref().map(|block| block.encode().expect("block")))
            .collect();
        self.broker
            .publish_batch_with_outcome(&handle, &payloads, None, Some(&blocks))
            .await
            .expect("publish");
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

    /// Append a generation-start record to a shard's log, as a promoted
    /// leader does, and return its offset.
    pub(super) async fn generation_start(&self, namespace: &str, stream: &str, shard: u32) -> u64 {
        let log = self
            .broker
            .shard_log(
                felix_broker::LogKind::Stream,
                TENANT,
                namespace,
                stream,
                shard,
            )
            .await
            .expect("log");
        let offset = log
            .append_generation_start(7)
            .await
            .expect("generation start");
        // The stream has to learn the offset is taken, or the next publish
        // waits on a commit turn nothing will take.
        self.broker
            .adopt_replicated(TENANT, namespace, stream, shard, offset + 1)
            .await
            .expect("adopt");
        offset
    }

    /// Append `records` in a v9 segment, which membership and counter records
    /// need. Returns the first one's offset. The log takes no ordinary
    /// publish after this.
    pub(super) async fn append_v9(
        &self,
        namespace: &str,
        stream: &str,
        shard: u32,
        records: &[(felix_storage::log::RecordMark, &'static str)],
    ) -> u64 {
        let log = self
            .broker
            .shard_log(
                felix_broker::LogKind::Stream,
                TENANT,
                namespace,
                stream,
                shard,
            )
            .await
            .expect("log");
        let records: Vec<_> = records
            .iter()
            .map(|&(mark, payload)| felix_storage::log::AppendRecord {
                payload: Bytes::from_static(payload.as_bytes()),
                timestamp_micros: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock")
                    .as_micros() as u64,
                mark,
                publisher: None,
                headers: None,
            })
            .collect();
        let appended = log
            .append_v9_for_tests(&records, 1)
            .await
            .expect("v9 append");
        self.broker
            .adopt_replicated(TENANT, namespace, stream, shard, appended.last_offset + 1)
            .await
            .expect("adopt");
        appended.first_offset
    }

    /// Trim a shard's history below `before`, at the tail, with headers a
    /// Kafka consumer must never be handed. Returns the trim record's offset.
    pub(super) async fn trim(&self, namespace: &str, stream: &str, shard: u32, before: u64) -> u64 {
        let handle = self
            .broker
            .resolve_stream_handle(TENANT, namespace, stream, shard)
            .await
            .expect("handle");
        let tail = handle
            .log()
            .expect("log")
            .tail_offset()
            .await
            .expect("tail");
        self.broker
            .trim_before_at(&handle, before, tail, Bytes::from_static(b""))
            .await
            .expect("trim")
            .offset
            .expect("written")
    }

    pub(super) fn connect(&self) -> Client {
        self.connect_until(CancellationToken::new())
    }

    /// A connection served until `shutdown` is cancelled.
    pub(super) fn connect_until(&self, shutdown: CancellationToken) -> Client {
        self.connect_as(Vec::new(), shutdown)
    }

    /// A connection whose TLS client presented `peer_certs`.
    pub(super) fn connect_as(
        &self,
        peer_certs: Vec<Vec<u8>>,
        shutdown: CancellationToken,
    ) -> Client {
        self.connect_buffered(1 << 20, peer_certs, shutdown)
    }

    /// A connection whose transport holds `capacity` unread bytes.
    pub(super) fn connect_buffered(
        &self,
        capacity: usize,
        peer_certs: Vec<Vec<u8>>,
        shutdown: CancellationToken,
    ) -> Client {
        let (client, server) = tokio::io::duplex(capacity);
        let service = self.service.clone();
        tokio::spawn(async move {
            service
                .serve_connection_with_peer(server, peer_certs, shutdown)
                .await;
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

    /// Write bytes as they are, with no length prefix.
    pub(super) async fn write_raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("write");
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

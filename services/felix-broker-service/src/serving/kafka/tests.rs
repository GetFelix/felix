//! The Kafka listener's own limits: the per-address cap, and a produce
//! charged to its tenant's quota.

use std::time::Duration;

use felix_storage::EphemeralCache;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use super::*;
use crate::config::{LimitsConfig, TenantQuota};

fn listener_config(max_connections_per_ip: usize) -> KafkaListenerConfig {
    KafkaListenerConfig {
        listen: "127.0.0.1:0".parse().expect("addr"),
        advertise: "127.0.0.1:9092".to_string(),
        tls: false,
        anonymous_tenant: None,
        default_namespace: None,
        max_connections: 64,
        max_connections_per_ip,
        auth_timeout_ms: 10_000,
        record_headers: false,
    }
}

fn cluster() -> BrokerCluster {
    let auth = Arc::new(BrokerAuth::new("http://127.0.0.1:1".to_string()));
    BrokerCluster::new(
        auth,
        Arc::new(crate::shards::routing::StreamEngine::default()),
        None,
        STANDALONE_NODE_ID,
        "127.0.0.1:9092",
    )
    .expect("cluster")
}

/// True when the server closed `socket` within a second.
async fn closed_by_server(socket: &mut TcpStream) -> bool {
    let mut byte = [0u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

#[tokio::test]
async fn a_connection_past_the_per_address_cap_is_closed_on_arrival() {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let listener = KafkaListener::bind(
        &listener_config(2),
        None,
        broker,
        Arc::new(cluster()),
        "felix-test".to_string(),
    )
    .await
    .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let shutdown = CancellationToken::new();
    let serving = tokio::spawn(listener.serve(shutdown.clone(), TaskTracker::new()));

    let mut first = TcpStream::connect(addr).await.expect("first");
    let mut second = TcpStream::connect(addr).await.expect("second");
    let mut third = TcpStream::connect(addr).await.expect("third");
    assert!(closed_by_server(&mut third).await, "the third is refused");
    assert!(!closed_by_server(&mut first).await, "the first is served");
    assert!(!closed_by_server(&mut second).await, "the second is served");

    drop(first);
    // The server notices the close on its next read, and gives the place back.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut next = TcpStream::connect(addr).await.expect("next");
        if !closed_by_server(&mut next).await {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a closed connection never gave its place back"
        );
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), serving).await;
}

// Paused so no real time refills the bucket between the two charges.
#[tokio::test(start_paused = true)]
async fn a_produce_is_charged_to_the_tenants_quota() {
    let quotas = Arc::new(TenantRates::new(&LimitsConfig {
        tenant_publish_default: TenantQuota {
            bytes_per_sec: 100,
            msgs_per_sec: 0,
        },
        tenant_publish_burst_ms: 1_000,
        ..LimitsConfig::default()
    }));
    let cluster = cluster().with_quotas(Arc::clone(&quotas));
    assert_eq!(
        cluster.admit_produce("t1", 1, 100),
        Duration::ZERO,
        "within the burst"
    );
    assert_eq!(
        cluster.admit_produce("t1", 1, 50),
        Duration::from_millis(500),
        "50 bytes over at 100 B/s"
    );
    assert_eq!(cluster.admit_produce("t2", 1, 10), Duration::ZERO);
    // The QUIC side sees the same debt.
    assert!(quotas.try_admit("t1", 1, 1).is_err());
}

/// With subject binding on, SASL over TLS refuses a token whose subject the
/// client certificate was not issued to, as the QUIC listeners do.
#[tokio::test]
async fn sasl_binds_the_token_to_the_client_certificate() {
    let demo = crate::serving::auth::demo::demo_auth_for_tenant("t1").expect("demo auth");
    let auth = Arc::new((*demo.auth).clone().with_subject_binding(true));
    let cluster = BrokerCluster::new(
        auth,
        Arc::new(crate::shards::routing::StreamEngine::default()),
        None,
        STANDALONE_NODE_ID,
        "127.0.0.1:9092",
    )
    .expect("cluster");
    let leaf = |params: rcgen::CertificateParams| {
        let key = rcgen::KeyPair::generate().expect("key");
        params.self_signed(&key).expect("cert").der().to_vec()
    };
    let mut issued_to_demo = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
    issued_to_demo.subject_alt_names.push(rcgen::SanType::URI(
        "felix:principal:p:demo".try_into().expect("uri"),
    ));
    let someone_else =
        rcgen::CertificateParams::new(vec!["someone-else.felix.test".to_string()]).expect("params");

    let refused = cluster
        .authenticate_peer("t1", &demo.token, &[leaf(someone_else)])
        .await;
    assert!(
        refused.is_err(),
        "a certificate for someone else was accepted"
    );
    cluster
        .authenticate_peer("t1", &demo.token, &[leaf(issued_to_demo)])
        .await
        .expect("the certificate issued to the token's subject");
    cluster
        .authenticate_peer("t1", &demo.token, &[])
        .await
        .expect("no certificate, nothing to bind to");
}

/// A token the control plane minted by delegation binds to its actor's
/// certificate, checked from the signed claims, on SASL as on QUIC. Without
/// `act` the gateway's certificate binds only the gateway's own tokens.
#[tokio::test]
async fn sasl_binds_a_delegated_token_to_its_actors_certificate() {
    use crate::serving::auth::demo::demo_token_with_actor;
    let demo = crate::serving::auth::demo::demo_auth_for_tenant("t1").expect("demo auth");
    let auth = Arc::new((*demo.auth).clone().with_subject_binding(true));
    let cluster = BrokerCluster::new(
        auth,
        Arc::new(crate::shards::routing::StreamEngine::default()),
        None,
        STANDALONE_NODE_ID,
        "127.0.0.1:9092",
    )
    .expect("cluster");
    let issued_to = |principal: &str| {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        params.subject_alt_names.push(rcgen::SanType::URI(
            format!("felix:principal:{principal}")
                .try_into()
                .expect("uri"),
        ));
        let key = rcgen::KeyPair::generate().expect("key");
        params.self_signed(&key).expect("cert").der().to_vec()
    };
    let delegated = demo_token_with_actor("t1", "p:alice", Some("p:gateway")).expect("token");
    let own = demo_token_with_actor("t1", "p:alice", None).expect("token");

    cluster
        .authenticate_peer("t1", &delegated, &[issued_to("p:gateway")])
        .await
        .expect("alice's token, delegated to the gateway presenting it");
    assert!(
        cluster
            .authenticate_peer("t1", &delegated, &[issued_to("p:other-gateway")])
            .await
            .is_err(),
        "a token delegated to one gateway was accepted from another"
    );
    assert!(
        cluster
            .authenticate_peer("t1", &own, &[issued_to("p:gateway")])
            .await
            .is_err(),
        "the gateway presented alice's own token"
    );
    cluster
        .authenticate_peer("t1", &delegated, &[issued_to("p:alice")])
        .await
        .expect("alice's own certificate still binds her subject");
}

/// **A Kafka produce to a shard on Raft is proposed to its group**, and an
/// idempotent producer's re-send lands once: each record is its own batch
/// at its own sequence, as the hybrid stores it.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_produce_to_a_raft_shard_lands_once() {
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker(root.path()).await;
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    fixture::take_formed(&shards, assigned).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let cluster = BrokerCluster::new(
        Arc::new(BrokerAuth::new("http://127.0.0.1:1".to_string())),
        Arc::new(fixture::engine(&shards)),
        None,
        STANDALONE_NODE_ID,
        "127.0.0.1:9092",
    )
    .expect("cluster");
    let shard = ShardRef {
        tenant_id: "t1",
        namespace: "ns",
        stream: "orders",
        shard: 0,
    };
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let payloads = |values: &[&'static str]| -> Vec<bytes::Bytes> {
        values
            .iter()
            .map(|value| bytes::Bytes::from(*value))
            .collect()
    };
    let produce = |payloads: &[bytes::Bytes], producer| {
        let payloads = payloads.to_vec();
        let cluster = &cluster;
        let broker = &broker;
        let handle = &handle;
        async move {
            let permit = cluster.admit_write(&shard).await.expect("admitted");
            let produce = Produce {
                payloads: &payloads,
                headers: None,
                producer,
            };
            cluster
                .produce(broker, &shard, handle, &permit, produce)
                .await
                .expect("proposed on Raft")
                .expect("handed over")
                .await
                .expect("written")
        }
    };

    let plain = produce(&payloads(&["p"]), None).await;
    let first = produce(&payloads(&["a", "b"]), Some((7, 0))).await;
    let again = produce(&payloads(&["a", "b"]), Some((7, 0))).await;
    let next = produce(&payloads(&["c"]), Some((7, 2))).await;

    // A record's offset is its entry's index, after the group's own.
    let (base, _) = plain.outcome.offsets.expect("written");
    let at = |first: u64, last: u64| Some((base + first, base + last));
    assert_eq!((first.outcome.offsets, first.duplicate), (at(1, 2), false));
    assert_eq!((again.outcome.offsets, again.duplicate), (at(1, 2), true));
    assert_eq!((next.outcome.offsets, next.duplicate), (at(3, 3), false));
    let gap = {
        let permit = cluster.admit_write(&shard).await.expect("admitted");
        let records = payloads(&["e"]);
        let produce = Produce {
            payloads: &records,
            headers: None,
            producer: Some((7, 5)),
        };
        cluster
            .produce(&broker, &shard, &handle, &permit, produce)
            .await
            .expect("proposed on Raft")
    };
    assert!(matches!(
        gap,
        Err(ProduceError::Broker(
            felix_broker::BrokerError::SequenceGap { expected: 3 }
        ))
    ));
    EngineShards::shutdown(&*shards).await;
}

/// **Keys and headers for a Raft shard are refused until the fleet stores
/// them**, not dropped: the listener asks before charging the quota, and a
/// produce that reaches the group with them is refused too.
#[cfg(feature = "raft")]
#[tokio::test]
async fn keys_and_headers_for_a_raft_shard_wait_for_the_fleet() {
    use crate::shards::raft::tests as fixture;
    use crate::shards::routing::EngineShards;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::broker(root.path()).await;
    broker.record_headers_when(|| true);
    let allowed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = Arc::clone(&allowed);
    broker.allow_raft_record_headers_when(move || gate.load(std::sync::atomic::Ordering::SeqCst));
    let shards = fixture::shards(&broker);
    let orders = fixture::key("orders", 0);
    let assigned = fixture::assigned(orders.clone(), fixture::INCARNATION);
    fixture::take_formed(&shards, assigned).await;
    fixture::until("the term is served", || shards.leading(&orders).is_some()).await;
    let cluster = BrokerCluster::new(
        Arc::new(BrokerAuth::new("http://127.0.0.1:1".to_string())),
        Arc::new(fixture::engine(&shards)),
        None,
        STANDALONE_NODE_ID,
        "127.0.0.1:9092",
    )
    .expect("cluster");
    let shard = ShardRef {
        tenant_id: "t1",
        namespace: "ns",
        stream: "orders",
        shard: 0,
    };
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let payloads = [bytes::Bytes::from_static(b"a")];
    let headers = [Some(bytes::Bytes::from_static(b"block"))];
    let produce = Produce {
        payloads: &payloads,
        headers: Some(&headers),
        producer: Some((7, 0)),
    };

    let asked = cluster.refuses_record_headers(&broker, &shard);
    let permit = cluster.admit_write(&shard).await.expect("admitted");
    let reached = cluster
        .produce(&broker, &shard, &handle, &permit, produce)
        .await
        .expect("proposed on Raft");
    allowed.store(true, std::sync::atomic::Ordering::SeqCst);

    assert_eq!(asked, Some("raft_record_headers"));
    assert!(
        matches!(
            reached,
            Err(ProduceError::Broker(
                felix_broker::BrokerError::RecordHeadersNotEnabled { .. }
            ))
        ),
        "{:?}",
        reached.err()
    );
    assert_eq!(cluster.refuses_record_headers(&broker, &shard), None);
    EngineShards::shutdown(&*shards).await;
}

/// A cluster whose `orders` shard 0 is led by a Raft group on this broker.
#[cfg(feature = "raft")]
struct OnRaft {
    _root: tempfile::TempDir,
    broker: Arc<Broker>,
    shards: Arc<crate::shards::raft::RaftShards>,
    key: ShardKey,
    cluster: BrokerCluster,
    handle: StreamHandle,
}

#[cfg(feature = "raft")]
impl OnRaft {
    const SHARD: ShardRef<'static> = ShardRef {
        tenant_id: "t1",
        namespace: "ns",
        stream: "orders",
        shard: 0,
    };

    async fn start() -> Self {
        use crate::shards::raft::tests as fixture;

        let root = tempfile::tempdir().expect("tempdir");
        let broker = fixture::broker(root.path()).await;
        let shards = fixture::shards(&broker);
        let key = fixture::key("orders", 0);
        let assigned = fixture::assigned(key.clone(), fixture::INCARNATION);
        fixture::take_formed(&shards, assigned).await;
        fixture::until("the term is served", || shards.leading(&key).is_some()).await;
        let cluster = BrokerCluster::new(
            Arc::new(BrokerAuth::new("http://127.0.0.1:1".to_string())),
            Arc::new(fixture::engine(&shards)),
            None,
            STANDALONE_NODE_ID,
            "127.0.0.1:9092",
        )
        .expect("cluster");
        let handle = broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .expect("handle");
        Self {
            _root: root,
            broker,
            shards,
            key,
            cluster,
            handle,
        }
    }

    /// Stream batches handed to the group so far.
    fn proposals(&self) -> u64 {
        let engine = self.shards.engine_for(&self.key).expect("a member here");
        engine.proposals(&self.key)
    }

    /// Hand `values` to the group, without waiting for the answer.
    async fn propose(
        &self,
        values: &[&'static str],
        producer: Option<(u64, u64)>,
    ) -> Result<Proposed, ProduceError> {
        let payloads: Vec<bytes::Bytes> = values.iter().map(|v| bytes::Bytes::from(*v)).collect();
        let permit = self
            .cluster
            .admit_write(&Self::SHARD)
            .await
            .expect("admitted");
        let produce = Produce {
            payloads: &payloads,
            headers: None,
            producer,
        };
        self.cluster
            .produce(&self.broker, &Self::SHARD, &self.handle, &permit, produce)
            .await
            .expect("proposed on Raft")
    }

    async fn produce(
        &self,
        values: &[&'static str],
        producer: Option<(u64, u64)>,
    ) -> felix_broker::IdempotentOutcome {
        let proposed = self.propose(values, producer).await.expect("handed over");
        proposed.await.expect("written")
    }

    /// The payloads the log holds from `from` on.
    async fn log_from(&self, from: u64) -> Vec<bytes::Bytes> {
        let read = self
            .broker
            .read_durable("t1", "ns", "orders", 0, from, 1 << 20);
        let records = read.await.expect("read");
        records.into_iter().map(|record| record.payload).collect()
    }

    async fn stop(self) {
        crate::shards::routing::EngineShards::shutdown(&*self.shards).await;
    }
}

/// **An idempotent producer's records go to the group as one proposal**,
/// not one per record, and re-sends of the whole batch or of an overlap
/// are answered from the log with the original offsets.
#[cfg(feature = "raft")]
#[tokio::test]
async fn an_idempotent_produce_is_one_proposal() {
    let raft = OnRaft::start().await;
    let plain = raft.produce(&["p"], None).await;
    let (base, _) = plain.outcome.offsets.expect("written");
    let at = |first: u64, last: u64| Some((base + first, base + last));

    let before = raft.proposals();
    let first = raft.produce(&["a", "b", "c", "d"], Some((7, 0))).await;
    let after_first = raft.proposals();
    let again = raft.produce(&["a", "b", "c", "d"], Some((7, 0))).await;
    let after_again = raft.proposals();
    let overlap = raft.produce(&["c", "d", "e", "f"], Some((7, 2))).await;
    let after_overlap = raft.proposals();

    assert_eq!(after_first - before, 1, "four records, one proposal");
    assert_eq!((first.outcome.offsets, first.duplicate), (at(1, 4), false));
    assert_eq!(after_again, after_first, "a whole re-send proposes nothing");
    assert_eq!((again.outcome.offsets, again.duplicate), (at(1, 4), true));
    assert_eq!(after_overlap - after_again, 1, "only the new records");
    assert_eq!(
        (overlap.outcome.offsets, overlap.duplicate),
        (at(3, 6), false)
    );
    let held: Vec<_> = ["p", "a", "b", "c", "d", "e", "f"]
        .into_iter()
        .map(bytes::Bytes::from)
        .collect();
    assert_eq!(raft.log_from(base).await, held);
    raft.stop().await;
}

/// **A produce cancelled while it waits for its producer's turn writes all
/// of its records or none.** The turn is taken once for the whole batch,
/// so another of the producer's batches never gets in between its records.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_cancelled_produce_writes_its_records_whole_or_not_at_all() {
    let raft = OnRaft::start().await;
    let plain = raft.produce(&["p"], None).await;
    let (base, _) = plain.outcome.offsets.expect("written");
    let wait = Duration::from_millis(100);

    let held = raft.broker.producer_turn(&raft.handle, 7).await;
    let mut produce = Box::pin(raft.propose(&["a", "b", "c"], Some((7, 0))));
    assert!(tokio::time::timeout(wait, &mut produce).await.is_err());
    // Queued behind the produce's turn.
    let mut next = Box::pin(raft.broker.producer_turn(&raft.handle, 7));
    assert!(tokio::time::timeout(wait, &mut next).await.is_err());
    drop(held);
    // Whatever it got to before it is cancelled.
    let _ = tokio::time::timeout(wait, &mut produce).await;
    drop(produce);
    let next = next.await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let whole: Vec<_> = ["p", "a", "b", "c"]
        .into_iter()
        .map(bytes::Bytes::from)
        .collect();
    loop {
        let log = raft.log_from(base).await;
        if log == whole {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the log holds part of the batch: {log:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(next);
    raft.stop().await;
}

/// **A fetch and a ListOffsets on a Raft shard are bounded by its commit
/// index**, at every consistency level: the bound covers every acknowledged
/// record and a produce moves it, so a consumer sees what it was acknowledged
/// and a log-end query never points below it.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_raft_shard_bounds_kafka_reads_by_its_commit_index() {
    use felix_kafka::Cluster as _;

    let raft = OnRaft::start().await;
    let bound = |raft: &OnRaft| {
        raft.cluster
            .committed_until(&OnRaft::SHARD, &raft.handle)
            .expect("a served shard")
            .expect("a Raft shard is always bounded")
    };
    let first = raft.produce(&["a", "b"], Some((7, 0))).await;
    let (_, last) = first.outcome.offsets.expect("written");
    let after_first = bound(&raft);
    assert!(
        after_first > last,
        "the bound {after_first} does not cover the acknowledged offset {last}"
    );
    let second = raft.produce(&["c"], Some((7, 2))).await;
    let (_, last) = second.outcome.offsets.expect("written");
    assert!(bound(&raft) > last);
    assert!(bound(&raft) >= after_first, "the bound went backwards");
    raft.stop().await;
}

/// A CA, a server certificate it issued for `localhost`, and a client
/// certificate it issued that ends `client_lifetime` from now.
struct Pki {
    ca: rustls::pki_types::CertificateDer<'static>,
    server: (
        rustls::pki_types::CertificateDer<'static>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    client: (
        rustls::pki_types::CertificateDer<'static>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    client_not_after: i64,
}

fn pki(client_lifetime: Duration) -> Pki {
    let ca_key = rcgen::KeyPair::generate().expect("key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("ca");
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let issue = |name: &str, not_after: Option<i64>| {
        let key = rcgen::KeyPair::generate().expect("key");
        let mut params = rcgen::CertificateParams::new(vec![name.to_string()]).expect("params");
        if let Some(not_after) = not_after {
            params.not_after = rcgen::date_time_ymd(1970, 1, 1)
                + Duration::from_secs(u64::try_from(not_after).expect("after 1970"));
        }
        let cert = params.signed_by(&key, &issuer).expect("cert");
        (
            cert.der().clone(),
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
    };
    let now = i64::try_from(felix_common::clock::unix_now().as_secs()).expect("now");
    let client_not_after = now + i64::try_from(client_lifetime.as_secs()).expect("secs");
    Pki {
        ca: ca.der().clone(),
        server: issue("localhost", None),
        client: issue("client", Some(client_not_after)),
        client_not_after,
    }
}

/// An `ApiVersions` v0 request, framed: answered on any authenticated
/// connection.
fn api_versions(correlation_id: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&18i16.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&correlation_id.to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    let mut frame = (body.len() as i32).to_be_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

/// Whether `stream` answers an `ApiVersions` request within two seconds.
async fn answers<S>(stream: &mut S, correlation_id: i32) -> bool
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    if stream
        .write_all(&api_versions(correlation_id))
        .await
        .is_err()
    {
        return false;
    }
    let read = async {
        let mut size = [0u8; 4];
        stream.read_exact(&mut size).await?;
        let mut body = vec![0u8; usize::try_from(i32::from_be_bytes(size)).unwrap_or(0)];
        stream.read_exact(&mut body).await
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), read).await,
        Ok(Ok(_))
    )
}

/// On the anonymous dev switch, a connection whose client presented a
/// certificate closes at its `notAfter`, as an authenticated one does, and
/// is not served after it. One without a certificate stays open.
#[tokio::test]
async fn an_anonymous_connection_closes_when_its_client_certificate_expires() {
    // Long enough for both handshakes and a first request under CI load.
    let pki = pki(Duration::from_secs(4));
    let mut roots = rustls::RootCertStore::empty();
    roots.add(pki.ca.clone()).expect("root");
    let roots = Arc::new(roots);
    let provider = crate::serving::tls::provider();

    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::clone(&roots),
        Arc::clone(&provider),
    )
    .allow_unauthenticated()
    .build()
    .expect("verifier");
    let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(rustls::ALL_VERSIONS)
        .expect("versions")
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![pki.server.0.clone()], pki.server.1.clone_key())
        .expect("server config");
    let auth = Arc::new(
        BrokerAuth::new("http://127.0.0.1:1".to_string())
            .with_client_roots(Some(Arc::clone(&roots))),
    );
    let cluster = Arc::new(
        BrokerCluster::new(
            auth,
            Arc::new(crate::shards::routing::StreamEngine::default()),
            None,
            STANDALONE_NODE_ID,
            "127.0.0.1:9092",
        )
        .expect("cluster"),
    );
    let mut config = listener_config(16);
    config.tls = true;
    config.anonymous_tenant = Some("t1".to_string());
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let listener = KafkaListener::bind(
        &config,
        Some(Arc::new(server)),
        broker,
        Arc::clone(&cluster) as Arc<dyn Cluster>,
        "felix-test".to_string(),
    )
    .await
    .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let shutdown = CancellationToken::new();
    let serving = tokio::spawn(listener.serve(shutdown.clone(), TaskTracker::new()));

    let client_config = |with_cert: bool| {
        let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(rustls::ALL_VERSIONS)
            .expect("versions")
            .with_root_certificates(Arc::clone(&roots));
        Arc::new(if with_cert {
            builder
                .with_client_auth_cert(vec![pki.client.0.clone()], pki.client.1.clone_key())
                .expect("client cert")
        } else {
            builder.with_no_client_auth()
        })
    };
    let connect = |with_cert: bool| {
        let connector = tokio_rustls::TlsConnector::from(client_config(with_cert));
        async move {
            let socket = TcpStream::connect(addr).await.expect("connect");
            connector
                .connect("localhost".try_into().expect("name"), socket)
                .await
                .expect("handshake")
        }
    };
    let mut with_cert = connect(true).await;
    let mut bare = connect(false).await;
    assert!(answers(&mut with_cert, 1).await, "served before expiry");
    assert!(answers(&mut bare, 1).await, "served without a certificate");

    // The principal the switch hands out refuses a request past `notAfter`
    // by itself, before its close has fired.
    let principal = cluster
        .anonymous("t1", &[pki.client.0.to_vec()])
        .expect("principal");
    assert!(!principal.is_revoked());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut byte = [0u8; 1];
    let closed = tokio::time::timeout_at(deadline, with_cert.read(&mut byte)).await;
    assert!(
        matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
        "a connection with a client certificate outlived its notAfter: {closed:?}"
    );
    let now = i64::try_from(felix_common::clock::unix_now().as_secs()).expect("now");
    assert!(
        now >= pki.client_not_after,
        "closed before the certificate ended"
    );
    assert!(principal.is_revoked(), "a request after notAfter is served");
    assert!(!answers(&mut with_cert, 2).await, "served after expiry");
    assert!(
        answers(&mut bare, 2).await,
        "a connection without a certificate closed"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), serving).await;
}

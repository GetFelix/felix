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
    shards.take(&mut fixture::assignments([assigned])).await;
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
    shards.take(&mut fixture::assignments([assigned])).await;
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
        "{reached:?}"
    );
    assert_eq!(cluster.refuses_record_headers(&broker, &shard), None);
    EngineShards::shutdown(&*shards).await;
}

/// **A record refused after an earlier one of its batch failed is answered
/// with the earlier failure.** The later refusal is a sequence gap, which a
/// Kafka producer takes as fatal, when the batch only needs sending again.
#[tokio::test]
async fn a_refusal_behind_a_failed_record_is_answered_with_that_failure() {
    let gap = || ProduceError::Broker(felix_broker::BrokerError::SequenceGap { expected: 1 });
    let lost = || ProduceError::Write(WriteError::LeadershipLost);

    let behind_failure = earlier_failure(
        vec![
            Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = _>>>,
            Box::pin(async move { Err(lost()) }),
        ],
        gap(),
    )
    .await;
    let alone = earlier_failure(vec![std::future::ready(Ok(()))], gap()).await;

    assert!(
        matches!(
            behind_failure,
            ProduceError::Write(WriteError::LeadershipLost)
        ),
        "{behind_failure:?}"
    );
    assert!(
        matches!(
            alone,
            ProduceError::Broker(felix_broker::BrokerError::SequenceGap { .. })
        ),
        "{alone:?}"
    );
}

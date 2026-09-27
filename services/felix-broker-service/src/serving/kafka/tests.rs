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
    }
}

fn cluster() -> BrokerCluster {
    let auth = Arc::new(BrokerAuth::new("http://127.0.0.1:1".to_string()));
    BrokerCluster::new(auth, None, None, STANDALONE_NODE_ID, "127.0.0.1:9092").expect("cluster")
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

#[tokio::test]
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

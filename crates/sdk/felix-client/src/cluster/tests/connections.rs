//! How many connections a [`ClusterClient`] holds to each broker, counted
//! from the brokers' side.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_wire::binary::PublishOwner;
use felix_wire::{AckMode, Message};

use super::stub_broker::StubBroker;
use crate::ClientConfig;
use crate::cluster::{ClusterClient, ReconnectPolicy};
use crate::test_support::{build_server_config, quinn_client_config};

pub(super) fn policy() -> ReconnectPolicy {
    ReconnectPolicy {
        attempts: 3,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        deadline: Some(Duration::from_secs(5)),
    }
}

/// The shipped defaults, so a regression back to a pool per role shows up
/// as the pool sizes it would open.
pub(super) fn default_config(
    cert: rustls::pki_types::CertificateDer<'static>,
) -> Result<ClientConfig> {
    let mut config = ClientConfig::optimized_defaults(quinn_client_config(cert)?);
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some("test-token".to_string());
    Ok(config)
}

fn not_leader(node_id: &str, addr: std::net::SocketAddr) -> Message {
    Message::NotLeader {
        node_id: node_id.to_string(),
        addr: Some(addr.to_string()),
        generation: 1,
    }
}

/// **Every role a broker plays shares one connection to it.** Here broker A
/// is the entry and a redirect target, and broker B is a shard owner, a
/// redirect target and a producer's leader. Each is reached over exactly one
/// connection, carrying the publish and cache workers, the redirected
/// subscribes and the owner's traffic alike.
#[tokio::test]
async fn one_connection_per_broker_under_light_mixed_role_use() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    // Bound before either script is written, since each names the other.
    let b_addr = Arc::new(std::sync::OnceLock::new());
    let a_addr = Arc::new(std::sync::OnceLock::new());
    let a = StubBroker::start_with(server_config.clone(), {
        let b_addr = Arc::clone(&b_addr);
        move |id| match id {
            0 => not_leader("b", *b_addr.get().expect("b bound")),
            id => Message::PublishOk {
                request_id: id,
                offset: None,
            },
        }
    })?;
    a_addr.set(a.addr).expect("a once");
    let b = StubBroker::start_with(server_config, {
        let a_addr = Arc::clone(&a_addr);
        move |id| match id {
            0 => not_leader("a", *a_addr.get().expect("a bound")),
            id => Message::PublishOk {
                request_id: id,
                offset: None,
            },
        }
    })?;
    b_addr.set(b.addr).expect("b once");

    let cluster = Arc::new(
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?,
    );

    // Entry: publish through A.
    cluster
        .publish(
            "t1",
            "default",
            "orders",
            b"one".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    // Owner: B owns a shard, and the next publish for it goes there.
    let shard = ("t1".into(), "default".into(), "orders".into(), 0);
    cluster
        .remember_owner(
            shard,
            PublishOwner {
                node_id: "b".to_string(),
                addr: Some(b.addr.to_string()),
                generation: 1,
            },
        )
        .await;
    cluster
        .publish(
            "t1",
            "default",
            "orders",
            b"two".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    assert_eq!(b.publishes(), 1, "the second publish went to the owner");
    // Redirects: A names B, B names A, and A is not asked twice in one
    // attempt. A cycle is a cluster that has not settled, so it is retried.
    assert!(cluster.subscribe("t1", "default", "orders").await.is_err());
    assert_eq!(a.subscribes() + b.subscribes(), 3 * policy().attempts);
    // Leader: what an idempotent producer reaches when refused.
    let leader = cluster.connect_to(b.addr).await?;
    assert!(leader.is_usable());

    assert_eq!(a.live_connections(), 1, "one connection to A");
    assert_eq!(b.live_connections(), 1, "one connection to B");
    assert_eq!(
        cluster.connections_per_node().await,
        vec![(a.addr.min(b.addr), 1), (a.addr.max(b.addr), 1)]
    );
    Ok(())
}

/// **Load grows a broker's connections to the ceiling and no further.**
/// Subscriptions each hold a stream, so enough of them at once saturate a
/// connection's stream budget and a neighbour opens; past the ceiling they
/// share what is there.
#[tokio::test]
async fn connections_grow_to_the_ceiling_under_concurrent_load() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let broker = StubBroker::start_with(server_config, |_| Message::Subscribed {
        subscription_id: 0,
        start_offset: None,
        live_offset: None,
        queue_capacity: None,
    })?;
    let mut config = default_config(cert)?;
    config.publish_conn_pool = 1;
    config.publish_streams_per_conn = 1;
    config.cache_conn_pool = 1;
    config.cache_streams_per_conn = 1;
    config.cluster_conn_pool = 3;
    config.cluster_streams_per_conn = 4;
    let cluster = Arc::new(
        ClusterClient::connect_with_policy(&[broker.addr], "localhost", config, policy()).await?,
    );
    assert_eq!(broker.live_connections(), 1);

    let mut opening = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let cluster = Arc::clone(&cluster);
        opening.spawn(async move { cluster.subscribe("t1", "default", "orders").await });
    }
    let mut subscriptions = Vec::new();
    while let Some(opened) = opening.join_next().await {
        subscriptions.push(opened??);
    }

    assert_eq!(broker.live_connections(), 3, "grown to the ceiling");
    assert_eq!(broker.accepted_connections(), 3, "and never past it");
    assert_eq!(cluster.connections_per_node().await, vec![(broker.addr, 3)]);
    Ok(())
}

/// **A broker whose connection dies is rebuilt, and no other broker is
/// touched.** The dead client is replaced on the next use, over a fresh
/// connection, while the client for the other broker is the same one it was
/// and still publishes.
#[tokio::test]
async fn a_dead_broker_connection_is_rebuilt_without_touching_another() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), |id| Message::PublishOk {
        request_id: id,
        offset: None,
    })?;
    let b = StubBroker::start_with(server_config, |id| Message::PublishOk {
        request_id: id,
        offset: None,
    })?;
    let cluster =
        ClusterClient::connect_with_policy(&[a.addr], "localhost", default_config(cert)?, policy())
            .await?;
    let entry = cluster.client().await;
    let before = cluster.connect_to(b.addr).await?;

    b.drop_connections();
    tokio::time::timeout(Duration::from_secs(5), async {
        while before.is_usable() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(
        before
            .publisher()
            .await?
            .publish(
                "t1",
                "default",
                "orders",
                b"lost".to_vec(),
                AckMode::PerMessage
            )
            .await
            .is_err(),
        "work on the dead connection fails"
    );

    let after = cluster.connect_to(b.addr).await?;
    assert!(!Arc::ptr_eq(&before, &after), "B's client was rebuilt");
    assert_eq!(b.accepted_connections(), 2);
    assert_eq!(b.live_connections(), 1);
    after
        .publisher()
        .await?
        .publish(
            "t1",
            "default",
            "orders",
            b"b".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    assert!(Arc::ptr_eq(&entry, &cluster.connect_to(a.addr).await?));
    assert_eq!(a.accepted_connections(), 1, "A was left alone");
    cluster
        .publish(
            "t1",
            "default",
            "orders",
            b"a".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    Ok(())
}

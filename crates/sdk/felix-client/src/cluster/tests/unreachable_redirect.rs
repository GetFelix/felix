//! A redirect to a broker that will not connect is not followed again.

use std::time::{Duration, Instant};

use anyhow::Result;
use felix_wire::Message;

use super::connections::policy;
use super::stub_broker::StubBroker;
use crate::cluster::Client;
use crate::test_support::{build_client_config_with_overrides, build_server_config};

/// **A group call whose redirect names a dead broker is answered by another
/// one.** The entry broker keeps naming the dead owner, as brokers do while
/// their routes are stale. The client must not give up, or retry the dead
/// address on every call.
#[tokio::test]
#[serial_test::serial]
async fn a_dead_redirect_target_is_skipped_for_another_broker() -> Result<()> {
    let dead = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
        socket.local_addr()?
    };
    let (server_config, cert) = build_server_config()?;
    let entry = StubBroker::start_with(server_config.clone(), move |_| Message::NotLeader {
        node_id: "dead".into(),
        addr: Some(dead.to_string()),
        generation: 1,
    })?;
    let other =
        StubBroker::start_with(server_config, |request_id| Message::CacheOk { request_id })?;
    let cluster = Client::connect_with_policy(
        &[entry.addr, other.addr],
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        policy(),
    )
    .await?;

    cluster
        .group_ack("t1", "default", "orders", 0, "workers", 1)
        .await?;
    assert_eq!(other.group_requests(), 1);

    // Another shard has no cached route, so the entry broker is asked again
    // and names the dead address again: the client must not dial it.
    cluster.group_routes.write().await.clear();
    let entry_before = entry.group_requests();
    let started = Instant::now();
    cluster
        .group_ack("t1", "default", "orders", 0, "workers", 2)
        .await?;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(entry.group_requests() > entry_before, "entry saw the retry");
    assert_eq!(other.group_requests(), 2);
    Ok(())
}

/// **A marked-down target is dialled once when no other broker is left.** One
/// connect timeout during an election must not lock the new leader out.
#[tokio::test]
#[serial_test::serial]
async fn a_down_target_is_dialled_when_nothing_else_is_left() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let leader = StubBroker::start_with(server_config.clone(), |request_id| Message::CacheOk {
        request_id,
    })?;
    let leader_addr = leader.addr;
    let entry = StubBroker::start_with(server_config, move |_| Message::NotLeader {
        node_id: "leader".into(),
        addr: Some(leader_addr.to_string()),
        generation: 1,
    })?;
    let cluster = Client::connect_with_policy(
        &[entry.addr],
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        policy(),
    )
    .await?;
    cluster
        .down_redirects
        .lock()
        .unwrap()
        .insert(leader_addr, Instant::now());

    cluster
        .group_ack("t1", "default", "orders", 0, "workers", 1)
        .await?;
    assert_eq!(leader.group_requests(), 1);
    Ok(())
}

/// **When the owner stays unreachable the error is coded**, so the paced
/// retry loops ask again instead of giving up.
#[tokio::test]
#[serial_test::serial]
async fn an_unreachable_owner_is_a_coded_retryable_error() -> Result<()> {
    let dead = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
        socket.local_addr()?
    };
    let (server_config, cert) = build_server_config()?;
    let entry = StubBroker::start_with(server_config, move |_| Message::NotLeader {
        node_id: "dead".into(),
        addr: Some(dead.to_string()),
        generation: 1,
    })?;
    let cluster = Client::connect_with_policy(
        &[entry.addr],
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        policy(),
    )
    .await?;
    let err = cluster
        .follow_shard(
            ("t1".into(), "default".into(), "orders".into(), 0),
            None,
            |client| async move {
                client
                    .group_ack("t1", "default", "orders", 0, "workers", 1)
                    .await
            },
        )
        .await
        .unwrap_err();
    let coded = err.downcast_ref::<crate::BrokerError>().expect("coded");
    assert_eq!(coded.code, felix_wire::ErrorCode::NotLeader);
    Ok(())
}

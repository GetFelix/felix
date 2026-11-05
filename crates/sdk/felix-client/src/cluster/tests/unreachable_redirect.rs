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

    // The dead address is remembered: the second call does not wait on it.
    let started = Instant::now();
    cluster
        .group_ack("t1", "default", "orders", 0, "workers", 2)
        .await?;
    assert!(started.elapsed() < Duration::from_secs(1));
    Ok(())
}

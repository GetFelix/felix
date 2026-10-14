//! What a reconnect replaces, and how long dialling dead addresses takes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use felix_wire::Message;

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::ClusterClient;
use crate::test_support::build_server_config;

fn publish_ok(id: u64) -> Message {
    Message::PublishOk {
        request_id: id,
        offset: None,
    }
}

/// **A reconnect for a client already replaced does nothing.** Every
/// operation that failed on the same broker asks for a reconnect; only the
/// first may replace the client, or each one after it throws away the
/// working replacement and dials again.
#[tokio::test]
async fn a_reconnect_for_a_client_already_replaced_does_nothing() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), publish_ok)?;
    let b = StubBroker::start_with(server_config, publish_ok)?;
    let cluster = ClusterClient::connect_with_policy(
        &[a.addr, b.addr],
        "localhost",
        default_config(cert)?,
        policy(),
    )
    .await?;
    let failed = cluster.client().await;
    assert_eq!(failed.dialled(), a.addr);

    cluster.reconnect(&failed).await?;
    let replacement = cluster.client().await;
    assert_eq!(replacement.dialled(), b.addr);

    cluster.reconnect(&failed).await?;
    assert!(
        Arc::ptr_eq(&replacement, &cluster.client().await),
        "the replacement was kept"
    );
    assert_eq!(b.accepted_connections(), 1);
    assert_eq!(a.accepted_connections(), 1, "A was not dialled again");
    Ok(())
}

/// **Addresses that drop every packet do not each cost a QUIC idle
/// timeout.** Three of them come before the broker that answers; dialled one
/// after another that is three idle timeouts before it is tried.
#[tokio::test]
async fn silent_addresses_do_not_each_cost_an_idle_timeout() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let live = StubBroker::start_with(server_config, publish_ok)?;
    // Bound and never read: a handshake sent here is never answered.
    let silent: Vec<std::net::UdpSocket> = (0..3)
        .map(|_| std::net::UdpSocket::bind("127.0.0.1:0"))
        .collect::<std::io::Result<_>>()?;
    let mut seeds = silent
        .iter()
        .map(std::net::UdpSocket::local_addr)
        .collect::<std::io::Result<Vec<_>>>()?;
    seeds.push(live.addr);

    let started = Instant::now();
    let cluster =
        ClusterClient::connect_with_policy(&seeds, "localhost", default_config(cert)?, policy())
            .await?;
    let took = started.elapsed();
    assert_eq!(cluster.client().await.dialled(), live.addr);
    assert!(
        took < Duration::from_secs(4),
        "reaching the live broker took {took:?}"
    );
    Ok(())
}

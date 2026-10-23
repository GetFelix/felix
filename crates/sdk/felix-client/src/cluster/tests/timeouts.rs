//! A stalled broker fails the caller with an error instead of hanging it.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_wire::{AckMode, Message};

use super::connections::{default_config, policy};
use super::stub_broker::StubBroker;
use crate::cluster::ClusterClient;
use crate::test_support::build_server_config;

/// Outer bound so a regression fails the test instead of hanging it.
const TEST_LIMIT: Duration = Duration::from_secs(20);

fn ok(id: u64) -> Message {
    Message::PublishOk {
        request_id: id,
        offset: None,
    }
}

/// **`finish` against a broker that never settles a publish errors.** The
/// batch is received and its ack held, as with a commit that cannot settle.
#[tokio::test]
async fn finish_errors_when_the_broker_never_acks() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let broker = StubBroker::start_with(server_config, ok)?;
    let mut config = default_config(cert)?;
    config.finish_timeout = Duration::from_millis(300);
    let cluster = Arc::new(
        ClusterClient::connect_with_policy(&[broker.addr], "localhost", config, policy()).await?,
    );
    broker.hold_acks();
    let publisher = cluster.client().await.publisher_handle();
    let publish = tokio::spawn(async move {
        publisher
            .publish("t1", "default", "orders", b"x".to_vec(), AckMode::PerBatch)
            .await
    });
    while broker.publishes() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let finished = tokio::time::timeout(TEST_LIMIT, cluster.finish())
        .await
        .expect("finish hung");
    let err = finished.expect_err("finish must fail");
    assert!(format!("{err:#}").contains("timed out"), "{err:#}");
    // The aborted stream fails the publish still waiting for its ack.
    let waited = tokio::time::timeout(TEST_LIMIT, publish)
        .await
        .expect("pending publish hung")?;
    assert!(waited.is_err(), "the unconfirmed publish errors");
    Ok(())
}

/// **A dial to a broker that never answers the handshake errors.** A bound
/// UDP socket that reads nothing stands in for an unreachable broker.
#[tokio::test]
async fn connect_errors_when_the_broker_never_answers() -> Result<()> {
    let (_server_config, cert) = build_server_config()?;
    let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let mut config = default_config(cert)?;
    config.connect_timeout = Duration::from_millis(300);

    let started = std::time::Instant::now();
    let connected = tokio::time::timeout(
        TEST_LIMIT,
        ClusterClient::connect_with_policy(&[silent.local_addr()?], "localhost", config, policy()),
    )
    .await
    .expect("connect hung");
    let err = connected.err().expect("connect must fail");
    assert!(format!("{err:#}").contains("timed out after"), "{err:#}");
    // The transport's own handshake limit is much longer than this.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    Ok(())
}

/// **Brokers that all stall cost one `finish_timeout` between them**, not
/// one each.
#[tokio::test]
async fn finish_waits_on_stalled_brokers_together() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let a = StubBroker::start_with(server_config.clone(), ok)?;
    let b = StubBroker::start_with(server_config, ok)?;
    let mut config = default_config(cert)?;
    config.finish_timeout = Duration::from_millis(1500);
    let cluster = Arc::new(
        ClusterClient::connect_with_policy(&[a.addr, b.addr], "localhost", config, policy())
            .await?,
    );
    a.hold_acks();
    b.hold_acks();
    for broker in [&a, &b] {
        let publisher = cluster.connect_to(broker.addr).await?.publisher_handle();
        tokio::spawn(async move {
            publisher
                .publish("t1", "default", "orders", b"x".to_vec(), AckMode::PerBatch)
                .await
        });
        while broker.publishes() < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    let started = std::time::Instant::now();
    let finished = tokio::time::timeout(TEST_LIMIT, cluster.finish())
        .await
        .expect("finish hung");
    assert!(finished.is_err());
    assert!(
        started.elapsed() < Duration::from_millis(2700),
        "finished one broker after another: {:?}",
        started.elapsed()
    );
    Ok(())
}

/// **Publishes are not queued behind a reconnect's dial.** The dead address
/// is tried first and takes the whole connect timeout to give up.
#[tokio::test]
async fn reading_the_client_does_not_wait_for_a_reconnect_dial() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let live = StubBroker::start_with(server_config, ok)?;
    let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let mut config = default_config(cert)?;
    config.connect_timeout = Duration::from_secs(2);
    let cluster = Arc::new(
        ClusterClient::connect_with_policy(
            &[live.addr, silent.local_addr()?],
            "localhost",
            config,
            policy(),
        )
        .await?,
    );
    let failed = cluster.client().await;
    assert_eq!(failed.dialled(), live.addr);

    let reconnecting = {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move { cluster.reconnect(&failed).await })
    };
    // Past the point where the reconnect is dialling the silent address.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let started = std::time::Instant::now();
    tokio::time::timeout(TEST_LIMIT, cluster.client())
        .await
        .expect("client() hung");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the client lock was held across the dial: {:?}",
        started.elapsed()
    );
    reconnecting.await??;
    Ok(())
}

/// **A reconnect tries every address**, however many dead ones come first.
#[tokio::test]
async fn reconnect_reaches_a_live_broker_behind_dead_addresses() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let live = StubBroker::start_with(server_config, ok)?;
    let dead: Vec<std::net::UdpSocket> = (0..3)
        .map(|_| std::net::UdpSocket::bind("127.0.0.1:0"))
        .collect::<std::io::Result<_>>()?;
    // The live broker first, so the setup connect does not wait on a dead one.
    let mut seeds = vec![live.addr];
    seeds.extend(dead.iter().map(|s| s.local_addr().unwrap()));
    let mut config = default_config(cert)?;
    config.connect_timeout = Duration::from_secs(1);
    let cluster = ClusterClient::connect_with_policy(&seeds, "localhost", config, policy()).await?;
    let failed = cluster.client().await;
    assert_eq!(failed.dialled(), live.addr);

    // The live broker is tried last, after the three dead ones time out.
    tokio::time::timeout(TEST_LIMIT, cluster.reconnect(&failed))
        .await
        .expect("reconnect hung")?;
    assert_eq!(cluster.client().await.dialled(), live.addr);
    assert!(!Arc::ptr_eq(&failed, &cluster.client().await));
    Ok(())
}

/// **`bounded`, which guards the topology, shard-count and subscribe
/// exchanges, fails a step the broker never answers.**
#[tokio::test]
async fn bounded_fails_a_step_that_never_finishes() -> Result<()> {
    let stalled = tokio::time::timeout(
        TEST_LIMIT,
        crate::client::discovery::bounded_by(
            Duration::from_millis(200),
            "read the answer",
            std::future::pending::<Result<()>>(),
        ),
    )
    .await
    .expect("bounded hung");
    let err = stalled.expect_err("must time out");
    assert!(format!("{err:#}").contains("timed out after"), "{err:#}");
    Ok(())
}

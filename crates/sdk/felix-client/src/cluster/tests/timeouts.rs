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
    let _publish = tokio::spawn(async move {
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

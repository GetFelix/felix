//! Cache and group calls obey the client's `ReconnectPolicy`: the attempt
//! limit and the deadline, as publishes do. Against a stub that refuses every
//! request.

use std::time::{Duration, Instant};

use anyhow::Result;
use felix_wire::{ErrorCode, Message};

use super::stub_broker::StubBroker;
use crate::BrokerError;
use crate::cluster::{ClusterClient, ReconnectPolicy};
use crate::test_support::build_client_config_with_overrides;

fn refusal(code: ErrorCode) -> Message {
    Message::Error {
        message: format!("refused with {code}"),
        retry: Some(code.default_retry()),
        code: Some(code),
        detail: None,
    }
}

async fn refused_by(
    code: ErrorCode,
    policy: ReconnectPolicy,
) -> Result<(StubBroker, ClusterClient)> {
    let (broker, cert) = StubBroker::start(move |_| refusal(code.clone()))?;
    let cluster = ClusterClient::connect_with_policy(
        &[broker.addr],
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        policy,
    )
    .await?;
    Ok((broker, cluster))
}

fn limited(attempts: usize) -> ReconnectPolicy {
    ReconnectPolicy {
        attempts,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        deadline: None,
    }
}

fn bounded() -> ReconnectPolicy {
    ReconnectPolicy {
        attempts: 10_000,
        backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(100),
        deadline: Some(Duration::from_millis(600)),
    }
}

/// **A cache call stops at the attempt limit.** Every try is refused with a
/// code that says nothing was applied, so the policy's attempts are all it
/// spends, write or read.
#[tokio::test]
#[serial_test::serial]
async fn a_cache_call_stops_at_the_attempt_limit() -> Result<()> {
    let (broker, cluster) = refused_by(ErrorCode::ShardUnavailable, limited(3)).await?;
    let err = cluster
        .cache_put("t1", "default", "sessions", "alice", "v".into(), None)
        .await
        .expect_err("every try is refused");
    assert!(err.is::<BrokerError>(), "{err:#}");
    assert_eq!(broker.cache_requests(), 3);

    let err = cluster
        .cache_get("t1", "default", "sessions", "alice")
        .await
        .expect_err("every try is refused");
    assert!(err.is::<BrokerError>(), "{err:#}");
    assert_eq!(broker.cache_requests(), 6);
    Ok(())
}

/// **A cache call stops at the deadline with attempts to spare.**
#[tokio::test]
#[serial_test::serial]
async fn a_cache_call_stops_at_the_deadline() -> Result<()> {
    let (broker, cluster) = refused_by(ErrorCode::ShardUnavailable, bounded()).await?;
    let started = Instant::now();
    let err = cluster
        .cache_put("t1", "default", "sessions", "alice", "v".into(), None)
        .await
        .expect_err("every try is refused");
    assert!(err.is::<BrokerError>(), "{err:#}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}: the deadline did not stop the retries",
        started.elapsed()
    );
    assert!(
        broker.cache_requests() > 1,
        "the deadline left room to retry"
    );
    Ok(())
}

/// **A write whose outcome is unknown is not sent again**, whatever attempts
/// remain.
#[tokio::test]
#[serial_test::serial]
async fn a_cache_write_with_an_unknown_outcome_is_sent_once() -> Result<()> {
    let (broker, cluster) = refused_by(ErrorCode::QuorumTimeout, limited(5)).await?;
    cluster
        .cache_put("t1", "default", "sessions", "alice", "v".into(), None)
        .await
        .expect_err("refused");
    assert_eq!(broker.cache_requests(), 1);
    Ok(())
}

/// **A group call stops at the attempt limit.**
#[tokio::test]
#[serial_test::serial]
async fn a_group_call_stops_at_the_attempt_limit() -> Result<()> {
    let (broker, cluster) = refused_by(ErrorCode::ShardUnavailable, limited(3)).await?;
    let err = cluster
        .group_ack("t1", "default", "orders", 0, "workers", 1)
        .await
        .expect_err("every try is refused");
    assert!(err.is::<BrokerError>(), "{err:#}");
    assert_eq!(broker.group_requests(), 3);
    Ok(())
}

/// **A group call stops at the deadline with attempts to spare.**
#[tokio::test]
#[serial_test::serial]
async fn a_group_call_stops_at_the_deadline() -> Result<()> {
    let (broker, cluster) = refused_by(ErrorCode::ShardUnavailable, bounded()).await?;
    let started = Instant::now();
    let err = cluster
        .group_ack("t1", "default", "orders", 0, "workers", 1)
        .await
        .expect_err("every try is refused");
    assert!(err.is::<BrokerError>(), "{err:#}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}: the deadline did not stop the retries",
        started.elapsed()
    );
    assert!(
        broker.group_requests() > 1,
        "the deadline left room to retry"
    );
    Ok(())
}

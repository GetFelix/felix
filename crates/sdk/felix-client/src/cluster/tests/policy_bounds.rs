//! Cache and group calls obey the client's `ReconnectPolicy`: the attempt
//! limit and the deadline, as publishes do. Against a stub that refuses every
//! request.

use std::time::{Duration, Instant};

use anyhow::Result;
use felix_wire::{ErrorCode, Message};

use super::stub_broker::StubBroker;
use crate::BrokerError;
use crate::cluster::{Client, ReconnectPolicy};
use crate::test_support::build_client_config_with_overrides;

fn refusal(code: ErrorCode) -> Message {
    Message::Error {
        message: format!("refused with {code}"),
        retry: Some(code.default_retry()),
        code: Some(code),
        detail: None,
    }
}

async fn refused_by(code: ErrorCode, policy: ReconnectPolicy) -> Result<(StubBroker, Client)> {
    let (broker, cert) = StubBroker::start(move |_| refusal(code.clone()))?;
    let cluster = Client::connect_with_policy(
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

/// **A call over every shard of a stream spends one deadline, not one per
/// shard.** Each of the stub's four shards answers in 250 ms, inside the 600 ms
/// deadline on its own; together they are not.
#[tokio::test]
#[serial_test::serial]
async fn a_stream_wide_group_call_shares_one_deadline() -> Result<()> {
    let (broker, cert) = StubBroker::start(|_| {
        std::thread::sleep(Duration::from_millis(250));
        Message::Ok
    })?;
    let cluster = Client::connect_with_policy(
        &[broker.addr],
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        bounded(),
    )
    .await?;
    let cluster = std::sync::Arc::new(cluster);
    let started = Instant::now();
    cluster
        .group_delete_stream("t1", "default", "orders", "workers")
        .await
        .expect_err("the shards together outlast the deadline");

    let took = started.elapsed();
    assert!(
        took < Duration::from_millis(1000),
        "took {took:?} against a 600ms deadline"
    );
    assert!(broker.group_requests() < 4, "every shard was still asked");
    Ok(())
}

/// **A cached owner's refusal does not fall through to the entry broker once
/// the deadline has passed.**
#[tokio::test]
#[serial_test::serial]
async fn a_cache_reroute_stops_at_the_deadline() -> Result<()> {
    let (server_config, cert) = crate::test_support::build_server_config()?;
    let owner = StubBroker::start_with(server_config.clone(), |_| {
        refusal(ErrorCode::ShardUnavailable)
    })?;
    let entry = StubBroker::start_with(server_config, |_| Message::Ok)?;
    entry.set_cache_owners(vec![felix_wire::ShardOwner {
        shard: 0,
        node_id: Some("broker-2".into()),
        addr: Some(owner.addr.to_string()),
        generation: 1,
        unavailable: None,
    }]);
    let policy = ReconnectPolicy {
        attempts: 1,
        deadline: Some(Duration::from_millis(1)),
        ..limited(1)
    };
    let cluster = Client::connect_with_policy(
        &[entry.addr],
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        policy,
    )
    .await?;
    cluster
        .cache_put("t1", "default", "sessions", "alice", "v".into(), None)
        .await
        .expect_err("the owner refused");
    assert_eq!(owner.cache_requests(), 1);
    assert_eq!(entry.cache_requests(), 0, "fell through past the deadline");
    Ok(())
}

/// **A redirect is not followed once the deadline has passed.**
#[tokio::test]
#[serial_test::serial]
async fn a_group_redirect_stops_at_the_deadline() -> Result<()> {
    let (server_config, cert) = crate::test_support::build_server_config()?;
    let leader = StubBroker::start_with(server_config.clone(), |_| Message::Ok)?;
    let leader_addr = leader.addr;
    // Slow enough that the deadline has passed by the time it answers.
    let entry = StubBroker::start_with(server_config, move |_| {
        std::thread::sleep(Duration::from_millis(30));
        Message::NotLeader {
            node_id: "leader".into(),
            addr: Some(leader_addr.to_string()),
            generation: 1,
        }
    })?;
    let policy = ReconnectPolicy {
        deadline: Some(Duration::from_millis(10)),
        ..limited(1)
    };
    let cluster = Client::connect_with_policy(
        &[entry.addr],
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        policy,
    )
    .await?;
    cluster
        .group_ack("t1", "default", "orders", 0, "workers", 1)
        .await
        .expect_err("redirected past the deadline");
    assert_eq!(entry.group_requests(), 1);
    assert_eq!(leader.group_requests(), 0, "followed past the deadline");
    Ok(())
}

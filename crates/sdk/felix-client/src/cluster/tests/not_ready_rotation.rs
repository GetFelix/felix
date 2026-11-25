//! A shard that one broker keeps answering `not_ready` is asked of the other
//! brokers in the book, on every path that waits for a shard to open.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::{ErrorCode, ErrorDetail, Message, StartPosition};

use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::Client;
use crate::cluster::ReconnectPolicy;
use crate::cluster::sharded::ShardEvent;
use crate::test_support::{build_client_config_with_overrides, build_server_config};

fn policy() -> ReconnectPolicy {
    ReconnectPolicy {
        attempts: 5,
        backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        deadline: None,
    }
}

fn not_ready() -> Message {
    Message::Error {
        message: "not ready".into(),
        retry: Some(ErrorCode::ShardUnavailable.default_retry()),
        code: Some(ErrorCode::ShardUnavailable),
        detail: Some(ErrorDetail {
            reason: Some("not_ready".into()),
            ..ErrorDetail::default()
        }),
    }
}

fn fenced() -> Message {
    Message::Error {
        message: "fenced".into(),
        retry: Some(ErrorCode::ShardUnavailable.default_retry()),
        code: Some(ErrorCode::ShardUnavailable),
        detail: Some(ErrorDetail {
            reason: Some("fenced".into()),
            ..ErrorDetail::default()
        }),
    }
}

fn subscribed() -> Message {
    Message::Subscribed {
        subscription_id: 0,
        start_offset: Some(5),
        live_offset: Some(5),
        queue_capacity: None,
    }
}

fn event(offset: u64) -> Message {
    Message::Event {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        payload: offset.to_string().into_bytes(),
        offset: Some(offset),
    }
}

async fn client(
    seeds: &[&StubBroker],
    cert: rustls::pki_types::CertificateDer<'static>,
) -> Result<Arc<Client>> {
    let addrs: Vec<_> = seeds.iter().map(|stub| stub.addr).collect();
    Ok(Arc::new(
        Client::connect_with_policy(
            &addrs,
            "localhost",
            build_client_config_with_overrides(cert, 0)?,
            policy(),
        )
        .await?,
    ))
}

/// A broker that serves the first `served` subscribes and answers `not_ready`
/// after, delivering `delivered` on each of those.
fn serves_then_not_ready(
    config: quinn::ServerConfig,
    served: usize,
    delivered: u64,
) -> Result<StubBroker> {
    let asked = AtomicUsize::new(0);
    let stub = StubBroker::start_with(config, move |_| {
        if asked.fetch_add(1, Ordering::SeqCst) < served {
            subscribed()
        } else {
            not_ready()
        }
    })?;
    stub.set_events(move |_, _| vec![event(delivered)]);
    Ok(stub)
}

/// A broker that serves every subscribe, delivering the offset it was asked
/// to resume at.
fn serving(config: quinn::ServerConfig) -> Result<StubBroker> {
    let stub = StubBroker::start_with(config, |_| subscribed())?;
    stub.set_events(|_, start| match start {
        Some(StartPosition::Offset(at)) => vec![event(at)],
        _ => Vec::new(),
    });
    Ok(stub)
}

/// **A broker that cannot be dialled is stepped over, not retried.** With
/// the book [not ready, down, serving], the rotation must reach the third
/// broker instead of falling back to the first after the connect failure.
#[tokio::test]
#[serial_test::serial]
async fn a_down_broker_in_the_rotation_is_stepped_over() -> Result<()> {
    let (config, cert) = build_server_config()?;
    let stuck = StubBroker::start_with(config.clone(), |_| not_ready())?;
    let down = StubBroker::start_with(config.clone(), |_| not_ready())?;
    let leader = serving(config)?;
    let cluster = client(&[&stuck, &down, &leader], cert).await?;
    down.shut_down();

    cluster
        .subscribe("t1", "default", "orders")
        .await
        .context("the third broker serves the shard")?;

    assert_eq!(leader.subscribes(), 1);
    assert_eq!(down.subscribes(), 0);
    Ok(())
}

/// **A redirect target that lost the shard sends the ask back to the broker
/// the attempt started from.** Not to the entry broker, which is the one
/// that is not ready.
#[tokio::test]
#[serial_test::serial]
async fn a_stale_redirect_goes_back_to_where_the_attempt_started() -> Result<()> {
    let (config, cert) = build_server_config()?;
    let stuck = StubBroker::start_with(config.clone(), |_| not_ready())?;
    let old_owner = StubBroker::start_with(config.clone(), |_| fenced())?;
    let old_addr = old_owner.addr.to_string();
    let asked = AtomicUsize::new(0);
    let second = StubBroker::start_with(config, move |_| {
        if asked.fetch_add(1, Ordering::SeqCst) == 0 {
            Message::NotLeader {
                node_id: "broker-old".into(),
                addr: Some(old_addr.clone()),
                generation: 1,
            }
        } else {
            subscribed()
        }
    })?;
    let cluster = client(&[&stuck, &second], cert).await?;

    cluster
        .subscribe("t1", "default", "orders")
        .await
        .context("opened through the second broker")?;

    assert_eq!(stuck.subscribes(), 1, "the entry broker is not asked again");
    assert_eq!(second.subscribes(), 2);
    assert_eq!(old_owner.subscribes(), 1);
    Ok(())
}

/// **A followed subscription that reopens after losing its connection does not
/// stay on brokers that answer `not_ready`.** Two of three brokers keep
/// refusing; the third serves, and is asked to resume after the last record.
#[tokio::test]
#[serial_test::serial]
async fn a_followed_subscription_resumes_past_brokers_that_are_not_ready() -> Result<()> {
    let (config, cert) = build_server_config()?;
    let entry = serves_then_not_ready(config.clone(), 1, 5)?;
    let stuck = StubBroker::start_with(config.clone(), |_| not_ready())?;
    let leader = serving(config)?;
    let cluster = client(&[&entry, &stuck, &leader], cert).await?;
    let mut feed = cluster.subscribe("t1", "default", "orders").await?;
    let first = feed.next_event().await?.context("ended")?;
    assert_eq!(first.offset, Some(5));

    entry.drop_connections();

    let resumed = tokio::time::timeout(Duration::from_secs(20), feed.next_event())
        .await
        .context("never resumed on the third broker")??
        .context("ended")?;
    assert_eq!(resumed.offset, Some(6));
    assert_eq!(
        leader.subscribe_starts()[0].1,
        Some(StartPosition::Offset(6))
    );
    Ok(())
}

/// The same for every shard of a sharded subscription.
#[tokio::test]
#[serial_test::serial]
async fn a_sharded_subscription_resumes_past_brokers_that_are_not_ready() -> Result<()> {
    let (config, cert) = build_server_config()?;
    let entry = serves_then_not_ready(config.clone(), STREAM_SHARDS as usize, 5)?;
    let stuck = StubBroker::start_with(config.clone(), |_| not_ready())?;
    let leader = serving(config)?;
    let cluster = client(&[&entry, &stuck, &leader], cert).await?;
    let mut subscription = cluster
        .subscribe_sharded("t1", "default", "orders", Some(StartPosition::Latest))
        .await?;
    let mut shards = 0;
    while shards < STREAM_SHARDS {
        if let Some(ShardEvent::Record { .. }) = subscription.next().await {
            shards += 1;
        }
    }

    entry.drop_connections();

    let mut resumed = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while resumed.len() < STREAM_SHARDS as usize {
            if let Some(ShardEvent::Record { shard, event }) = subscription.next().await {
                resumed.push((shard, event.offset));
            }
        }
    })
    .await
    .context("a shard never resumed on the third broker")?;
    assert!(resumed.iter().all(|(_, offset)| *offset == Some(6)));
    Ok(())
}

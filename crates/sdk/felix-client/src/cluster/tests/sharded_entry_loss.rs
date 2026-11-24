//! A sharded subscription whose entry broker dies under every shard's feed.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::{Message, StartPosition};

use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::Client;
use crate::cluster::ReconnectPolicy;
use crate::cluster::sharded::{ShardEvent, ShardedSubscription};
use crate::test_support::{build_client_config_with_overrides, build_server_config};

/// Where every shard's subscription began.
const STARTED: u64 = 3;

/// A broker serving every shard. Resumed at an offset, a shard delivers that
/// one record, standing in for what was written while its feed was down.
/// Opened at the tail, it delivers [`STARTED`] when `deliver_first` is set.
fn broker(server_config: quinn::ServerConfig, deliver_first: bool) -> Result<StubBroker> {
    let stub = StubBroker::start_with(server_config, |_| Message::Subscribed {
        subscription_id: 0,
        start_offset: Some(STARTED),
        live_offset: Some(STARTED),
        queue_capacity: None,
    })?;
    stub.set_events(move |_, start| match start {
        Some(StartPosition::Offset(at)) => vec![event(at)],
        _ if deliver_first => vec![event(STARTED)],
        _ => Vec::new(),
    });
    Ok(stub)
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

/// Every shard's record, read until each shard has delivered `per_shard`.
async fn records(
    subscription: &mut ShardedSubscription,
    per_shard: usize,
) -> Result<Vec<(u32, u64)>> {
    let mut delivered = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while delivered.len() < per_shard * STREAM_SHARDS as usize {
            let next = subscription
                .next()
                .await
                .context("the subscription ended")?;
            if let ShardEvent::Record { shard, event } = next {
                delivered.push((shard, event.offset.context("offset")?));
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("a shard never delivered")??;
    delivered.sort_unstable();
    Ok(delivered)
}

/// Enter through one broker, subscribe to every shard there, kill it, and
/// return what each shard delivered afterwards.
async fn entry_dies(deliver_first: bool) -> Result<Vec<(u32, u64)>> {
    let (server_config, cert) = build_server_config()?;
    let entry = broker(server_config.clone(), deliver_first)?;
    let other = broker(server_config, deliver_first)?;
    let cluster = Arc::new(
        Client::connect_with_policy(
            &[entry.addr, other.addr],
            "localhost",
            build_client_config_with_overrides(cert, 0)?,
            ReconnectPolicy::default(),
        )
        .await?,
    );
    anyhow::ensure!(
        cluster.client().await.dialled() == entry.addr,
        "the client did not enter through the first seed"
    );
    let mut subscription = cluster
        .subscribe_sharded("t1", "default", "orders", Some(StartPosition::Latest))
        .await?;
    anyhow::ensure!(
        other.subscribes() == 0,
        "a shard opened on the other broker"
    );
    if deliver_first {
        let want: Vec<_> = (0..STREAM_SHARDS).map(|shard| (shard, STARTED)).collect();
        assert_eq!(records(&mut subscription, 1).await?, want);
    }

    entry.shut_down();

    let after = records(&mut subscription, 1)
        .await
        .context("after the entry broker died")?;
    assert_eq!(cluster.client().await.dialled(), other.addr);
    Ok(after)
}

/// **Losing the entry broker moves every shard's feed to one that answers.**
/// Each feed was opened on the entry connection, so resubscribing on it
/// again would be refused for as long as the client ran.
#[tokio::test]
#[serial_test::serial]
async fn every_shard_resumes_on_another_broker_when_the_entry_broker_dies_before_delivering()
-> Result<()> {
    let want: Vec<_> = (0..STREAM_SHARDS).map(|shard| (shard, STARTED)).collect();
    assert_eq!(entry_dies(false).await?, want);
    Ok(())
}

/// The same after each shard delivered a record: each resumes after it.
#[tokio::test]
#[serial_test::serial]
async fn every_shard_resumes_after_its_last_record_when_the_entry_broker_dies() -> Result<()> {
    let want: Vec<_> = (0..STREAM_SHARDS)
        .map(|shard| (shard, STARTED + 1))
        .collect();
    assert_eq!(entry_dies(true).await?, want);
    Ok(())
}

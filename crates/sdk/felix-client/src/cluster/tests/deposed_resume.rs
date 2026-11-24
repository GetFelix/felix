//! A shard moved by a deposed Raft leader: no `resume_from`, no new owner.
//!
//! The subscription resumes after its last event, or where it started if it
//! had none. `Latest` would silently skip what the new leader committed while
//! the shard moved, with no offset jump to show it.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::{Message, StartPosition};

use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::Client;
use crate::cluster::ReconnectPolicy;
use crate::test_support::build_client_config_with_overrides;

/// Where every subscription began, as the stub answers it.
const STARTED: u64 = 7;

fn event(offset: u64) -> Message {
    Message::Event {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        payload: offset.to_string().into_bytes(),
        offset: Some(offset),
    }
}

fn deposed() -> Message {
    Message::ShardMoved {
        subscription_id: 0,
        resume_from: None,
        node_id: None,
        addr: None,
        generation: 2,
    }
}

/// A stub whose first subscribe of each shard delivers `delivered` and then
/// ends with a deposed leader's move. Later subscribes stay open and quiet.
async fn stub(delivered: Option<u64>) -> Result<(StubBroker, Arc<Client>)> {
    let (stub, cert) = StubBroker::start(|_| Message::Subscribed {
        subscription_id: 0,
        start_offset: Some(STARTED),
        live_offset: Some(STARTED),
        queue_capacity: None,
    })?;
    let moved = Mutex::new(HashSet::new());
    stub.set_events(move |shard, _| {
        if !moved.lock().unwrap().insert(shard) {
            return Vec::new();
        }
        delivered
            .map(event)
            .into_iter()
            .chain([deposed()])
            .collect()
    });
    let cluster = Arc::new(
        Client::connect_with_policy(
            &[stub.addr],
            "localhost",
            build_client_config_with_overrides(cert, 0)?,
            ReconnectPolicy::default(),
        )
        .await?,
    );
    Ok((stub, cluster))
}

async fn wait_for_subscribes(stub: &StubBroker, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while stub.subscribes() < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("the moved shard was never re-subscribed")
}

/// Where a single subscription re-subscribed after the move.
async fn single_resume(delivered: Option<u64>) -> Result<Option<StartPosition>> {
    let (stub, cluster) = stub(delivered).await?;
    let mut feed = cluster.subscribe("t1", "default", "orders").await?;
    if let Some(offset) = delivered {
        let first = tokio::time::timeout(Duration::from_secs(5), feed.next_event())
            .await
            .context("no event")??
            .context("the subscription ended")?;
        assert_eq!(first.offset, Some(offset));
    }
    // The followed subscription stays open and quiet, so this times out once
    // the move has been followed.
    let _ = tokio::time::timeout(Duration::from_millis(300), feed.next_event()).await;
    wait_for_subscribes(&stub, 2).await?;
    assert_eq!(feed.moves(), 1);
    Ok(stub.subscribe_starts()[1].1)
}

/// Where each shard of a sharded subscription re-subscribed after the move.
async fn sharded_resumes(delivered: Option<u64>) -> Result<Vec<Option<StartPosition>>> {
    let (stub, cluster) = stub(delivered).await?;
    let mut subscription = cluster
        .subscribe_sharded("t1", "default", "orders", Some(StartPosition::Latest))
        .await?;
    let drain = tokio::spawn(async move { while subscription.next().await.is_some() {} });
    let shards = STREAM_SHARDS as usize;
    let waited = wait_for_subscribes(&stub, 2 * shards).await;
    drain.abort();
    waited?;
    let mut resumes = Vec::new();
    for shard in 0..STREAM_SHARDS {
        let starts: Vec<_> = stub
            .subscribe_starts()
            .into_iter()
            // Shard 0 is sent unset.
            .filter(|(at, _)| at.unwrap_or(0) == shard)
            .map(|(_, start)| start)
            .collect();
        resumes.push(*starts.get(1).context("shard not re-subscribed")?);
    }
    Ok(resumes)
}

/// **A subscription moved before its first event resumes where it started.**
#[tokio::test]
#[serial_test::serial]
async fn a_subscription_deposed_before_any_event_resumes_where_it_started() -> Result<()> {
    assert_eq!(
        single_resume(None).await?,
        Some(StartPosition::Offset(STARTED))
    );
    Ok(())
}

/// **A subscription moved after an event resumes right after it.**
#[tokio::test]
#[serial_test::serial]
async fn a_subscription_deposed_after_an_event_resumes_after_it() -> Result<()> {
    assert_eq!(
        single_resume(Some(STARTED + 3)).await?,
        Some(StartPosition::Offset(STARTED + 4))
    );
    Ok(())
}

/// **A sharded feed moved before its first event resumes where it started.**
#[tokio::test]
#[serial_test::serial]
async fn a_sharded_feed_deposed_before_any_event_resumes_where_it_started() -> Result<()> {
    let want = vec![Some(StartPosition::Offset(STARTED)); STREAM_SHARDS as usize];
    assert_eq!(sharded_resumes(None).await?, want);
    Ok(())
}

/// **A sharded feed moved after an event resumes right after it.**
#[tokio::test]
#[serial_test::serial]
async fn a_sharded_feed_deposed_after_an_event_resumes_after_it() -> Result<()> {
    let want = vec![Some(StartPosition::Offset(STARTED + 4)); STREAM_SHARDS as usize];
    assert_eq!(sharded_resumes(Some(STARTED + 3)).await?, want);
    Ok(())
}

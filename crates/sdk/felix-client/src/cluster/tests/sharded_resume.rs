//! A sharded subscription whose feeds are lost before they delivered anything.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::{CursorErrorReason, Message, StartPosition};

use super::stub_broker::{STREAM_SHARDS, StubBroker};
use crate::Client;
use crate::cluster::ReconnectPolicy;
use crate::cluster::sharded::{ShardEvent, ShardedSubscription};
use crate::test_support::build_client_config_with_overrides;

/// Where every shard's subscription began.
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

/// A stub whose shards each start at [`STARTED`] and, resumed at an offset,
/// deliver that one record. `refuse_at` is the subscribe (counting from 0) it
/// answers as a cursor below the oldest retained offset, 12.
async fn subscribed(refuse_at: Option<usize>) -> Result<(StubBroker, ShardedSubscription)> {
    let calls = Arc::new(AtomicUsize::new(0));
    let (stub, cert) = StubBroker::start(move |_| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        if Some(call) == refuse_at {
            return Message::SubscribeCursorError {
                reason: CursorErrorReason::TooOld,
                requested: STARTED,
                available: 12,
                trim_headers: None,
            };
        }
        Message::Subscribed {
            subscription_id: 0,
            start_offset: Some(STARTED),
            live_offset: Some(STARTED),
            queue_capacity: None,
        }
    })?;
    stub.set_events(|_, start| match start {
        Some(StartPosition::Offset(at)) => vec![event(at)],
        _ => Vec::new(),
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
    let subscription = cluster
        .subscribe_sharded("t1", "default", "orders", Some(StartPosition::Latest))
        .await?;
    Ok((stub, subscription))
}

/// **A feed lost before its first record resumes where it began.** Resuming
/// at the tail would skip what was written while the shard was being
/// re-established, so the whole stream would read as silent.
#[tokio::test]
#[serial_test::serial]
async fn a_feed_lost_before_any_record_resumes_where_it_started() -> Result<()> {
    let (stub, mut subscription) = subscribed(None).await?;
    stub.drop_connections();

    let mut delivered = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while delivered.len() < STREAM_SHARDS as usize {
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
    .context("a shard resumed past the records written while it was lost")??;

    delivered.sort_unstable();
    let want: Vec<_> = (0..STREAM_SHARDS).map(|shard| (shard, STARTED)).collect();
    assert_eq!(delivered, want);
    Ok(())
}

/// **A resume below the retained floor is reported, not retried forever.**
/// The refused shard says what was skipped and carries on from the oldest
/// retained offset.
#[tokio::test]
#[serial_test::serial]
async fn a_resume_below_the_retained_floor_is_reported_and_continues_at_the_floor() -> Result<()> {
    // Subscribes 0 to 3 open the shards; 4 is the first resume.
    let (stub, mut subscription) = subscribed(Some(STREAM_SHARDS as usize)).await?;
    stub.drop_connections();

    let (mut skipped, mut at_floor) = (false, false);
    tokio::time::timeout(Duration::from_secs(20), async {
        while !(skipped && at_floor) {
            match subscription
                .next()
                .await
                .context("the subscription ended")?
            {
                ShardEvent::ShardLost { error, .. }
                    if error.contains("records before it are skipped") =>
                {
                    skipped = true
                }
                ShardEvent::Record { event, .. } if event.offset == Some(12) => at_floor = true,
                _ => {}
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("the refused shard was neither reported nor resumed")??;
    Ok(())
}

//! A shard of a sharded subscription that fell behind.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::{Message, StartPosition};

use super::stub_broker::StubBroker;
use crate::ClusterClient;
use crate::cluster::ReconnectPolicy;
use crate::cluster::sharded::ShardEvent;
use crate::test_support::build_client_config_with_overrides;

const LAGGING: u32 = 2;

fn event(offset: u64) -> Message {
    Message::Event {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        payload: offset.to_string().into_bytes(),
        offset: Some(offset),
    }
}

/// **A lagged shard resumes after its last event, not at `resume_from`.**
/// The broker names where its own queue first dropped (9 here), but a frame
/// dropped later on the way out, in the connection writer's queue, can sit
/// below that: this shard delivered 0 to 4, so 5 to 8 never arrived either.
/// Resuming at 9 would skip them for good.
#[tokio::test]
#[serial_test::serial]
async fn a_lagged_shard_resumes_after_its_last_event() -> Result<()> {
    let (stub, cert) = StubBroker::start(|_| Message::Subscribed {
        subscription_id: 0,
        start_offset: Some(0),
        live_offset: Some(0),
    })?;
    stub.set_events(|shard, start| {
        if shard == Some(LAGGING) && start == Some(StartPosition::Offset(0)) {
            let mut sent: Vec<Message> = (0..5).map(event).collect();
            sent.push(Message::SubscriptionLagged {
                subscription_id: 0,
                resume_from: 9,
            });
            sent
        } else {
            Vec::new()
        }
    });
    let cluster = Arc::new(
        ClusterClient::connect_with_policy(
            &[stub.addr],
            "localhost",
            build_client_config_with_overrides(cert, 0)?,
            ReconnectPolicy::default(),
        )
        .await?,
    );
    let mut subscription = cluster
        .subscribe_sharded("t1", "default", "orders", Some(StartPosition::Offset(0)))
        .await?;

    let mut delivered = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(next) = subscription.next().await {
            match next {
                ShardEvent::Record { shard, event } if shard == LAGGING => {
                    delivered.push(event.offset.context("offset")?);
                }
                ShardEvent::ShardRecovered { shard } if shard == LAGGING => return Ok(()),
                _ => {}
            }
        }
        anyhow::bail!("the subscription ended")
    })
    .await
    .context("the lagged shard did not recover")??;

    assert_eq!(delivered, [0, 1, 2, 3, 4]);
    let resumed: Vec<_> = stub
        .subscribe_starts()
        .into_iter()
        .filter(|(shard, _)| *shard == Some(LAGGING))
        .map(|(_, start)| start)
        .collect();
    assert_eq!(
        resumed,
        [
            Some(StartPosition::Offset(0)),
            Some(StartPosition::Offset(5))
        ]
    );
    Ok(())
}

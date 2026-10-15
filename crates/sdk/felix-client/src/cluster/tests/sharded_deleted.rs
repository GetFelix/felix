//! A sharded subscription whose stream is deleted.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_wire::{Message, StartPosition};

use super::stub_broker::StubBroker;
use crate::ClusterClient;
use crate::cluster::ReconnectPolicy;
use crate::cluster::sharded::ShardEvent;
use crate::test_support::build_client_config_with_overrides;

const DELETED: u32 = 1;

/// **A deleted stream ends the shard instead of reconnecting.** A resubscribe
/// could only be refused, so the shard reports `StreamDeleted` once and is
/// not opened again.
#[tokio::test]
#[serial_test::serial]
async fn a_deleted_stream_ends_the_shard_without_resubscribing() -> Result<()> {
    let (stub, cert) = StubBroker::start(|_| Message::Subscribed {
        subscription_id: 0,
        start_offset: Some(0),
        live_offset: Some(0),
        queue_capacity: None,
    })?;
    stub.set_events(|shard, _| {
        if shard == Some(DELETED) {
            vec![Message::StreamDeleted { subscription_id: 0 }]
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

    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(next) = subscription.next().await {
            match next {
                ShardEvent::StreamDeleted { shard } if shard == DELETED => return Ok(()),
                ShardEvent::ShardLost { shard, .. } if shard == DELETED => {
                    anyhow::bail!("a deleted shard was reported lost")
                }
                _ => {}
            }
        }
        anyhow::bail!("the subscription ended")
    })
    .await
    .context("the deletion was not reported")??;

    // Long enough for a reconnect attempt to have been made, had there been one.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let opened = stub
        .subscribed_shards()
        .into_iter()
        .filter(|shard| *shard == Some(DELETED))
        .count();
    assert_eq!(opened, 1);
    Ok(())
}

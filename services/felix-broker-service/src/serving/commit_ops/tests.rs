//! The write fence around a head trim.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::config::BrokerConfig;
use crate::serving::quic::ClusterContext;
use crate::serving::quic::handlers::publish::build_publish_context;
use crate::test_support::leader::{self, Leader, NAMESPACE, QUORUM, TENANT};

/// **A drain is not held up by a trim waiting on its commit.** The trim's
/// guard covers the record taking its place in the log; the wait for the
/// committed mark runs after it drops. Here no follower ever acknowledges, so
/// the trim waits out its whole timeout while the shard is moved.
#[tokio::test]
async fn a_drain_is_not_held_up_by_a_trim_waiting_on_commit() {
    let mut leader = Leader::start().await;
    leader.broker.allow_head_trims_when(|| true);
    leader
        .broker
        .publish_batch(
            TENANT,
            NAMESPACE,
            QUORUM,
            0,
            &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .await
        .expect("publish");
    let publish_ctx = build_publish_context(
        Arc::clone(&leader.broker),
        &BrokerConfig::default(),
        ClusterContext {
            ingress: Some(Arc::clone(&leader.ingress)),
            ..ClusterContext::default()
        },
    );
    let broker = Arc::clone(&leader.broker);
    let trim = tokio::spawn(async move {
        trim_at(
            &broker,
            &publish_ctx,
            TENANT,
            NAMESPACE,
            QUORUM,
            0,
            1,
            2,
            Bytes::new(),
        )
        .await
    });
    // The trim record is in the log: the trim is now waiting on the mark.
    tokio::time::timeout(Duration::from_secs(10), async {
        while leader.tail(QUORUM).await < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the trim record lands");

    let key = leader::stream_key(QUORUM);
    leader.fence_move(&key);
    tokio::time::timeout(Duration::from_secs(5), leader.ingress.fence().quiesce(&key))
        .await
        .expect("the shard drains while the trim waits on its commit");
    assert!(!trim.is_finished(), "the trim is still waiting");
    trim.abort();
}

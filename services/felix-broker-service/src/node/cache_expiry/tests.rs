use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::test_support::leader::{self, CACHE, Leader, NAMESPACE, TENANT};

async fn expiring_entry(fixture: &Leader) {
    fixture
        .broker
        .cache()
        .put(
            TENANT,
            NAMESPACE,
            CACHE,
            0,
            "presence",
            Bytes::from_static(b"here"),
            Some(Duration::from_millis(1)),
        )
        .await
        .expect("put");
    tokio::time::sleep(Duration::from_millis(20)).await;
}

/// **The leader writes the delete for an entry whose TTL passed.** A watch
/// hears only what reaches the log, so without it a member that stopped
/// refreshing its key stayed in every watcher's view.
#[tokio::test]
async fn the_leader_writes_a_delete_for_an_expired_entry() {
    let fixture = Leader::start().await;
    expiring_entry(&fixture).await;
    let log = fixture
        .broker
        .cache()
        .shard_log(TENANT, NAMESPACE, CACHE, 0)
        .await
        .expect("log");
    let before = felix_storage::log::AppendOnlyLog::tail_offset(&log)
        .await
        .expect("tail");

    expire_once(&fixture.broker, Some(&fixture.ingress)).await;

    let after = felix_storage::log::AppendOnlyLog::tail_offset(&log)
        .await
        .expect("tail");
    assert_eq!(after, before + 1, "one delete record");
}

/// A shard whose fence has closed is not written: its leader is elsewhere.
#[tokio::test]
async fn a_fenced_shard_is_left_to_its_new_leader() {
    let mut fixture = Leader::start().await;
    expiring_entry(&fixture).await;
    let log = fixture
        .broker
        .cache()
        .shard_log(TENANT, NAMESPACE, CACHE, 0)
        .await
        .expect("log");
    let before = felix_storage::log::AppendOnlyLog::tail_offset(&log)
        .await
        .expect("tail");
    fixture.fence_move(&leader::cache_key());

    expire_once(&fixture.broker, Some(&fixture.ingress)).await;

    let after = felix_storage::log::AppendOnlyLog::tail_offset(&log)
        .await
        .expect("tail");
    assert_eq!(after, before);
}

//! A cache shard a Raft member claims lives under the cache's own root,
//! apart from a stream of the same name, and the cache store never opens it.

use super::*;

#[tokio::test]
async fn a_claimed_shard_is_not_opened_by_the_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(&dir.path().join("caches")).await;

    let (claimed, _) = cache.claim_for_raft(T, NS, C, 0, 0).expect("claim");
    let stream_dir = crate::disk_log::layout::incarnation_dir(
        &dir.path().join("streams"),
        &crate::log::ShardKey {
            tenant: T.into(),
            namespace: NS.into(),
            stream: C.into(),
            shard: 0,
        },
        0,
    );

    assert!(claimed.starts_with(dir.path().join("caches")));
    assert_ne!(
        claimed, stream_dir,
        "a cache and a stream of one name share no directory"
    );
    let refused = cache
        .put(T, NS, C, 0, "k", Bytes::from_static(b"v"), None)
        .await;
    assert!(
        refused.is_err(),
        "the store wrote a Raft member's directory"
    );
}

#[tokio::test]
async fn a_claim_at_another_incarnation_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;
    cache.set_incarnation(T, NS, C, Some(3));

    let refused = cache.claim_for_raft(T, NS, C, 0, 2);

    assert!(matches!(refused, Err(StorageError::Closed(_))));
}

#[tokio::test]
async fn a_claim_is_read_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = cache(dir.path()).await;

    assert_eq!(
        cache.claimed_for_raft(T, NS, C, 0, 0).expect("read"),
        Some(false)
    );
    cache.claim_for_raft(T, NS, C, 0, 0).expect("claim");

    assert_eq!(
        cache.claimed_for_raft(T, NS, C, 0, 0).expect("read"),
        Some(true)
    );
    assert_eq!(cache.claimed_for_raft(T, NS, C, 0, 1).expect("read"), None);
}

use std::time::Duration;

use tokio::time::sleep;

use super::*;
use crate::cache::{CacheChange, CacheObserver};

#[tokio::test]
async fn cache_ttl_expiry() {
    // Ensure TTL logic expires keys after the deadline.
    let cache = EphemeralCache::new();
    cache
        .put(
            "t1",
            "default",
            "primary",
            0,
            "k",
            Bytes::from_static(b"v"),
            Some(Duration::from_millis(10)),
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(15)).await;
    assert!(
        cache
            .get("t1", "default", "primary", 0, "k")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn put_get_delete_round_trip() {
    let cache = EphemeralCache::new();
    cache
        .put(
            "t1",
            "default",
            "primary",
            0,
            "k",
            Bytes::from_static(b"value"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        cache.get("t1", "default", "primary", 0, "k").await.unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(
        cache
            .delete("t1", "default", "primary", 0, "k")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    assert!(
        cache
            .get("t1", "default", "primary", 0, "k")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn len_and_is_empty_reflect_state() {
    let cache = EphemeralCache::new();
    assert!(cache.is_empty().await);
    assert_eq!(cache.len().await, 0);
    cache
        .put(
            "t1",
            "default",
            "primary",
            0,
            "k1",
            Bytes::from_static(b"a"),
            None,
        )
        .await
        .unwrap();
    assert!(!cache.is_empty().await);
    assert_eq!(cache.len().await, 1);
    cache
        .delete("t1", "default", "primary", 0, "k1")
        .await
        .unwrap();
    assert!(cache.is_empty().await);
    assert_eq!(cache.len().await, 0);
}

#[tokio::test]
async fn capacity_enforces_placeholder_eviction() {
    let cache = EphemeralCache::with_capacity(1);
    cache
        .put(
            "t1",
            "default",
            "primary",
            0,
            "k1",
            Bytes::from_static(b"a"),
            None,
        )
        .await
        .unwrap();
    cache
        .put(
            "t1",
            "default",
            "primary",
            0,
            "k2",
            Bytes::from_static(b"b"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(cache.len().await, 1);
}

#[test]
fn cache_key_construction() {
    let key = CacheKey::new("tenant1", "ns1", "cache1", "key1");
    assert_eq!(key.tenant_id, "tenant1");
    assert_eq!(key.namespace, "ns1");
    assert_eq!(key.cache, "cache1");
    assert_eq!(key.key, "key1");
}

#[test]
fn cache_key_equality() {
    let key1 = CacheKey::new("t1", "ns", "c", "k");
    let key2 = CacheKey::new("t1", "ns", "c", "k");
    let key3 = CacheKey::new("t2", "ns", "c", "k");
    assert_eq!(key1, key2);
    assert_ne!(key1, key3);
}

#[tokio::test]
async fn get_nonexistent_key_returns_none() {
    let cache = EphemeralCache::new();
    assert!(
        cache
            .get("t1", "ns", "c", 0, "nonexistent")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delete_nonexistent_key_returns_none() {
    let cache = EphemeralCache::new();
    assert!(
        cache
            .delete("t1", "ns", "c", 0, "nonexistent")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn put_overwrites_existing_value() {
    let cache = EphemeralCache::new();
    cache
        .put("t1", "ns", "c", 0, "k", Bytes::from_static(b"v1"), None)
        .await
        .unwrap();
    cache
        .put("t1", "ns", "c", 0, "k", Bytes::from_static(b"v2"), None)
        .await
        .unwrap();
    assert_eq!(
        cache.get("t1", "ns", "c", 0, "k").await.unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(cache.len().await, 1);
}

#[tokio::test]
async fn conditional_writes_compare_versions() {
    use crate::cache::{CacheCondition, ConditionalWrite};
    let cache = EphemeralCache::new();
    let put_if = |value: &'static [u8], condition| {
        cache.put_if(
            "t1",
            "ns",
            "c",
            0,
            "k",
            Bytes::from_static(value),
            None,
            condition,
        )
    };

    let first = put_if(b"a", CacheCondition::Absent).await.unwrap();
    assert!(first.applied);
    let version = first.version.unwrap();
    assert_eq!(
        put_if(b"b", CacheCondition::Absent).await.unwrap(),
        ConditionalWrite {
            applied: false,
            version: Some(version),
        }
    );
    let found = cache
        .get_versioned("t1", "ns", "c", 0, "k")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.version, version);

    let swapped = put_if(b"c", CacheCondition::Version(version))
        .await
        .unwrap();
    assert!(swapped.applied);
    let stale = cache
        .delete_if("t1", "ns", "c", 0, "k", version)
        .await
        .unwrap();
    assert_eq!(stale.version, swapped.version);
    assert!(
        cache
            .delete_if("t1", "ns", "c", 0, "k", swapped.version.unwrap())
            .await
            .unwrap()
            .applied
    );
    assert!(put_if(b"d", CacheCondition::Absent).await.unwrap().applied);
}

#[tokio::test]
async fn a_version_from_before_a_restart_names_nothing_after_it() {
    use crate::cache::CacheCondition;
    async fn put(
        cache: &EphemeralCache,
        value: &'static [u8],
        condition: CacheCondition,
    ) -> crate::cache::ConditionalWrite {
        cache
            .put_if(
                "t1",
                "ns",
                "c",
                0,
                "k",
                Bytes::from_static(value),
                None,
                condition,
            )
            .await
            .unwrap()
    }

    let before = EphemeralCache::new();
    let seen = put(&before, b"old", CacheCondition::Absent)
        .await
        .version
        .unwrap();
    drop(before);
    // A real restart takes far longer than this; it only keeps the two
    // stores from starting in the same microsecond.
    sleep(Duration::from_millis(2)).await;

    let after = EphemeralCache::new();
    assert!(put(&after, b"new", CacheCondition::Absent).await.applied);
    let stale = put(&after, b"clobber", CacheCondition::Version(seen)).await;
    assert!(!stale.applied, "a pre-restart version matched {stale:?}");
    assert_eq!(
        after.get("t1", "ns", "c", 0, "k").await.unwrap(),
        Some(Bytes::from_static(b"new"))
    );
}

#[tokio::test]
async fn an_expired_entry_is_absent_to_a_condition() {
    use crate::cache::CacheCondition;
    let cache = EphemeralCache::new();
    cache
        .put(
            "t1",
            "ns",
            "c",
            0,
            "k",
            Bytes::from_static(b"a"),
            Some(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let taken = cache
        .put_if(
            "t1",
            "ns",
            "c",
            0,
            "k",
            Bytes::from_static(b"b"),
            None,
            CacheCondition::Absent,
        )
        .await
        .unwrap();
    assert!(taken.applied);
}

#[tokio::test]
async fn forget_cache_drops_only_that_caches_entries() {
    let cache = EphemeralCache::new();
    for name in ["orders", "users"] {
        cache
            .put(
                "t1",
                "default",
                name,
                0,
                "k",
                Bytes::from_static(b"v"),
                None,
            )
            .await
            .unwrap();
    }
    cache.forget_cache("t1", "default", "orders").await.unwrap();
    assert_eq!(
        cache.get("t1", "default", "orders", 0, "k").await.unwrap(),
        None
    );
    assert_eq!(
        cache.get("t1", "default", "users", 0, "k").await.unwrap(),
        Some(Bytes::from_static(b"v"))
    );
}

/// An observer that never hears anything, to offer to a store with no log.
#[derive(Debug)]
struct Deaf;

impl CacheObserver for Deaf {
    fn cache_changed(&self, _change: CacheChange) -> bool {
        false
    }
}

/// **A store with no log offers nothing that needs one**: no shard log, no
/// applied offset, no expiry or index work, and it refuses a change observer,
/// so no watch is ever served over offsets it cannot give. Shard lifecycle
/// calls are harmless no-ops.
#[tokio::test]
async fn a_store_without_a_log_offers_nothing_that_needs_one() {
    let cache = EphemeralCache::new();
    cache
        .put("t1", "ns", "c", 0, "k", Bytes::from_static(b"v"), None)
        .await
        .expect("put");
    assert!(!cache.has_log());
    assert!(cache.as_log_cache().is_none());
    assert!(cache.shard_log("t1", "ns", "c", 0).await.is_none());
    assert!(cache.shard_log_at("t1", "ns", "c", 0, 5).await.is_none());
    assert!(cache.open_shards().is_empty());
    assert_eq!(
        cache
            .applied_through("t1", "ns", "c", 0)
            .await
            .expect("applied"),
        None
    );
    assert_eq!(
        cache
            .expire_due("t1", "ns", "c", 0, 100, &|| true)
            .await
            .expect("expire"),
        0
    );
    assert!(
        !cache.set_change_observer(std::sync::Arc::new(Deaf)),
        "a store with no log must refuse to be watched"
    );
    cache.close_shard("t1", "ns", "c", 0).await.expect("close");
    cache
        .forget_index("t1", "ns", "c", 0)
        .await
        .expect("forget");
    cache
        .set_retention_hold("t1", "ns", "c", true)
        .await
        .expect("hold");
    let err = cache
        .live_entries("t1", "ns", "c", 0)
        .await
        .expect_err("no snapshot to begin a watch from");
    assert!(matches!(err, crate::StorageError::Unsupported(_)), "{err}");
    cache.shutdown().await.expect("shutdown");
    assert_eq!(
        cache.get("t1", "ns", "c", 0, "k").await.expect("get"),
        Some(Bytes::from_static(b"v")),
        "the no-ops leave the data alone"
    );

    // An observer that does not override them is always watching, and a
    // reset reaches no one.
    assert!(Deaf.watching());
    Deaf.cache_reset("t1", "ns", "c", 0, 9);
}

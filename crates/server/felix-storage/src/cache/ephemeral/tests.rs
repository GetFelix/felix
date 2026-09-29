use std::time::Duration;

use tokio::time::sleep;

use super::*;

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

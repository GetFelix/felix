//! A cache deleted and created again under the same name starts empty: no
//! entries and no counters from the cache that held the name before it.

use std::sync::Arc;

use bytes::Bytes;
use felix_broker::{Broker, CacheMetadata, LogKind};
use felix_storage::log::{FsyncMode, LogConfig};
use felix_storage::{CounterStore, EphemeralCache, LogCache, MarkerRead, StreamMarker};
use tempfile::{TempDir, tempdir};

const CACHE: &str = "sessions";

fn log_config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 4 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker(dir: &TempDir) -> Broker {
    let cache = LogCache::open(dir.path().join("caches"), log_config()).expect("caches");
    let counters =
        Arc::new(CounterStore::open(dir.path().join("counters"), log_config()).expect("counters"));
    let broker = Broker::new(Box::new(cache)).with_counters(counters);
    scopes(&broker).await;
    broker
}

async fn scopes(broker: &Broker) {
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
}

fn at(incarnation: u64) -> CacheMetadata {
    CacheMetadata {
        incarnation,
        ..Default::default()
    }
}

async fn register(broker: &Broker, name: &str, incarnation: u64) {
    broker
        .register_cache("t1", "default", name, at(incarnation))
        .await
        .expect("register");
}

/// An entry and a counter in `CACHE`.
async fn write(broker: &Broker, value: &str) {
    broker
        .cache()
        .put(
            "t1",
            "default",
            CACHE,
            0,
            "k",
            Bytes::copy_from_slice(value.as_bytes()),
            None,
        )
        .await
        .expect("put");
    broker
        .counters()
        .expect("counters")
        .add("t1", "default", CACHE, 0, "hits", 3)
        .await
        .expect("add");
}

async fn entry(broker: &Broker) -> Option<Bytes> {
    broker
        .cache()
        .get("t1", "default", CACHE, 0, "k")
        .await
        .expect("get")
}

async fn counter(broker: &Broker) -> Option<i64> {
    broker
        .counters()
        .expect("counters")
        .get("t1", "default", CACHE, 0, "hits")
        .await
        .expect("counter")
}

/// The incarnations of `CACHE` with a directory in each store, read afresh
/// from disk. Retired directories wait in `.deleted/`, which this skips.
async fn on_disk(dir: &TempDir) -> (Vec<u64>, Vec<u64>) {
    let caches = LogCache::open(dir.path().join("caches"), log_config()).expect("caches");
    let counters = CounterStore::open(dir.path().join("counters"), log_config()).expect("counters");
    let mut entries: Vec<u64> = caches
        .dirs_on_disk("t1", "default", CACHE)
        .await
        .expect("dirs")
        .into_iter()
        .map(|(_, incarnation)| incarnation)
        .collect();
    let mut sums: Vec<u64> = counters
        .dirs_on_disk("t1", "default", CACHE)
        .await
        .expect("dirs")
        .into_iter()
        .map(|(_, incarnation)| incarnation)
        .collect();
    entries.sort_unstable();
    entries.dedup();
    sums.sort_unstable();
    sums.dedup();
    (entries, sums)
}

fn store(broker: &Broker) -> &LogCache {
    broker.cache().as_log_cache().expect("log cache")
}

async fn marker(broker: &Broker) -> Option<StreamMarker> {
    match store(broker)
        .marker("t1", "default", CACHE)
        .await
        .expect("marker")
    {
        MarkerRead::Valid(marker) => Some(marker),
        MarkerRead::Absent | MarkerRead::Unreadable => None,
    }
}

#[tokio::test]
async fn a_recreated_cache_starts_empty() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    register(&broker, CACHE, 5).await;
    write(&broker, "old").await;
    assert_eq!(on_disk(&dir).await, (vec![5], vec![5]));

    assert!(
        broker
            .remove_cache_from_catalog("t1", "default", CACHE, Some(5))
            .await
            .expect("delete")
    );
    register(&broker, CACHE, 9).await;

    assert_eq!(entry(&broker).await, None, "entry inherited");
    assert_eq!(counter(&broker).await, None, "counter inherited");
    let (entries, sums) = on_disk(&dir).await;
    assert!(!entries.contains(&5), "entries of 5 kept: {entries:?}");
    assert!(!sums.contains(&5), "counters of 5 kept: {sums:?}");
}

#[tokio::test]
async fn an_older_incarnation_is_refused() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    register(&broker, CACHE, 9).await;
    write(&broker, "nine").await;

    register(&broker, CACHE, 5).await;
    assert_eq!(broker.cache_incarnation("t1", "default", CACHE), Some(9));
    assert_eq!(entry(&broker).await, Some(Bytes::from_static(b"nine")));
    assert_eq!(counter(&broker).await, Some(3));

    register(&broker, CACHE, 0).await;
    assert_eq!(broker.cache_incarnation("t1", "default", CACHE), Some(9));
    assert_eq!(entry(&broker).await, Some(Bytes::from_static(b"nine")));
}

#[tokio::test]
async fn a_deleted_incarnation_is_not_registered_again() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    register(&broker, CACHE, 9).await;
    write(&broker, "nine").await;
    broker
        .remove_cache_from_catalog("t1", "default", CACHE, Some(9))
        .await
        .expect("delete");

    for stale in [7, 9] {
        register(&broker, CACHE, stale).await;
        assert!(
            !broker.cache_exists("t1", "default", CACHE).await,
            "deleted incarnation {stale} registered"
        );
    }
    register(&broker, CACHE, 12).await;
    assert_eq!(broker.cache_incarnation("t1", "default", CACHE), Some(12));
    assert_eq!(entry(&broker).await, None);
}

#[tokio::test]
async fn a_delete_naming_no_incarnation_keeps_the_data() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    register(&broker, CACHE, 0).await;
    write(&broker, "kept").await;

    assert!(
        broker
            .remove_cache_from_catalog("t1", "default", CACHE, None)
            .await
            .expect("delete")
    );
    assert!(!broker.cache_exists("t1", "default", CACHE).await);
    register(&broker, CACHE, 0).await;

    assert_eq!(entry(&broker).await, Some(Bytes::from_static(b"kept")));
    assert_eq!(counter(&broker).await, Some(3));
}

#[tokio::test]
async fn a_tombstone_deletes_a_cache_deleted_while_the_broker_was_down() {
    let dir = tempdir().expect("dir");
    {
        let broker = broker(&dir).await;
        register(&broker, CACHE, 3).await;
        write(&broker, "three").await;
    }
    let broker = broker(&dir).await;
    broker
        .require_cache_registration_if_in_play()
        .expect("in play");
    assert!(broker.cache_registration_required());
    broker
        .apply_cache_tombstones(&[("t1", "default", CACHE, 3)])
        .await
        .expect("tombstones");
    register(&broker, CACHE, 7).await;

    assert_eq!(broker.cache_incarnation("t1", "default", CACHE), Some(7));
    assert_eq!(entry(&broker).await, None, "entry inherited");
    assert_eq!(counter(&broker).await, None, "counter inherited");
    let (entries, sums) = on_disk(&dir).await;
    assert!(!entries.contains(&3), "entries of 3 kept: {entries:?}");
    assert!(!sums.contains(&3), "counters of 3 kept: {sums:?}");
}

#[tokio::test]
async fn a_tombstone_for_a_cache_never_held_records_a_floor() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    broker
        .apply_cache_tombstones(&[("t1", "default", "ghost", 4)])
        .await
        .expect("tombstones");
    assert!(
        store(&broker)
            .marker("t1", "default", "ghost")
            .await
            .expect("marker")
            == MarkerRead::Absent,
        "a floor only, no marker"
    );

    for stale in [3, 4] {
        register(&broker, "ghost", stale).await;
        assert!(!broker.cache_exists("t1", "default", "ghost").await);
    }
    // The floor is on disk, so a restart refuses too.
    drop(broker);
    let broker = self::broker(&dir).await;
    register(&broker, "ghost", 4).await;
    assert!(!broker.cache_exists("t1", "default", "ghost").await);
    register(&broker, "ghost", 5).await;
    assert!(broker.cache_exists("t1", "default", "ghost").await);
}

#[tokio::test]
async fn a_cache_from_before_incarnations_survives_the_upgrade_until_deleted() {
    let dir = tempdir().expect("dir");
    {
        // A broker that never registered with incarnations.
        let broker = broker(&dir).await;
        write(&broker, "legacy").await;
    }
    assert!(!dir.path().join("caches/.streams").exists());
    let broker = broker(&dir).await;
    broker
        .require_cache_registration_if_in_play()
        .expect("in play");
    assert!(!broker.cache_registration_required());
    register(&broker, CACHE, 0).await;
    assert_eq!(entry(&broker).await, Some(Bytes::from_static(b"legacy")));
    assert_eq!(counter(&broker).await, Some(3));
    // Registering at 0 writes nothing new.
    assert_eq!(marker(&broker).await, None);

    broker
        .apply_cache_tombstones(&[("t1", "default", CACHE, 0)])
        .await
        .expect("tombstones");
    assert!(!broker.cache_exists("t1", "default", CACHE).await);
    assert_eq!(on_disk(&dir).await, (vec![], vec![]));
    register(&broker, CACHE, 0).await;
    assert!(
        !broker.cache_exists("t1", "default", CACHE).await,
        "a deleted 0 registered again"
    );
}

#[tokio::test]
async fn a_delete_cut_short_is_finished_at_start() {
    let dir = tempdir().expect("dir");
    {
        let broker = broker(&dir).await;
        register(&broker, CACHE, 6).await;
        write(&broker, "six").await;
    }
    assert_eq!(on_disk(&dir).await, (vec![6], vec![6]));
    let broker = broker(&dir).await;
    let pending = StreamMarker {
        tenant: "t1".into(),
        namespace: "default".into(),
        stream: CACHE.into(),
        incarnation: 6,
        retiring: true,
        shards: None,
        deleted: Some(6),
    };
    store(&broker).write_marker(&pending).await.expect("marker");

    broker
        .finish_pending_cache_deletes()
        .await
        .expect("finish deletes");

    assert_eq!(on_disk(&dir).await, (vec![], vec![]));
    let marker = marker(&broker).await.expect("marker kept for its floor");
    assert!(!marker.retiring, "delete left unfinished: {marker:?}");
    assert_eq!(marker.deleted, Some(6));
    register(&broker, CACHE, 6).await;
    assert!(!broker.cache_exists("t1", "default", CACHE).await);
}

#[tokio::test]
async fn an_operator_removal_deletes_the_data() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    register(&broker, CACHE, 0).await;
    write(&broker, "gone").await;

    assert!(
        broker
            .remove_cache("t1", "default", CACHE)
            .await
            .expect("remove")
    );
    assert_eq!(on_disk(&dir).await, (vec![], vec![]));
    register(&broker, CACHE, 0).await;
    assert!(broker.cache_exists("t1", "default", CACHE).await);
    assert_eq!(entry(&broker).await, None, "entry inherited");
    assert_eq!(counter(&broker).await, None, "counter inherited");
    assert_eq!(
        marker(&broker).await,
        None,
        "no marker left for a cache at 0"
    );
}

#[tokio::test]
async fn an_operator_removal_of_an_incarnated_cache_holds_its_floor() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    register(&broker, CACHE, 4).await;
    write(&broker, "gone").await;

    assert!(
        broker
            .remove_cache("t1", "default", CACHE)
            .await
            .expect("remove")
    );
    assert_eq!(on_disk(&dir).await, (vec![], vec![]));
    register(&broker, CACHE, 4).await;
    assert!(!broker.cache_exists("t1", "default", CACHE).await);
}

#[tokio::test]
async fn logs_open_only_for_the_registered_incarnation() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    register(&broker, CACHE, 5).await;
    assert!(broker.cache_registration_required());

    for kind in [LogKind::Cache, LogKind::Counters] {
        assert!(
            broker
                .shard_log_for(kind, "t1", "default", CACHE, 0, None, Some(4))
                .await
                .is_none(),
            "{kind:?} opened at an incarnation not registered"
        );
        assert!(
            broker
                .shard_log_for(kind, "t1", "default", CACHE, 0, None, Some(5))
                .await
                .is_some(),
            "{kind:?} refused at the registered incarnation"
        );
        assert!(
            broker
                .shard_log(kind, "t1", "default", "unregistered", 0)
                .await
                .is_none(),
            "{kind:?} opened for an unregistered cache"
        );
    }
    // The refused opens created nothing.
    let (entries, sums) = on_disk(&dir).await;
    assert!(!entries.contains(&4) && !sums.contains(&4));
}

#[tokio::test]
async fn an_unregistered_cache_opens_before_caches_carry_incarnations() {
    let dir = tempdir().expect("dir");
    let broker = broker(&dir).await;
    assert!(!broker.cache_registration_required());
    assert!(
        broker
            .shard_log(LogKind::Cache, "t1", "default", "unregistered", 0)
            .await
            .is_some()
    );
    broker.require_registered_caches();
    assert!(
        broker
            .shard_log(LogKind::Counters, "t1", "default", "other", 0)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn a_recreated_ephemeral_cache_starts_empty() {
    let broker = Broker::new(EphemeralCache::new().into());
    scopes(&broker).await;
    register(&broker, CACHE, 5).await;
    broker
        .cache()
        .put(
            "t1",
            "default",
            CACHE,
            0,
            "k",
            Bytes::from_static(b"old"),
            None,
        )
        .await
        .expect("put");

    broker
        .remove_cache_from_catalog("t1", "default", CACHE, Some(5))
        .await
        .expect("delete");
    register(&broker, CACHE, 9).await;

    assert!(broker.cache_exists("t1", "default", CACHE).await);
    assert_eq!(entry(&broker).await, None, "entry inherited");
}

#[tokio::test]
async fn a_restart_reopens_the_registered_incarnation() {
    let dir = tempdir().expect("dir");
    {
        let broker = broker(&dir).await;
        register(&broker, CACHE, 5).await;
        write(&broker, "five").await;
    }
    let broker = broker(&dir).await;
    broker
        .require_cache_registration_if_in_play()
        .expect("in play");
    register(&broker, CACHE, 5).await;
    assert_eq!(entry(&broker).await, Some(Bytes::from_static(b"five")));
    assert_eq!(counter(&broker).await, Some(3));
}

#[tokio::test]
async fn a_later_incarnation_after_a_restart_keeps_the_earlier_until_its_tombstone() {
    let dir = tempdir().expect("dir");
    {
        let broker = broker(&dir).await;
        register(&broker, CACHE, 5).await;
        write(&broker, "five").await;
    }
    let broker = broker(&dir).await;
    broker
        .require_cache_registration_if_in_play()
        .expect("in play");
    register(&broker, CACHE, 9).await;
    assert_eq!(entry(&broker).await, None, "entry inherited");
    assert_eq!(counter(&broker).await, None, "counter inherited");
    assert_eq!(on_disk(&dir).await.0, vec![5, 9]);

    broker
        .apply_cache_tombstones(&[("t1", "default", CACHE, 5)])
        .await
        .expect("tombstones");
    assert_eq!(broker.cache_incarnation("t1", "default", CACHE), Some(9));
    let (entries, sums) = on_disk(&dir).await;
    assert!(!entries.contains(&5) && !sums.contains(&5));
}

//! The watch hub's contract: exact filtering, loud loss, clean teardown.

use felix_storage::{CacheChange, CacheObserver};

use super::*;

fn change(key: &str, offset: u64) -> CacheChange {
    CacheChange {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: "sessions".to_string(),
        shard: 0,
        key: key.to_string(),
        value: Some(Bytes::from_static(b"v")),
        offset,
        expires_at_millis: 0,
    }
}

fn register(hub: &Arc<CacheWatchHub>, filter: CacheWatchFilter) -> CacheWatchSubscription {
    hub.register("t1", "ns", "sessions", 0, filter, 8)
}

/// A key watch receives that key's changes and no others; a prefix watch
/// receives exactly the prefix.
#[tokio::test]
async fn filters_deliver_exactly_what_they_name() {
    let hub = CacheWatchHub::new();
    let mut by_key = register(&hub, CacheWatchFilter::Key("user:42".to_string()));
    let mut by_prefix = register(&hub, CacheWatchFilter::Prefix("user:".to_string()));
    let mut everything = register(&hub, CacheWatchFilter::Prefix(String::new()));

    hub.cache_changed(change("user:42", 0));
    hub.cache_changed(change("user:7", 1));
    hub.cache_changed(change("order:9", 2));

    assert_eq!(by_key.try_recv().expect("own key").offset, 0);
    assert!(by_key.try_recv().is_err(), "another key leaked through");

    assert_eq!(by_prefix.try_recv().expect("user:42").offset, 0);
    assert_eq!(by_prefix.try_recv().expect("user:7").offset, 1);
    assert!(by_prefix.try_recv().is_err(), "a non-prefix key leaked");

    let seen: Vec<u64> = std::iter::from_fn(|| everything.try_recv().ok())
        .map(|event| event.offset)
        .collect();
    assert_eq!(seen, vec![0, 1, 2], "the empty prefix is every key");
}

/// Changes for another shard of the same cache stay out: keys sharing a prefix
/// hash apart, and a watch reads one shard.
#[tokio::test]
async fn a_watch_is_scoped_to_its_shard() {
    let hub = CacheWatchHub::new();
    let mut watch = register(&hub, CacheWatchFilter::Prefix(String::new()));

    let mut other_shard = change("user:42", 0);
    other_shard.shard = 1;
    hub.cache_changed(other_shard);

    assert!(watch.try_recv().is_err(), "another shard's change leaked");
}

/// A full queue ends the watch loudly: what was queued is still delivered,
/// then the close, and the lag offset names the first missed change so a
/// re-watch from it is gapless.
#[tokio::test]
async fn overflow_ends_the_watch_and_names_the_first_missed_offset() {
    let hub = CacheWatchHub::new();
    let mut watch = hub.register(
        "t1",
        "ns",
        "sessions",
        0,
        CacheWatchFilter::Prefix(String::new()),
        2,
    );

    hub.cache_changed(change("a", 0));
    hub.cache_changed(change("b", 1));
    // The queue holds two; this is the first change it could not.
    hub.cache_changed(change("c", 2));
    // Applied after the overflow: must not be delivered as if nothing happened.
    hub.cache_changed(change("d", 3));

    assert_eq!(watch.recv().await.expect("queued").offset, 0);
    assert_eq!(watch.recv().await.expect("queued").offset, 1);
    assert!(
        watch.recv().await.is_none(),
        "an overflowed watch must end, not skip",
    );
    assert_eq!(watch.lagged(), Some(2), "the first missed offset");
    assert_eq!(
        hub.registered_watchers("t1", "ns", "sessions", 0),
        0,
        "an ended watch must not stay registered",
    );
}

/// Dropping the subscription unregisters it, and fanout reaps a receiver that
/// went away between changes.
#[tokio::test]
async fn teardown_leaves_nothing_registered() {
    let hub = CacheWatchHub::new();
    let watch = register(&hub, CacheWatchFilter::Key("k".to_string()));
    assert_eq!(hub.registered_watchers("t1", "ns", "sessions", 0), 1);
    drop(watch);
    assert_eq!(
        hub.registered_watchers("t1", "ns", "sessions", 0),
        0,
        "dropping the subscription must unregister it",
    );

    // A receiver dropped without its guard running first (into_parts-style
    // leaks are impossible here, but a task can be aborted mid-recv): the next
    // matching change reaps it.
    let mut watch = register(&hub, CacheWatchFilter::Key("k".to_string()));
    watch.receiver.close();
    hub.cache_changed(change("k", 0));
    assert_eq!(
        hub.registered_watchers("t1", "ns", "sessions", 0),
        0,
        "fanout must reap a closed receiver",
    );
}

/// A healthy watch never reports lag.
#[tokio::test]
async fn a_kept_up_watch_reports_no_lag() {
    let hub = CacheWatchHub::new();
    let mut watch = register(&hub, CacheWatchFilter::Key("k".to_string()));
    hub.cache_changed(change("k", 0));
    assert_eq!(watch.recv().await.expect("delivered").offset, 0);
    assert_eq!(watch.lagged(), None);
}

/// Ending a shard's watches delivers what each had queued, then closes it
/// cleanly -- not as a lag -- and leaves other shards' watchers alone.
#[tokio::test]
async fn ending_a_shard_drains_then_closes_its_watchers() {
    let hub = CacheWatchHub::new();
    let mut watch = register(&hub, CacheWatchFilter::Prefix(String::new()));
    let mut other = hub.register(
        "t1",
        "ns",
        "sessions",
        1,
        CacheWatchFilter::Prefix(String::new()),
        8,
    );
    hub.cache_changed(change("a", 0));
    hub.cache_changed(change("b", 1));

    assert_eq!(hub.end_shard("t1", "ns", "sessions", 0, None), 1);

    assert_eq!(watch.recv().await.expect("queued").offset, 0);
    assert_eq!(watch.recv().await.expect("queued").offset, 1);
    assert!(watch.recv().await.is_none(), "the watch should end");
    assert_eq!(watch.lagged(), None, "an ended watch is not a lagged one");
    assert!(
        matches!(other.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "a watcher on another shard was ended"
    );
}

/// A watch ended by a move drains what it had and then says where to resume.
#[tokio::test]
async fn a_watch_ended_by_a_move_says_where_to_resume() {
    let hub = CacheWatchHub::new();
    let mut watch = register(&hub, CacheWatchFilter::Prefix(String::new()));
    hub.cache_changed(change("a", 3));
    let moved = crate::ShardMoved {
        resume_from: Some(4),
        to: crate::ShardHandoff {
            node_id: Some("broker-b".to_string()),
            addr: None,
            generation: 2,
        },
    };

    assert_eq!(
        hub.end_shard("t1", "ns", "sessions", 0, Some(moved.clone())),
        1
    );

    assert_eq!(watch.recv().await.expect("queued").offset, 3);
    assert!(watch.recv().await.is_none());
    assert_eq!(watch.moved(), Some(&moved));
    assert_eq!(watch.lagged(), None);
}

/// Watchers of one key share an index slot. Dropping one leaves the other
/// delivered to, and dropping the last frees the slot for a later watch.
#[tokio::test]
async fn key_watchers_come_and_go_independently() {
    let hub = CacheWatchHub::new();
    let first = register(&hub, CacheWatchFilter::Key("user:42".to_string()));
    let mut second = register(&hub, CacheWatchFilter::Key("user:42".to_string()));
    let mut other = register(&hub, CacheWatchFilter::Key("user:7".to_string()));
    assert_eq!(hub.registered_watchers("t1", "ns", "sessions", 0), 3);

    drop(first);
    hub.cache_changed(change("user:42", 0));
    assert_eq!(second.try_recv().expect("still watching").offset, 0);
    assert!(other.try_recv().is_err());

    drop(second);
    drop(other);
    assert_eq!(hub.registered_watchers("t1", "ns", "sessions", 0), 0);

    let mut later = register(&hub, CacheWatchFilter::Key("user:42".to_string()));
    hub.cache_changed(change("user:42", 1));
    assert_eq!(later.try_recv().expect("a later watch").offset, 1);
}

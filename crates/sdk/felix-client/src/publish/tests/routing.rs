use std::sync::atomic::Ordering;

use felix_wire::AckMode;
use serial_test::serial;

use super::make_publisher;
use crate::publish::routing::{PublishSharding, StreamKey, StreamKeyRef, StreamShardCache};

fn clear_env() {
    unsafe {
        std::env::remove_var("FELIX_PUB_SHARDING");
    }
}

#[test]
#[serial]
fn from_env_missing_returns_none() {
    clear_env();
    assert!(PublishSharding::from_env().is_none());
}

#[test]
#[serial]
fn from_env_rr_returns_round_robin() {
    clear_env();
    unsafe {
        std::env::set_var("FELIX_PUB_SHARDING", "rr");
    }
    assert_eq!(
        PublishSharding::from_env(),
        Some(PublishSharding::RoundRobin)
    );
    clear_env();
}

#[test]
#[serial]
fn from_env_hash_stream_returns_hash_stream() {
    clear_env();
    unsafe {
        std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
    }
    assert_eq!(
        PublishSharding::from_env(),
        Some(PublishSharding::HashStream)
    );
    clear_env();
}

#[test]
#[serial]
fn from_env_invalid_returns_none() {
    clear_env();
    unsafe {
        std::env::set_var("FELIX_PUB_SHARDING", "invalid");
    }
    assert!(PublishSharding::from_env().is_none());
    clear_env();
}

#[tokio::test]
async fn select_worker_round_robin_advances() {
    let publisher = make_publisher(PublishSharding::RoundRobin, 2);
    let rr_start = publisher.inner.rr.load(Ordering::Relaxed);
    let _ = publisher
        .publish("t", "ns", "s", b"p1".to_vec(), AckMode::None)
        .await;
    let _ = publisher
        .publish("t", "ns", "s", b"p2".to_vec(), AckMode::None)
        .await;
    let rr_end = publisher.inner.rr.load(Ordering::Relaxed);
    assert!(rr_end >= rr_start + 2);
    publisher.finish().await.expect("finish");
}

#[test]
fn stream_cache_eviction_preserves_recent_entries() {
    let mut cache = StreamShardCache::new(2);
    cache.insert(StreamKey::new("t1", "ns", "a"), 0);
    cache.insert(StreamKey::new("t1", "ns", "b"), 1);
    assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "a")), Some(0));
    cache.insert(StreamKey::new("t1", "ns", "c"), 2);
    assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "a")), None);
    assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "b")), Some(1));
    assert_eq!(cache.get(StreamKeyRef::new("t1", "ns", "c")), Some(2));
}

fn selected_index(publisher: &crate::publish::Publisher, stream: &str) -> usize {
    let selected = publisher
        .select_worker("t", "ns", stream)
        .expect("select worker");
    publisher
        .inner
        .workers
        .iter()
        .position(|worker| std::ptr::eq(worker, selected))
        .expect("selected worker is in the pool")
}

/// Every client in a process publishing one stream must not pick the same
/// pool slot: the slot is a connection, and a connection is one broker
/// listener, so a shared pick piles a whole load generator onto one port.
#[tokio::test]
async fn separate_clients_spread_one_stream_across_the_pool() {
    let picks: std::collections::HashSet<usize> = (0..32)
        .map(|_| selected_index(&make_publisher(PublishSharding::HashStream, 4), "orders"))
        .collect();
    assert!(
        picks.len() > 1,
        "32 clients all hashed one stream to worker {picks:?}"
    );
}

/// End one worker's writer the way a broken stream does: it stops reading its
/// queue and drops it.
async fn kill_worker(publisher: &crate::publish::Publisher, index: usize) {
    let worker = &publisher.inner.workers[index];
    let (response, answered) = tokio::sync::oneshot::channel();
    worker
        .tx
        .send(crate::publish::writer::PublishRequest::Finish { response })
        .await
        .expect("worker running");
    let _ = answered.await;
    while !worker.tx.is_closed() {
        tokio::task::yield_now().await;
    }
}

/// A writer that died on a broken stream must leave the pool: the streams
/// hashed to it move to a live writer, and stay there, rather than failing
/// every publish from then on.
#[tokio::test]
async fn a_dead_worker_is_skipped_and_its_streams_stay_together() {
    let publisher = make_publisher(PublishSharding::HashStream, 4);
    let first = selected_index(&publisher, "orders");
    kill_worker(&publisher, first).await;

    let moved = selected_index(&publisher, "orders");
    assert_ne!(moved, first, "a stream stayed on a dead writer");
    assert_eq!(selected_index(&publisher, "orders"), moved);
    publisher
        .publish("t", "ns", "orders", b"p".to_vec(), AckMode::None)
        .await
        .expect("publish through a live writer");

    for index in 0..4 {
        if !publisher.inner.workers[index].tx.is_closed() {
            kill_worker(&publisher, index).await;
        }
    }
    assert!(publisher.select_worker("t", "ns", "orders").is_err());
}

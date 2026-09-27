use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

#[tokio::test]
async fn same_shard_id_runs_on_same_thread() {
    let shards = CoreShards::new(2);
    let thread_name = |handle_id: u64, shards: &Arc<CoreShards>| {
        let (tx, rx) = oneshot::channel();
        shards.handle_for(handle_id).spawn(async move {
            let _ = tx.send(std::thread::current().name().map(str::to_owned));
        });
        rx
    };
    let a = thread_name(0, &shards).await.expect("shard 0 task");
    let b = thread_name(2, &shards).await.expect("shard 0 again");
    let c = thread_name(1, &shards).await.expect("shard 1 task");
    assert_eq!(a, b, "same shard id must map to the same thread");
    assert_ne!(a, c, "different shards must map to different threads");
    assert_eq!(a.as_deref(), Some("felix-shard-0"));
    assert_eq!(c.as_deref(), Some("felix-shard-1"));
}

#[tokio::test]
async fn tasks_execute_and_shutdown_on_drop() {
    let shards = CoreShards::new(1);
    let counter = Arc::new(AtomicUsize::new(0));
    let mut waiters = Vec::new();
    for _ in 0..8 {
        let counter = Arc::clone(&counter);
        let (tx, rx) = oneshot::channel();
        shards.handle_for(7).spawn(async move {
            counter.fetch_add(1, Ordering::Relaxed);
            let _ = tx.send(());
        });
        waiters.push(rx);
    }
    for rx in waiters {
        tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("shard task timed out")
            .expect("shard task dropped");
    }
    assert_eq!(counter.load(Ordering::Relaxed), 8);
    drop(shards);
}

#[test]
fn shard_mapping_is_stable_modulo() {
    let shards = CoreShards::new(3);
    assert_eq!(shards.len(), 3);
    assert_eq!(shards.shard_for(0), 0);
    assert_eq!(shards.shard_for(4), 1);
    assert_eq!(shards.shard_for(5), 2);
    assert_eq!(shards.shard_for(6), 0);
}

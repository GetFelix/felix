use tokio::sync::mpsc;

use super::{ShardItem, ShardStep, ShardedCacheWatch, ShardedCacheWatchItem};
use crate::cache::{CacheChange, CacheWatchItem};
use crate::cluster::follow::WatchProgress;

fn retained_watch(shards: &[u64]) -> (ShardedCacheWatch, mpsc::Sender<ShardItem>) {
    let (tx, rx) = mpsc::channel(16);
    let progress: Vec<_> = shards
        .iter()
        .map(|count| WatchProgress::new(100, None, Some(*count), false))
        .collect();
    let state_pending = progress.iter().filter(|p| p.in_state_phase()).count();
    let watch = ShardedCacheWatch {
        items: rx,
        progress,
        retained_counts: shards.to_vec(),
        retained: true,
        state_pending,
        state_announced: false,
        tasks: Vec::new(),
    };
    (watch, tx)
}

fn change(offset: u64) -> ShardStep {
    ShardStep::Item(CacheWatchItem::Change(CacheChange {
        key: format!("k{offset}"),
        value: None,
        offset,
        expires_at_millis: 0,
    }))
}

/// A shard reopened mid-state onto an empty state has no item to hand out,
/// so the reopen itself has to end its state phase.
#[tokio::test]
async fn a_reopen_onto_an_empty_state_completes_the_state() {
    let (mut watch, tx) = retained_watch(&[2]);
    let mut progress = WatchProgress::new(100, None, Some(2), false);
    progress.observe_change(40);
    tx.send((0, change(40), progress)).await.unwrap();
    assert!(matches!(
        watch.recv().await,
        Some(ShardedCacheWatchItem::Change { shard: 0, .. })
    ));

    progress.reopened(120, false, Some(0));
    tx.send((0, ShardStep::Reopened, progress)).await.unwrap();
    assert_eq!(
        watch.recv().await,
        Some(ShardedCacheWatchItem::StateComplete)
    );
}

/// A restarted state phase keeps the shard pending until the new owner's
/// values have all been handed out.
#[tokio::test]
async fn a_restarted_state_phase_holds_back_state_complete() {
    let (mut watch, tx) = retained_watch(&[2]);
    let mut progress = WatchProgress::new(100, None, Some(2), false);
    progress.reopened(120, false, Some(1));
    tx.send((0, ShardStep::Reopened, progress)).await.unwrap();
    progress.observe_change(110);
    tx.send((0, change(110), progress)).await.unwrap();
    assert!(matches!(
        watch.recv().await,
        Some(ShardedCacheWatchItem::Change { shard: 0, .. })
    ));
    assert_eq!(
        watch.recv().await,
        Some(ShardedCacheWatchItem::StateComplete)
    );
}

#[tokio::test]
async fn a_closed_shard_carries_its_cause() {
    let (mut watch, tx) = retained_watch(&[0]);
    assert_eq!(
        watch.recv().await,
        Some(ShardedCacheWatchItem::StateComplete)
    );
    let progress = WatchProgress::new(100, None, Some(0), false);
    tx.send((0, ShardStep::Closed(Some("gone".into())), progress))
        .await
        .unwrap();
    assert_eq!(
        watch.recv().await,
        Some(ShardedCacheWatchItem::ShardClosed {
            shard: 0,
            error: Some("gone".into())
        })
    );
}

//! A prefix watch over every shard of a cache, read as one thing.
//!
//! A cache watch reads **one** shard, and keys sharing a prefix hash to
//! different shards, so covering a prefix of a multi-shard cache means one watch
//! per shard, each on its own shard's owner. This opens them and merges what
//! comes back; each follows its shard when it moves. See [`ShardedCacheWatch`]
//! for what is and is not promised.

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::ShardOffsets;
use crate::cache::{CacheChange, CacheWatchFilter, CacheWatchItem};
use crate::cluster::Client;
use crate::cluster::follow::{ClusterCacheWatch, WatchProgress, WatchStep, WatchTarget};
use crate::subscribe::ShardMoved;

/// A prefix watch over every shard of one cache, merged into a single handle.
///
/// **Ordering is per key.** A key lives on exactly one shard, so every change
/// to one key arrives in write order. Changes to keys on different shards
/// arrive in an arbitrary order relative to each other, and each change's
/// `offset` belongs to its own shard's log.
///
/// **Resumption is a vector.** [`ShardedCacheWatch::resume_offsets`] is one
/// offset per shard, never ahead of what has been handed out; pass it to
/// [`Client::watch_cache_sharded`] to resume.
///
/// **A shard that moves is followed.** Its watch reopens on the new owner
/// where it left off, announced by [`ShardedCacheWatchItem::ShardMoved`]. A
/// lost connection is followed the same way, without a notice.
///
/// Dropping this ends every shard's watch.
#[derive(Debug)]
pub struct ShardedCacheWatch {
    items: mpsc::Receiver<ShardItem>,
    /// Each shard's position as of the last item handed out from it.
    progress: Vec<WatchProgress>,
    retained_counts: Vec<u64>,
    retained: bool,
    /// Shards still delivering their current state.
    state_pending: usize,
    state_announced: bool,
    tasks: Vec<JoinHandle<()>>,
}

impl ShardedCacheWatch {
    /// How many shards this watch covers.
    pub fn shards(&self) -> u32 {
        self.progress.len() as u32
    }

    /// How many retained values precede live delivery, summed across shards,
    /// on a retained watch.
    ///
    /// `Some(0)` means the prefix held nothing on any shard at join. The
    /// shards' state phases interleave with each other's live changes, so
    /// counting items does not tell when the state is complete:
    /// [`ShardedCacheWatchItem::StateComplete`] does.
    pub fn retained_count(&self) -> Option<u64> {
        self.retained.then(|| self.retained_counts.iter().sum())
    }

    /// Where to resume each shard, as the `from_offset` a new watch should
    /// start at.
    ///
    /// Every shard is listed. A shard still in its state phase resumes at 0,
    /// because its retained values can carry any offset below where live
    /// delivery began and none of them may be skipped; that replays or
    /// resnapshots the shard's history rather than missing a key.
    pub fn resume_offsets(&self) -> ShardOffsets {
        self.progress
            .iter()
            .enumerate()
            .map(|(shard, progress)| (shard as u32, progress.resume_from))
            .collect()
    }

    /// The next item from any shard.
    ///
    /// `None` once every shard's watch has ended and its items have been
    /// handed out.
    pub async fn recv(&mut self) -> Option<ShardedCacheWatchItem> {
        loop {
            if self.retained && self.state_pending == 0 && !self.state_announced {
                self.state_announced = true;
                return Some(ShardedCacheWatchItem::StateComplete);
            }
            let (shard, item, after) = self.items.recv().await?;
            let progress = &mut self.progress[shard as usize];
            // A shard whose state phase restarted after a reopen stays in it,
            // so it stays pending until its new state has all been handed out.
            if progress.in_state_phase() && !after.in_state_phase() {
                self.state_pending -= 1;
            }
            *progress = after;
            return Some(match item {
                ShardStep::Item(CacheWatchItem::Change(change)) => {
                    ShardedCacheWatchItem::Change { shard, change }
                }
                ShardStep::Item(CacheWatchItem::Lagged { resume_from }) => {
                    ShardedCacheWatchItem::Lagged { shard, resume_from }
                }
                ShardStep::Item(CacheWatchItem::ShardMoved(moved)) => {
                    ShardedCacheWatchItem::ShardMoved { shard, moved }
                }
                ShardStep::Reopened => continue,
                ShardStep::Closed(error) => ShardedCacheWatchItem::ShardClosed { shard, error },
            });
        }
    }
}

impl Drop for ShardedCacheWatch {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// What arrives from a sharded cache watch.
///
/// Losing a shard is an item, not silence: the other shards keep delivering,
/// and a consumer that ignores these variants is choosing to watch part of the
/// prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardedCacheWatchItem {
    /// A change, and the shard it was read from.
    Change { shard: u32, change: CacheChange },
    /// Every shard has delivered its current state; from here the watch holds
    /// the prefix's state and everything further is live. Only on a retained
    /// watch, at most once, and never if a shard ended before its state was
    /// fully delivered.
    StateComplete,
    /// This shard fell behind and the broker ended its watch. The other shards
    /// are unaffected. [`ShardedCacheWatch::resume_offsets`] already accounts
    /// for it, so resuming from those is gapless.
    Lagged { shard: u32, resume_from: u64 },
    /// This shard moved to another broker, and its watch is following it
    /// there. The changes that follow resume where the old owner left off; if
    /// the new owner cannot be reached in time, a
    /// [`ShardedCacheWatchItem::ShardClosed`] comes next. A move inside a
    /// retained watch's state phase restarts that shard's state phase on the
    /// new owner, so its values may arrive again before
    /// [`ShardedCacheWatchItem::StateComplete`].
    ShardMoved { shard: u32, moved: ShardMoved },
    /// This shard's watch ended without a lag signal: the broker ended it, or
    /// it could not be followed to another broker within the client's
    /// reconnect deadline, and `error` then says why. The other shards are
    /// unaffected.
    ShardClosed { shard: u32, error: Option<String> },
}

/// One shard's step, with that shard's position once it is handed out.
type ShardItem = (u32, ShardStep, WatchProgress);

enum ShardStep {
    Item(CacheWatchItem),
    /// Carries only the new position, which matters when a reopen restarted
    /// or ended the state phase.
    Reopened,
    /// The watch ended without a lag signal, with the cause when there was
    /// one.
    Closed(Option<String>),
}

/// Open one prefix watch per shard and forward all of them into one channel.
///
/// Every shard is opened before this returns, or the whole call fails naming
/// the shards that could not be.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn watch_sharded(
    cluster: &Arc<Client>,
    tenant_id: &str,
    namespace: &str,
    cache: &str,
    prefix: &str,
    shards: u32,
    resume: Option<ShardOffsets>,
    retained: bool,
) -> Result<ShardedCacheWatch> {
    anyhow::ensure!(shards > 0, "a cache with no shards cannot be watched");

    // Opened concurrently, so the cost is one round trip rather than one per
    // shard.
    let mut opening = Vec::with_capacity(shards as usize);
    for shard in 0..shards {
        let cluster = Arc::clone(cluster);
        let (tenant_id, namespace, cache) = (
            tenant_id.to_string(),
            namespace.to_string(),
            cache.to_string(),
        );
        let filter = CacheWatchFilter::Prefix(prefix.to_string());
        let target = WatchTarget::new(&tenant_id, &namespace, &cache, filter, Some(shard));
        let from_offset = resume.as_ref().and_then(|at| at.get(&shard).copied());
        opening.push(tokio::spawn(async move {
            let opened = cluster.open_watch(target, from_offset, retained).await;
            (shard, opened)
        }));
    }

    let mut opened: Vec<(u32, ClusterCacheWatch)> = Vec::with_capacity(shards as usize);
    let mut failures: Vec<String> = Vec::new();
    for task in opening {
        match task.await {
            Ok((shard, Ok(watch))) => opened.push((shard, watch)),
            Ok((shard, Err(err))) => failures.push(format!("shard {shard}: {err:#}")),
            Err(err) => failures.push(format!("a shard's open task failed: {err}")),
        }
    }
    if !failures.is_empty() {
        anyhow::bail!(
            "could not watch every shard of cache {cache}, so the watch would be silently \
             incomplete: {}",
            failures.join("; ")
        );
    }
    opened.sort_by_key(|(shard, ..)| *shard);

    let (tx, rx) = mpsc::channel(64 * shards as usize);
    let mut progress = Vec::with_capacity(opened.len());
    let mut retained_counts = Vec::with_capacity(opened.len());
    let mut tasks = Vec::with_capacity(opened.len());
    for (shard, watch) in opened {
        progress.push(watch.progress());
        retained_counts.push(watch.retained_count().unwrap_or(0));
        tasks.push(tokio::spawn(forward_shard(shard, watch, tx.clone())));
    }
    let state_pending = progress
        .iter()
        .filter(|shard| shard.in_state_phase())
        .count();

    Ok(ShardedCacheWatch {
        items: rx,
        progress,
        retained_counts,
        retained,
        state_pending,
        state_announced: false,
        tasks,
    })
}

/// Pump one shard's watch into the shared channel.
async fn forward_shard(shard: u32, mut watch: ClusterCacheWatch, tx: mpsc::Sender<ShardItem>) {
    while let Some(step) = watch.step().await {
        let (step, lagged) = match step {
            WatchStep::Item(item) => {
                let lagged = matches!(item, CacheWatchItem::Lagged { .. });
                (ShardStep::Item(item), lagged)
            }
            WatchStep::Reopened => (ShardStep::Reopened, false),
        };
        if tx.send((shard, step, watch.progress())).await.is_err() || lagged {
            return;
        }
    }
    let error = watch.error().map(|err| format!("{err:#}"));
    let _ = tx
        .send((shard, ShardStep::Closed(error), watch.progress()))
        .await;
}

#[cfg(test)]
mod tests;

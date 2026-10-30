//! Following a cache watch's shard when it moves to another broker.
//!
//! The old owner ends the watch with `shard_moved`, after every change it
//! applied has been queued to the watcher. A watch reads the shard's cache log,
//! which moves with the shard and keeps its offsets, so the watch carries on by
//! opening a new one on the new owner from the offset after what it has seen.

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cache::{CacheWatch, CacheWatchFilter, CacheWatchItem};
use crate::client::Connection;
use crate::cluster::Client;
use crate::subscribe::ShardMoved;

/// A cache watch through a [`Client`] that follows its shard when it
/// moves.
///
/// When the shard's owner hands it to another broker, the watch yields
/// [`CacheWatchItem::ShardMoved`] as a notice and the next [`Self::recv`]
/// opens a watch on the new owner and carries on from
/// [`Self::resume_from`]. The changes that follow pick up exactly where the
/// old owner left off: none is repeated, and none is skipped unless it was
/// compacted away on the new owner first, in which case the watch resnapshots
/// as any resume from that offset would.
///
/// A lost connection, a broker killed under the watch for one, is followed
/// the same way, without a notice: the next [`Self::recv`] opens a watch on
/// whichever broker owns the shard now, from [`Self::resume_from`].
///
/// A retained watch that moves or loses its connection inside its state
/// phase is reopened retained, and the state phase starts over with the new
/// owner's current values. Values handed out before the reopen may then
/// arrive again before [`Self::in_state_phase`] turns false. Outside the
/// state phase nothing is repeated.
///
/// A [`CacheWatchItem::Lagged`] still ends the watch, as on a
/// [`CacheWatch`]. So does failing to reach the new owner within the client's
/// reconnect deadline; [`Self::error`] then says why, and
/// [`Self::resume_from`] is where a new watch should start.
///
/// Dropping this ends the watch.
pub struct ClusterCacheWatch {
    cluster: Arc<Client>,
    target: WatchTarget,
    /// Held so the connection the changes arrive on stays open.
    client: Arc<Connection>,
    watch: CacheWatch,
    progress: WatchProgress,
    /// Set by a `ShardMoved` and cleared once the new owner is watched, so a
    /// `recv` cancelled while following tries again next time.
    following: Option<ShardMoved>,
    /// Set when the connection broke and cleared once a broker is watched
    /// again, for the same reason as `following`.
    lost: bool,
    /// Why the watch could not be followed, once it could not.
    error: Option<anyhow::Error>,
    moves: u64,
    reconnects: u64,
    resume_offset: u64,
    resnapshot: bool,
    retained_count: Option<u64>,
}

impl ClusterCacheWatch {
    pub(crate) fn new(
        cluster: Arc<Client>,
        target: WatchTarget,
        from_offset: Option<u64>,
        client: Arc<Connection>,
        watch: CacheWatch,
    ) -> Self {
        Self {
            cluster,
            target,
            client,
            progress: WatchProgress::new(
                watch.resume_offset(),
                from_offset,
                watch.retained_count(),
                watch.resnapshot(),
            ),
            following: None,
            lost: false,
            error: None,
            moves: 0,
            reconnects: 0,
            resume_offset: watch.resume_offset(),
            resnapshot: watch.resnapshot(),
            retained_count: watch.retained_count(),
            watch,
        }
    }

    /// The next item, or `None` once the watch has ended.
    ///
    /// Following a move happens inside this call, after the
    /// [`CacheWatchItem::ShardMoved`] notice has been handed out, and so does
    /// following a lost connection. It is cancel-safe: a call dropped
    /// mid-follow leaves the follow to the next. When a follow fails this
    /// returns `None` and [`Self::error`] says why; calling again after a lost
    /// connection tries again.
    pub async fn recv(&mut self) -> Option<CacheWatchItem> {
        loop {
            if let WatchStep::Item(item) = self.step().await? {
                return Some(item);
            }
        }
    }

    /// Like [`Self::recv`], but also says when the watch was reopened, so a
    /// caller tracking [`Self::progress`] sees a state phase that restarted
    /// or ended without an item.
    pub(crate) async fn step(&mut self) -> Option<WatchStep> {
        loop {
            if let Some(moved) = self.following.clone() {
                let followed = self.follow(&moved).await;
                self.following = None;
                if let Err(err) = followed {
                    tracing::warn!(
                        error = %format!("{err:#}"),
                        resume_from = self.progress.resume_from,
                        "a cache watch could not follow its shard to the new owner",
                    );
                    self.error = Some(err);
                    return None;
                }
                return Some(WatchStep::Reopened);
            }
            if self.lost {
                match self.reopen().await {
                    Ok(()) => {
                        self.lost = false;
                        return Some(WatchStep::Reopened);
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %format!("{err:#}"),
                            resume_from = self.progress.resume_from,
                            "a cache watch could not be reopened after losing its connection",
                        );
                        self.error = Some(err);
                        return None;
                    }
                }
            }
            let Some(item) = self.watch.recv().await else {
                // A clean end is final. A broken connection is not: the shard
                // is still served somewhere, often by a promoted replica.
                if self.watch.lost().is_some() {
                    self.lost = true;
                    continue;
                }
                return None;
            };
            match &item {
                CacheWatchItem::Change(change) => {
                    self.progress.observe_change(change.offset);
                }
                CacheWatchItem::Lagged { resume_from } => self.progress.observe_lag(*resume_from),
                CacheWatchItem::ShardMoved(moved) => {
                    self.progress.observe_move(moved.resume_from);
                    self.following = Some(moved.clone());
                }
            }
            return Some(WatchStep::Item(item));
        }
    }

    /// Why the watch ended, when it ended because it could not be followed to
    /// another broker within the client's reconnect deadline.
    pub fn error(&self) -> Option<&anyhow::Error> {
        self.error.as_ref()
    }

    /// How many times this watch has been reopened after losing its
    /// connection.
    pub fn reconnects(&self) -> u64 {
        self.reconnects
    }

    /// True while a retained watch's current values are still to come. It
    /// turns false once they have all been handed out, and can turn true
    /// again when a move or a lost connection restarts the state phase.
    pub fn in_state_phase(&self) -> bool {
        self.progress.in_state_phase()
    }

    /// The `from_offset` that resumes this watch without a gap, counting
    /// everything handed out so far.
    ///
    /// On a retained watch still in its state phase it is 0: values not yet
    /// delivered can carry any offset below where live delivery began.
    pub fn resume_from(&self) -> u64 {
        self.progress.resume_from
    }

    /// [`CacheWatch::resume_offset`] of the watch as first opened.
    pub fn resume_offset(&self) -> u64 {
        self.resume_offset
    }

    /// [`CacheWatch::resnapshot`] of the watch as first opened.
    pub fn resnapshot(&self) -> bool {
        self.resnapshot
    }

    /// [`CacheWatch::retained_count`] of the watch as first opened.
    pub fn retained_count(&self) -> Option<u64> {
        self.retained_count
    }

    /// How many times this watch has followed its shard to a new owner.
    pub fn moves(&self) -> u64 {
        self.moves
    }

    /// The client connected to the broker currently serving this watch. It
    /// changes when the shard moves.
    pub fn client(&self) -> &Arc<Connection> {
        &self.client
    }

    pub(crate) fn progress(&self) -> WatchProgress {
        self.progress
    }

    async fn follow(&mut self, moved: &ShardMoved) -> Result<()> {
        let from = self.progress.resume_from;
        // Retained values still to come can sit anywhere below where live
        // delivery began, so only a fresh state phase is sure to hold them.
        let retained = self.progress.in_state_phase();
        let (cluster, target) = (&self.cluster, &self.target);
        let (client, watch) = cluster
            .on_new_owner(moved, |first| {
                cluster.watch_via(first, target, Some(from), retained)
            })
            .await?;
        self.progress.reopened(
            watch.resume_offset(),
            watch.resnapshot(),
            retained.then(|| watch.retained_count().unwrap_or(0)),
        );
        self.client = client;
        self.watch = watch;
        self.moves += 1;
        Ok(())
    }

    async fn reopen(&mut self) -> Result<()> {
        let from = self.progress.resume_from;
        let retained = self.progress.in_state_phase();
        let (cluster, target) = (&self.cluster, &self.target);
        let (client, watch) = cluster
            .reopen_after_loss(&self.client, || async {
                cluster
                    .watch_via(cluster.client().await, target, Some(from), retained)
                    .await
            })
            .await
            .with_context(|| {
                format!(
                    "reopen the watch on cache {} after losing the connection",
                    target.cache
                )
            })?;
        self.progress.reopened(
            watch.resume_offset(),
            watch.resnapshot(),
            retained.then(|| watch.retained_count().unwrap_or(0)),
        );
        self.client = client;
        self.watch = watch;
        self.reconnects += 1;
        Ok(())
    }
}

impl std::fmt::Debug for ClusterCacheWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterCacheWatch")
            .field("target", &self.target)
            .field("progress", &self.progress)
            .field("following", &self.following)
            .field("lost", &self.lost)
            .field("moves", &self.moves)
            .finish_non_exhaustive()
    }
}

/// One step of a [`ClusterCacheWatch`].
pub(crate) enum WatchStep {
    Item(CacheWatchItem),
    /// The watch was reopened on another broker, without an item to show for
    /// it yet.
    Reopened,
}

/// What a watch reads: which cache, which keys, and which shard when the
/// caller names one.
#[derive(Debug, Clone)]
pub(crate) struct WatchTarget {
    pub(crate) tenant_id: String,
    pub(crate) namespace: String,
    pub(crate) cache: String,
    pub(crate) filter: CacheWatchFilter,
    pub(crate) shard: Option<u32>,
}

impl WatchTarget {
    pub(crate) fn new(
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        filter: CacheWatchFilter,
        shard: Option<u32>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            filter,
            shard,
        }
    }
}

/// Where a watch has reached, advanced as items are handed out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WatchProgress {
    /// The `from_offset` that resumes the watch without a gap.
    pub(crate) resume_from: u64,
    /// Where live delivery began.
    live_from: u64,
    /// Whatever precedes live delivery arrives in log order, so each item can
    /// advance the position. False for retained values and resnapshots, which
    /// arrive in key order.
    in_log_order: bool,
    retained_left: u64,
}

impl WatchProgress {
    pub(crate) fn new(
        live_from: u64,
        from_offset: Option<u64>,
        retained: Option<u64>,
        resnapshot: bool,
    ) -> Self {
        let retained_left = retained.unwrap_or(0);
        let resume_from = if retained_left > 0 {
            0
        } else {
            from_offset.unwrap_or(live_from)
        };
        Self {
            resume_from,
            live_from,
            in_log_order: retained.is_none() && !resnapshot,
            retained_left,
        }
    }

    /// True while retained values are still to come.
    pub(crate) fn in_state_phase(&self) -> bool {
        self.retained_left > 0
    }

    /// True when this change completed the state phase.
    pub(crate) fn observe_change(&mut self, offset: u64) -> bool {
        if self.retained_left > 0 {
            self.retained_left -= 1;
            if self.retained_left == 0 {
                self.resume_from = self.resume_from.max(self.live_from);
                return true;
            }
            return false;
        }
        if offset >= self.live_from || self.in_log_order {
            self.resume_from = self.resume_from.max(offset.saturating_add(1));
        }
        false
    }

    /// The broker names the offset that resumes gaplessly, except mid-state:
    /// retained values not yet delivered can sit below it, so such a watch
    /// stays at 0.
    pub(crate) fn observe_lag(&mut self, resume_from: u64) {
        if self.retained_left == 0 {
            self.resume_from = resume_from;
        }
    }

    /// Every change below the old owner's `resume_from` was queued to the
    /// watch before the move, so it is safe to skip to it. Without one, the
    /// position after the last change handed out is.
    pub(crate) fn observe_move(&mut self, resume_from: Option<u64>) {
        if let (0, Some(resume_from)) = (self.retained_left, resume_from) {
            self.resume_from = self.resume_from.max(resume_from);
        }
    }

    /// The watch was reopened on another broker. A reopen that asked for
    /// retained values, `retained` of them, starts the state phase over; one
    /// that did not resumes from `resume_from`.
    pub(crate) fn reopened(&mut self, live_from: u64, resnapshot: bool, retained: Option<u64>) {
        match retained {
            Some(count) => *self = Self::new(live_from, None, Some(count), resnapshot),
            None => self.rebase(live_from, resnapshot),
        }
    }

    /// The watch was reopened from `resume_from` without retained values.
    pub(crate) fn rebase(&mut self, live_from: u64, resnapshot: bool) {
        self.live_from = live_from;
        self.in_log_order = !resnapshot;
        self.retained_left = 0;
    }
}

#[cfg(test)]
mod tests;

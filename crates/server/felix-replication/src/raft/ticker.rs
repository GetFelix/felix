//! The node ticker: the one timer behind every group on a broker.
//!
//! openraft notices an election timeout or a due heartbeat only when its
//! timer check runs. Each group is built with its own tick off, and this
//! task looks at every group each interval and asks for a check from the
//! ones whose timer can have run out. A follower that hears its leader is
//! never asked, and a leader is asked once per heartbeat. See
//! `docs/per-shard-raft.md`, "One timer for every group".

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers};
use super::runtime::Registry;

pub(crate) async fn run(
    registry: Arc<Registry>,
    every: Duration,
    timers: Timers,
    stop: CancellationToken,
) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = interval.tick() => {}
        }
        let groups = registry.ticked();
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tick_all(&groups, timers.now(), Instant::now() + every) => {}
        }
    }
}

/// One timer check on every group that is due at `now`, until `deadline`.
///
/// The checks are polled together in this task. A check waits while its
/// group's queue is full, so asking the groups in turn would let one busy
/// group hold up the rest. A check still waiting at the deadline is dropped,
/// never queued behind the next interval's: openraft reads the clock when it
/// runs a check, so the next one does the same work.
async fn tick_all(groups: &[Arc<Group>], now: u64, deadline: Instant) {
    let mut waiting: FuturesUnordered<_> = groups
        .iter()
        .filter(|group| group.due(now))
        .map(|group| group.tick())
        .collect();
    if waiting.is_empty() {
        return;
    }
    let all = async { while waiting.next().await.is_some() {} };
    let _ = tokio::time::timeout_at(deadline, all).await;
}

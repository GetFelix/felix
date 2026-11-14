//! The node ticker: the one timer behind every group on a broker.
//!
//! openraft notices an election timeout or a due heartbeat only when its
//! timer check runs. Each group is built with its own tick off, and this
//! task asks for a check, each interval, from the groups whose timer can
//! have run out. A follower that hears its leader is
//! never asked, and a leader is asked once per heartbeat. See
//! `docs/per-shard-raft.md`, "One timer for every group".

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers};
use super::runtime::Registry;

/// Times the ticker ended on a panic and was started again. Any increase
/// is a bug: no group on the broker was ticked in between.
const TICKER_RESTARTS_TOTAL: &str = "felix_broker_raft_ticker_restarts_total";

/// Run the ticker until `stop`, starting it again if it panics.
///
/// Without a ticker no leader here heartbeats and no follower here stands,
/// and nothing else would show it. A restart puts both back within a
/// heartbeat, where failing the runtime would stop every group for a bug
/// that may be in one. The restarts are logged and counted for whoever
/// watches the broker.
pub(crate) async fn supervise(
    registry: Arc<Registry>,
    every: Duration,
    timers: Timers,
    stop: CancellationToken,
) {
    loop {
        let ticker = run(Arc::clone(&registry), every, timers, stop.clone());
        let Err(panic) = AssertUnwindSafe(ticker).catch_unwind().await else {
            return;
        };
        let reason = panic
            .downcast_ref::<&str>()
            .map(|reason| reason.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "no message".into());
        let restarts = registry.ticker_restarts.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::counter!(TICKER_RESTARTS_TOTAL).increment(1);
        tracing::error!(%reason, restarts, "the Raft ticker panicked and is being started again");
        // A ticker that panics every time must not spin.
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tokio::time::sleep(Duration::from_millis(timers.heartbeat)) => {}
        }
    }
}

async fn run(registry: Arc<Registry>, every: Duration, timers: Timers, stop: CancellationToken) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut looks = Looks::default();
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = interval.tick() => {}
        }
        #[cfg(test)]
        if registry.trip_ticker.swap(false, Ordering::Relaxed) {
            panic!("tripped by a test");
        }
        registry.tail_budget.report();
        let due = looks.due(registry.ticked(), &timers);
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tick_all(&due, Instant::now() + every) => {}
        }
    }
}

/// When the ticker next has to read each group's due time.
///
/// Reading it means touching the group, and with thousands of groups that
/// is most of what an interval costs. Anything other than a timer check
/// that moves a group's due time moves it at least a heartbeat ahead of the
/// moment it runs (`Pace::wrote`, `Pace::followed`), so a group read now
/// and found not due cannot become due sooner than that.
#[derive(Default)]
struct Looks {
    groups: Arc<Vec<Arc<Group>>>,
    /// By position in `groups`, on the runtime's clock.
    at: Vec<u64>,
}

impl Looks {
    /// The groups whose timer check is due now.
    fn due(&mut self, groups: Arc<Vec<Arc<Group>>>, timers: &Timers) -> Vec<Arc<Group>> {
        if !Arc::ptr_eq(&self.groups, &groups) {
            // A group was opened or closed, and positions moved.
            self.at.clear();
            self.at.resize(groups.len(), 0);
            self.groups = groups;
        }
        let now = timers.now();
        let mut due = Vec::new();
        for (group, look) in self.groups.iter().zip(&mut self.at) {
            if now < *look {
                continue;
            }
            let at = group.due_at();
            if now >= at {
                // The check sets a new due time: read it next interval.
                due.push(Arc::clone(group));
            } else {
                *look = at.min(now + timers.heartbeat);
            }
        }
        due
    }
}

/// One timer check on each of `groups`, until `deadline`.
///
/// The checks are polled together in this task. A check waits while its
/// group's queue is full, so asking the groups in turn would let one busy
/// group hold up the rest. A check still waiting at the deadline is dropped,
/// never queued behind the next interval's: openraft reads the clock when it
/// runs a check, so the next one does the same work.
async fn tick_all(groups: &[Arc<Group>], deadline: Instant) {
    if groups.is_empty() {
        return;
    }
    let mut waiting: FuturesUnordered<_> = groups.iter().map(|group| group.tick()).collect();
    let all = async { while waiting.next().await.is_some() {} };
    let _ = tokio::time::timeout_at(deadline, all).await;
}

//! The task that keeps every shard this broker leads shipping.
//!
//! One pass visits each shard this node leads that has followers, ships what it
//! can to each, and reports the lag. Cursors live for as long as the assignment
//! does: a generation change discards them, because a cursor is a belief about
//! a follower's position under a particular leadership, and a new generation
//! invalidates the belief rather than the follower.

mod shard;
mod shards;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_router::{ShardKey, ShardRouter};
use futures::StreamExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::halted::{HaltedReplica, HaltedReplicas};
use super::promotion::{self, NoGate, Outcome, PromotionGate};
use super::quorum::QuorumMarks;
use super::reporter::Reporter;
use super::reporter::ShardReport;
use super::{MoveThrottle, RebuildPolicy, Rebuilds, metrics};
use crate::peer::PeerRequester;
use shard::{AuxCursors, ShardCursors, ShardPass, Stragglers, replicate_shard, watch_key};
use shards::{Context, Event, Shards};

/// How many shards a pass ships at the same time.
///
/// Each one in flight holds a peer request and may hold an HTTP report, so a
/// broker leading thousands of shards must not open thousands of those at
/// once. High enough that one slow follower does not gate the rest, low enough
/// to stay a bounded amount of concurrent work.
const SHARD_CONCURRENCY: usize = 16;

/// How soon a pass follows one that left a fenced shard undrained. Short,
/// because clients are held for the switch-over; not zero, because a
/// destination that is gone would otherwise be dialled in a tight loop until
/// the control plane drops it.
const DRAIN_RETRY: Duration = Duration::from_millis(10);

/// What a pass publishes for the rest of the broker to read.
///
/// Together because they are the same thing from two sides: the mark is what a
/// `Quorum` publish waits on, and the listing is what an operator reads when a
/// replica stops contributing to one.
pub struct Published {
    pub marks: Arc<QuorumMarks>,
    pub halted: Arc<HaltedReplicas>,
}

/// How soon a pass follows one that left a promoted shard unfenced. The shard
/// serves nothing meanwhile; each attempt already waits on the replicas it
/// cannot reach, so this only spaces out the ones that answer and refuse.
const FENCE_RETRY: Duration = Duration::from_millis(200);

/// How soon a stopping broker asks for another pass when the last one left a
/// shard's followers behind. Not at once: a follower that is unreachable would
/// be redialled in a tight loop for the rest of the drain.
const CATCH_UP_RETRY: Duration = Duration::from_millis(50);

/// The running driver, as a stopping broker needs it.
pub struct Replication {
    task: tokio::task::JoinHandle<()>,
    passes: watch::Receiver<PassesSeen>,
    wake: Arc<tokio::sync::Notify>,
    stop: CancellationToken,
}

/// What the passes so far have established.
///
/// Each wake-up starts a scan over every shard led here, numbered.
#[derive(Debug, Clone, Copy, Default)]
struct PassesSeen {
    /// The latest scan.
    scan: u64,
    /// Every shard led here has finished a pass started at this scan or later.
    finished: u64,
    /// The latest pass of some shard left no follower holding all of its log.
    behind: bool,
}

impl Replication {
    /// Wait until a pass that started after this call ends with every shard
    /// led here held whole by at least one follower.
    ///
    /// For a broker that has stopped taking writes and is about to stop
    /// shipping. The control plane promotes only a follower the leader last
    /// named as holding everything, so a leader that stops with a record none
    /// of its followers has leaves a shard that never fails over. Returns only
    /// once that is not so; the caller bounds the wait.
    pub async fn caught_up(&self) {
        let mut passes = self.passes.clone();
        // A pass under way may have started before the last write landed, so
        // only one from a later scan counts.
        let started_after = passes.borrow_and_update().scan;
        loop {
            self.wake.notify_one();
            if passes.changed().await.is_err() {
                return;
            }
            let seen = *passes.borrow_and_update();
            if seen.finished > started_after {
                if !seen.behind {
                    return;
                }
                tokio::time::sleep(CATCH_UP_RETRY).await;
            }
        }
    }

    /// Stop shipping and reporting, and wait for the pass under way to end.
    pub async fn stop(self) {
        self.stop.cancel();
        let _ = self.task.await;
    }
}

/// Run replication until `shutdown` or [`Replication::stop`].
///
/// A pass runs on each tick, after each durable append, and whenever
/// `routes_changed` is notified.
#[allow(clippy::too_many_arguments)]
pub fn spawn<R: PeerRequester + Send + Sync + 'static>(
    requester: Arc<R>,
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    fence: Arc<dyn WriteFence>,
    gate: Arc<dyn PromotionGate>,
    published: Published,
    reporter: Option<Reporter>,
    interval: Duration,
    routes_changed: Arc<tokio::sync::Notify>,
    rebuild_policy: RebuildPolicy,
    move_throttle: MoveThrottle,
    shutdown: CancellationToken,
) -> Replication {
    let stop = shutdown.child_token();
    let shutdown = stop.clone();
    let wake = Arc::clone(&routes_changed);
    let (seen, passes) = watch::channel(PassesSeen::default());
    let task = tokio::spawn(async move {
        let rebuilds = Rebuilds::new(rebuild_policy);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Woken by a durable append as well as by the tick. Under `Quorum` the
        // publish that just landed is about to wait on a majority, and waiting
        // out a tick first put seconds in front of milliseconds of shipping.
        //
        // Also woken when the routing feed acts on an ownership change: a
        // fence or a cut-over is otherwise a tick away from its drained report
        // or first shipment.
        //
        // The tick stays: it covers shards with no recent appends, the
        // auxiliary logs, and the replica report, none of which a wake
        // signals.
        let appended = broker.appended();
        let mut shards = Shards::new(Context {
            requester: requester.as_ref(),
            broker: &broker,
            router: &router,
            fence: &*fence,
            gate: &*gate,
            marks: &published.marks,
            reporter: reporter.as_ref(),
            rebuilds: &rebuilds,
            throttle: &move_throttle,
        });
        loop {
            // An append during the previous pass left a permit, so this
            // returns at once rather than waiting for the tick — see
            // `Broker::appended`.
            let woken = appended.notified();
            let retry_in = shards.retry_in();
            let retry = async move {
                match retry_in {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let event = tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => Event::Wake,
                _ = woken => Event::Wake,
                _ = routes_changed.notified() => Event::Wake,
                _ = retry => Event::Retry,
                Some((scan, pass)) = shards.passes.next(), if !shards.passes.is_empty() => {
                    Event::Passed(scan, Box::new(pass))
                }
                Some(fenced) = shards.fences.next(), if !shards.fences.is_empty() => {
                    Event::Fenced(fenced)
                }
                Some(answered) = shards.exchanges.next(), if !shards.exchanges.is_empty() => {
                    Event::Answered(answered)
                }
            };
            match event {
                Event::Wake => {
                    shards.scan();
                    seen.send_modify(|seen| seen.scan = shards.scan);
                }
                Event::Retry => shards.retry(),
                Event::Passed(scan, pass) => shards.fold(scan, *pass),
                Event::Fenced(fenced) => shards.fenced(fenced),
                // Changes no pass's outcome, so there is nothing new to say.
                Event::Answered(answered) => {
                    shards.apply(answered);
                    continue;
                }
            }
            // Replaced wholesale, so a halt that has since resolved stops being
            // listed rather than sending an operator after a replica that is
            // already shipping again.
            let (halted, worst_lag, finished, behind) = shards.summary();
            metrics::record_halted(halted.len());
            if let Some(lag) = worst_lag {
                metrics::record_lag(lag);
            }
            published.halted.publish(halted);
            seen.send_if_modified(|seen| {
                let changed = seen.finished != finished || seen.behind != behind;
                seen.finished = finished;
                seen.behind = behind;
                changed
            });
        }
    });
    Replication {
        task,
        passes,
        wake,
        stop,
    }
}

/// The write fence a draining shard is held behind until its writes stop.
///
/// The broker's shard fence answers it; a trait so this crate does not depend
/// on the broker service.
pub trait WriteFence: Send + Sync {
    /// Whether `key` is closed with no write still inside. A shard that was
    /// never fenced here has nothing in flight, so it counts as quiet.
    fn quiesced(&self, key: &crate::ShardKey) -> bool;

    /// `key` acknowledges by its followers at `generation`, so its writes at
    /// that generation no longer need the lease to get in.
    fn serve_without_lease(&self, _key: &crate::ShardKey, _generation: u64) {}
}

/// A fence that holds nothing back: every shard is quiet.
pub(crate) struct Unfenced;

impl WriteFence for Unfenced {
    fn quiesced(&self, _key: &crate::ShardKey) -> bool {
        true
    }
}

/// Ship for every shard this broker leads, once.
///
/// Returns the largest lag seen, so a caller can report it without recomputing.
/// Nothing is fenced here, so a draining shard counts as quiet at once; use
/// [`replicate_once_with`] to drain against a real fence.
// One cursor map per log kind the pass ships, plus the shared context: folding
// the maps into a struct would move the argument list rather than shorten it.
#[allow(clippy::too_many_arguments)]
pub async fn replicate_once<R: PeerRequester + Sync>(
    requester: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    marks: &QuorumMarks,
    reporter: Option<&Reporter>,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    group_cursors: &mut HashMap<ShardKey, ShardCursors>,
    dead_letter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    counter_cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Pass {
    replicate_once_with(
        requester,
        broker,
        router,
        &Unfenced,
        &NoGate,
        marks,
        reporter,
        cursors,
        group_cursors,
        dead_letter_cursors,
        counter_cursors,
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await
}

/// [`replicate_once`] with halted followers rebuilt under `rebuilds`, a move's
/// destination paced by `throttle`, and a draining shard held back until
/// `fence` says its writes have stopped.
#[allow(clippy::too_many_arguments)]
pub async fn replicate_once_with<R: PeerRequester + Sync>(
    requester: &R,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    fence: &dyn WriteFence,
    gate: &dyn PromotionGate,
    marks: &QuorumMarks,
    reporter: Option<&Reporter>,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    group_cursors: &mut HashMap<ShardKey, ShardCursors>,
    dead_letter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    counter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    rebuilds: &Rebuilds,
    throttle: &MoveThrottle,
) -> Pass {
    if broker.durable_storage().is_none() {
        // Nothing to replicate from. Without durable storage a broker's streams
        // are ephemeral and its cache is in memory, so no shard it leads has a
        // log to ship.
        return Pass::default();
    }
    // Slots in use are whatever the cursors still say is rebuilding. A cursor
    // discarded on a generation change or a lost shard took its slot with it,
    // and nothing else would give it back.
    rebuilds.set_in_flight(rebuilding_count(&[
        cursors,
        group_cursors,
        dead_letter_cursors,
        counter_cursors,
    ]));

    let table = router.snapshot();
    let mut worst_lag: Option<u64> = None;
    let mut copying = false;
    let mut drain_pending = false;
    let mut behind = false;
    let mut halted: Vec<HaltedReplica> = Vec::new();
    let mut live_shards = Vec::new();
    let mut reports = Vec::new();
    let mut promoted = Vec::new();

    // Every shard this broker leads, each with the cursors it owns for the
    // duration. Taken out of the maps rather than borrowed from them, which is
    // what lets the shards run at the same time below.
    let mut work = Vec::new();
    for (key, route) in table.iter() {
        if route.leader.node_id != router.local_node_id() {
            continue;
        }
        live_shards.push(key.clone());
        // Promoted and not yet fenced: nothing ships until it is, because it
        // may yet take a tail from a follower that shipping would truncate.
        if gate.awaiting(&watch_key(key)) == Some(route.generation) {
            promoted.push((key.clone(), route.clone()));
            continue;
        }

        if let Some((entry, aux)) = take_work(
            key,
            route,
            cursors,
            group_cursors,
            dead_letter_cursors,
            counter_cursors,
        ) {
            work.push((key.clone(), route.clone(), entry, aux));
        }
    }

    // Once the followers decide acknowledgements, a shard never opens on the
    // lease alone: the old leader no longer stops writing when its lease does.
    let lease_fallback = !marks.acks_by_followers();
    let fencing = open_promoted(
        requester,
        broker,
        router.local_node_id(),
        gate,
        promoted,
        lease_fallback,
    )
    .await;

    // Shards at the same time, not one after another.
    //
    // Sequentially, a shard whose follower is slow held up every shard behind
    // it — including their quorum marks, and so every `Quorum` publish waiting
    // on them. One tenant's unlucky replica became every tenant's latency.
    //
    // Bounded, because each shard in flight holds a peer request and may hold
    // an HTTP report: a broker leading thousands of shards must not open
    // thousands of those at once.
    let no_one = HashSet::new();
    let passes: Vec<ShardPass> =
        futures::stream::iter(work.into_iter().map(|(key, route, entry, aux)| {
            replicate_shard(
                requester,
                broker,
                fence,
                marks,
                reporter,
                rebuilds,
                throttle,
                key,
                route,
                entry,
                aux,
                &no_one,
                Stragglers::Await,
            )
        }))
        .buffer_unordered(SHARD_CONCURRENCY)
        .collect()
        .await;

    for pass in passes {
        cursors.insert(pass.key.clone(), pass.cursors);
        group_cursors.insert(pass.key.clone(), pass.aux.group);
        dead_letter_cursors.insert(pass.key.clone(), pass.aux.dead_letters);
        counter_cursors.insert(pass.key, pass.aux.counters);
        halted.extend(pass.halted);
        copying |= pass.copying;
        drain_pending |= pass.drain_pending;
        behind |= pass.behind;
        if let Some(lag) = pass.lag {
            worst_lag = Some(worst_lag.map_or(lag, |worst: u64| worst.max(lag)));
        }
        if let Some(report) = pass.report {
            reports.push(report);
        }
    }

    // A shard this broker no longer leads keeps no cursors: they would be a
    // belief about a follower under a leadership that has ended.
    cursors.retain(|key, _| live_shards.contains(key));
    group_cursors.retain(|key, _| live_shards.contains(key));
    dead_letter_cursors.retain(|key, _| live_shards.contains(key));
    counter_cursors.retain(|key, _| live_shards.contains(key));
    // A shard this broker no longer leads stops promising a quorum. Dropping
    // the mark ends any publish still waiting on it, rather than leaving it to
    // run out its timeout for an answer that can no longer come.
    marks.retain(&live_shards.iter().map(watch_key).collect::<Vec<_>>());

    metrics::record_halted(halted.len());
    if let Some(lag) = worst_lag {
        metrics::record_lag(lag);
    }
    Pass {
        worst_lag,
        reports,
        halted,
        copying,
        drain_pending,
        behind,
        fencing,
    }
}

async fn fence_one<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    local_node_id: &str,
    key: ShardKey,
    route: felix_router::Route,
    lease_fallback: bool,
) -> (ShardKey, u64, Outcome) {
    let outcome = promotion::fence_shard(
        requester,
        broker,
        local_node_id,
        &key,
        &route,
        lease_fallback,
    )
    .await;
    (key, route.generation, outcome)
}

/// Fence each shard this broker was just promoted to lead, and open the ones
/// that are done. Returns whether any is still waiting.
async fn open_promoted<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    local_node_id: &str,
    gate: &dyn PromotionGate,
    promoted: Vec<(ShardKey, felix_router::Route)>,
    lease_fallback: bool,
) -> bool {
    let outcomes: Vec<(ShardKey, u64, Outcome)> =
        futures::stream::iter(promoted.into_iter().map(|(key, route)| {
            fence_one(requester, broker, local_node_id, key, route, lease_fallback)
        }))
        .buffer_unordered(SHARD_CONCURRENCY)
        .collect()
        .await;
    let mut pending = false;
    for (key, generation, outcome) in outcomes {
        pending |= !open_fenced(gate, &key, generation, outcome).await;
    }
    pending
}

/// Open a promoted shard whose fence has settled. False while it has not.
async fn open_fenced(
    gate: &dyn PromotionGate,
    key: &ShardKey,
    generation: u64,
    outcome: Outcome,
) -> bool {
    match outcome {
        Outcome::Fenced { caught_up_from } => {
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                caught_up_from = ?caught_up_from,
                "a majority took the fence; opening the shard for writes",
            );
            metrics::record_promotion_opened(metrics::PATH_FENCED);
            gate.open(&watch_key(key), generation).await;
            true
        }
        Outcome::Lease { lacking } => {
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                lacking = %lacking,
                "a replica does not offer the fence; opening the shard on the lease",
            );
            metrics::record_promotion_opened(metrics::PATH_LEASE);
            gate.open(&watch_key(key), generation).await;
            true
        }
        Outcome::Pending(why) => {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                why = %why,
                "the promoted shard is not fenced yet; it does not serve until it is",
            );
            false
        }
    }
}

/// Take a shard's cursors out of the maps for a pass over `route`.
///
/// `None` when there is nothing to ship: the cursors stay in the map, so a
/// destination staged later is known to be one this shard did not have.
fn take_work(
    key: &ShardKey,
    route: &felix_router::Route,
    cursors: &mut HashMap<ShardKey, ShardCursors>,
    group_cursors: &mut HashMap<ShardKey, ShardCursors>,
    dead_letter_cursors: &mut HashMap<ShardKey, ShardCursors>,
    counter_cursors: &mut HashMap<ShardKey, ShardCursors>,
) -> Option<(ShardCursors, AuxCursors)> {
    let previous = cursors.remove(key);
    let learner = shard::staged_learner(previous.as_ref(), route);
    let mut entry = match previous {
        Some(entry) if entry.generation == route.generation => entry,
        _ => ShardCursors::at(route.generation),
    };
    entry.learner = learner;
    // A draining shard reports even with nobody to ship to: its move lost
    // its destination, and the cut-over waits on the drained report.
    if route.replicas.is_empty() && !route.draining {
        cursors.insert(key.clone(), entry);
        return None;
    }
    let aux = AuxCursors {
        group: group_cursors
            .remove(key)
            .unwrap_or_else(|| ShardCursors::at(route.generation)),
        dead_letters: dead_letter_cursors
            .remove(key)
            .unwrap_or_else(|| ShardCursors::at(route.generation)),
        counters: counter_cursors
            .remove(key)
            .unwrap_or_else(|| ShardCursors::at(route.generation)),
    };
    Some((entry, aux))
}

/// What one replication pass established.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Pass {
    /// How far the slowest follower is behind, across every shard led here.
    pub worst_lag: Option<u64>,
    /// Per shard, who could take it over.
    pub reports: Vec<ShardReport>,
    /// Replicas this broker has stopped shipping to, named rather than counted.
    /// The metric cannot carry a shard without a label per tenant; this is what
    /// an operator reads instead.
    pub halted: Vec<HaltedReplica>,
    /// A move's destination is still copying and was cut off at the end of
    /// its slice. The next pass runs at once rather than on the next wake.
    pub copying: bool,
    /// A shard fenced here has not reported drained yet. The next pass runs
    /// after [`DRAIN_RETRY`] rather than on the next wake: the remainder of
    /// a fence made within the lag bound, or a destination that has not yet
    /// seen the new generation, holds the switch-over, and the shard is not
    /// served meanwhile.
    pub drain_pending: bool,
    /// Some shard led here ended the pass with no follower holding all of its
    /// log, not counting halted followers, which waiting does not bring back.
    pub behind: bool,
    /// A shard this broker was promoted to lead is still waiting for a
    /// majority to take its fence. The next pass runs after [`FENCE_RETRY`].
    pub fencing: bool,
}

fn rebuilding_count(maps: &[&HashMap<ShardKey, ShardCursors>]) -> usize {
    maps.iter()
        .flat_map(|map| map.values())
        .flat_map(|entry| entry.followers.iter())
        .filter(|cursor| cursor.rebuilding)
        .count()
}

#[cfg(test)]
mod tests;

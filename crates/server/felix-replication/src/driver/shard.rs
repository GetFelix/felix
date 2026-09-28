//! One shard's pass: ship to each follower, advance the quorum mark, and
//! build the report.

use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_router::{Route, ShardKey};
use felix_wire::internal::ShardRef;
use futures::StreamExt;

use super::WriteFence;
use crate::halted::HaltedReplica;
use crate::peer::PeerRequester;
use crate::quorum::QuorumMarks;
use crate::reporter::Reporter;
use crate::reporter::{ShardReport, shard_report};
use crate::throttle::{MoveThrottle, paced_destination};
use crate::{
    FollowerCursor, Progress, Rebuilds, caught_up, lag_records, metrics, quorum_offset_without,
    ship_once_with,
};

/// How much of the log one exchange may carry.
///
/// Bounds the leader's memory per follower. A follower far behind costs one
/// batch, not the distance it is behind.
pub(super) const MAX_BATCH_BYTES: usize = 1024 * 1024;

/// How long one pass ships to a destination that is still copying.
///
/// A pass ends when its slowest follower does, and the next quorum mark waits
/// for the next pass. A copy left to run to the tail would hold every `Quorum`
/// publish on the shard for the length of the copy; cut into slices, it costs
/// them at most this much. The driver runs the next pass at once while a copy
/// is unfinished, so the copy itself is not slowed.
pub(super) const COPY_SLICE: Duration = Duration::from_millis(50);

/// Cursors for one shard, valid only at `generation`.
pub struct ShardCursors {
    pub(super) generation: u64,
    /// Where a follower with no cursor yet starts. See [`compare_from`].
    pub(super) base: u64,
    pub(super) followers: Vec<FollowerCursor>,
    /// A move's destination this broker saw added to the replica set, still
    /// copying: left out of the quorum. See [`staged_learner`].
    pub(super) learner: Option<String>,
}

impl ShardCursors {
    /// Empty, at `generation`. A leadership change discards every belief about
    /// where a follower stood under the old one rather than carrying it
    /// forward.
    pub(super) fn at(generation: u64) -> Self {
        Self {
            generation,
            base: 0,
            followers: Vec::new(),
            learner: None,
        }
    }
}

/// One shard's pass: what it shipped, and the cursors it owned while doing it.
pub(super) struct ShardPass {
    pub(super) key: ShardKey,
    pub(super) cursors: ShardCursors,
    pub(super) aux: AuxCursors,
    pub(super) report: Option<ShardReport>,
    pub(super) halted: Vec<HaltedReplica>,
    pub(super) lag: Option<u64>,
    /// A destination still copying was cut off at [`COPY_SLICE`] with more to
    /// send.
    pub(super) copying: bool,
    /// Fenced here and not yet drained: the move is waiting on this broker.
    pub(super) drain_pending: bool,
    /// No follower that could still catch up holds all of the log.
    pub(super) behind: bool,
}

impl ShardPass {
    /// Nothing shipped, nothing to report — the shard could not be opened or
    /// read. The cursors travel back untouched so the next pass resumes from
    /// where this one found them.
    fn quiet(key: ShardKey, cursors: ShardCursors, aux: AuxCursors) -> Self {
        Self {
            key,
            cursors,
            aux,
            report: None,
            halted: Vec::new(),
            lag: None,
            copying: false,
            drain_pending: false,
            behind: false,
        }
    }
}

/// The auxiliary logs that ride a shard's replica set.
pub(super) struct AuxCursors {
    pub(super) group: ShardCursors,
    pub(super) dead_letters: ShardCursors,
    pub(super) counters: ShardCursors,
}

/// Ship one shard, and everything that rides with it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn replicate_shard<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    fence: &dyn WriteFence,
    marks: &QuorumMarks,
    reporter: Option<&Reporter>,
    rebuilds: &Rebuilds,
    throttle: &MoveThrottle,
    key: ShardKey,
    route: felix_router::Route,
    mut entry: ShardCursors,
    mut aux: AuxCursors,
) -> ShardPass {
    let route = &route;
    let shard_key = key.clone();
    let key = &key;
    // Resolved per pass rather than cached: a cache shard's log is replaced
    // by compaction, and a handle held across one reads the retired copy.
    let log_kind = match key.kind {
        felix_router::ShardKind::Cache => felix_broker::LogKind::Cache,
        felix_router::ShardKind::Stream => felix_broker::LogKind::Stream,
    };
    let Some(log) = broker
        .shard_log(
            log_kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
        )
        .await
    else {
        tracing::warn!(
            kind = ?key.kind,
            name = %key.stream,
            shard = key.shard,
            "could not open a shard to replicate",
        );
        return ShardPass::quiet(shard_key, entry, aux);
    };
    let tail = match log.tail_offset().await {
        Ok(tail) => tail,
        Err(err) => {
            tracing::warn!(stream = %key.stream, error = %err, "could not read a shard's tail");
            return ShardPass::quiet(shard_key, entry, aux);
        }
    };

    // Read only when a cursor has to be created — a generation change, or a
    // replica added to the set. The usual pass creates none, and the history
    // is a few hundred entries to clone.
    let needs_base = route.replicas.iter().any(|replica| {
        !entry
            .followers
            .iter()
            .any(|cursor| cursor.node_id == replica.node_id)
    });
    if needs_base {
        entry.base = compare_from(&log.generations(), route.generation);
    }
    reconcile_followers(&mut entry, route);

    let shard = ShardRef {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        generation: route.generation,
    };
    // Followers are shipped to concurrently.
    //
    // Sequentially, one follower that is gone holds up every follower
    // behind it *and* the mark published below, so the cost of unreachable
    // replicas adds up instead of overlapping. A minority failure is the
    // case `Quorum` exists to tolerate, so it must not be the case that
    // stalls it.
    //
    // Nothing here is given a deadline. A pass that cancelled a follower
    // mid-exchange would be cancelling the slow ones as readily as the dead
    // ones, and a dial cut short caches no connection -- so the next pass
    // dials again and is cut again. What a peer that is gone costs is
    // bounded by the pool's handshake timeout and then by its reconnect
    // backoff.
    // **The mark advances at the majority, not at the last follower.**
    //
    // Waiting for every follower before publishing meant a `Quorum` publish
    // was held by the *slowest* replica, which is not what quorum means: with
    // three replicas a record is on a majority the moment one follower has it,
    // and the second is a durability margin, not a precondition. One dead
    // replica put its whole handshake timeout in front of every acknowledgement
    // on the shard, every pass, which is the failure `Quorum` is meant to
    // tolerate rather than be stalled by.
    //
    // Each follower's cursor goes into its own future and comes back from it,
    // so a position can be read while others are still in flight. A follower
    // that has not answered yet keeps the position it came in with, which is
    // behind where it may already be — so the mark it contributes to is a
    // floor, never a claim beyond what has been established.
    let learner = entry.learner.clone();
    // A follower is offered for promotion when it holds every record a client
    // may have been told is stored. Under `Quorum` that ends at the mark, which
    // moves only once a report naming who holds it has landed. Measured against
    // the tail instead, a publish landing between shipping and the report
    // leaves every follower one short, and a leader dying then can never be
    // replaced. `Leader` acknowledges before shipping, and a move hands over an
    // exact copy, so both keep the tail.
    let quorum_shard = acknowledges_at_quorum(broker, key).await;
    let acks_at_quorum = !route.draining && quorum_shard;
    let mark_key = watch_key(key);
    let mark_key = &mark_key;
    let acknowledged = |tail: u64, followers: &[FollowerCursor]| {
        if !acks_at_quorum {
            return tail;
        }
        // Never below a mark already published, even if a follower's
        // position went back since: that mark's records were promised.
        quorum_offset_without(tail, followers, learner.as_deref())
            .max(marks.offset(&watch_key(key), route.generation).unwrap_or(0))
    };
    // Counters on a `Quorum` cache are acknowledged at their own mark, under
    // the same rule as the shard's: shipped first, so the reports below can
    // leave out a follower missing an acknowledged counter update.
    let counters = if acks_at_quorum && key.kind == felix_router::ShardKind::Cache {
        counter_level(
            requester,
            broker,
            marks,
            key,
            route,
            &mut aux,
            learner.as_deref(),
            rebuilds,
        )
        .await
    } else {
        None
    };
    let mut positions: Vec<FollowerCursor> = entry.followers.clone();
    let (log_ref, shard_ref) = (&log, &shard);
    // Not once the leader is fenced: the shard is not serving until the
    // remainder is across, and the remainder is bounded by the fence's lag
    // bound anyway. A learner is never needed by the quorum; any other
    // destination only when the rest of the set can make a majority alone.
    let paced = (throttle.is_limited() && !route.draining)
        .then(|| {
            learner
                .clone()
                .or_else(|| paced_destination(route, &entry.followers).map(str::to_string))
        })
        .flatten();
    let mut in_flight: futures::stream::FuturesUnordered<_> = entry
        .followers
        .drain(..)
        .map(|mut cursor| {
            let sliced = learner.as_deref() == Some(cursor.node_id.as_str());
            let paced = paced.as_deref() == Some(cursor.node_id.as_str());
            async move {
                // Keep going while there is more to send, so a follower
                // catching up is not limited to one batch per tick. It ends on
                // the first answer that is not progress, which bounds the work
                // per pass -- or, for a destination still copying, at the end
                // of its slice, or once it has waited its slice on the limit.
                let started = tokio::time::Instant::now();
                let mut cut = false;
                loop {
                    if paced && !throttle.pace(started).await {
                        cut = true;
                        break;
                    }
                    let before = cursor.shipped_bytes;
                    // Read per batch: the mark moves while followers ship.
                    let commit = quorum_shard
                        .then(|| marks.offset(mark_key, route.generation))
                        .flatten();
                    let progress = ship_once_with(
                        requester,
                        log_ref,
                        shard_ref,
                        log_kind,
                        &mut cursor,
                        MAX_BATCH_BYTES,
                        rebuilds,
                        commit,
                    )
                    .await;
                    if paced {
                        throttle.charge(cursor.shipped_bytes - before);
                    }
                    if !matches!(progress, Progress::Stored { .. }) {
                        break;
                    }
                    if sliced && started.elapsed() >= COPY_SLICE {
                        cut = true;
                        break;
                    }
                }
                (cursor, cut)
            }
        })
        .collect();

    /// Put a finished cursor back where its stale copy was.
    fn settle(positions: &mut [FollowerCursor], cursor: FollowerCursor) {
        if let Some(slot) = positions
            .iter_mut()
            .find(|held| held.node_id == cursor.node_id)
        {
            *slot = cursor;
        }
    }

    // Drain until a majority is established, or until everyone is in.
    //
    // The tail is re-read after each answer rather than once before shipping,
    // for the reason it was already re-read once: a publish landing in between
    // leaves it describing a log that is already shorter than the one on disk,
    // and a follower level with the *old* tail would be counted toward the
    // quorum for a record it does not have.
    //
    // Only followers that count toward the quorum are waited for here. With
    // none, the leader's own copy is the majority.
    let counts = |node: &str| learner.as_deref() != Some(node);
    let mut waiting = positions.iter().filter(|c| counts(&c.node_id)).count();
    let mut copying = false;
    let mut majority = None;
    if waiting == 0 {
        let offset = quorum_offset_without(tail, &positions, learner.as_deref());
        if offset > 0 {
            majority = Some((
                shard_report(
                    key,
                    route.generation,
                    tail,
                    acknowledged(tail, &positions),
                    &positions,
                    false,
                ),
                offset,
            ));
        }
    }
    while waiting > 0
        && let Some((cursor, cut)) = in_flight.next().await
    {
        copying |= cut;
        if counts(&cursor.node_id) {
            waiting -= 1;
        }
        settle(&mut positions, cursor);
        let tail = log.tail_offset().await.unwrap_or(tail);
        let offset = quorum_offset_without(tail, &positions, learner.as_deref());
        if offset > 0 {
            majority = Some((
                shard_report(
                    key,
                    route.generation,
                    tail,
                    acknowledged(tail, &positions),
                    &positions,
                    false,
                ),
                offset,
            ));
            break;
        }
    }

    // The report goes out *beside* the followers still shipping, not in front
    // of them. Awaiting it inside the drain loop suspends every future still in
    // `in_flight`, so a slow control plane would stall replication to the rest
    // of the replica set — the same head-of-line block this change exists to
    // remove, just moved onto the reporting hop.
    let (rest, reported) = futures::future::join(
        async {
            let mut rest = Vec::new();
            while let Some(finished) = in_flight.next().await {
                rest.push(finished);
            }
            rest
        },
        async {
            let (mut report, offset) = majority?;
            if let Some(counters) = &counters {
                counters.limit(&mut report);
            }

            // **Reported before the mark is published, and awaited.**
            //
            // The mark is what releases a `Quorum` publish, and the report is
            // what promotion later reads. Releasing the publish first leaves a
            // window in which a leader has told a client its record is on a
            // majority and has told the control plane nothing about which
            // replica holds it — and a leader that dies in that window is
            // replaced by whichever replica scores highest, which may be the
            // one that does not have it. The acknowledged record is then gone,
            // which is the one thing `Quorum` is supposed to rule out.
            //
            // A report that did not land leaves the mark where it was, because
            // the argument above rests on the control plane knowing who holds
            // the record: releasing on a failed report reaches the same window
            // by another route. The publish waits, the next pass retries, and a
            // client is told a timeout rather than an acknowledgement this
            // broker cannot stand behind.
            if publish_mark(
                broker,
                reporter,
                marks,
                key,
                route.generation,
                &report,
                offset,
            )
            .await
                && let Some(counters) = &counters
            {
                counters.publish(marks, key, route.generation);
            }
            Some(report)
        },
    )
    .await;
    for (cursor, cut) in rest {
        copying |= cut;
        settle(&mut positions, cursor);
    }
    let mut report_out = reported;

    entry.followers = positions;

    // What the pass ended up seeing, when that is not what was already sent.
    //
    // Two cases reach here. A pass where no majority ever advanced sent
    // nothing, and the report is the promotion signal as much as the mark's
    // precondition — a shard whose followers are all stuck is the one the
    // control plane most needs a current view of. And a follower that answered
    // after the majority report went out has moved since; leaving it until the
    // next pass would keep a replica that is level looking behind, and so out
    // of promotion, for no reason.
    //
    // Equal reports send nothing, which is the healthy case: followers finish
    // together, so the majority report already described all of them.
    // A draining shard says so once its fence is closed with no write inside,
    // and only then. Nothing can land after that, so the tail read next is
    // final and the control plane hands the shard on against exactly it.
    // Checked before the read: the other way round, a write finishing in
    // between would be quiesced but not in the tail.
    let quiesced = route.draining && fence.quiesced(&watch_key(key));
    // The control plane cuts over on the drained report, so the logs that ride
    // the shard have to be on the destination by then too: a dead letter or a
    // counter add left behind is lost at the cut-over. Once quiesced they
    // cannot grow either, so they are shipped first and their level is final.
    let aux_behind = if quiesced {
        ship_aux_logs(requester, broker, key, route, &mut aux, rebuilds).await
    } else {
        Vec::new()
    };
    let tail = log.tail_offset().await.unwrap_or(tail);
    let drained =
        quiesced && drain_ready(key, route, &caught_up(tail, &entry.followers), &aux_behind);
    let mut settled = shard_report(
        key,
        route.generation,
        tail,
        acknowledged(tail, &entry.followers),
        &entry.followers,
        drained,
    );
    // A follower missing some of that state would lose it if it led, so it is
    // not offered as a candidate.
    settled
        .caught_up
        .retain(|node| !aux_behind.iter().any(|(_, behind)| behind == node));
    if let Some(counters) = &counters {
        counters.limit(&mut settled);
    }
    let behind = settled.caught_up.is_empty()
        && entry
            .followers
            .iter()
            .any(|follower| follower.halted.is_none());
    if report_out.as_ref() != Some(&settled) {
        // And the mark with it. Usually a no-op — the mark is monotonic and the
        // majority already moved it — but with five replicas a second follower
        // answering raises the offset a majority holds, and that is this pass's
        // to publish rather than the next one's.
        if publish_mark(
            broker,
            reporter,
            marks,
            key,
            route.generation,
            &settled,
            quorum_offset_without(tail, &entry.followers, learner.as_deref()),
        )
        .await
            && let Some(counters) = &counters
        {
            counters.publish(marks, key, route.generation);
        }
        report_out = Some(settled);
    }

    // The leader's own copy is bound by its mark as a follower's is: a later
    // leader asking it to drop records a majority acknowledged is refused.
    if quorum_shard
        && let Some(mark) = marks.offset(mark_key, route.generation)
        && let Err(err) = log.advance_commit_offset(mark).await
    {
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            error = %err,
            "could not write the commit offset through; it holds in memory",
        );
    }

    // Otherwise after the report and the quorum mark, and never gating
    // either: no publish waits on group state or counters, and those lagging
    // must not hold up the records they describe.
    if !quiesced {
        ship_aux_logs(requester, broker, key, route, &mut aux, rebuilds).await;
    }

    // Named, not counted. The metric cannot carry the shard without a label
    // per tenant; the listing can, and a halt is useless to act on without it.
    let halted: Vec<HaltedReplica> = entry
        .followers
        .iter()
        .filter_map(|follower| {
            let (reason, remedy) = crate::halted::describe(follower.halted?);
            Some(HaltedReplica {
                tenant_id: key.tenant_id.clone(),
                namespace: key.namespace.clone(),
                stream: key.stream.clone(),
                shard: key.shard,
                kind: match key.kind {
                    felix_router::ShardKind::Cache => "cache",
                    felix_router::ShardKind::Stream => "stream",
                },
                node_id: follower.node_id.clone(),
                generation: route.generation,
                next_offset: follower.next_offset,
                reason,
                remedy,
            })
        })
        .collect();
    let lag = lag_records(tail, &entry.followers);

    ShardPass {
        key: shard_key,
        cursors: entry,
        aux,
        report: report_out,
        halted,
        lag,
        copying,
        drain_pending: route.draining && !drained,
        behind,
    }
}

/// How far a majority holds a `Quorum` cache's counter log, and who holds it
/// that far.
pub(super) struct CounterLevel {
    /// The counter mark to publish once a report lands.
    acknowledged: u64,
    /// Followers holding the counter log up to `acknowledged`.
    level: Vec<String>,
}

impl CounterLevel {
    /// Leave out of a report's candidates any follower missing a counter
    /// update that may have been acknowledged: promoted, it would lose it.
    fn limit(&self, report: &mut ShardReport) {
        report
            .caught_up
            .retain(|node| self.level.iter().any(|level| level == node));
    }

    /// Release counter adds up to the level, now that a report naming only
    /// followers that hold them has landed.
    fn publish(&self, marks: &QuorumMarks, key: &ShardKey, generation: u64) {
        marks
            .counters()
            .publish(&watch_key(key), generation, self.acknowledged);
    }
}

/// Ship a cache shard's counter log and measure it, for [`CounterLevel`].
/// `None` when the shard has no counter log here.
#[allow(clippy::too_many_arguments)]
async fn counter_level<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    marks: &QuorumMarks,
    key: &ShardKey,
    route: &Route,
    aux: &mut AuxCursors,
    learner: Option<&str>,
    rebuilds: &Rebuilds,
) -> Option<CounterLevel> {
    let log = broker
        .shard_log(
            felix_broker::LogKind::Counters,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
        )
        .await?;
    ship_aux_log(
        requester,
        broker,
        key,
        route,
        felix_broker::LogKind::Counters,
        &mut aux.counters,
        rebuilds,
    )
    .await;
    let tail = log.tail_offset().await.ok()?;
    // Never below a mark already published: those updates were acknowledged.
    let acknowledged = quorum_offset_without(tail, &aux.counters.followers, learner).max(
        marks
            .counters()
            .offset(&watch_key(key), route.generation)
            .unwrap_or(0),
    );
    Some(CounterLevel {
        acknowledged,
        level: caught_up(acknowledged, &aux.counters.followers),
    })
}

/// Ship the logs that ride a shard's replica set, and say which followers
/// they left behind.
///
/// A stream shard has two: the positions its consumer groups have reached,
/// and the offsets those groups gave up on. A cache shard has its counters.
/// They ride the shard's replica set and generation rather than being placed
/// separately, because that state has to be wherever the shard's leader is,
/// and move when the shard moves.
pub(super) async fn ship_aux_logs<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    route: &Route,
    aux: &mut AuxCursors,
    rebuilds: &Rebuilds,
) -> Vec<(felix_broker::LogKind, String)> {
    let logs: Vec<(felix_broker::LogKind, &mut ShardCursors)> = match key.kind {
        felix_router::ShardKind::Stream => vec![
            (felix_broker::LogKind::GroupCursors, &mut aux.group),
            (
                felix_broker::LogKind::GroupDeadLetters,
                &mut aux.dead_letters,
            ),
        ],
        felix_router::ShardKind::Cache => {
            vec![(felix_broker::LogKind::Counters, &mut aux.counters)]
        }
    };
    let mut behind = Vec::new();
    for (log_kind, entry) in logs {
        for node in ship_aux_log(requester, broker, key, route, log_kind, entry, rebuilds).await {
            behind.push((log_kind, node));
        }
    }
    behind
}

/// Whether a quiesced draining shard can say it is drained: the follower the
/// control plane will cut over to holds the shard's log and every log that
/// rides it.
///
/// With a named successor only it gates the report; another follower lagging
/// on an auxiliary log is left out of `caught_up` instead of holding the move.
/// Without one — the move lost its destination and the control plane will
/// promote whoever is level — every follower level on the shard's log must be
/// level on the rest too, since any of them may be chosen.
fn drain_ready(
    key: &ShardKey,
    route: &Route,
    level: &[String],
    behind: &[(felix_broker::LogKind, String)],
) -> bool {
    let successor = route
        .successor
        .as_deref()
        .filter(|successor| route.replicas.iter().any(|r| r.node_id == *successor));
    match successor {
        Some(successor) => {
            level.iter().any(|node| node == successor)
                && !withheld(key, behind, |node| node == successor)
        }
        None => !withheld(key, behind, |node| level.iter().any(|l| l == node)),
    }
}

/// Log and count each follower `gates` picks out that is still behind on an
/// auxiliary log; true if there was one.
fn withheld(
    key: &ShardKey,
    behind: &[(felix_broker::LogKind, String)],
    gates: impl Fn(&str) -> bool,
) -> bool {
    let mut held = false;
    for (log_kind, node) in behind.iter().filter(|(_, node)| gates(node)) {
        held = true;
        let log = aux_log_label(*log_kind);
        metrics::record_drain_withheld(log);
        tracing::warn!(
            kind = ?key.kind,
            stream = %key.stream,
            shard = key.shard,
            follower = %node,
            log,
            "holding the drained report: the follower has the shard's log but \
             not all of its {log} yet, and a cut-over now would lose them",
        );
    }
    held
}

fn aux_log_label(log_kind: felix_broker::LogKind) -> &'static str {
    match log_kind {
        felix_broker::LogKind::GroupCursors => metrics::LOG_GROUP_CURSORS,
        felix_broker::LogKind::GroupDeadLetters => metrics::LOG_DEAD_LETTERS,
        felix_broker::LogKind::Counters => metrics::LOG_COUNTERS,
        felix_broker::LogKind::Stream | felix_broker::LogKind::Cache => "shard",
    }
}

/// Ship one auxiliary log to the shard's replicas, and name the followers
/// still behind its tail afterwards.
///
/// Separate from the shard's own shipping because it must not affect it: no
/// report is sent for it, no quorum mark is published, and a failure only
/// leaves the follower behind. These logs are small and written only when the
/// state actually changes, so this is usually a no-op pass.
#[allow(clippy::too_many_arguments)]
pub(super) async fn ship_aux_log<R: PeerRequester>(
    requester: &R,
    broker: &Arc<Broker>,
    key: &ShardKey,
    route: &felix_router::Route,
    log_kind: felix_broker::LogKind,
    entry: &mut ShardCursors,
    rebuilds: &Rebuilds,
) -> Vec<String> {
    let Some(log) = broker
        .shard_log(
            log_kind,
            &key.tenant_id,
            &key.namespace,
            &key.stream,
            key.shard,
        )
        .await
    else {
        // No such state on this broker, so there is nothing to ship.
        return Vec::new();
    };

    if entry.generation != route.generation {
        *entry = ShardCursors::at(route.generation);
    }
    // Base stays zero here. These logs are written only when group state
    // changes, so comparing from the start costs almost nothing, and the
    // leader does not open them at takeover to record a generation against.
    reconcile_followers(entry, route);

    let shard = ShardRef {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        generation: route.generation,
    };
    let shipping = entry.followers.iter_mut().map(|cursor| async {
        while let Progress::Stored { .. } = crate::ship_once(
            requester,
            &log,
            &shard,
            log_kind,
            cursor,
            MAX_BATCH_BYTES,
            rebuilds,
        )
        .await
        {}
    });
    futures::future::join_all(shipping).await;

    // Read after shipping. A tail that cannot be read says nothing about who
    // is level, so everyone counts as behind.
    let level = match log.tail_offset().await {
        Ok(tail) => caught_up(tail, &entry.followers),
        Err(err) => {
            tracing::warn!(stream = %key.stream, error = %err, "could not read an auxiliary log's tail");
            Vec::new()
        }
    };
    entry
        .followers
        .iter()
        .filter(|cursor| !level.contains(&cursor.node_id))
        .map(|cursor| cursor.node_id.clone())
        .collect()
}

/// Add cursors for new replicas and drop those no longer in the set.
///
/// A new follower starts at `entry.base`, never at the tail: the leader does
/// not know what it holds, and starting at the tail would declare it caught up
/// without comparing anything. One behind that point it either agrees — and
/// the first batch appends — or conflicts, which is the answer worth having.
/// A follower further behind still says so with a `LogGap`, and the leader
/// rewinds to it in that one exchange.
/// The destination to leave out of the quorum, if the replica set gained it
/// for a move.
///
/// Known from what this broker shipped to before: a successor that was not
/// among the followers of the previous generation was added for the move and
/// is copying. One that was already a replica keeps counting, since the
/// stream's own replica set is what the quorum promises. With no earlier pass
/// to compare against -- this broker just started leading -- the successor
/// counts too: a publish may wait for the copy, but never on fewer replicas
/// than the stream asked for.
pub(super) fn staged_learner(previous: Option<&ShardCursors>, route: &Route) -> Option<String> {
    let successor = route.successor.as_ref()?;
    if !route.replicas.iter().any(|r| &r.node_id == successor) {
        return None;
    }
    let previous = previous?;
    if previous.learner.as_ref() == Some(successor) {
        return Some(successor.clone());
    }
    let added = previous.generation < route.generation
        && !previous
            .followers
            .iter()
            .any(|follower| &follower.node_id == successor);
    added.then(|| successor.clone())
}

pub(super) fn reconcile_followers(entry: &mut ShardCursors, route: &Route) {
    entry
        .followers
        .retain(|cursor| route.replicas.iter().any(|r| r.node_id == cursor.node_id));
    for replica in &route.replicas {
        // A follower that restarts comes back at a new address while the
        // generation, and so its cursor, carries on. The route has the
        // current address; the cursor only the one it was created with.
        if let Some(cursor) = entry
            .followers
            .iter_mut()
            .find(|cursor| cursor.node_id == replica.node_id)
        {
            cursor.addr = replica.advertise_addr;
        } else {
            entry.followers.push(FollowerCursor::new(
                replica.node_id.clone(),
                replica.advertise_addr,
                entry.base,
            ));
        }
    }
}

/// Where to start comparing with a follower whose position is unknown.
///
/// Offset zero re-reads the whole log — a batch per pass, per shard, at the
/// sweep's interval — which after a failover is minutes to hours of shipping
/// records the follower already has. The generation history bounds it.
///
/// `generations` says where each leadership began *here*. Below the current
/// one, this broker's records were taken from earlier leaders as a follower,
/// and so were the follower's; two prefixes of the same log agree. At or above
/// it is where they can differ: records this leadership wrote, and records a
/// predecessor left on the follower alone.
///
/// One record earlier than that, so the first batch overlaps something the
/// follower already holds and the boundary itself gets compared rather than
/// assumed — the same check Raft makes at `prevLogIndex`.
///
/// Zero when the history has no entry for this generation: a shard written
/// before the history existed, or a log that could not record it. Slow, and
/// the behaviour that was there before.
pub(super) fn compare_from(generations: &[felix_storage::log::Epoch], generation: u64) -> u64 {
    generations
        .iter()
        .find(|epoch| epoch.generation == generation)
        .map(|epoch| epoch.start_offset.saturating_sub(1))
        .unwrap_or(0)
}

/// Whether the shard acknowledges a write only once a majority holds it. A
/// stream or cache this broker has not registered counts as no, which keeps
/// the stricter test.
async fn acknowledges_at_quorum(broker: &Broker, key: &ShardKey) -> bool {
    let consistency = match key.kind {
        felix_router::ShardKind::Cache => {
            broker
                .cache_consistency(&key.tenant_id, &key.namespace, &key.stream)
                .await
        }
        felix_router::ShardKind::Stream => {
            broker
                .stream_consistency(&key.tenant_id, &key.namespace, &key.stream)
                .await
        }
    };
    consistency == Some(felix_broker::ConsistencyLevel::Quorum)
}

/// The watch's key for a route, carrying the kind across rather than assuming
/// it. A cache shard filed under a stream key would take the mark belonging to
/// the stream of the same name.
pub(super) fn watch_key(key: &ShardKey) -> crate::ShardKey {
    crate::ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: match key.kind {
            felix_router::ShardKind::Cache => crate::ShardKind::Cache,
            felix_router::ShardKind::Stream => crate::ShardKind::Stream,
        },
    }
}

/// Tell the control plane who holds what, then move the mark if it listened.
///
/// Returns whether the mark moved. A caller with nothing waiting on the mark
/// can ignore it; a caller on the quorum path cannot.
pub(super) async fn publish_mark(
    broker: &Broker,
    reporter: Option<&Reporter>,
    marks: &QuorumMarks,
    key: &ShardKey,
    generation: u64,
    report: &ShardReport,
    offset: u64,
) -> bool {
    let reported = match reporter {
        Some(reporter) => reporter.send(report.clone()).await,
        // Nothing to report to, so nothing to be behind: a broker with no
        // cluster membership has no promotion to inform.
        None => true,
    };
    if reported {
        marks.publish(&watch_key(key), generation, offset);
        // A `Quorum` stream's readers stop at the mark (see
        // `quorum::read_bound`), so a fetch waiting for records is waiting
        // for this as much as for the append, and the batches the stream held
        // back from its subscribers go out now.
        if key.kind == felix_router::ShardKind::Stream
            && let Ok(handle) = broker
                .resolve_stream_handle(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await
        {
            handle.release_committed();
            handle.appended().notify_waiters();
        }
    } else {
        metrics::record_mark_withheld();
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            "holding the quorum mark: the replica report did not reach the \
             control plane, so an acknowledgement now could not be made good \
             on at failover",
        );
    }
    reported
}

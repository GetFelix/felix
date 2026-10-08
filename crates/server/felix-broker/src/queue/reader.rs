//! Reading a stream shard as a consumer group.
//!
//! Joins the three pieces: the shard's log holds the records, the durable
//! cursor in [`ConsumerGroups`] says where the group has finished, and
//! [`GroupTracker`] holds what is currently handed out.
//!
//! Everything here is about keeping those three consistent. The cursor is
//! written only when a contiguous run of acknowledgements closes, because that
//! is the only moment the group has genuinely finished a prefix — writing it
//! per acknowledgement would either lie about progress or need a second
//! structure on disk to say which of the acknowledged offsets were contiguous.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex as SyncMutex;
use tokio::sync::{Mutex, Notify, OnceCell, OwnedMutexGuard};

use super::cursors::ConsumerGroups;
use super::dead_letters::DeadLetters;
use super::tracker::{GroupConsumer, GroupTracker};
use crate::error::{BrokerError, Result};

/// Every group this broker is serving, and the state each one holds.
///
/// Trackers are built on first touch from the durable cursor and the redrives
/// still outstanding, and are in memory only: losing them redelivers whatever
/// was in flight, which is the same thing losing the leader does. That is also
/// what lets an idle one be dropped; see [`GroupReader::evict_idle`]. They belong to one term of leading the shard; see
/// [`GroupReader::reset_shard`].
#[derive(Debug)]
pub struct GroupReader {
    cursors: Arc<ConsumerGroups>,
    dead_letters: Arc<DeadLetters>,
    max_attempts: u32,
    /// A plain lock, never held across I/O: a group is hydrated from disk
    /// inside its own slot's once-cell, so one slow read does not stall every
    /// other group on the broker.
    trackers: SyncMutex<Trackers>,
    visibility: Duration,
    /// Records a group was owed and can never receive, because retention
    /// removed them first.
    ///
    /// Counted rather than logged: this crate takes no logging dependency, and
    /// silently skipping them would hide the one case where a queue drops work
    /// nobody asked it to drop. The service layer reports it.
    trimmed: AtomicU64,
    /// Most records one group may have handed out and unsettled at once.
    max_in_flight: AtomicUsize,
    /// Polls the in-flight cap held short of available work.
    capped: AtomicU64,
}

impl GroupReader {
    pub fn new(
        cursors: Arc<ConsumerGroups>,
        dead_letters: Arc<DeadLetters>,
        visibility: Duration,
        max_attempts: u32,
    ) -> Self {
        Self {
            cursors,
            dead_letters,
            max_attempts: max_attempts.max(1),
            trackers: SyncMutex::new(Trackers::default()),
            visibility,
            trimmed: AtomicU64::new(0),
            max_in_flight: AtomicUsize::new(DEFAULT_MAX_IN_FLIGHT),
            capped: AtomicU64::new(0),
        }
    }

    /// Bound how many records one group may have handed out and unsettled at
    /// once. A poll past it answers empty until acknowledgements or lapsed
    /// claims free room. Applies from each group's next poll.
    pub fn set_max_in_flight(&self, max_in_flight: usize) {
        self.max_in_flight
            .store(max_in_flight.max(1), Ordering::Relaxed);
    }

    /// Most records one group may have handed out and unsettled at once.
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::Relaxed)
    }

    /// Polls the in-flight cap held short of work that was available. Also
    /// counted as `felix_group_polls_capped_total`.
    pub fn capped_polls(&self) -> u64 {
        self.capped.load(Ordering::Relaxed)
    }

    /// Woken when a group's own state frees work or room: an
    /// acknowledgement, a hand-back, or a redrive. New records are signalled
    /// by the shard's append notifier instead.
    ///
    /// Enable the `notified()` future before polling, as with
    /// [`crate::StreamHandle::appended`], or a change landing in between is
    /// missed. Holding the returned notifier keeps the group from being
    /// evicted as idle, which is right: a poll is waiting on it.
    pub fn changed(&self, key: &GroupKey) -> Arc<Notify> {
        let mut trackers = self.trackers.lock();
        let slot = trackers.slot(key, Instant::now());
        Arc::clone(&slot.changed)
    }

    /// When the group's earliest standing claim lapses, making that record
    /// owed again. `None` when nothing is in flight or the group is not
    /// loaded.
    pub async fn next_lapse(&self, key: &GroupKey) -> Option<Instant> {
        let cell = {
            let trackers = self.trackers.lock();
            Arc::clone(&trackers.slots.get(key)?.cell)
        };
        let tracker = Arc::clone(cell.get()?);
        tracker.lock().await.next_lapse()
    }

    /// The dead-letter store, for replication: its logs ship beside the
    /// cursors', and both have to reach whichever replica may lead next.
    pub fn dead_letters(&self) -> &Arc<DeadLetters> {
        &self.dead_letters
    }

    /// How many times a record is handed out before the group gives up on it.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Offsets this group has given up on, in the order it gave up.
    ///
    /// The records themselves are still in the stream's log — this is a list of
    /// what to look at, not a copy of it, so nothing is duplicated and nothing
    /// is lost.
    pub async fn dead_lettered(&self, key: &GroupKey) -> Result<Vec<u64>> {
        self.dead_letters.list(key).await
    }

    /// How long a claim stands before the record is owed again.
    pub fn visibility(&self) -> Duration {
        self.visibility
    }

    /// Records skipped because retention removed them before the group got to
    /// them. Non-zero means a queue dropped work; it is a retention setting too
    /// short for how far a group is allowed to fall behind.
    pub fn trimmed_skipped(&self) -> u64 {
        self.trimmed.load(Ordering::Relaxed)
    }

    /// Take up to `max` records for `group`, claimed until the visibility
    /// timeout lapses.
    ///
    /// Reads are by offset one at a time rather than as a range: the offsets a
    /// group is owed are not contiguous once anything has been redelivered, so
    /// a range read would return records the group is not owed and skip ones it
    /// is.
    ///
    /// The one-byte budget asks for exactly one record. It relies on the read
    /// path's rule that a range holding data never answers empty — the first
    /// record is returned whatever the budget, so a record larger than the
    /// budget is delivered rather than skipped. Without that rule a large
    /// record would be owed for ever and the group would stall on it.
    pub async fn poll(
        &self,
        key: &GroupKey,
        log: &crate::durable::StreamLog,
        max: usize,
        now: Instant,
    ) -> Result<Vec<Claimed>> {
        self.poll_below(key, log, u64::MAX, max, now, None, None)
            .await
    }

    /// [`Self::poll`], handing out nothing at or past `committed`.
    ///
    /// For a shard whose records count as written only once a majority holds
    /// them: a record past that point can still be lost at failover, and a
    /// group that had consumed it would have moved on from an offset the next
    /// leader fills with something else. Records already handed out are
    /// redelivered as usual; they were committed when they went out.
    ///
    /// The claims stand for `visibility`, or the reader's own when `None`.
    #[allow(clippy::too_many_arguments)]
    pub async fn poll_below(
        &self,
        key: &GroupKey,
        log: &crate::durable::StreamLog,
        committed: u64,
        max: usize,
        now: Instant,
        consumer: Option<&GroupConsumer>,
        visibility: Option<Duration>,
    ) -> Result<Vec<Claimed>> {
        let log_tail = log.tail_offset().await?;
        // Past a poisoned log's durable offset may be a batch whose publish
        // failed.
        let committed = committed.min(log.poisoned_read_end().unwrap_or(u64::MAX));
        let tail = log_tail.min(committed);
        let (held, mut locked) = self.current(key).await?;
        let claim = {
            locked.inherit_below(log_tail);
            locked.set_max_in_flight(self.max_in_flight());
            let visibility = visibility.unwrap_or(self.visibility);
            locked.claim_as(tail, max, now, visibility, consumer)
        };
        drop(locked);
        let tracker = Arc::clone(&held.tracker);
        if claim.capped {
            self.capped.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("felix_group_polls_capped_total").increment(1);
        }

        // Recorded before being settled. A crash in between would otherwise
        // move the cursor past a record with nothing anywhere saying the group
        // ever tried it -- the record would be silently skipped rather than
        // dead-lettered.
        for (i, dead) in claim.dead_lettered.iter().enumerate() {
            let recorded = self.dead_letters.record(key, dead.offset).await;
            if recorded.is_ok() {
                // The write replaced any redrive record.
                tracker.lock().await.take_redriven(dead.offset);
            }
            let settled = match recorded {
                Ok(()) => self.settle(key, &held, dead.offset).await,
                Err(err) => Err(err),
            };
            if let Err(err) = settled {
                // Owed again, still at its bound, so the next poll retries the
                // write. Dropped here they would be in no set at all, and the
                // cursor would stall below them until the tracker was rebuilt.
                // One already settled in memory is left settled by `owe`.
                let mut tracker = tracker.lock().await;
                for dead in &claim.dead_lettered[i..] {
                    tracker.owe(dead.offset);
                }
                for &offset in &claim.offsets {
                    tracker.unclaim(offset);
                }
                return Err(err);
            }
        }

        let mut claimed = Vec::with_capacity(claim.offsets.len());
        let mut bytes = 0usize;
        for (i, &offset) in claim.offsets.iter().enumerate() {
            if bytes >= MAX_POLL_BYTES {
                // Enough for one answer. The rest go back unattempted rather
                // than riding in a frame the connection may refuse.
                let mut tracker = tracker.lock().await;
                for &offset in &claim.offsets[i..] {
                    tracker.unclaim(offset);
                }
                break;
            }
            // The raw log: the filtered read would answer a generation-start
            // offset with the record after it, delivering that record twice
            // and the first time under the wrong offset.
            match log.read_log_from(offset, 1).await {
                Ok(records) => match records.into_iter().next().filter(|r| r.offset == offset) {
                    // Not a client's record, so nothing to deliver. Settled like a
                    // trimmed one; left owed it would stall the group here.
                    Some(record) if record.mark.is_generation_start() => {
                        tracker.lock().await.skip(offset);
                        self.settle(key, &held, offset).await?;
                    }
                    Some(record) => {
                        let record = crate::commit::client_record(record);
                        let (attempts, skipped_before) = {
                            let tracker = tracker.lock().await;
                            (tracker.attempts(offset), tracker.skipped_before(offset))
                        };
                        bytes += record.payload.len();
                        claimed.push(Claimed {
                            offset,
                            payload: record.payload,
                            attempts,
                            skipped_before,
                            publisher: record.publisher,
                            timestamp_micros: record.timestamp_micros,
                        })
                    }
                    // The offset is below the tail and yet holds nothing. Give
                    // the claim back rather than dropping it silently: a record
                    // the group is owed and never receives would stall the
                    // cursor at that offset for ever.
                    None => tracker.lock().await.nack(offset),
                },
                Err(BrokerError::CursorTooOld { .. }) => {
                    // Retention removed it while the group was behind. Nobody
                    // can deliver it, so it is settled rather than left owed --
                    // leaving it owed would stall the group for ever on a record
                    // that no longer exists anywhere.
                    self.trimmed.fetch_add(1, Ordering::Relaxed);
                    tracker.lock().await.skip(offset);
                    self.settle(key, &held, offset).await?;
                }
                Err(err) => {
                    // The read failed for a reason that may not repeat. None of
                    // this batch reaches the consumer, so all of it is owed
                    // again now rather than after the visibility timeout.
                    let mut tracker = tracker.lock().await;
                    for claimed in &claimed {
                        tracker.unclaim(claimed.offset);
                    }
                    for &offset in &claim.offsets[i..] {
                        tracker.unclaim(offset);
                    }
                    return Err(err);
                }
            }
        }
        Ok(claimed)
    }

    /// Finish one record. Persists the cursor when a contiguous run closes.
    ///
    /// Refused for an offset this group has not handed out: accepting one at
    /// or past the tail would skip a record before it is written, and wild
    /// offsets would pile up in memory waiting for a run that never closes.
    pub async fn ack(&self, key: &GroupKey, offset: u64) -> Result<()> {
        let (held, locked) = self.current(key).await?;
        check_handed_out(&locked, offset)?;
        drop(locked);
        self.settle(key, &held, offset).await
    }

    /// Tell `key`'s tracker the log tail, if it has not seen one yet: claims a
    /// predecessor made before a move, failover or eviction are all below it,
    /// and settles for them are taken. [`Self::poll`] does this itself; call
    /// it before an [`Self::ack`] or [`Self::nack`] that may be the first
    /// operation on this broker.
    pub async fn inherit_below(&self, key: &GroupKey, tail: u64) -> Result<()> {
        let (_held, mut locked) = self.current(key).await?;
        locked.inherit_below(tail);
        Ok(())
    }

    /// Give one record back, to be handed out again at once.
    ///
    /// Refused, like [`GroupReader::ack`], for an offset never handed out:
    /// owing one that does not exist yet would have every later poll try to
    /// read it.
    pub async fn nack(&self, key: &GroupKey, offset: u64) -> Result<()> {
        let (_held, mut tracker) = self.current(key).await?;
        check_handed_out(&tracker, offset)?;
        tracker.nack(offset);
        drop(tracker);
        self.wake(key);
        Ok(())
    }

    /// Give one record back, to be owed again `delay` after `now`.
    ///
    /// Until then it holds a place in flight, as a claim would, so a group
    /// cannot park more records than its in-flight cap. Leader memory, like
    /// every claim: a failover redelivers it sooner.
    pub async fn nack_after(
        &self,
        key: &GroupKey,
        offset: u64,
        delay: Duration,
        now: Instant,
    ) -> Result<()> {
        let (_held, mut tracker) = self.current(key).await?;
        check_handed_out(&tracker, offset)?;
        tracker.nack_after(offset, now + delay);
        drop(tracker);
        // A waiting poll may have been sleeping until a later lapse.
        self.wake(key);
        Ok(())
    }

    /// Keep the claim `attempts` names on `offset` standing until `by` after
    /// `now`.
    ///
    /// Refused with [`BrokerError::GroupClaimLapsed`] once that claim no
    /// longer stands. Extending whatever claim is there instead would let a
    /// consumer that lost the record keep it from the one now holding it,
    /// should that one die.
    pub async fn extend(
        &self,
        key: &GroupKey,
        offset: u64,
        attempts: u32,
        by: Duration,
        now: Instant,
    ) -> Result<()> {
        let (_held, mut tracker) = self.current(key).await?;
        check_handed_out(&tracker, offset)?;
        if tracker.extend(offset, attempts, now, now + by) {
            return Ok(());
        }
        Err(BrokerError::GroupClaimLapsed { offset })
    }

    /// Give up on one record for a consumer: list it as a dead letter, then
    /// finish it. Returns false, writing nothing, when it is already finished.
    ///
    /// Recorded before it is settled, as when the group gives up on its own,
    /// so a crash in between leaves it listed and owed rather than finished
    /// with no trace. The group is held across the write so a poll cannot hand
    /// it out again before it settles.
    pub async fn dead_letter(&self, key: &GroupKey, offset: u64) -> Result<bool> {
        let (held, mut tracker) = self.current(key).await?;
        check_handed_out(&tracker, offset)?;
        if !tracker.in_play(offset) {
            return Ok(false);
        }
        self.dead_letters.record(key, offset).await?;
        // The write replaced any redrive record.
        tracker.take_redriven(offset);
        let advanced = tracker.ack(offset);
        drop(tracker);
        self.wake(key);
        if let Some(committed) = advanced {
            self.cursors
                .commit_unless(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &key.group,
                    committed,
                    &held.retired,
                )
                .await?;
        }
        Ok(true)
    }

    /// Where `group` has finished, as recorded on disk.
    pub async fn committed(&self, key: &GroupKey) -> Result<Option<u64>> {
        self.cursors
            .committed(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &key.group,
            )
            .await
    }

    /// Stop tracking one dead letter. Returns whether it was listed.
    pub async fn discard(&self, key: &GroupKey, offset: u64) -> Result<bool> {
        self.dead_letters.discard(key, offset).await
    }

    /// Put one dead letter back in the queue, its attempt count reset.
    ///
    /// Returns whether it was taken. The redrive is written to the dead-letter
    /// log before it is applied here, so a leader that dies before the record
    /// is finished leaves the next one to owe it again: it is never both
    /// unlisted and unowed. The group is held across the write so a poll
    /// cannot give up on the record again between the check and the apply.
    pub async fn redrive(&self, key: &GroupKey, offset: u64) -> Result<bool> {
        let (_held, mut tracker) = self.current(key).await?;
        if !tracker.can_redrive(offset) {
            return Ok(false);
        }
        if !self.dead_letters.redrive(key, offset).await? {
            return Ok(false);
        }
        tracker.redrive(offset);
        tracker.mark_redriven(offset);
        drop(tracker);
        self.wake(key);
        Ok(true)
    }

    /// Move `group`'s cursor to `offset`, backwards or forwards.
    ///
    /// With `if_new`, only a group with no cursor that nothing has been
    /// handed out from is moved; an existing one is left where it is and its
    /// position returned. That is how a group is created somewhere other than
    /// the start.
    ///
    /// Every claim standing at the seek is void. The tracker is replaced by
    /// one at `offset` that inherits nothing, and the old one can no longer
    /// commit, so a late ack from before the seek cannot move the new cursor
    /// or mark a record the group now owes as finished. Dead letters are kept.
    ///
    /// The caller picks `offset`; this does not check it against the log.
    pub async fn seek(&self, key: &GroupKey, offset: u64, if_new: bool) -> Result<Seek> {
        let _shard = self.lock_shard(key).await;
        let (slot, old) = self.claim_slot(key).await;
        let committed = self.committed(key).await?;
        if if_new {
            let current = match (&committed, &old) {
                (Some(committed), _) => Some(*committed),
                (None, Some(tracker)) if handed_out_any(tracker) => Some(tracker.committed()),
                (None, _) => None,
            };
            if let Some(offset) = current {
                return Ok(Seek {
                    offset,
                    moved: false,
                });
            }
        }
        self.cursors
            .set_locked(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &key.group,
                Some(offset),
            )
            .await?;
        self.replace(key, offset, &slot).await?;
        drop(old);
        self.wake(key);
        Ok(Seek {
            offset,
            moved: true,
        })
    }

    /// Delete `group` on this shard: its cursor, its dead letters, and
    /// whatever it has in flight. Returns whether there was anything to
    /// delete.
    ///
    /// A consumer that polls the group again starts it afresh, as if it had
    /// never existed. Claims standing at the delete are void, as for
    /// [`Self::seek`].
    pub async fn delete(&self, key: &GroupKey) -> Result<bool> {
        let _shard = self.lock_shard(key).await;
        let (slot, old) = self.claim_slot(key).await;
        let committed = self.committed(key).await?;
        self.cursors
            .set_locked(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &key.group,
                None,
            )
            .await?;
        let dead = self.dead_letters.forget_group(key).await?;
        // Where a group with no cursor starts; see `hydrate`.
        self.replace(key, 0, &slot).await?;
        let existed = committed.is_some() || dead > 0 || old.as_deref().is_some_and(handed_out_any);
        drop(old);
        self.wake(key);
        Ok(existed)
    }

    /// Where `group` stands on this shard. The cursor and dead letters are
    /// read from disk; the in-flight and owed counts are this broker's memory
    /// and start again from zero when the shard changes leader.
    pub async fn describe(&self, key: &GroupKey) -> Result<GroupSnapshot> {
        let committed = self.committed(key).await?;
        let dead_letters = self.dead_letters.list(key).await?.len();
        // Not built for the asking: a group nobody is consuming has nothing
        // in flight.
        let tracker = {
            let trackers = self.trackers.lock();
            trackers
                .slots
                .get(key)
                .and_then(|slot| slot.cell.get().cloned())
        };
        let (in_flight, owed) = match tracker {
            Some(tracker) => {
                let mut tracker = tracker.lock().await;
                tracker.expire(Instant::now());
                tracker.outstanding()
            }
            None => (0, 0),
        };
        Ok(GroupSnapshot {
            committed,
            in_flight: in_flight as u64,
            owed: owed as u64,
            dead_letters: dead_letters as u64,
        })
    }

    async fn lock_shard(&self, key: &GroupKey) -> OwnedMutexGuard<()> {
        self.cursors
            .lock_shard(&key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await
    }

    /// The group's slot, made if missing, and its tracker locked if it has
    /// one. Held across a seek so an operation already inside the tracker
    /// finishes first, and one waiting for it finds it retired.
    async fn claim_slot(
        &self,
        key: &GroupKey,
    ) -> (
        Arc<OnceCell<Arc<Mutex<GroupTracker>>>>,
        Option<OwnedMutexGuard<GroupTracker>>,
    ) {
        let cell = {
            let mut trackers = self.trackers.lock();
            Arc::clone(&trackers.slot(key, Instant::now()).cell)
        };
        let old = match cell.get() {
            Some(tracker) => Some(Arc::clone(tracker).lock_owned().await),
            None => None,
        };
        (cell, old)
    }

    /// Retire the tracker in `cell` and put one at `committed` in its place.
    /// The caller holds the shard's cursor lock and has written the cursor.
    async fn replace(
        &self,
        key: &GroupKey,
        committed: u64,
        cell: &Arc<OnceCell<Arc<Mutex<GroupTracker>>>>,
    ) -> Result<()> {
        let mut tracker = GroupTracker::moved_to(committed, self.max_attempts);
        for offset in self.dead_letters.redriven(key).await? {
            tracker.restore_redrive(offset);
        }
        let fresh = Arc::new(OnceCell::new_with(Some(Arc::new(Mutex::new(tracker)))));
        let mut trackers = self.trackers.lock();
        let slot = trackers.slot(key, Instant::now());
        // `claim_slot`'s clone keeps the slot from being evicted, so this is
        // the one it found.
        debug_assert!(Arc::ptr_eq(&slot.cell, cell));
        slot.retired.store(true, Ordering::Release);
        slot.retired = Arc::new(AtomicBool::new(false));
        // The same notifier, so a poll waiting on the group wakes and finds
        // the new tracker.
        slot.cell = fresh;
        Ok(())
    }

    /// Forget what every group has in flight on one shard, so the next
    /// operation rebuilds it from the durable cursor.
    ///
    /// For a broker about to lead the shard again after someone else may have:
    /// its trackers are from its last term, and whatever the other leader
    /// finished is in the cursor log copied back with the shard, not here. Kept,
    /// they would hand out records the group has already acknowledged.
    pub async fn reset_shard(&self, tenant_id: &str, namespace: &str, stream: &str, shard: u32) {
        self.trackers.lock().slots.retain(|key, _| {
            !(key.shard == shard
                && key.stream == stream
                && key.namespace == namespace
                && key.tenant_id == tenant_id)
        });
    }

    /// Trackers not touched for this long are dropped, and rebuilt from disk
    /// if the group comes back. Never shorter than twice the visibility
    /// timeout, so a claim that still stands is not handed out again early.
    fn idle_after(&self) -> Duration {
        TRACKER_IDLE.max(self.visibility * 2)
    }

    /// Drop every tracker nobody is using that has been idle since before
    /// `now - idle_after()`. Returns how many went.
    ///
    /// Losing a tracker costs what losing the leader does: anything in flight
    /// is owed again from the durable cursor, and attempt counts start over.
    /// That is fine for a group nobody has touched in minutes, and it is what
    /// keeps a broker from holding every group name any client ever used.
    pub(crate) fn evict_idle(&self, now: Instant) -> usize {
        let idle_after = self.idle_after();
        let mut trackers = self.trackers.lock();
        let before = trackers.slots.len();
        trackers.slots.retain(|_, slot| {
            // One count is the map's own. Anyone else holding the slot is mid
            // operation and would go on using a tracker that is no longer the
            // group's, handing out what a rebuilt one hands out too.
            // A claim still standing is kept too: an extended claim or a
            // delayed nack can outlast the idle time, and a rebuilt tracker
            // would hand that record out early.
            let unused = Arc::strong_count(&slot.cell) == 1
                && Arc::strong_count(&slot.changed) == 1
                && slot.cell.get().is_none_or(|tracker| {
                    Arc::strong_count(tracker) == 1
                        && tracker.try_lock().is_ok_and(|tracker| {
                            tracker.next_lapse().is_none_or(|lapse| lapse <= now)
                        })
                });
            !(unused && now.saturating_duration_since(slot.last_used) >= idle_after)
        });
        trackers.last_sweep = now;
        before - trackers.slots.len()
    }

    /// How many groups this broker holds a tracker for.
    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.trackers.lock().slots.len()
    }

    async fn settle(&self, key: &GroupKey, held: &Held, offset: u64) -> Result<()> {
        let tracker = &held.tracker;
        let (advanced, redriven) = {
            let mut tracker = tracker.lock().await;
            (tracker.ack(offset), tracker.take_redriven(offset))
        };
        // A settled claim is room under the in-flight cap.
        self.wake(key);
        if redriven && let Err(err) = self.dead_letters.finish_redrive(key, offset).await {
            // Still recorded as redriven on disk, so a rebuilt tracker would owe
            // it again; keep saying so here until the clear lands.
            tracker.lock().await.mark_redriven(offset);
            return Err(err);
        }
        // Only when the run closed. An acknowledgement above a gap has not
        // finished anything the group can resume from.
        if let Some(committed) = advanced {
            self.cursors
                .commit_unless(
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    &key.group,
                    committed,
                    &held.retired,
                )
                .await?;
        }
        Ok(())
    }

    /// The group's tracker, locked, and never one a seek or delete has
    /// replaced: an operation that reached the old one is retried on the new.
    async fn current(&self, key: &GroupKey) -> Result<(Held, OwnedMutexGuard<GroupTracker>)> {
        loop {
            let held = self.tracker_for(key).await?;
            let locked = Arc::clone(&held.tracker).lock_owned().await;
            if !held.retired.load(Ordering::Acquire) {
                return Ok((held, locked));
            }
        }
    }

    async fn tracker_for(&self, key: &GroupKey) -> Result<Held> {
        let now = Instant::now();
        let (cell, retired, sweep) = {
            let mut trackers = self.trackers.lock();
            let sweep = now.saturating_duration_since(trackers.last_sweep) >= SWEEP_EVERY;
            let slot = trackers.slot(key, now);
            (Arc::clone(&slot.cell), Arc::clone(&slot.retired), sweep)
        };
        if sweep {
            self.evict_idle(now);
        }
        // Hydrated once per slot. Two pollers racing share the cell, so only
        // one reads the cursor and both get the same tracker: two trackers for
        // one group would each hand out the same records. A failed read leaves
        // the cell empty for the next caller to retry.
        let tracker = cell.get_or_try_init(|| self.hydrate(key)).await?;
        Ok(Held {
            tracker: Arc::clone(tracker),
            retired,
        })
    }

    /// Wake polls waiting on `key`. Skips creating a slot for a group nobody
    /// has touched: with no slot there is no waiter.
    fn wake(&self, key: &GroupKey) {
        if let Some(slot) = self.trackers.lock().slots.get(key) {
            slot.changed.notify_waiters();
        }
    }

    async fn hydrate(&self, key: &GroupKey) -> Result<Arc<Mutex<GroupTracker>>> {
        let committed = self
            .cursors
            .committed(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                &key.group,
            )
            .await?
            // A group that has never committed starts at the beginning of what
            // the log still holds. Starting at the tail would silently skip
            // everything published before the group first connected.
            .unwrap_or(0);
        let mut tracker = GroupTracker::new(committed, self.max_attempts);
        for offset in self.dead_letters.redriven(key).await? {
            tracker.restore_redrive(offset);
        }
        Ok(Arc::new(Mutex::new(tracker)))
    }
}

/// Payload bytes after which a poll stops reading and gives the rest of its
/// claim back. Well under the wire's frame limit; a single larger record is
/// still delivered, alone.
const MAX_POLL_BYTES: usize = 4 * 1024 * 1024;

/// Records one group may have in flight unless configured otherwise. Ten
/// times the most one poll takes, so a handful of consumers each holding a
/// full poll never meet it; a consumer that keeps polling without answering
/// does.
pub(crate) const DEFAULT_MAX_IN_FLIGHT: usize = 10_000;

/// How long a group goes untouched before its tracker may be dropped.
const TRACKER_IDLE: Duration = Duration::from_secs(10 * 60);

/// How often an operation also sweeps for idle trackers.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// Every tracker this broker holds, and when it last swept for idle ones.
#[derive(Debug)]
struct Trackers {
    slots: HashMap<GroupKey, Slot>,
    last_sweep: Instant,
}

impl Trackers {
    /// The slot for `key`, made if missing, marked used at `now`.
    fn slot(&mut self, key: &GroupKey, now: Instant) -> &mut Slot {
        let slot = self.slots.entry(key.clone()).or_insert_with(|| Slot {
            cell: Arc::new(OnceCell::new()),
            retired: Arc::new(AtomicBool::new(false)),
            changed: Arc::new(Notify::new()),
            last_used: now,
        });
        slot.last_used = now;
        slot
    }
}

impl Default for Trackers {
    fn default() -> Self {
        Self {
            slots: HashMap::new(),
            last_sweep: Instant::now(),
        }
    }
}

/// One group's tracker, built on first use.
#[derive(Debug)]
struct Slot {
    cell: Arc<OnceCell<Arc<Mutex<GroupTracker>>>>,
    /// Set when a seek or delete replaces this slot's tracker. Whatever still
    /// holds the old one must not commit from it.
    retired: Arc<AtomicBool>,
    /// See [`GroupReader::changed`].
    changed: Arc<Notify>,
    last_used: Instant,
}

/// A group's tracker, and the flag that says it has been replaced.
#[derive(Debug)]
struct Held {
    tracker: Arc<Mutex<GroupTracker>>,
    retired: Arc<AtomicBool>,
}

/// Whether a group with no cursor has been consumed here. A tracker that
/// never handed anything out, such as the one a delete leaves, is no group.
fn handed_out_any(tracker: &GroupTracker) -> bool {
    tracker.high_water() > 0
}

fn check_handed_out(tracker: &GroupTracker, offset: u64) -> Result<()> {
    if tracker.handed_out(offset) {
        return Ok(());
    }
    Err(BrokerError::GroupOffsetNotHandedOut {
        offset,
        next: tracker.high_water(),
    })
}

/// A group reading one shard of one stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    pub group: String,
}

/// What [`GroupReader::seek`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seek {
    /// Where the group now resumes on the shard.
    pub offset: u64,
    /// False when `if_new` found the group already there and left it alone.
    pub moved: bool,
}

/// Where one group stands on one shard. See [`GroupReader::describe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupSnapshot {
    /// Everything below this is finished. `None` for a group with no cursor.
    pub committed: Option<u64>,
    /// Records handed out and not yet settled.
    pub in_flight: u64,
    /// Records owed again after a nack or a lapsed claim.
    pub owed: u64,
    /// Records the group gave up on.
    pub dead_letters: u64,
}

/// One record handed to a consumer, with the offset it must acknowledge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimed {
    pub offset: u64,
    pub payload: bytes::Bytes,
    /// How many times this record has been handed out, this delivery included.
    /// `1` is the first attempt; anything higher is a redelivery.
    pub attempts: u32,
    /// How many offsets directly below this one were settled without being
    /// delivered: generation-start records, and records retention removed
    /// before the group reached them. A gap with this count is not a record
    /// still to come.
    pub skipped_before: u64,
    /// The principal that published the record, when the log stored one.
    pub publisher: Option<bytes::Bytes>,
    /// When the record was appended, in microseconds since the Unix epoch.
    pub timestamp_micros: u64,
}

#[cfg(test)]
mod tests;

//! Subscribing: live delivery, resuming from a position, and paging history
//! off disk.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use felix_wire::StartPosition;

use super::Broker;
use crate::error::{BrokerError, NotReadable, Result};
use crate::stream::{ReadBound, Subscription, SubscriptionGuard};

/// How often a history read waiting for the committed mark looks again
/// without being woken. The mark wakes it when it moves; this catches a bound
/// that changed another way, such as a lapsed lease.
const COMMIT_RECHECK: std::time::Duration = std::time::Duration::from_millis(250);

impl Broker {
    /// Subscribe to records published to one shard from now on.
    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<Subscription> {
        self.subscribe_sized(tenant_id, namespace, stream, shard, None)
            .await
    }

    /// [`Broker::subscribe`] with this subscriber's queue holding
    /// `queue_capacity` envelopes instead of the stream's default. The caller
    /// bounds it; the stream's overflow policy still applies.
    pub async fn subscribe_sized(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        queue_capacity: Option<usize>,
    ) -> Result<Subscription> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let stream_state = handle.state;
        let (subscriber_id, receiver) = stream_state.register_subscriber_sized(queue_capacity);
        Ok(Subscription {
            receiver,
            guard: SubscriptionGuard {
                stream_state: Arc::downgrade(&stream_state),
                subscriber_id,
            },
            pending: VecDeque::new(),
            skip_below: None,
        })
    }

    /// A cursor positioned at the tail of the stream shard.
    ///
    /// For a durable stream the log is authoritative, not the replay ring. A
    /// publish can consume offsets without reaching the ring — a cancelled
    /// request does exactly that — so the ring's counter can sit behind the
    /// durable tail. Handing out a cursor from the ring would then name a
    /// position that is already in the past on disk, and replaying from it
    /// fails as too old rather than resuming where the caller actually was.
    /// The tail is read after appends already started have landed, so a
    /// cancelled publish still being written is not left above the cursor.
    ///
    /// On a `Quorum` stream the tail is the committed mark: a position past it
    /// may name records a failover replaces.
    pub async fn cursor_tail(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<Cursor> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;

        let next_seq = match &handle.state.durable {
            Some(log) => committed_tail(
                handle.state.read_bound(),
                log.settled_tail_offset().await?,
                stream,
                shard,
            )?,
            None => handle.state.tail_seq(),
        };
        Ok(Cursor { next_seq })
    }

    /// Subscribe from an earlier position, replaying from the in-memory ring.
    /// A position older than the ring still holds is an error.
    pub async fn subscribe_with_cursor(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        cursor: Cursor,
    ) -> Result<(Vec<Bytes>, Subscription)> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let stream_state = handle.state;

        // Backlog and registration are captured together. Taking the snapshot
        // first and registering after left a window in which a publish landed
        // in neither: appended after the snapshot, fanned out to a subscriber
        // list that did not yet include this one.
        let (backlog, subscriber_id, receiver) = stream_state
            .register_with_backlog(cursor.next_seq)
            .map_err(|oldest| BrokerError::CursorTooOld {
                oldest,
                requested: cursor.next_seq,
            })?;
        Ok((
            backlog,
            Subscription {
                receiver,
                guard: SubscriptionGuard {
                    stream_state: Arc::downgrade(&stream_state),
                    subscriber_id,
                },
                pending: VecDeque::new(),
                // A publish that claimed this offset before the ring saw it
                // will arrive live; the caller asked to resume at `next_seq`.
                skip_below: Some(cursor.next_seq),
            },
        ))
    }

    /// Subscribe from a chosen position, joining stored history to live
    /// delivery without a gap or a duplicate.
    ///
    /// The ordering is what makes this correct, and it is not the obvious one.
    /// The live subscription is registered **first**, clamped to the oldest
    /// entry the replay ring still holds, and only then is the older history
    /// read from disk. Registering first pins the live edge: every record from
    /// `backlog_start` onward is already captured, either in the returned
    /// backlog or on the subscription's receiver. The disk range left to serve,
    /// `[requested, backlog_start)`, is therefore closed -- it cannot grow, and
    /// nothing can be evicted out of it into a gap while it is being read.
    ///
    /// Reading history first and subscribing after is the version that looks
    /// natural and loses records: publishes landing between the read and the
    /// registration reach neither.
    ///
    /// The caller delivers in three phases: `history` (paged from disk),
    /// then `backlog`, then whatever arrives on the subscription.
    ///
    /// On a `Quorum` stream `Latest` is the committed mark, not the tail, and
    /// the history must be read with [`Broker::read_committed`], which waits
    /// for the mark. An offset up to the tail is still accepted: a reader that
    /// saw it from an earlier leader resumes here once the mark reaches it.
    pub async fn subscribe_from(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        start: StartPosition,
    ) -> Result<ResumedSubscription> {
        self.subscribe_from_sized(tenant_id, namespace, stream, shard, start, None)
            .await
    }

    /// [`Broker::subscribe_from`] with a queue capacity, as
    /// [`Broker::subscribe_sized`] takes it.
    pub async fn subscribe_from_sized(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        start: StartPosition,
        queue_capacity: Option<usize>,
    ) -> Result<ResumedSubscription> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let stream_state = handle.state;
        let durable = stream_state.durable.clone();

        let tail = match &durable {
            Some(log) => log.tail_offset().await?,
            None => stream_state.tail_seq(),
        };
        // Only a log has offsets to bound; an in-memory stream's ring is its
        // whole story.
        let bound = match &durable {
            Some(_) => stream_state.read_bound(),
            None => ReadBound::Unbounded,
        };
        if bound == ReadBound::Refused {
            return Err(not_readable(stream, shard, NotReadable::Refused));
        }
        // Resolve the requested position to an offset before touching the ring.
        // `Latest` is the tail read above rather than a second read, so it
        // joins exactly where the reported live edge is.
        let requested = match start {
            StartPosition::Latest => committed_tail(bound, tail, stream, shard)?,
            StartPosition::Earliest => match &durable {
                // The oldest offset still on disk, which retention raises as it
                // trims. Never 0 for a trimmed stream.
                Some(log) => log.base_offset(),
                None => stream_state.oldest_seq(),
            },
            StartPosition::Offset(offset) => offset,
        };

        // Resuming past the tail would register for live delivery and then hand
        // over records *below* the requested offset -- the opposite of what was
        // asked for. Rejected rather than silently reinterpreted; a client that
        // wants to wait for an offset that does not exist yet should ask for
        // `Latest` and track its own position.
        if requested > tail {
            return Err(BrokerError::CursorInFuture { requested, tail });
        }

        // Where a reader of the history has everything that was in the stream.
        // Reported past a trailing generation-start record, a reader that
        // caught up would wait at it for an event that never comes.
        let live = match &durable {
            Some(log) => log.event_end(requested, tail).await?,
            None => tail,
        };

        let (backlog, backlog_start, subscriber_id, receiver) =
            stream_state.register_clamped(requested, queue_capacity);
        // Built the moment the subscriber exists, so every error path below
        // releases the registration by `Drop` instead of stranding a closed
        // sender in the registry for the publish path to reap later. Repeated
        // rejected subscribes would otherwise grow the registry without ever
        // touching the per-connection subscription cap.
        let guard = SubscriptionGuard {
            stream_state: Arc::downgrade(&stream_state),
            subscriber_id,
        };

        // Anything older than the ring has to come from disk, and only a
        // durable stream has any. For an in-memory stream this is the same
        // "your cursor is too old" condition `subscribe_with_cursor` reports.
        let history = if requested < backlog_start {
            if durable.is_none() {
                return Err(BrokerError::CursorTooOld {
                    oldest: backlog_start,
                    requested,
                });
            }
            Some(HistoryRange {
                from_offset: requested,
                until_offset: backlog_start,
            })
        } else {
            None
        };

        // A durable stream can also have been trimmed past the request, which
        // the ring cannot tell us about -- it only knows its own oldest entry.
        if let (Some(range), Some(log)) = (&history, &durable)
            && range.from_offset < log.base_offset()
        {
            return Err(BrokerError::CursorTooOld {
                oldest: log.base_offset(),
                requested,
            });
        }

        Ok(ResumedSubscription {
            history,
            backlog,
            backlog_start,
            // `tail` was read before registering, so anything published since
            // is at or past it and already captured: the join has no gap.
            join: durable.as_ref().map(|_| JoinOffsets {
                start_offset: requested,
                live_offset: live,
            }),
            subscription: Subscription {
                receiver,
                guard,
                pending: VecDeque::new(),
                skip_below: Some(requested),
            },
        })
    }

    /// Read persisted records for a durable stream, starting at `from_offset`.
    ///
    /// This is the historical replay path, and it is deliberately separate from
    /// [`Broker::subscribe_with_cursor`]. Cursor replay serves the recent tail
    /// out of memory and hands back a live subscription in the same call, so it
    /// cannot also stream an arbitrarily long history without either buffering
    /// it all or leaving a hole between the history and the live edge.
    ///
    /// This call pages instead: it returns at most `max_bytes` of payload (and
    /// no more than the storage layer's per-read record cap), and the caller
    /// advances by the last returned offset. An empty result means the reader
    /// has caught up with the tail.
    ///
    /// For a durable stream a cursor's sequence number is the same value as a
    /// record's offset, so a `Cursor` obtained from [`Broker::cursor_tail`] can
    /// be used here directly via [`Cursor::next_seq`].
    ///
    /// Returns [`BrokerError::StreamNotDurable`] for an in-memory stream, whose
    /// history exists only in the bounded replay ring.
    pub async fn read_durable(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<felix_storage::log::LogRecord>> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::StreamNotDurable {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        log.read_from(from_offset, max_bytes).await
    }

    /// [`Broker::read_durable`], stopping at the committed mark.
    ///
    /// What a subscription's history is read with. Below the mark it reads as
    /// `read_durable` does and cuts the page at the mark. At the mark it waits
    /// for it to move, since the history range a resume is given is closed and
    /// every record in it is on its way to being committed or to being refused
    /// with the shard. Empty only once `from_offset` is at the log's tail.
    /// [`BrokerError::NotReadable`] when this broker stops serving the shard
    /// meanwhile, so the reader resumes on whoever serves it now.
    pub async fn read_committed(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        from_offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<felix_storage::log::LogRecord>> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::StreamNotDurable {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        let mut from_offset = from_offset;
        loop {
            let appended = Arc::clone(&handle.state.appended);
            let moved = appended.notified();
            tokio::pin!(moved);
            // Registered before the bound is read, so a mark moving in between
            // still wakes this.
            moved.as_mut().enable();
            match handle.state.read_bound() {
                ReadBound::Unbounded => return log.read_from(from_offset, max_bytes).await,
                ReadBound::Committed(mark) if from_offset < mark => {
                    let mut records = log.read_from(from_offset, max_bytes).await?;
                    records.retain(|record| record.offset < mark);
                    if !records.is_empty() {
                        return Ok(records);
                    }
                    // Nothing below the mark but generation-start records,
                    // which `read_from` leaves out. Empty means the tail to a
                    // caller, so wait at the mark instead.
                    from_offset = mark;
                    continue;
                }
                ReadBound::Refused => {
                    return Err(not_readable(stream, shard, NotReadable::Refused));
                }
                ReadBound::Committed(_) | ReadBound::Settling => {}
            }
            // A poisoned log's durable offset is as far as it will ever get.
            let end = match log.poisoned_read_end() {
                Some(end) => end,
                None => log.tail_offset().await?,
            };
            if from_offset >= end {
                return Ok(Vec::new());
            }
            let _ = tokio::time::timeout(COMMIT_RECHECK, moved).await;
        }
    }

    /// The first offset on a durable stream shard whose record was appended
    /// at or after `at_micros`, searching only what a subscriber may read.
    /// `None` when no such record is readable yet, so `latest` is where to
    /// wait for one. See [`StreamLog::offset_for_time`] for how exact it is.
    ///
    /// [`StreamLog::offset_for_time`]: crate::StreamLog::offset_for_time
    pub async fn offset_for_time(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        at_micros: u64,
    ) -> Result<Option<u64>> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::StreamNotDurable {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        let until = committed_tail(
            handle.state.read_bound(),
            log.settled_tail_offset().await?,
            stream,
            shard,
        )?;
        Ok(log
            .offset_for_time(at_micros, until)
            .await?
            .map(|(offset, _)| offset))
    }

    /// Number of subscriber slots currently registered for a stream.
    ///
    /// Exposed for tests that need to prove a failed subscribe left nothing
    /// behind: a stranded registration is invisible from the outside until some
    /// later publish happens to reap it.
    pub async fn registered_subscribers(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<usize> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream, shard)
            .await?;
        Ok(handle.state.subscriber_count())
    }
}

/// The durable `tail` as a reader may see it.
pub(super) fn committed_tail(bound: ReadBound, tail: u64, stream: &str, shard: u32) -> Result<u64> {
    match bound {
        ReadBound::Unbounded => Ok(tail),
        ReadBound::Committed(mark) => Ok(mark.min(tail)),
        ReadBound::Settling => Err(not_readable(stream, shard, NotReadable::Settling)),
        ReadBound::Refused => Err(not_readable(stream, shard, NotReadable::Refused)),
    }
}

fn not_readable(stream: &str, shard: u32, reason: NotReadable) -> BrokerError {
    BrokerError::NotReadable {
        stream: stream.to_string(),
        shard,
        reason,
    }
}

/// A position in a stream shard: the offset of the next record to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    next_seq: u64,
}

impl Cursor {
    /// The offset of the next record.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

/// A subscription resumed from a position, with the history needed to reach it.
///
/// Deliver in order: `history` (paged from disk by the caller, so an arbitrarily
/// long backlog is never buffered here), then `backlog`, then the live
/// subscription. The three are contiguous by construction.
#[derive(Debug)]
pub struct ResumedSubscription {
    /// Older records to page off disk, or `None` when the ring covered the
    /// whole request.
    pub history: Option<HistoryRange>,
    /// Records already in the replay ring, each with its own offset.
    ///
    /// Offsets are carried rather than derived because the ring can contain
    /// holes -- a publish that took disk offsets and was cancelled before
    /// reaching the ring leaves one. A caller that numbered these sequentially
    /// from `backlog_start` would mislabel everything after a hole.
    pub backlog: Vec<RingRecord>,
    /// Offset of the first backlog entry, and of the live edge when the backlog
    /// is empty.
    pub backlog_start: u64,
    /// Where delivery starts and where it turns live, for a durable stream.
    /// `None` for an in-memory stream, whose events carry no offsets to
    /// compare these against.
    pub join: Option<JoinOffsets>,
    pub subscription: Subscription,
}

/// A record held in the replay ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingRecord {
    pub offset: u64,
    pub payload: Bytes,
    /// The principal that published it, when one was recorded.
    pub publisher: Option<Bytes>,
    /// When it was appended, in microseconds since the Unix epoch. `None` on
    /// an in-memory stream.
    pub timestamp_micros: Option<u64>,
}

/// The disk-backed range a resumed subscription must replay before its backlog.
///
/// Half-open: `[from_offset, until_offset)`. Closed at the moment it is
/// produced, because the live subscription is already registered at
/// `until_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryRange {
    pub from_offset: u64,
    pub until_offset: u64,
}

/// Where a resumed subscription joins its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinOffsets {
    /// The first offset delivered.
    pub start_offset: u64,
    /// The tail when the subscriber was registered, less any generation-start
    /// records the log ended with (they carry no event), never below
    /// `start_offset`. Records in `[start_offset, live_offset)` were already
    /// written at join; every event from `live_offset` on was written after.
    pub live_offset: u64,
}

#[cfg(test)]
mod tests;

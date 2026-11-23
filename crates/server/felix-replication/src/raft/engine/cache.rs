//! Cache and counter writes as the broker hands them to a Raft cache shard.
//! See `docs/per-shard-raft.md`, "Cache and counters".

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use super::RaftEngine;
use crate::ShardKey;
use crate::engine::CommitError;
use crate::raft::cache::{Answer, CacheCommand, Condition, Request};

/// The top bit marks the producer ids this crate takes for writes that came
/// with none, so a client's id can never collide with one.
const ANONYMOUS: u64 = 1 << 63;

/// Where a cache watch joins a Raft cache shard's member. See
/// [`RaftEngine::cache_watch_point`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheWatchPoint {
    /// Changes from this offset on reach the watch live.
    pub tail: u64,
    pub entries: Vec<CacheWatchEntry>,
}

/// One key's value below a [`CacheWatchPoint`]'s tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheWatchEntry {
    pub key: String,
    pub value: bytes::Bytes,
    /// The entry index of the put that wrote it.
    pub offset: u64,
    /// Zero is never.
    pub expires_at_millis: u64,
}

/// A cache or counter write to a Raft cache shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheOp {
    /// Set `key` to `value`, if `condition` holds. A put with a `ttl`
    /// expires that long after the leader's stamp on its entry.
    Put {
        key: String,
        value: bytes::Bytes,
        ttl: Option<Duration>,
        condition: CacheCondition,
    },
    /// Remove `key`, only if it holds `version` when one is named.
    Delete { key: String, version: Option<u64> },
    /// Add `delta` to the counter `key`.
    Add { key: String, delta: i64 },
}

/// What a put requires of its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheCondition {
    None,
    /// The key has no unexpired value.
    Absent,
    /// The key holds this version, unexpired.
    Version(u64),
}

/// What a cache write answered when its entry was applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheReply {
    /// A put wrote this version.
    Written { version: u64 },
    /// A delete removed this version.
    Removed { version: u64 },
    /// A delete found no unexpired value.
    Absent,
    /// The condition did not hold. The version is the key's, if it has an
    /// unexpired value.
    Refused { version: Option<u64> },
    /// A counter's sum after the add.
    Sum(i64),
    /// A client's request below its floor: it was applied by an earlier
    /// entry, and its answer is gone.
    Forgotten,
}

/// A client producer's request, which the shard applies at most once across
/// retries and leader changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerRequest(Request);

impl ProducerRequest {
    /// `floor` is the lowest sequence the producer has no answer for: the
    /// shard drops the answers below it. `None` for a producer id in the
    /// range kept for writes that carry none.
    pub fn new(producer: u64, sequence: u64, floor: u64) -> Option<Self> {
        (producer & ANONYMOUS == 0).then_some(Self(Request {
            producer,
            sequence,
            floor,
        }))
    }
}

/// The producer a broker writes as when a write names none.
///
/// The id is drawn once per engine, so a restarted broker or a new leader
/// never reuses an earlier one's sequences, whose floor may be past them.
/// Each id leaves its floor and at most its last answer in each shard it
/// wrote to. A write's floor is the lowest sequence of its shard still
/// waiting for an answer, so concurrent writes keep each other's answers,
/// the shard keeps no more than are in flight, and a slow shard holds up
/// no other's floor.
pub(super) struct Anonymous {
    id: u64,
    next: parking_lot::Mutex<u64>,
    waiting: parking_lot::Mutex<HashMap<ShardKey, BTreeSet<u64>>>,
}

/// A write's place among its shard's waiting sequences. Dropping it, on an
/// answer, an error or a cancelled call, lets the floor pass the write:
/// nothing sends it again, and a copy still in flight is an outcome its
/// caller was not told.
struct Waiting<'a> {
    anonymous: &'a Anonymous,
    shard: &'a ShardKey,
    sequence: u64,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut waiting = self.anonymous.waiting.lock();
        if let Some(shard) = waiting.get_mut(self.shard) {
            shard.remove(&self.sequence);
            if shard.is_empty() {
                waiting.remove(self.shard);
            }
        }
    }
}

impl Anonymous {
    pub(super) fn new() -> Self {
        Self {
            id: rand::random::<u64>() | ANONYMOUS,
            next: parking_lot::Mutex::new(1),
            waiting: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    fn take<'a>(&'a self, shard: &'a ShardKey) -> (Request, Waiting<'a>) {
        let sequence = {
            let mut next = self.next.lock();
            let taken = *next;
            *next += 1;
            taken
        };
        let mut waiting = self.waiting.lock();
        let held = waiting.entry(shard.clone()).or_default();
        held.insert(sequence);
        let floor = held.first().copied().unwrap_or(sequence);
        let request = Request {
            producer: self.id,
            sequence,
            floor,
        };
        let place = Waiting {
            anonymous: self,
            shard,
            sequence,
        };
        (request, place)
    }
}

#[cfg(test)]
impl RaftEngine {
    /// The producer id this engine writes as when a write names none.
    pub(crate) fn anonymous_producer(&self) -> u64 {
        self.anonymous.id
    }
}

impl RaftEngine {
    /// Commit `op` on `shard`'s leader here and return what it answered.
    ///
    /// With `producer`, a retry of the same request, on this leader or a
    /// later one, gets the first answer and is not applied again. Without
    /// one there is no such guarantee: the write is applied at most once
    /// per call, and a retry after an error may apply it again.
    ///
    /// An error is an unknown outcome: the write may still commit.
    pub async fn write_cache(
        &self,
        shard: &ShardKey,
        op: CacheOp,
        producer: Option<ProducerRequest>,
        timeout: Duration,
    ) -> Result<CacheReply, CommitError> {
        let (request, waiting) = match producer {
            Some(ProducerRequest(request)) => (request, None),
            None => {
                let (request, waiting) = self.anonymous.take(shard);
                (request, Some(waiting))
            }
        };
        let (command, ttl) = command(op, request);
        let answer = self.cache_write(shard, command, ttl, timeout).await;
        drop(waiting);
        match answer? {
            // Only a producer's floor drops an answer, and this one's floor
            // stayed at or below the write until now.
            Answer::Forgotten if producer.is_none() => Err(CommitError::LeadershipLost {
                what: "cache write",
                detail: "the shard holds no answer for a write it was sent once",
            }),
            answer => Ok(reply(answer)),
        }
    }
}

fn command(op: CacheOp, request: Request) -> (CacheCommand, u64) {
    match op {
        CacheOp::Put {
            key,
            value,
            ttl,
            condition,
        } => {
            // A TTL shorter than the stamp's microsecond still expires.
            let ttl = ttl.map_or(0, |ttl| {
                u64::try_from(ttl.as_micros()).unwrap_or(u64::MAX).max(1)
            });
            let cond = match condition {
                CacheCondition::None => Condition::None,
                CacheCondition::Absent => Condition::Absent,
                CacheCondition::Version(version) => Condition::Version(version),
            };
            let put = CacheCommand::Put {
                key,
                value,
                // Set from `ttl` and the leader's stamp when it is stamped.
                expires_at: 0,
                cond,
                request,
            };
            (put, ttl)
        }
        CacheOp::Delete { key, version } => (
            CacheCommand::Delete {
                key,
                version,
                request,
            },
            0,
        ),
        CacheOp::Add { key, delta } => (
            CacheCommand::Add {
                key,
                delta,
                request,
            },
            0,
        ),
    }
}

fn reply(answer: Answer) -> CacheReply {
    match answer {
        Answer::Written { version } => CacheReply::Written { version },
        Answer::Removed { version } => CacheReply::Removed { version },
        Answer::Absent => CacheReply::Absent,
        Answer::Refused { version } => CacheReply::Refused { version },
        Answer::Sum(sum) => CacheReply::Sum(sum),
        Answer::Forgotten => CacheReply::Forgotten,
    }
}

#[cfg(test)]
mod tests;

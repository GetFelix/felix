//! Cache and counter writes as the broker hands them to a Raft cache shard.
//! See `docs/per-shard-raft.md`, "Cache and counters".

use std::collections::BTreeSet;
use std::time::Duration;

use super::RaftEngine;
use crate::ShardKey;
use crate::engine::CommitError;
use crate::raft::cache::{Answer, CacheCommand, Condition, Request};

/// The top bit marks the producer ids this crate takes for writes that came
/// with none, so a client's id can never collide with one.
const ANONYMOUS: u64 = 1 << 63;

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
/// Each write's floor is the lowest sequence still waiting for its answer,
/// so concurrent writes keep each other's answers and the shard keeps no
/// more than are in flight.
pub(super) struct Anonymous {
    id: u64,
    next: parking_lot::Mutex<(u64, BTreeSet<u64>)>,
}

impl Anonymous {
    pub(super) fn new() -> Self {
        Self {
            id: rand::random::<u64>() | ANONYMOUS,
            next: parking_lot::Mutex::new((1, BTreeSet::new())),
        }
    }

    fn take(&self) -> Request {
        let mut next = self.next.lock();
        let (sequence, waiting) = &mut *next;
        let taken = *sequence;
        *sequence += 1;
        waiting.insert(taken);
        let floor = waiting.first().copied().unwrap_or(taken);
        Request {
            producer: self.id,
            sequence: taken,
            floor,
        }
    }

    fn answered(&self, sequence: u64) {
        self.next.lock().1.remove(&sequence);
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
    /// one there is no such guarantee: the write is applied once per call
    /// that reaches the log.
    ///
    /// An error is an unknown outcome: the write may still commit.
    pub async fn write_cache(
        &self,
        shard: &ShardKey,
        op: CacheOp,
        producer: Option<ProducerRequest>,
        timeout: Duration,
    ) -> Result<CacheReply, CommitError> {
        let request = match producer {
            Some(ProducerRequest(request)) => request,
            None => self.anonymous.take(),
        };
        let (command, ttl) = command(op, request);
        let answer = self.cache_write(shard, command, ttl, timeout).await;
        if producer.is_none() {
            // Given up on an error as well: nothing sends it again, and a
            // copy still in flight is an outcome the caller was not told.
            self.anonymous.answered(request.sequence);
        }
        match answer? {
            // Only a producer's floor drops an answer, and this one's floor
            // stayed at or below the write until now.
            Answer::Forgotten if producer.is_none() => Err(CommitError::LeadershipLost {
                what: "cache write",
                detail: "its answer was dropped",
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
            let ttl = ttl.map_or(0, |ttl| (ttl.as_micros() as u64).max(1));
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

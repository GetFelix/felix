//! Cache and counter operations on a shard a Raft group runs on this broker.
//!
//! The group's log holds the entries and the counters, and its leader
//! answers them, so none of the hybrid's fence, store or quorum wait applies.
//! See `docs/per-shard-raft.md`, "Cache and counters".

use std::time::Duration;

use bytes::Bytes;
use felix_replication::engine::CommitError;
use felix_wire::internal::ForwardCacheOutcome;

use crate::serving::forward::CacheRequest;
use crate::shards::ShardKey;
use crate::shards::routing::{RaftRefusal, StreamEngine};

/// What a Raft cache shard answered, shaped as the hybrid paths answer.
#[derive(Debug, Default)]
pub(crate) struct Answered {
    /// What a get found or a delete removed.
    pub(crate) value: Option<Bytes>,
    pub(crate) outcome: Option<ForwardCacheOutcome>,
    /// A counter's sum.
    pub(crate) sum: Option<i64>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NotServed {
    /// Nothing was proposed; the client may resolve the route and retry.
    #[error(transparent)]
    Refused(RaftRefusal),
    /// For a write, an unknown outcome: it may still commit.
    #[error(transparent)]
    Commit(#[from] CommitError),
    #[cfg(feature = "raft")]
    #[error("the Raft cache shard answered {0}")]
    Unexpected(String),
}

impl NotServed {
    /// As the client is answered, the way the stream Raft paths answer.
    pub(crate) fn into_anyhow(self) -> anyhow::Error {
        use crate::serving::quic::client_error::ClientError;
        match self {
            Self::Refused(refused) => refused.into_anyhow(),
            Self::Commit(err) => ClientError::from_anyhow(&err.into()).into(),
            #[cfg(feature = "raft")]
            Self::Unexpected(_) => ClientError::internal(self.to_string()).into(),
        }
    }
}

/// Apply `request` for `key` to `shard`'s Raft group, at the `term` routing
/// answered. `None` when the hybrid runs the shard.
pub(crate) async fn apply(
    engine: &StreamEngine,
    shard: &ShardKey,
    term: u64,
    key: &str,
    request: &CacheRequest,
    timeout: Duration,
) -> Option<Result<Answered, NotServed>> {
    #[cfg(feature = "raft")]
    {
        let raft = engine.raft(shard)?;
        // Only the term the operation was routed at, as for a publish. A
        // member that leads a later term answers once routing has seen it.
        if engine.raft_term(shard) != Some(term) {
            return Some(Err(NotServed::Refused(RaftRefusal::NotLeading { term })));
        }
        Some(on_group(raft, shard, key, request, timeout).await)
    }
    #[cfg(not(feature = "raft"))]
    {
        let _ = (engine, shard, term, key, request, timeout);
        None
    }
}

#[cfg(feature = "raft")]
async fn on_group(
    raft: &felix_replication::raft::RaftEngine,
    shard: &ShardKey,
    key: &str,
    request: &CacheRequest,
    timeout: Duration,
) -> Result<Answered, NotServed> {
    use felix_replication::raft::{CacheCondition, CacheOp, CacheReply};

    let put = |value: &Bytes, ttl_ms: u64, condition| CacheOp::Put {
        key: key.to_owned(),
        value: value.clone(),
        ttl: (ttl_ms > 0).then(|| Duration::from_millis(ttl_ms)),
        condition,
    };
    let outcome = |applied, version| Answered {
        outcome: Some(ForwardCacheOutcome { applied, version }),
        ..Answered::default()
    };
    let unexpected = |reply: CacheReply| NotServed::Unexpected(format!("{reply:?}"));
    Ok(match request {
        CacheRequest::Put { value, ttl_ms } => {
            let op = put(value, *ttl_ms, CacheCondition::None);
            match raft.write_cache(shard, op, None, timeout).await? {
                CacheReply::Written { .. } => Answered::default(),
                other => return Err(unexpected(other)),
            }
        }
        CacheRequest::PutIf {
            value,
            ttl_ms,
            condition,
        } => {
            let condition = match condition {
                felix_storage::CacheCondition::Absent => CacheCondition::Absent,
                felix_storage::CacheCondition::Version(version) => {
                    CacheCondition::Version(*version)
                }
            };
            let op = put(value, *ttl_ms, condition);
            match raft.write_cache(shard, op, None, timeout).await? {
                CacheReply::Written { version } => outcome(true, Some(version)),
                CacheReply::Refused { version } => outcome(false, version),
                other => return Err(unexpected(other)),
            }
        }
        CacheRequest::Delete => Answered {
            value: delete(raft, shard, key, timeout).await?,
            ..Answered::default()
        },
        CacheRequest::DeleteIf { version } => {
            let op = CacheOp::Delete {
                key: key.to_owned(),
                version: Some(*version),
            };
            match raft.write_cache(shard, op, None, timeout).await? {
                // As the hybrid answers: the key has no live entry after.
                CacheReply::Removed { .. } => outcome(true, None),
                CacheReply::Refused { version } => outcome(false, version),
                CacheReply::Absent => outcome(false, None),
                other => return Err(unexpected(other)),
            }
        }
        CacheRequest::Get => Answered {
            value: raft
                .cache_read(shard, key, timeout)
                .await?
                .map(|(value, _)| value),
            ..Answered::default()
        },
        CacheRequest::GetVersioned => {
            let found = raft.cache_read(shard, key, timeout).await?;
            let version = found.as_ref().map(|(_, version)| *version);
            Answered {
                value: found.map(|(value, _)| value),
                ..outcome(true, version)
            }
        }
        CacheRequest::CounterAdd { delta } => {
            let op = CacheOp::Add {
                key: key.to_owned(),
                delta: *delta,
            };
            match raft.write_cache(shard, op, None, timeout).await? {
                CacheReply::Sum(sum) => Answered {
                    sum: Some(sum),
                    ..Answered::default()
                },
                other => return Err(unexpected(other)),
            }
        }
        CacheRequest::CounterGet => Answered {
            sum: raft.counter_read(shard, key, timeout).await?,
            ..Answered::default()
        },
    })
}

/// How many times a plain delete reads and deletes on the version it read
/// before it deletes whatever is there.
#[cfg(feature = "raft")]
const DELETE_ROUNDS: usize = 3;

/// A plain delete, answered with the value it removed as the hybrid's is.
#[cfg(feature = "raft")]
async fn delete(
    raft: &felix_replication::raft::RaftEngine,
    shard: &ShardKey,
    key: &str,
    timeout: Duration,
) -> Result<Option<Bytes>, NotServed> {
    delete_within(raft, shard, key, DELETE_ROUNDS, timeout).await
}

/// The group's delete answers only a version, so the value is read first and
/// the delete made on that version: a write between the two sends it round
/// again. A key written faster than it can be read and deleted would starve
/// the delete, so after `rounds` it removes the key whatever it holds, and
/// answers the value it last read, which a later write may have replaced.
#[cfg(feature = "raft")]
pub(crate) async fn delete_within(
    raft: &felix_replication::raft::RaftEngine,
    shard: &ShardKey,
    key: &str,
    rounds: usize,
    timeout: Duration,
) -> Result<Option<Bytes>, NotServed> {
    use felix_replication::raft::{CacheOp, CacheReply};

    let deadline = tokio::time::Instant::now() + timeout;
    let mut round = 0;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        // No live entry: a delete would change nothing, so none is written.
        let Some((value, version)) = raft.cache_read(shard, key, left).await? else {
            return Ok(None);
        };
        let version = (round < rounds).then_some(version);
        round += 1;
        let op = CacheOp::Delete {
            key: key.to_owned(),
            version,
        };
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match raft.write_cache(shard, op, None, left).await? {
            CacheReply::Removed { .. } => return Ok(Some(value)),
            // Expired or deleted since the read: nothing is left to remove.
            CacheReply::Absent if version.is_none() => return Ok(None),
            CacheReply::Refused { .. } | CacheReply::Absent => {}
            other => return Err(NotServed::Unexpected(format!("{other:?}"))),
        }
    }
}

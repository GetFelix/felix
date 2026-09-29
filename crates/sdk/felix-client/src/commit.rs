//! Atomic commits: an event and the state updates that go with it, written
//! together on the shard that owns an entity.
//!
//! A commit is atomic because it is one record in one shard's log. Each
//! stream shard is its own log, so everything a commit touches has to be on
//! the entity's shard of one stream: its event, whether it is published or
//! enqueued for a consumer group, and its state. An operation that names a
//! different stream is refused with [`CommitError::NotOnOwningShard`] before
//! anything is sent; a commit is never split across logs. See
//! `docs/atomic-commit.md`.

use bytes::Bytes;

/// One part of a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOp {
    /// The commit's event, delivered to the stream's subscribers.
    Publish { stream: String, payload: Bytes },
    /// The commit's event, for the stream's consumer groups. A queue is a
    /// stream read through a group, so this is the same record as a publish;
    /// it is spelled separately so a caller can say what it means.
    Enqueue { queue: String, payload: Bytes },
    /// Set `key` in the stream's state.
    Put {
        stream: String,
        key: String,
        value: Bytes,
    },
    /// Remove `key` from the stream's state.
    Delete { stream: String, key: String },
}

impl CommitOp {
    pub fn publish(stream: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self::Publish {
            stream: stream.into(),
            payload: payload.into(),
        }
    }

    pub fn enqueue(queue: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self::Enqueue {
            queue: queue.into(),
            payload: payload.into(),
        }
    }

    pub fn put(stream: impl Into<String>, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self::Put {
            stream: stream.into(),
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn delete(stream: impl Into<String>, key: impl Into<String>) -> Self {
        Self::Delete {
            stream: stream.into(),
            key: key.into(),
        }
    }

    fn stream(&self) -> &str {
        match self {
            Self::Publish { stream, .. }
            | Self::Put { stream, .. }
            | Self::Delete { stream, .. } => stream,
            Self::Enqueue { queue, .. } => queue,
        }
    }
}

/// Why a commit was refused before it was sent.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    /// Operation `index` names `stream`, but the commit is on `owner`. A
    /// different stream is a different log, and one commit writes one log.
    #[error(
        "operation {index} is on stream {stream:?}, not {owner:?}: a commit writes one shard's log, and a different stream is a different log"
    )]
    NotOnOwningShard {
        index: usize,
        stream: String,
        owner: String,
    },
    /// A commit carries exactly one event: its offset is the commit's
    /// version, and a record has one offset.
    #[error("a commit carries exactly one event (publish or enqueue); this one has {0}")]
    EventCount(usize),
    /// The broker does not serve commits.
    #[error("this broker does not support atomic commits")]
    Unsupported,
}

/// A commit the broker has made durable (on a `Quorum` stream, on a
/// majority).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReceipt {
    /// The commit's offset in the shard's log: where its event is read, and
    /// the version of every key it wrote.
    pub offset: u64,
}

/// A key in a stream shard's state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateValue {
    /// `None` when the key was never written or was deleted.
    pub value: Option<Bytes>,
    /// Offset of the commit that wrote `value`.
    pub version: Option<u64>,
    /// Offset of the last commit the answer reflects. Its event, and every
    /// earlier commit's, can be read from the stream.
    pub as_of: Option<u64>,
}

/// A commit checked and reduced to what goes on the wire.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub(crate) stream: String,
    pub(crate) event: Bytes,
    pub(crate) changes: Vec<felix_wire::StateChange>,
}

/// Check that `ops` all belong to one stream and carry one event.
pub(crate) fn prepare(ops: Vec<CommitOp>) -> Result<Prepared, CommitError> {
    let events = ops
        .iter()
        .filter(|op| matches!(op, CommitOp::Publish { .. } | CommitOp::Enqueue { .. }))
        .count();
    if events != 1 {
        return Err(CommitError::EventCount(events));
    }
    let owner = ops
        .iter()
        .find(|op| matches!(op, CommitOp::Publish { .. } | CommitOp::Enqueue { .. }))
        .map(|op| op.stream().to_owned())
        .unwrap_or_default();
    if let Some((index, op)) = ops.iter().enumerate().find(|(_, op)| op.stream() != owner) {
        return Err(CommitError::NotOnOwningShard {
            index,
            stream: op.stream().to_owned(),
            owner,
        });
    }
    let mut event = Bytes::new();
    let mut changes = Vec::new();
    for op in ops {
        match op {
            CommitOp::Publish { payload, .. } | CommitOp::Enqueue { payload, .. } => {
                event = payload;
            }
            CommitOp::Put { key, value, .. } => {
                changes.push(felix_wire::StateChange::Put { key, value });
            }
            CommitOp::Delete { key, .. } => changes.push(felix_wire::StateChange::Delete { key }),
        }
    }
    Ok(Prepared {
        stream: owner,
        event,
        changes,
    })
}

#[cfg(test)]
mod tests;

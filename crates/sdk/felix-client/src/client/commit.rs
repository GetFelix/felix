//! Atomic commits, conditional publishes and state reads through a
//! [`Client`].

use std::sync::atomic::Ordering;

use anyhow::Result;
use bytes::Bytes;
use felix_wire::{Message, PublishRefusalReason};

use super::Client;
use crate::commit::{CommitError, CommitOp, CommitReceipt, StateValue, prepare};

impl Client {
    /// Commit `ops` on the shard `entity_key` routes to, as one record.
    ///
    /// Every reader sees all of it or none of it: the event at the commit's
    /// offset, and the state it writes at the same version. `ops` must carry
    /// exactly one event and name one stream; anything else is refused with a
    /// [`CommitError`] before anything is sent.
    ///
    /// Bound to this broker: a shard led elsewhere is refused with a
    /// [`crate::NotLeaderError`], which a [`crate::ClusterClient`] follows.
    pub async fn commit(
        &self,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<CommitOp>,
    ) -> Result<CommitReceipt> {
        match self
            .send_commit(tenant_id, namespace, entity_key, ops, None)
            .await?
        {
            Message::CommitOk { offset, .. } => Ok(CommitReceipt { offset }),
            other => Err(unexpected("commit", &other)),
        }
    }

    /// [`Self::commit`], only if the commit would land at exactly
    /// `expected_offset`, the shard's next offset: nothing has been appended
    /// to the shard since the caller saw its tail. A compare-and-set on the
    /// whole shard rather than on a key.
    ///
    /// A refusal is an answer, not an error: [`ConditionalWrite::Refused`]
    /// carries the shard's tail, and nothing was written.
    ///
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_PUBLISH_CONDITIONAL`]: an older broker ignores
    /// the expected offset and would commit unconditionally.
    pub async fn commit_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<CommitOp>,
        expected_offset: u64,
    ) -> Result<ConditionalWrite> {
        self.require_conditional_publish()?;
        let answer = self
            .send_commit(tenant_id, namespace, entity_key, ops, Some(expected_offset))
            .await?;
        match answer {
            Message::CommitOk { offset, .. } => Ok(ConditionalWrite::Written { offset }),
            other => conditional_answer("commit_if", other),
        }
    }

    async fn send_commit(
        &self,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<CommitOp>,
        expected_offset: Option<u64>,
    ) -> Result<Message> {
        let prepared = prepare(ops)?;
        self.require_commits()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::Commit {
            tenant_id: tenant_id.to_owned(),
            namespace: namespace.to_owned(),
            stream: prepared.stream,
            entity_key: Bytes::copy_from_slice(entity_key),
            event: prepared.event,
            changes: prepared.changes,
            request_id,
            expected_offset,
        };
        self.group_round_trip(message, request_id).await
    }

    /// Append `payloads` to the shard `key` routes to (shard 0 without one),
    /// only if the batch would start at exactly `expected_offset`, the
    /// shard's next offset.
    ///
    /// For a single writer that must not append after it has been replaced:
    /// of two writers expecting the same offset, exactly one is written. A
    /// refusal is an answer, not an error: [`ConditionalWrite::Refused`]
    /// carries the shard's tail, which counts every record, including one a
    /// leader change wrote. Written, the answer is the batch's first offset
    /// once it is durable (on a `Quorum` stream, on a majority).
    ///
    /// An answer lost in transit cannot be retried blindly: the retry would
    /// be refused by the write it is retrying. Read the shard at
    /// `expected_offset` to learn whether it landed.
    ///
    /// Bound to this broker: a shard led elsewhere is refused with a
    /// [`crate::NotLeaderError`], which a [`crate::ClusterClient`] follows.
    /// Fails without sending anything when the broker did not advertise
    /// [`felix_wire::FEATURE_PUBLISH_CONDITIONAL`].
    pub async fn publish_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Option<&[u8]>,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
    ) -> Result<ConditionalWrite> {
        self.require_conditional_publish()?;
        if payloads.is_empty() {
            return Err(anyhow::anyhow!(
                "a conditional publish needs at least one record"
            ));
        }
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::PublishIf {
            tenant_id: tenant_id.to_owned(),
            namespace: namespace.to_owned(),
            stream: stream.to_owned(),
            payloads,
            key: key.map(Bytes::copy_from_slice),
            expected_offset,
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::PublishOk {
                offset: Some(offset),
                ..
            } => Ok(ConditionalWrite::Written { offset }),
            other => conditional_answer("publish_if", other),
        }
    }

    /// `key` in the state of `stream`'s shard that `entity_key` routes to.
    pub async fn state_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        entity_key: &[u8],
        key: &str,
    ) -> Result<StateValue> {
        self.require_commits()?;
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::StateGet {
            tenant_id: tenant_id.to_owned(),
            namespace: namespace.to_owned(),
            stream: stream.to_owned(),
            entity_key: Bytes::copy_from_slice(entity_key),
            key: key.to_owned(),
            request_id,
        };
        match self.group_round_trip(message, request_id).await? {
            Message::StateValue {
                value,
                version,
                as_of,
                ..
            } => Ok(StateValue {
                value,
                version,
                as_of,
            }),
            other => Err(anyhow::anyhow!(
                "unexpected answer to state_get: {:?}",
                std::mem::discriminant(&other)
            )),
        }
    }

    fn require_conditional_publish(&self) -> Result<()> {
        if felix_wire::supports_feature(
            self.server_features,
            felix_wire::FEATURE_PUBLISH_CONDITIONAL,
        ) {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "this broker does not support conditional publishes"
            ))
        }
    }

    /// Refused without a round trip against a broker that did not advertise
    /// commits: an unknown request would cost the connection.
    fn require_commits(&self) -> Result<(), CommitError> {
        if felix_wire::supports_feature(self.server_features, felix_wire::FEATURE_ATOMIC_COMMIT) {
            Ok(())
        } else {
            Err(CommitError::Unsupported)
        }
    }
}

/// What a conditional publish or commit did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalWrite {
    /// Written at `offset`, the expected one.
    Written { offset: u64 },
    /// Not written, because the shard's next offset is `tail`. No offset was
    /// consumed.
    Refused { tail: u64 },
}

/// A refusal is an answer; any other refusal is the typed error it was.
fn conditional_answer(request: &str, answer: Message) -> Result<ConditionalWrite> {
    match answer {
        Message::PublishRefused {
            reason: PublishRefusalReason::OffsetMismatch { tail },
            ..
        } => Ok(ConditionalWrite::Refused { tail }),
        Message::PublishRefused {
            reason, message, ..
        } => Err(crate::PublishRefused { reason, message }.into()),
        Message::PublishError {
            message,
            code,
            retry,
            detail,
            ..
        } => Err(crate::error::refused(
            "conditional write refused",
            message,
            code,
            retry,
            detail,
        )),
        other => Err(unexpected(request, &other)),
    }
}

fn unexpected(request: &str, answer: &Message) -> anyhow::Error {
    anyhow::anyhow!(
        "unexpected answer to {request}: {:?}",
        std::mem::discriminant(answer)
    )
}

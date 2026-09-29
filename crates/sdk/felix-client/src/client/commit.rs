//! Atomic commits and state reads through a [`Client`].

use std::sync::atomic::Ordering;

use anyhow::Result;
use bytes::Bytes;
use felix_wire::Message;

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
        };
        match self.group_round_trip(message, request_id).await? {
            Message::CommitOk { offset, .. } => Ok(CommitReceipt { offset }),
            other => Err(anyhow::anyhow!(
                "unexpected answer to commit: {:?}",
                std::mem::discriminant(&other)
            )),
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

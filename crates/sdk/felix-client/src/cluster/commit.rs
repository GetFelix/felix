//! Atomic commits, conditional publishes and state reads, sent to whichever
//! broker leads the shard.

use anyhow::Result;

use super::ClusterClient;
use crate::ConditionalWrite;
use crate::commit::{CommitOp, CommitReceipt, StateValue, prepare};

impl ClusterClient {
    /// [`crate::Client::commit`], on whichever broker leads the shard
    /// `entity_key` routes to. A commit is never forwarded between brokers;
    /// this follows the leader's redirect instead.
    pub async fn commit(
        &self,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<CommitOp>,
    ) -> Result<CommitReceipt> {
        let stream = prepare(ops.clone())?.stream;
        let shard = self
            .shard_of(
                &(tenant_id.to_owned(), namespace.to_owned(), stream.clone()),
                Some(entity_key),
            )
            .await;
        self.on_group_shard(
            (tenant_id.to_owned(), namespace.to_owned(), stream, shard),
            |client| {
                let ops = ops.clone();
                async move { client.commit(tenant_id, namespace, entity_key, ops).await }
            },
        )
        .await
    }

    /// [`crate::Client::commit_if`], on whichever broker leads the shard
    /// `entity_key` routes to.
    pub async fn commit_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        entity_key: &[u8],
        ops: Vec<CommitOp>,
        expected_offset: u64,
    ) -> Result<ConditionalWrite> {
        let stream = prepare(ops.clone())?.stream;
        let shard = self
            .shard_of(
                &(tenant_id.to_owned(), namespace.to_owned(), stream.clone()),
                Some(entity_key),
            )
            .await;
        self.on_group_shard(
            (tenant_id.to_owned(), namespace.to_owned(), stream, shard),
            |client| {
                let ops = ops.clone();
                async move {
                    client
                        .commit_if(tenant_id, namespace, entity_key, ops, expected_offset)
                        .await
                }
            },
        )
        .await
    }

    /// [`crate::Client::publish_if`], on whichever broker leads the shard
    /// `key` routes to. Never forwarded between brokers; this follows the
    /// leader's redirect instead.
    pub async fn publish_if(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        key: Option<&[u8]>,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
    ) -> Result<ConditionalWrite> {
        let shard = self
            .shard_of(
                &(
                    tenant_id.to_owned(),
                    namespace.to_owned(),
                    stream.to_owned(),
                ),
                key,
            )
            .await;
        self.on_group_shard(
            (
                tenant_id.to_owned(),
                namespace.to_owned(),
                stream.to_owned(),
                shard,
            ),
            |client| {
                let payloads = payloads.clone();
                async move {
                    client
                        .publish_if(tenant_id, namespace, stream, key, payloads, expected_offset)
                        .await
                }
            },
        )
        .await
    }

    /// [`crate::Client::state_get`], on whichever broker leads the shard.
    pub async fn state_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        entity_key: &[u8],
        key: &str,
    ) -> Result<StateValue> {
        let shard = self
            .shard_of(
                &(
                    tenant_id.to_owned(),
                    namespace.to_owned(),
                    stream.to_owned(),
                ),
                Some(entity_key),
            )
            .await;
        self.on_group_shard(
            (
                tenant_id.to_owned(),
                namespace.to_owned(),
                stream.to_owned(),
                shard,
            ),
            |client| async move {
                client
                    .state_get(tenant_id, namespace, stream, entity_key, key)
                    .await
            },
        )
        .await
    }
}

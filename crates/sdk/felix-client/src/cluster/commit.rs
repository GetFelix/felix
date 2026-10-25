//! Atomic commits, conditional publishes and state reads, sent to whichever
//! broker leads the shard.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use felix_wire::ErrorCode;

use super::{ClusterClient, ShardKey};
use crate::BrokerError;
use crate::ConditionalWrite;
use crate::client::Client;
use crate::commit::{CommitOp, CommitReceipt, StateValue, prepare};

impl ClusterClient {
    /// [`Self::on_group_shard`], run again after a backoff when a broker
    /// answers a coded `not_leader`, as a plain publish is.
    ///
    /// Safe for commits, conditional publishes and state reads because the
    /// broker checks it leads the shard before it applies anything, so a
    /// `not_leader` means nothing was written. Anything else, including
    /// `leadership_lost` and `quorum_timeout`, which can follow an apply, is
    /// returned as it came. Bounded by the policy's attempts and deadline.
    async fn on_write_shard<T, F, Fut>(&self, key: ShardKey, op: F) -> Result<T>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let started = Instant::now();
        let attempts = self.policy.attempts.max(1);
        let mut attempt = 0;
        loop {
            let error = match self.on_group_shard(key.clone(), &op).await {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            attempt += 1;
            let refused_unapplied = error
                .downcast_ref::<BrokerError>()
                .is_some_and(|broker| broker.code == ErrorCode::NotLeader);
            if !refused_unapplied || attempt >= attempts {
                return Err(error);
            }
            // The failed call dropped the cached route, so the next one asks
            // the entry broker for the current owner.
            let delay = self.policy.delay_before(attempt - 1);
            if let Some(budget) = self.policy.deadline
                && started.elapsed() + delay >= budget
            {
                return Err(error);
            }
            tokio::time::sleep(delay).await;
        }
    }

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
        self.on_write_shard(
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
        self.on_write_shard(
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
        self.on_write_shard(
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
        self.on_write_shard(
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

    /// [`crate::Client::commit_on_shard`], on whichever broker leads `shard`.
    /// The named shard picks the leader, as the entity key's shard does for
    /// [`Self::commit`].
    pub async fn commit_on_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<CommitOp>,
    ) -> Result<CommitReceipt> {
        let stream = prepare(ops.clone())?.stream;
        self.on_write_shard(
            (tenant_id.to_owned(), namespace.to_owned(), stream, shard),
            |client| {
                let ops = ops.clone();
                async move {
                    client
                        .commit_on_shard(tenant_id, namespace, shard, entity_key, ops)
                        .await
                }
            },
        )
        .await
    }

    /// [`crate::Client::commit_if_on_shard`], on whichever broker leads
    /// `shard`.
    pub async fn commit_if_on_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        shard: u32,
        entity_key: &[u8],
        ops: Vec<CommitOp>,
        expected_offset: u64,
    ) -> Result<ConditionalWrite> {
        let stream = prepare(ops.clone())?.stream;
        self.on_write_shard(
            (tenant_id.to_owned(), namespace.to_owned(), stream, shard),
            |client| {
                let ops = ops.clone();
                async move {
                    client
                        .commit_if_on_shard(
                            tenant_id,
                            namespace,
                            shard,
                            entity_key,
                            ops,
                            expected_offset,
                        )
                        .await
                }
            },
        )
        .await
    }

    /// [`crate::Client::publish_if_on_shard`], on whichever broker leads
    /// `shard`.
    pub async fn publish_if_on_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        payloads: Vec<Vec<u8>>,
        expected_offset: u64,
    ) -> Result<ConditionalWrite> {
        self.on_write_shard(
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
                        .publish_if_on_shard(
                            tenant_id,
                            namespace,
                            stream,
                            shard,
                            payloads,
                            expected_offset,
                        )
                        .await
                }
            },
        )
        .await
    }

    /// [`crate::Client::state_get_on_shard`], on whichever broker leads
    /// `shard`.
    pub async fn state_get_on_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        key: &str,
    ) -> Result<StateValue> {
        self.on_write_shard(
            (
                tenant_id.to_owned(),
                namespace.to_owned(),
                stream.to_owned(),
                shard,
            ),
            |client| async move {
                client
                    .state_get_on_shard(tenant_id, namespace, stream, shard, key)
                    .await
            },
        )
        .await
    }
}

//! Consumer-group operations on one shard, sent to whichever broker leads it.
//!
//! A group's cursor and claims live with its shard's leader, and any other
//! broker answers with `NotLeader`. That includes the old leader once a move
//! has cut over, so a consumer that stays connected to one broker stops working
//! on the first rebalance unless something follows the redirect. This does,
//! and remembers where each shard was served so the next call goes straight
//! there.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use super::{ClusterClient, MAX_REDIRECTS, ShardKey};
use crate::client::Client;

impl ClusterClient {
    /// [`Client::group_poll`], on whichever broker leads the shard.
    pub async fn group_poll(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        self.group_poll_wait(
            tenant_id,
            namespace,
            stream,
            shard,
            group,
            max_records,
            Duration::ZERO,
        )
        .await
    }

    /// [`Client::group_poll_wait`], on whichever broker leads the shard.
    #[allow(clippy::too_many_arguments)]
    pub async fn group_poll_wait(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        max_records: u32,
        wait: Duration,
    ) -> Result<Vec<felix_wire::GroupRecord>> {
        self.on_group_shard(
            shard_key(tenant_id, namespace, stream, shard),
            |client| async move {
                client
                    .group_poll_wait(
                        tenant_id,
                        namespace,
                        stream,
                        shard,
                        group,
                        max_records,
                        wait,
                    )
                    .await
            },
        )
        .await
    }

    /// [`Client::group_ack`], on whichever broker leads the shard.
    pub async fn group_ack(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.on_group_shard(
            shard_key(tenant_id, namespace, stream, shard),
            |client| async move {
                client
                    .group_ack(tenant_id, namespace, stream, shard, group, offset)
                    .await
            },
        )
        .await
    }

    /// [`Client::group_nack`], on whichever broker leads the shard.
    pub async fn group_nack(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.on_group_shard(
            shard_key(tenant_id, namespace, stream, shard),
            |client| async move {
                client
                    .group_nack(tenant_id, namespace, stream, shard, group, offset)
                    .await
            },
        )
        .await
    }

    /// [`Client::group_dead_letters`], on whichever broker leads the shard.
    pub async fn group_dead_letters(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
    ) -> Result<Vec<u64>> {
        self.on_group_shard(
            shard_key(tenant_id, namespace, stream, shard),
            |client| async move {
                client
                    .group_dead_letters(tenant_id, namespace, stream, shard, group)
                    .await
            },
        )
        .await
    }

    /// [`Client::group_discard`], on whichever broker leads the shard.
    pub async fn group_discard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.on_group_shard(
            shard_key(tenant_id, namespace, stream, shard),
            |client| async move {
                client
                    .group_discard(tenant_id, namespace, stream, shard, group, offset)
                    .await
            },
        )
        .await
    }

    /// [`Client::group_redrive`], on whichever broker leads the shard.
    pub async fn group_redrive(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
        group: &str,
        offset: u64,
    ) -> Result<()> {
        self.on_group_shard(
            shard_key(tenant_id, namespace, stream, shard),
            |client| async move {
                client
                    .group_redrive(tenant_id, namespace, stream, shard, group, offset)
                    .await
            },
        )
        .await
    }

    /// Run `op` against the broker that leads the shard, following redirects.
    ///
    /// Starts from the broker that last served the shard, or the client in use
    /// when there is none. The remembered broker is forgotten on any failure,
    /// so the next call asks afresh instead of repeating a question to a broker
    /// that has stopped answering or lost the shard.
    pub(crate) async fn on_group_shard<T, F, Fut>(&self, key: ShardKey, op: F) -> Result<T>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let cached = self.group_routes.read().await.get(&key).cloned();
        let cached_route = cached.is_some();
        let mut client = match cached {
            Some(client) => client,
            None => self.client().await,
        };
        let mut visited: Vec<String> = Vec::new();
        let mut went_back = false;

        for _ in 0..=MAX_REDIRECTS {
            let error = match op(Arc::clone(&client)).await {
                Ok(value) => {
                    self.group_routes.write().await.insert(key, client);
                    return Ok(value);
                }
                Err(err) => err,
            };
            self.group_routes.write().await.remove(&key);
            // A remembered or redirected-to leader that answers
            // `shard_unavailable` or `draining` has lost the shard since it was
            // named. The broker in use routes by the current assignment, so ask
            // it again, once.
            if (cached_route || !visited.is_empty())
                && !went_back
                && super::route_went_stale(&error)
            {
                went_back = true;
                visited.clear();
                client = self.client().await;
                continue;
            }
            let Some(redirect) = error.downcast_ref::<crate::NotLeaderError>().cloned() else {
                return Err(error);
            };
            if visited.iter().any(|seen| seen == &redirect.node_id) {
                return Err(error.context(format!(
                    "redirected back to {}, which has already been asked",
                    redirect.node_id
                )));
            }
            let Some(addr) = redirect.addr.clone() else {
                return Err(error.context(
                    "the owner's client address is not published, so there is nowhere to follow to",
                ));
            };
            let addr = self
                .resolve(&addr)
                .await
                .with_context(|| format!("the owner's address {addr:?} is not usable"))?;
            visited.push(redirect.node_id.clone());
            client = self
                .connect_to(addr)
                .await
                .with_context(|| format!("connect to the shard owner at {addr}"))?;
        }

        Err(anyhow::anyhow!(
            "still being redirected after {MAX_REDIRECTS} hops; the cluster has not settled on an owner"
        ))
    }
}

fn shard_key(tenant_id: &str, namespace: &str, stream: &str, shard: u32) -> ShardKey {
    (
        tenant_id.to_string(),
        namespace.to_string(),
        stream.to_string(),
        shard,
    )
}

//! The owner cache: which broker to send a shard's publishes to, learned
//! from acks that say the publish was forwarded.

use anyhow::Result;

use std::sync::Arc;

use super::{ClusterClient, Owner, ShardKey, StreamKey};

impl ClusterClient {
    /// Which shard a routing key resolves to, using the same function the
    /// broker routes with (`felix_wire::routing::shard_for_routing`) and the
    /// stream's own mapping.
    ///
    /// The width and mapping come from `widths`. A broker that cannot answer
    /// -- one predating `FEATURE_STREAM_SHARDS` -- yields shard 0, which is
    /// the safe answer: everything shares one cache entry and the worst case
    /// is the forwarding this exists to avoid.
    pub(super) async fn shard_of(&self, key: &StreamKey, routing_key: Option<&[u8]>) -> u32 {
        let Some(routing_key) = routing_key else {
            return 0;
        };
        self.widths
            .shard_of(&key.0, &key.1, &key.2, routing_key)
            .await
            .0
    }

    /// Drop the stream's width when a publish to it says the width may be
    /// stale, so the next keyed publish asks again.
    pub(super) fn forget_width_if_stale<T>(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        outcome: &Result<T>,
    ) {
        if let Err(err) = outcome
            && crate::publish::width_went_stale(err)
        {
            self.widths.forget(tenant_id, namespace, stream);
            self.discovery.mark_stale(&(
                tenant_id.to_string(),
                namespace.to_string(),
                stream.to_string(),
            ));
        }
    }

    /// Record where a shard's publishes should go next time.
    ///
    /// Connecting is done here rather than on the publish path so the cost is
    /// paid once, by the publish that learned it, instead of by the first one
    /// that could have used it.
    ///
    /// A failure to connect is not an error: the owner is simply not cached,
    /// and publishes keep forwarding. That is the whole safety property of
    /// this cache -- it is an optimisation over a path that already works.
    pub(super) async fn remember_owner(
        &self,
        key: ShardKey,
        owner: felix_wire::binary::PublishOwner,
        // The width the shard number was worked out by, if keyed.
        width: Option<(u32, felix_wire::routing::ShardRouting)>,
    ) {
        // Nowhere to route to. The cluster has not been told where clients
        // reach this broker, the same gap `NotLeader` has.
        let Some(addr) = owner.addr.as_deref() else {
            return;
        };
        let Some(addr) = self.resolve(addr).await else {
            return;
        };

        // A stale answer must not replace a fresher one. Checked before
        // connecting, so a late report does not even pay for the connection.
        if let Some(existing) = self.owners.read().await.get(&key)
            && !replaces(existing, &owner, width)
        {
            return;
        }

        // Learning an owner is an optimisation and runs before the publish's
        // ack is returned, so the token fetch is inside the timer too: a slow
        // provider costs this route, not the publish. The fetch itself keeps
        // running for the next caller.
        let connected = tokio::time::timeout(super::discovery::CONNECT_TIMEOUT, async {
            self.nodes_prefetch().await?;
            self.connect_to(addr).await
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("connecting timed out")));
        match connected {
            Ok(client) => {
                let mut owners = self.owners.write().await;
                // Re-checked under the write lock: another publish may have
                // learned a newer owner while this one was connecting.
                if owners
                    .get(&key)
                    .is_some_and(|existing| !replaces(existing, &owner, width))
                {
                    return;
                }
                tracing::debug!(
                    owner = %owner.node_id,
                    %addr,
                    generation = owner.generation,
                    "routing this stream's publishes to the shard owner",
                );
                owners.insert(
                    key,
                    Arc::new(Owner {
                        node_id: owner.node_id,
                        generation: owner.generation,
                        addr,
                        client: tokio::sync::OnceCell::new_with(Some(client)),
                        width,
                    }),
                );
            }
            Err(err) => {
                tracing::debug!(
                    owner = %owner.node_id,
                    %addr,
                    error = %err,
                    "could not connect to the shard owner; still forwarding",
                );
            }
        }
    }

    /// The owner a publish routed to did not answer. Forget it and say which
    /// broker and shard failed -- the caller never chose that broker and would
    /// otherwise have no way to identify it.
    ///
    /// Only called when the publish that failed actually went to a cached
    /// owner. Forgetting it here is what makes the next publish for this shard
    /// forward instead of repeating the failure against a broker that may have
    /// lost the shard or gone away.
    pub(super) async fn forget_owner(
        &self,
        key: &ShardKey,
        node_id: String,
        err: anyhow::Error,
    ) -> anyhow::Error {
        self.owners.write().await.remove(key);
        self.discovery
            .mark_stale(&(key.0.clone(), key.1.clone(), key.2.clone()));
        err.context(format!(
            "publish to shard {}'s owner {node_id} failed; forgetting it and \
             forwarding the next one",
            key.3
        ))
    }
}

/// Whether an owner learned now should take the place of `existing`.
///
/// Generations order answers about one shard, so they decide whenever both
/// answers were worked out under the same width, or one of them under none.
/// Under two different known widths the shard number names different shards,
/// the generations say nothing about each other, and the newer report wins: an
/// entry from a stale width must not keep out the fresh owner.
fn replaces(
    existing: &super::Owner,
    owner: &felix_wire::binary::PublishOwner,
    width: Option<(u32, felix_wire::routing::ShardRouting)>,
) -> bool {
    match (existing.width, width) {
        (Some(known), Some(now)) if known != now => true,
        _ => existing.generation < owner.generation,
    }
}

//! The owner cache: which broker to send a shard's publishes to, learned
//! from acks that say the publish was forwarded.

use std::net::SocketAddr;

use super::{ClusterClient, Owner, ShardKey, StreamKey};

impl ClusterClient {
    /// Which shard a routing key resolves to, using the same function the
    /// broker routes with (`felix_wire::routing::shard_for_routing`) and the
    /// stream's own mapping.
    ///
    /// The width and mapping are asked once per stream and cached. A broker
    /// that cannot answer -- one predating `FEATURE_STREAM_SHARDS` -- yields
    /// shard 0, which is the safe answer: everything shares one cache entry
    /// and the worst case is the forwarding this exists to avoid.
    pub(super) async fn shard_of(&self, key: &StreamKey, routing_key: Option<&[u8]>) -> u32 {
        if let Some((shards, routing)) = self.shards.read().await.get(key) {
            return felix_wire::routing::shard_for_routing(*routing, *shards, routing_key);
        }
        let (shards, routing) = match self
            .client()
            .await
            .stream_routing(&key.0, &key.1, &key.2)
            .await
        {
            Ok((shards, routing)) => (shards.max(1), routing),
            Err(err) => {
                tracing::debug!(
                    stream = %key.2,
                    error = %err,
                    "could not learn the stream's width; routing keyed publishes as shard 0",
                );
                (1, Default::default())
            }
        };
        self.shards
            .write()
            .await
            .insert(key.clone(), (shards, routing));
        felix_wire::routing::shard_for_routing(routing, shards, routing_key)
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
    ) {
        // Nowhere to route to. The cluster has not been told where clients
        // reach this broker, the same gap `NotLeader` has.
        let Some(addr) = owner.addr.as_deref() else {
            return;
        };
        let Ok(addr) = addr.parse::<SocketAddr>() else {
            tracing::debug!(
                owner = %owner.node_id,
                addr,
                "the shard owner's address does not parse; still forwarding",
            );
            return;
        };

        // A stale answer must not replace a fresher one. Checked before
        // connecting, so a late report does not even pay for the connection.
        if let Some(existing) = self.owners.read().await.get(&key)
            && existing.generation >= owner.generation
        {
            return;
        }

        match self.connect_to(addr).await {
            Ok(client) => {
                let mut owners = self.owners.write().await;
                // Re-checked under the write lock: another publish may have
                // learned a newer owner while this one was connecting.
                if owners
                    .get(&key)
                    .is_some_and(|existing| existing.generation >= owner.generation)
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
                    Owner {
                        node_id: owner.node_id,
                        generation: owner.generation,
                        client,
                    },
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
        err.context(format!(
            "publish to shard {}'s owner {node_id} failed; forgetting it and \
             forwarding the next one",
            key.3
        ))
    }
}

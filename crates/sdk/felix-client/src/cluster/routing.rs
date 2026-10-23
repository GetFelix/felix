//! The owner cache: which broker to send a shard's publishes to, learned
//! from acks that say the publish was forwarded.

use anyhow::Result;

use std::time::{Duration, Instant};

use super::{ClusterClient, Discovered, Owner, ShardKey, StreamKey};

/// How long discovered owners are trusted before they are asked for again.
/// A publish that finds an owner wrong asks sooner, after
/// [`RETRY_AFTER_STALE`].
const OWNER_TTL: Duration = Duration::from_secs(5);

/// The least time between two discoveries for one stream, so a burst of
/// failures against a cluster that has not settled asks once, not once each.
const RETRY_AFTER_STALE: Duration = Duration::from_millis(250);

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

    /// Whether the stream's owners are worth asking for now, claiming the
    /// ask so concurrent publishes make one request between them.
    pub(super) fn claim_discovery(&self, key: &StreamKey) -> bool {
        let mut seen = self.discovered.lock().unwrap();
        let due = match seen.get(key) {
            None => true,
            Some(last) if last.stale => last.at.elapsed() >= RETRY_AFTER_STALE,
            Some(last) => last.at.elapsed() >= OWNER_TTL,
        };
        if due {
            seen.insert(
                key.clone(),
                Discovered {
                    at: Instant::now(),
                    stale: false,
                },
            );
        }
        due
    }

    /// Something found this stream's cached owners or width wrong: ask the
    /// cluster again at the next publish, soon.
    pub(super) fn mark_owners_stale(&self, key: &StreamKey) {
        if let Some(last) = self.discovered.lock().unwrap().get_mut(key) {
            last.stale = true;
        }
    }

    /// Ask the cluster who owns each of the stream's shards and cache the
    /// answer, so even the first publish for a shard goes to its owner.
    ///
    /// Without this an owner is learned only from an ack saying the batch was
    /// forwarded, which a fire-and-forget publish never gets: it would be
    /// forwarded for as long as it ran. The answer is the entry broker's
    /// routing snapshot, so it can be stale; a write that lands during an
    /// owner move may still be forwarded. Failure to ask or to connect is not
    /// an error: those shards keep going through the entry broker.
    ///
    /// Skipped when the broker predates `FEATURE_SHARD_OWNERS`, or when its
    /// answer does not match the stream's width (the stream is changing).
    pub(super) async fn discover_owners(&self, key: &StreamKey) {
        let client = self.client().await;
        if !client.supports_shard_owners() {
            return;
        }
        let owners = match client
            .shard_owners(&key.0, &key.1, &key.2, crate::ShardKind::Stream)
            .await
        {
            Ok(owners) => owners,
            Err(err) => {
                tracing::debug!(error = %err, "could not discover shard owners");
                return;
            }
        };
        // The mapping the shard numbers were chosen under. Owners are stored
        // with it so a stream recreated at another width drops them.
        let (_, width) = self.widths.shard_of(&key.0, &key.1, &key.2, b"").await;
        if !width.learned || width.shards as usize != owners.len() {
            return;
        }
        let learned = Some((width.shards, width.routing));
        for owner in owners {
            let (Some(node_id), Some(addr)) = (owner.node_id, owner.addr) else {
                continue;
            };
            let shard: ShardKey = (key.0.clone(), key.1.clone(), key.2.clone(), owner.shard);
            let owner = felix_wire::binary::PublishOwner {
                node_id,
                addr: Some(addr),
                generation: owner.generation,
            };
            self.remember_owner(shard, owner, learned).await;
        }
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
            self.mark_owners_stale(&(
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

        match self.connect_to(addr).await {
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
                    Owner {
                        node_id: owner.node_id,
                        generation: owner.generation,
                        client,
                        width,
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
        self.mark_owners_stale(&(key.0.clone(), key.1.clone(), key.2.clone()));
        err.context(format!(
            "publish to shard {}'s owner {node_id} failed; forgetting it and \
             forwarding the next one",
            key.3
        ))
    }
}

/// Whether an owner learned now should take the place of `existing`.
///
/// Generations order answers about one shard. Under another width the shard
/// number names another shard, so the generations say nothing about each
/// other and the newer report wins: an entry from a stale width must not
/// keep out the fresh owner. A late answer from the old width can still land
/// here, and is dropped on its first use under the current width.
fn replaces(
    existing: &super::Owner,
    owner: &felix_wire::binary::PublishOwner,
    width: Option<(u32, felix_wire::routing::ShardRouting)>,
) -> bool {
    existing.width != width || existing.generation < owner.generation
}

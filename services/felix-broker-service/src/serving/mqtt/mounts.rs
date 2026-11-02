//! Which stream an MQTT topic is published to, and the shard of it.
//!
//! Mounts reach the broker with the stream metadata it already syncs from the
//! control plane. This is the listener's view of them.

use std::sync::Arc;

use felix_broker::Broker;
use felix_mqtt::{MountedTopic, Mounts};

use crate::shards::ShardKind;
use crate::shards::routing::IngressRouter;

/// [`Mounts`] for this broker: the registry's mounts, and the routing table
/// for the shard.
pub struct BrokerMounts {
    broker: Arc<Broker>,
    ingress: Option<Arc<IngressRouter>>,
}

impl BrokerMounts {
    /// `ingress` is the cluster view; without it the broker is a single node
    /// and every topic routes to shard 0, as every key of a stream with one
    /// shard does.
    pub fn new(broker: Arc<Broker>, ingress: Option<Arc<IngressRouter>>) -> Self {
        Self { broker, ingress }
    }
}

impl Mounts for BrokerMounts {
    fn resolve(&self, tenant_id: &str, topic: &str) -> Option<MountedTopic> {
        let mounted = self.broker.resolve_mount(tenant_id, topic)?;
        // The whole topic is the routing key, so a topic keeps its order.
        let shard = self.ingress.as_ref().map_or(0, |ingress| {
            ingress.shard_for_key(
                ShardKind::Stream,
                tenant_id,
                &mounted.namespace,
                &mounted.stream,
                Some(topic.as_bytes()),
            )
        });
        Some(MountedTopic {
            namespace: mounted.namespace,
            stream: mounted.stream,
            shard,
        })
    }
}

#[cfg(test)]
mod tests;

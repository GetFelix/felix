//! Which stream a PUBLISH topic belongs to.
//!
//! The listener does not know about streams. The broker service answers
//! through [`Mounts`], from the mounts the control plane hands every broker
//! (`docs/mqtt-design.md`, "Topic model").

use std::sync::Arc;

/// Where a topic's messages go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountedTopic {
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
    /// The shard of the stream the full topic name routes to. Every message
    /// on one topic lands in one shard, so a topic keeps its order.
    pub shard: u32,
}

/// Resolves topics to streams. Called on every PUBLISH, so it must not block.
pub trait Mounts: Send + Sync {
    /// The stream mounted at the longest prefix of `topic` for `tenant_id`,
    /// or `None` when no mount covers it.
    fn resolve(&self, tenant_id: &str, topic: &str) -> Option<MountedTopic>;
}

/// Mounts nothing: every PUBLISH is to an unmounted topic.
pub struct NoMounts;

impl Mounts for NoMounts {
    fn resolve(&self, _tenant_id: &str, _topic: &str) -> Option<MountedTopic> {
        None
    }
}

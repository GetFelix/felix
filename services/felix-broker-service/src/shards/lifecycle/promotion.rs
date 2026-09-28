//! Opening a promoted shard once replication has fenced its replicas.
//!
//! The lifecycle holds a promoted stream shard in `Fencing`; replication runs
//! the fence and calls [`LifecycleGate::open`] when a majority has taken it,
//! or when some replica does not offer it and the shard opens on the lease.
//! See `felix_replication::promotion`.

use std::sync::Arc;

use super::{ShardLifecycle, record_term_start, write_generation_start};
use crate::shards::ShardKey;
use crate::shards::routing::IngressRouter;

/// The broker's side of `felix_replication::promotion::PromotionGate`.
pub struct LifecycleGate {
    lifecycle: Arc<tokio::sync::Mutex<ShardLifecycle>>,
    ingress: Arc<IngressRouter>,
    storage: Arc<felix_broker::DurableStorage>,
    broker: Arc<felix_broker::Broker>,
    fleet: Arc<felix_common::fleet::FleetGate>,
}

impl LifecycleGate {
    pub fn new(
        lifecycle: Arc<tokio::sync::Mutex<ShardLifecycle>>,
        ingress: Arc<IngressRouter>,
        storage: Arc<felix_broker::DurableStorage>,
        broker: Arc<felix_broker::Broker>,
        fleet: Arc<felix_common::fleet::FleetGate>,
    ) -> Self {
        Self {
            lifecycle,
            ingress,
            storage,
            broker,
            fleet,
        }
    }
}

#[async_trait::async_trait]
impl felix_replication::promotion::PromotionGate for LifecycleGate {
    fn awaiting(&self, key: &ShardKey) -> Option<u64> {
        self.ingress.fence().awaiting_promotion(key)
    }

    async fn open(&self, key: &ShardKey, generation: u64) -> bool {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.phase(key) != super::Phase::Fencing
            || lifecycle.generation(key) != Some(generation)
        {
            return true;
        }
        let start_record = self.fleet.supports(felix_common::fleet::GENERATION_START);
        // Recorded now rather than at open: the fence may have taken a
        // replica's tail, and those records belong to its generation.
        //
        // Once the fleet counts marks from the generation's start, the shard
        // stays closed without it: the next pass fences again and retries,
        // rather than serving on a mark that would never cover what this
        // leader inherited.
        match self
            .storage
            .open_stream(&key.tenant_id, &key.namespace, &key.stream, key.shard)
        {
            Ok(log) => {
                if let Err(err) = record_term_start(&log, key, generation, start_record).await {
                    tracing::warn!(stream = %key.stream, shard = key.shard, error = %err,
                        "could not record where this leadership begins; not serving yet");
                    return false;
                }
                if start_record
                    && let Err(err) =
                        write_generation_start(&self.broker, &log, key, generation).await
                {
                    tracing::warn!(stream = %key.stream, shard = key.shard, error = %err,
                        "could not write the generation-start record; not serving yet");
                    return false;
                }
            }
            Err(err) => {
                tracing::warn!(stream = %key.stream, shard = key.shard, error = %err,
                    "could not open the shard's log to record where this leadership begins");
                if start_record {
                    return false;
                }
            }
        }
        if lifecycle.fenced(key, generation) {
            self.ingress.publish_servable(lifecycle.servable());
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                "promoted shard now serving",
            );
        }
        true
    }
}

//! Publishing through a [`Client`], and the producer ids idempotent publishes need.

use std::sync::Arc;

use anyhow::Result;
use felix_wire::Message;

use super::Client;
use crate::publish::{IdempotentProducer, Publisher, PublisherInner};

impl Client {
    /// A publisher over this client's publish streams.
    ///
    /// Cheap to make: every publisher from one client shares its streams and
    /// its in-flight byte budget.
    ///
    /// Under [`crate::PublishSharding::HashStream`] each shard of a stream
    /// gets a stream of its own, so one stream's shards spread across the
    /// client's connections while every publish to one shard keeps one
    /// writer. A keyed publish learns the stream's width from the broker
    /// once, on the first keyed publish to it.
    pub async fn publisher(&self) -> Result<crate::publish::Publisher> {
        Ok(self.publisher_handle())
    }

    pub(crate) fn publisher_handle(&self) -> Publisher {
        Publisher {
            inner: Arc::new(PublisherInner::with_runtime_config(
                Arc::clone(&self.publish_workers),
                self.publish_sharding,
                Arc::clone(&self.publish_admission),
                self.publish_stream_hasher.clone(),
                Arc::clone(&self.publish_shard_streams),
                Arc::clone(&self.publish_widths),
                self.runtime_config.bench_embed_ts,
            )),
        }
    }

    /// A producer id from the broker, for idempotent publishes.
    ///
    /// Refused without a round trip against a broker that did not advertise
    /// [`felix_wire::FEATURE_IDEMPOTENT_PRODUCER`]: probing an older broker
    /// would cost the connection.
    pub async fn producer_init(&self) -> Result<u64> {
        if !felix_wire::supports_feature(
            self.server_features,
            felix_wire::FEATURE_IDEMPOTENT_PRODUCER,
        ) {
            anyhow::bail!("this broker does not support idempotent producers");
        }
        static REQUEST_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let request_id = REQUEST_IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match self
            .group_round_trip(Message::ProducerInit { request_id }, request_id)
            .await?
        {
            Message::ProducerInitOk { producer_id, .. } => Ok(producer_id),
            other => Err(anyhow::anyhow!(
                "unexpected answer to producer_init: {:?}",
                std::mem::discriminant(&other)
            )),
        }
    }

    /// A producer whose publishes through this client land once, however
    /// many times they are sent. See [`crate::IdempotentProducer`].
    ///
    /// Bound to this one broker: a batch for a shard led elsewhere is refused
    /// with the leader's address, which a [`crate::ClusterClient`]'s producer
    /// follows and this one reports.
    ///
    /// The producer keeps a handle on this client, so it can be stored or
    /// moved into a task of its own.
    pub async fn idempotent_producer(self: &Arc<Self>) -> Result<IdempotentProducer> {
        let producer_id = self.producer_init().await?;
        Ok(IdempotentProducer::for_client(
            Arc::clone(self),
            producer_id,
        ))
    }
}

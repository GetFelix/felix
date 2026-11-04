//! How an MQTT message is written: the broker's own publish path, on the
//! shard's owner.
//!
//! The same gates as a Kafka produce with `acks=all` (`serving/kafka.rs`):
//! the shard must be dispatched here and pass the commit fence, the batch is
//! an idempotent append under the connection's producer id, and the answer
//! waits for the stream's consistency level. A shard owned elsewhere is
//! refused; forwarding it needs the producer sequence on the internal
//! protocol, which is a later slice.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use felix_broker::{Broker, BrokerError, PublishOutcome, SequenceReuse};
use felix_mqtt::{Refusal, WriteBatch, WriteError, Writes};
use felix_replication::quorum::{QuorumError, QuorumMarks};

use crate::observability::tenants;
use crate::serving::limits::TenantRates;
use crate::shards::lifecycle::fence;
use crate::shards::routing::{Dispatch, IngressRouter, dispatch_write};
use crate::shards::{ShardKey, ShardKind};

/// [`Writes`] for this broker.
pub struct BrokerWrites {
    broker: Arc<Broker>,
    ingress: Option<Arc<IngressRouter>>,
    marks: Option<Arc<QuorumMarks>>,
    quorum_timeout: Duration,
    quotas: Arc<TenantRates>,
}

impl BrokerWrites {
    /// `ingress` and `marks` are the cluster view and `None` outside a
    /// cluster, where this broker owns every shard.
    pub fn new(
        broker: Arc<Broker>,
        ingress: Option<Arc<IngressRouter>>,
        marks: Option<Arc<QuorumMarks>>,
        quorum_timeout: Duration,
    ) -> Self {
        Self {
            broker,
            ingress,
            marks,
            quorum_timeout,
            quotas: Arc::new(TenantRates::unlimited()),
        }
    }

    /// The tenant publish quotas a write is charged to: the broker's own, so a
    /// tenant's QUIC, Kafka and MQTT publishes share one budget.
    pub(crate) fn with_quotas(mut self, quotas: Arc<TenantRates>) -> Self {
        self.quotas = quotas;
        self
    }
}

#[async_trait]
impl Writes for BrokerWrites {
    fn new_producer_id(&self) -> u64 {
        self.broker.new_producer_id()
    }

    fn admit_produce(&self, tenant_id: &str, records: u64, bytes: u64) -> Duration {
        tenants::record_published(tenant_id, records, bytes);
        let hold = self.quotas.charge(tenant_id, records, bytes);
        if !hold.is_zero() {
            tenants::record_throttled(tenant_id, tenants::THROTTLE_DELAYED);
        }
        hold
    }

    async fn write(&self, batch: WriteBatch) -> Result<(), WriteError> {
        // Every MQTT message carries headers, so a broker that does not
        // accept them takes none; checked first so nothing else is spent.
        if !self.broker.records_headers() {
            return Err(WriteError::Refused(Refusal::HeadersDisabled));
        }
        let target = &batch.target;
        let handle = self
            .broker
            .resolve_stream_handle(
                &batch.tenant_id,
                &target.namespace,
                &target.stream,
                target.shard,
            )
            .await
            .map_err(|_| WriteError::Refused(Refusal::StreamUnavailable))?;
        if handle.log().is_none() {
            return Err(WriteError::Refused(Refusal::StreamUnavailable));
        }

        let key = ShardKey {
            tenant_id: batch.tenant_id.to_string(),
            namespace: target.namespace.to_string(),
            stream: target.stream.to_string(),
            shard: target.shard,
            kind: ShardKind::Stream,
        };
        let ingress = self.ingress.as_deref();
        let permit = match dispatch_write(ingress, &key).await {
            (Dispatch::Local { generation }, mut held) => {
                fence::enter_or_keep(&mut held, ingress, Some(&key), generation)
                    .map_err(|_| WriteError::Refused(Refusal::NotOwner))?
            }
            _ => return Err(WriteError::Refused(Refusal::NotOwner)),
        };

        let payloads: Vec<Bytes> = batch.records.iter().map(|r| r.payload.clone()).collect();
        let headers: Vec<Option<Bytes>> = batch
            .records
            .iter()
            .map(|r| Some(r.headers.clone()))
            .collect();
        let claimed = self
            .broker
            .claim_batch_idempotent(
                &handle,
                batch.producer_id,
                batch.sequence,
                &payloads,
                SequenceReuse::Refuse,
                None,
                Some(&headers),
            )
            .await
            .map_err(claim_failure)?;
        let written = self.broker.complete_idempotent(claimed).await;
        // Held until the records are durable, so a move waits for them.
        drop(permit);
        let written = written.map_err(|err| {
            tracing::debug!(error = %err, "mqtt write failed after its offsets were claimed");
            WriteError::OutcomeUnknown("write_failed")
        })?;

        if batch.acknowledged && written.outcome.offsets.is_some() {
            let outcome = PublishOutcome {
                subscribers: 0,
                offsets: written.outcome.offsets,
            };
            let shard = self.ingress.as_ref().map(|_| &key);
            felix_replication::quorum::await_quorum(
                &handle,
                shard,
                &outcome,
                self.marks.as_deref(),
                ingress,
                self.quorum_timeout,
            )
            .await
            .map_err(|err| match err.downcast_ref::<QuorumError>() {
                Some(QuorumError::TimedOut { .. }) => WriteError::OutcomeUnknown("quorum_timeout"),
                _ => WriteError::OutcomeUnknown("leadership_lost"),
            })?;
        }
        Ok(())
    }
}

/// A failure before the batch took its offsets. Only the two that are known
/// to leave the log untouched are told to the client; anything else could
/// have been partway, so it is not.
fn claim_failure(err: BrokerError) -> WriteError {
    match err {
        BrokerError::RecordHeadersNotEnabled => WriteError::Refused(Refusal::HeadersDisabled),
        BrokerError::StreamHandleInactive(_) => WriteError::Refused(Refusal::StreamUnavailable),
        err => {
            tracing::debug!(error = %err, "mqtt write failed while claiming its offsets");
            WriteError::OutcomeUnknown("write_failed")
        }
    }
}

#[cfg(test)]
mod tests;

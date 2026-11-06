//! How an MQTT message is written: the broker's own publish path on the
//! shard's owner, or a forward to the owner from any other broker.
//!
//! On the owner, the same gates as a Kafka produce with `acks=all`
//! (`serving/kafka.rs`): the shard must be dispatched here and pass the commit
//! fence, the batch is an idempotent append under the connection's producer
//! id, and the answer waits for the stream's consistency level.
//!
//! Elsewhere the batch is forwarded with its producer id and sequence, and the
//! owner applies it as the same idempotent append. That is what lets a batch
//! whose answer was lost be sent again, to whichever broker owns the shard by
//! then, including this one: a log that already holds it answers with its
//! offsets and writes nothing. `docs/mqtt-design.md`, "Publishing".

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use felix_broker::{Broker, BrokerError, PublishOutcome, SequenceReuse, StreamHandle};
use felix_mqtt::{Refusal, WriteBatch, WriteError, Writes};
use felix_replication::peer::{PeerPool, PeerRequester};
use felix_replication::quorum::{QuorumError, QuorumMarks};
use felix_wire::internal::{AckMode, ErrorCode, ForwardProducer};

use crate::observability::tenants;
use crate::serving::forward::{ForwardError, ForwardKey, ForwardTarget, forward_publish};
use crate::serving::limits::TenantRates;
use crate::shards::lifecycle::fence;
use crate::shards::routing::{Dispatch, IngressRouter, dispatch_write};
use crate::shards::{ShardKey, ShardKind};

/// Longest wait between two sends of a batch whose answer was lost. A
/// failover takes about a lease, so waiting longer only delays the answer.
const MAX_RESEND_DELAY: Duration = Duration::from_millis(500);

/// [`Writes`] for this broker.
pub struct BrokerWrites<P = PeerPool> {
    broker: Arc<Broker>,
    ingress: Option<Arc<IngressRouter>>,
    marks: Option<Arc<QuorumMarks>>,
    quorum_timeout: Duration,
    quotas: Arc<TenantRates>,
    forwarding: Option<Forwarding<P>>,
}

/// How a batch reaches a shard another broker owns.
struct Forwarding<P> {
    peers: Arc<P>,
    /// Bounds the whole write, sends again included; the broker's
    /// `forward_budget`.
    budget: Duration,
}

/// How one send of a batch ended.
enum Sent {
    Written,
    /// Nothing was written by this send.
    Refused(Refusal),
    /// It may be in the log and sending it again cannot tell: the owner's
    /// write or its quorum wait failed after the batch took offsets.
    Unknown(&'static str),
    /// It may be in the log and its answer was lost. Sending it again under
    /// the same sequence settles it.
    Unanswered(&'static str),
}

impl BrokerWrites<PeerPool> {
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
            forwarding: None,
        }
    }
}

impl<P> BrokerWrites<P> {
    /// Forward a batch for a shard another broker owns over `peers`, within
    /// `budget` for the whole write. Without it such a batch is refused
    /// `not_owner`.
    pub(crate) fn with_forwarding<Q>(self, peers: Arc<Q>, budget: Duration) -> BrokerWrites<Q> {
        BrokerWrites {
            broker: self.broker,
            ingress: self.ingress,
            marks: self.marks,
            quorum_timeout: self.quorum_timeout,
            quotas: self.quotas,
            forwarding: Some(Forwarding { peers, budget }),
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
impl<P: PeerRequester + Send + Sync + 'static> Writes for BrokerWrites<P> {
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
        let payloads: Vec<Bytes> = batch.records.iter().map(|r| r.payload.clone()).collect();
        let headers: Vec<Option<Bytes>> = batch
            .records
            .iter()
            .map(|r| Some(r.headers.clone()))
            .collect();
        let deadline = self.forwarding.as_ref().map(|f| Instant::now() + f.budget);

        // Once a send's answer is lost, nothing short of a later send's
        // success says what happened to the batch, so every other ending is
        // an unknown outcome rather than a refusal.
        let mut unanswered: Option<&'static str> = None;
        let mut resends = 0u32;
        loop {
            let sent = self
                .send(&batch, &key, &handle, &payloads, &headers, deadline)
                .await;
            let reason = match sent {
                Sent::Written => return Ok(()),
                Sent::Unknown(reason) => return Err(WriteError::OutcomeUnknown(reason)),
                Sent::Refused(refusal) => match unanswered {
                    None => return Err(WriteError::Refused(refusal)),
                    // During a failover nobody may own the shard for a moment.
                    Some(reason) if refusal == Refusal::NotOwner => reason,
                    Some(reason) => return Err(WriteError::OutcomeUnknown(reason)),
                },
                Sent::Unanswered(reason) => {
                    unanswered = Some(reason);
                    reason
                }
            };
            let delay = Duration::from_millis(50u64 << resends.min(4)).min(MAX_RESEND_DELAY);
            match deadline {
                Some(deadline) if Instant::now() + delay < deadline => {
                    tokio::time::sleep(delay).await;
                    resends += 1;
                }
                _ => return Err(WriteError::OutcomeUnknown(reason)),
            }
        }
    }
}

impl<P: PeerRequester + Send + Sync + 'static> BrokerWrites<P> {
    /// Send the batch once, to wherever the shard is served right now.
    async fn send(
        &self,
        batch: &WriteBatch,
        key: &ShardKey,
        handle: &StreamHandle,
        payloads: &[Bytes],
        headers: &[Option<Bytes>],
        deadline: Option<Instant>,
    ) -> Sent {
        let ingress = self.ingress.as_deref();
        let (permit, generation) = match dispatch_write(ingress, key).await {
            (Dispatch::Local { generation }, mut held) => {
                match fence::enter_or_keep(&mut held, ingress, Some(key), generation) {
                    Ok(permit) => (permit, generation),
                    Err(_) => return Sent::Refused(Refusal::NotOwner),
                }
            }
            (
                Dispatch::Forward {
                    node_id,
                    advertise_addr,
                    generation,
                    incarnation,
                },
                _,
            ) => {
                let target = ForwardTarget {
                    node_id,
                    advertise_addr,
                    generation,
                    incarnation,
                };
                return self
                    .forward(batch, &target, payloads, headers, deadline)
                    .await;
            }
            (Dispatch::Unavailable(_), _) => return Sent::Refused(Refusal::NotOwner),
        };

        let claimed = match self
            .broker
            .claim_batch_idempotent(
                handle,
                batch.producer_id,
                batch.sequence,
                payloads,
                SequenceReuse::Refuse,
                None,
                Some(headers),
            )
            .await
        {
            Ok(claimed) => claimed,
            Err(err) => return claim_failure(err),
        };
        let written = self.broker.complete_idempotent(claimed).await;
        // Held until the records are durable, so a move waits for them.
        drop(permit);
        let written = match written {
            Ok(written) => written,
            Err(err) => {
                tracing::debug!(error = %err, "mqtt write failed after its offsets were claimed");
                return Sent::Unknown("write_failed");
            }
        };

        if batch.acknowledged && written.outcome.offsets.is_some() {
            let outcome = PublishOutcome {
                subscribers: 0,
                offsets: written.outcome.offsets,
            };
            let shard = self.ingress.as_ref().map(|_| key);
            // A re-send late in the budget does not wait past it.
            let wait = deadline.map_or(self.quorum_timeout, |deadline| {
                self.quorum_timeout
                    .min(deadline.saturating_duration_since(Instant::now()))
            });
            let quorum = felix_replication::quorum::await_quorum(
                handle,
                shard,
                &outcome,
                generation,
                self.marks.as_deref(),
                ingress,
                wait,
            )
            .await;
            if let Err(err) = quorum {
                return match err.downcast_ref::<QuorumError>() {
                    Some(QuorumError::TimedOut { .. }) => Sent::Unknown("quorum_timeout"),
                    // Whoever leads now holds the batch or does not; sent
                    // again, it is answered from the log either way.
                    _ => Sent::Unanswered("leadership_lost"),
                };
            }
        }
        Sent::Written
    }

    /// Hand the batch to the shard's owner under the connection's producer
    /// id and sequence.
    async fn forward(
        &self,
        batch: &WriteBatch,
        target: &ForwardTarget,
        payloads: &[Bytes],
        headers: &[Option<Bytes>],
        deadline: Option<Instant>,
    ) -> Sent {
        // The owner checks the client's own token again, so a connection
        // without one, the anonymous development tenant, has nothing to send.
        let (Some(forwarding), Some(deadline), Some(credential)) =
            (&self.forwarding, deadline, &batch.credential)
        else {
            return Sent::Refused(Refusal::NotOwner);
        };
        let key = ForwardKey {
            tenant_id: batch.tenant_id.to_string(),
            namespace: batch.target.namespace.to_string(),
            stream: batch.target.stream.to_string(),
            shard: batch.target.shard,
        };
        let ack = if batch.acknowledged {
            AckMode::OnCommit
        } else {
            AckMode::None
        };
        let forwarded = forward_publish(
            forwarding.peers.as_ref(),
            target,
            &key,
            ack,
            credential.expose(),
            payloads.to_vec(),
            Some(headers.to_vec()),
            Some(ForwardProducer {
                producer_id: batch.producer_id,
                sequence: batch.sequence,
            }),
            deadline.saturating_duration_since(Instant::now()),
        )
        .await;
        match forwarded {
            Ok(_) => Sent::Written,
            Err(ForwardError::Indeterminate { node_id, detail }) => {
                tracing::debug!(%node_id, %detail, "mqtt forward was not answered");
                Sent::Unanswered("forward_unanswered")
            }
            Err(ForwardError::Refused { code, detail, .. }) => {
                tracing::debug!(?code, %detail, "mqtt forward refused");
                match code {
                    // The owner took the batch and its write or quorum wait
                    // failed: it may be in the log.
                    Some(ErrorCode::StorageFailed) => Sent::Unknown("write_failed"),
                    Some(ErrorCode::Unauthorized) => Sent::Refused(Refusal::NotAuthorized),
                    // An owner from before sequenced forwards, one that has
                    // not opened record headers yet, or one that cannot be
                    // reached or keeps moving: nothing was written.
                    _ => Sent::Refused(Refusal::NotOwner),
                }
            }
        }
    }
}

/// A failure before the batch took its offsets. Only the two that are known
/// to leave the log untouched are told to the client; anything else could
/// have been partway, so it is not.
fn claim_failure(err: BrokerError) -> Sent {
    match err {
        BrokerError::RecordHeadersNotEnabled => Sent::Refused(Refusal::HeadersDisabled),
        BrokerError::StreamHandleInactive(_) => Sent::Refused(Refusal::StreamUnavailable),
        err => {
            tracing::debug!(error = %err, "mqtt write failed while claiming its offsets");
            Sent::Unknown("write_failed")
        }
    }
}

#[cfg(test)]
mod tests;

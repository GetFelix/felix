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
//!
//! A Raft shard takes an idempotent batch of one record only, and a shard
//! another broker owns may be on Raft too. So on a broker that runs Raft each
//! record is a batch of its own at its own sequence, as a Kafka producer's
//! are. On a Raft group led here they are proposed one after another without
//! waiting for each commit, and answered once all are committed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use felix_broker::{Attribution, Broker, BrokerError, PublishOutcome, SequenceReuse, StreamHandle};
use felix_mqtt::{Refusal, WriteBatch, WriteError, Writes};
use felix_replication::engine::{CommitError, Engine};
use felix_replication::peer::{PeerPool, PeerRequester};
use felix_replication::quorum::QuorumError;
use felix_wire::internal::{AckMode, ErrorCode, ForwardProducer};

use crate::observability::tenants;
use crate::serving::forward::{ForwardError, ForwardKey, ForwardTarget, forward_publish};
use crate::serving::limits::TenantRates;
use crate::shards::lifecycle::fence;
use crate::shards::routing::{
    Dispatch, RaftIdempotentPublish, RaftRefusal, StreamEngine, dispatch_write,
};
use crate::shards::{ShardKey, ShardKind};

/// Longest wait between two sends of a batch whose answer was lost. A
/// failover takes about a lease, so waiting longer only delays the answer.
const MAX_RESEND_DELAY: Duration = Duration::from_millis(500);

/// [`Writes`] for this broker.
pub struct BrokerWrites<P = PeerPool> {
    broker: Arc<Broker>,
    engine: Arc<StreamEngine>,
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
    /// Nothing was written: the shard's Raft group here has no leader to
    /// take it right now, as during an election.
    Leaderless,
    /// It may be in the log and sending it again cannot tell: the owner's
    /// write or its quorum wait failed after the batch took offsets.
    Unknown(&'static str),
    /// It may be in the log and its answer was lost. Sending it again under
    /// the same sequence settles it.
    Unanswered(&'static str),
}

impl BrokerWrites<PeerPool> {
    /// `engine` carries the cluster view (its shard routing and quorum marks),
    /// which is absent outside a cluster, where this broker owns every shard.
    pub fn new(broker: Arc<Broker>, engine: Arc<StreamEngine>, quorum_timeout: Duration) -> Self {
        Self {
            broker,
            engine,
            quorum_timeout,
            quotas: Arc::new(TenantRates::unlimited()),
            forwarding: None,
        }
    }
}

impl<P> BrokerWrites<P> {
    #[cfg(test)]
    pub(crate) fn engine(&self) -> &Arc<StreamEngine> {
        &self.engine
    }

    /// Forward a batch for a shard another broker owns over `peers`, within
    /// `budget` for the whole write. Without it such a batch is refused
    /// `not_owner`.
    pub(crate) fn with_forwarding<Q>(self, peers: Arc<Q>, budget: Duration) -> BrokerWrites<Q> {
        BrokerWrites {
            broker: self.broker,
            engine: self.engine,
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

    fn sequence_per_record(&self) -> bool {
        self.engine.runs_raft()
    }

    async fn write(&self, batch: WriteBatch) -> Result<(), WriteError> {
        // Every MQTT message carries headers, so a broker that does not
        // accept them takes none; checked first so nothing else is spent.
        if !self.broker.records_headers() {
            return Err(WriteError::Refused(Refusal::HeadersDisabled));
        }
        let target = &batch.target;
        let key = ShardKey {
            tenant_id: batch.tenant_id.to_string(),
            namespace: target.namespace.to_string(),
            stream: target.stream.to_string(),
            shard: target.shard,
            kind: ShardKind::Stream,
        };
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

        if self.engine.runs_raft() {
            self.write_records(&batch, &key, &handle).await
        } else {
            self.write_batch(&batch, &key, &handle, None).await
        }
    }
}

impl<P: PeerRequester + Send + Sync + 'static> BrokerWrites<P> {
    /// Write each record of `batch` as a one-record batch at its own
    /// sequence, from `batch.sequence` on.
    ///
    /// On a Raft group led here, each is proposed once the one before it is
    /// on this broker's log (the producer's turn), not once it commits, so
    /// the batch costs about one commit round trip. A record proposed after
    /// one that was never written is refused as a gap, so the log never
    /// holds a record without the ones before it. The records are settled
    /// in order: one whose proposal failed, and every one that was never
    /// proposed, is sent again on its own, which answers it from whatever
    /// log holds it by then.
    async fn write_records(
        &self,
        batch: &WriteBatch,
        key: &ShardKey,
        handle: &StreamHandle,
    ) -> Result<(), WriteError> {
        let deadline = self.forwarding.as_ref().map(|f| Instant::now() + f.budget);
        let mut proposed = Vec::with_capacity(batch.records.len());
        if self.engine.on_raft(key)
            && let Some(term) = self.engine.raft_term(key)
        {
            for at in 0..batch.records.len() {
                let record = record_batch(batch, at);
                match self
                    .propose_on_raft(&record, key, handle, term, deadline)
                    .await
                {
                    Ok(record) => proposed.push(record),
                    Err(_) => break,
                }
            }
        }
        let mut proposed = proposed.into_iter();
        for at in 0..batch.records.len() {
            let unanswered = match proposed.next() {
                Some(record) => match committed(record.complete().await) {
                    Sent::Written => continue,
                    Sent::Unknown(reason) => return Err(WriteError::OutcomeUnknown(reason)),
                    Sent::Unanswered(reason) => Some(reason),
                    Sent::Refused(_) | Sent::Leaderless => None,
                },
                None => None,
            };
            let record = record_batch(batch, at);
            match self.write_batch(&record, key, handle, unanswered).await {
                Ok(()) => {}
                // The records before it are written, so the client cannot
                // be told the batch was not.
                Err(WriteError::Refused(_)) if at > 0 => {
                    return Err(WriteError::OutcomeUnknown("partly_written"));
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Write `batch` whole, sending it again until its outcome is known or
    /// the budget ends. `unanswered` is set when an earlier send of it was
    /// not answered.
    async fn write_batch(
        &self,
        batch: &WriteBatch,
        key: &ShardKey,
        handle: &StreamHandle,
        mut unanswered: Option<&'static str>,
    ) -> Result<(), WriteError> {
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
        let mut resends = 0u32;
        loop {
            let sent = self
                .send(batch, key, handle, &payloads, &headers, deadline)
                .await;
            let delay = Duration::from_millis(50u64 << resends.min(4)).min(MAX_RESEND_DELAY);
            let reason = match sent {
                Sent::Written => return Ok(()),
                Sent::Unknown(reason) => return Err(WriteError::OutcomeUnknown(reason)),
                Sent::Leaderless => match (unanswered, deadline) {
                    // During a failover nobody may own the shard for a moment.
                    (Some(reason), _) => reason,
                    // A Raft shard has no leader during an election, which
                    // the hybrid's dispatch waits out. Nothing was written,
                    // so it is waited out here, and still a refusal if the
                    // budget ends.
                    (None, Some(deadline)) if Instant::now() + delay < deadline => {
                        tokio::time::sleep(delay).await;
                        resends += 1;
                        continue;
                    }
                    (None, _) => return Err(WriteError::Refused(Refusal::NotOwner)),
                },
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
            match deadline {
                Some(deadline) if Instant::now() + delay < deadline => {
                    tokio::time::sleep(delay).await;
                    resends += 1;
                }
                _ => return Err(WriteError::OutcomeUnknown(reason)),
            }
        }
    }

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
        let on_raft = self.engine.on_raft(key);
        if on_raft && let Some(term) = self.engine.raft_term(key) {
            return self.send_on_raft(batch, key, handle, term, deadline).await;
        }
        let ingress = self.engine.serving().map(|ingress| &**ingress);
        let (permit, generation) = match dispatch_write(ingress, key).await {
            (Dispatch::Local { .. }, _) if on_raft => return Sent::Leaderless,
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
            (Dispatch::Unavailable(_), _) if on_raft => return Sent::Leaderless,
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
            let shard = self.engine.serving().map(|_| key);
            let wait = self.wait_within(deadline);
            let quorum = self
                .engine
                .await_commit(handle, shard, &outcome, generation, wait)
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

    /// Propose the batch to the shard's Raft group, which this broker leads
    /// at `term`, as the producer's idempotent batch. Answered at its
    /// commit, whatever the stream's level.
    async fn send_on_raft(
        &self,
        batch: &WriteBatch,
        key: &ShardKey,
        handle: &StreamHandle,
        term: u64,
        deadline: Option<Instant>,
    ) -> Sent {
        match self
            .propose_on_raft(batch, key, handle, term, deadline)
            .await
        {
            Ok(proposed) => committed(proposed.complete().await),
            Err(sent) => sent,
        }
    }

    /// Hand the batch to the shard's Raft group once the producer's
    /// earlier batches are on this broker's log. `Err` when nothing was
    /// handed over.
    async fn propose_on_raft(
        &self,
        batch: &WriteBatch,
        key: &ShardKey,
        handle: &StreamHandle,
        term: u64,
        deadline: Option<Instant>,
    ) -> Result<RaftIdempotentPublish, Sent> {
        let turn = self.broker.producer_turn(handle, batch.producer_id).await;
        let wait = self.wait_within(deadline);
        let proposed = self.engine.publish_idempotent_on_raft(
            &self.broker,
            handle,
            key,
            term,
            turn,
            batch.sequence,
            SequenceReuse::Refuse,
            batch.records.iter().map(|r| r.payload.clone()).collect(),
            Attribution {
                publisher: None,
                headers: Some(
                    batch
                        .records
                        .iter()
                        .map(|r| Some(r.headers.clone()))
                        .collect(),
                ),
            },
            wait,
        );
        match proposed.unwrap_or(Err(RaftRefusal::NotLeading { term })) {
            Ok(proposed) => Ok(proposed),
            // Deposed since the check: nothing was written.
            Err(RaftRefusal::NotLeading { .. }) => Err(Sent::Leaderless),
            Err(RaftRefusal::Broker(err)) => Err(claim_failure(err)),
        }
    }

    /// The quorum wait, cut short so a re-send late in the budget does not
    /// wait past it.
    fn wait_within(&self, deadline: Option<Instant>) -> Duration {
        deadline.map_or(self.quorum_timeout, |deadline| {
            self.quorum_timeout
                .min(deadline.saturating_duration_since(Instant::now()))
        })
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

/// Record `at` of `batch` as a batch of its own, at its own sequence.
fn record_batch(batch: &WriteBatch, at: usize) -> WriteBatch {
    WriteBatch {
        tenant_id: Arc::clone(&batch.tenant_id),
        credential: batch.credential.clone(),
        target: batch.target.clone(),
        producer_id: batch.producer_id,
        sequence: batch.sequence + at as u64,
        records: vec![batch.records[at].clone()],
        acknowledged: batch.acknowledged,
    }
}

/// How a batch proposed to a Raft group ended.
fn committed(result: Result<Option<(u64, u64)>, CommitError>) -> Sent {
    match result {
        Ok(_) => Sent::Written,
        Err(CommitError::TimedOut { .. }) => Sent::Unknown("quorum_timeout"),
        // As on the hybrid: whoever leads now answers a re-send from the
        // log.
        Err(_) => Sent::Unanswered("leadership_lost"),
    }
}

/// A failure before the batch took its offsets. Only the two that are known
/// to leave the log untouched are told to the client; anything else could
/// have been partway, so it is not.
fn claim_failure(err: BrokerError) -> Sent {
    match err {
        BrokerError::RecordHeadersNotEnabled { .. } => Sent::Refused(Refusal::HeadersDisabled),
        BrokerError::StreamHandleInactive(_) => Sent::Refused(Refusal::StreamUnavailable),
        err => {
            tracing::debug!(error = %err, "mqtt write failed while claiming its offsets");
            Sent::Unknown("write_failed")
        }
    }
}

#[cfg(test)]
mod tests;

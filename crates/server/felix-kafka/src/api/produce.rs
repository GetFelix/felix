//! `Produce`: a Kafka producer's batches, written through the broker's own
//! publish path on the shard's leader.
//!
//! Per partition, in this order: the topic must name a durable stream the
//! principal may publish to; the records must decode; the cluster must admit
//! the write (this broker leads the shard, holds its lease, and the shard is
//! not mid-move); then each batch is published. An idempotent producer's
//! batch goes through the log's per-record sequences, so a batch re-sent
//! after a failover or a move is answered as a duplicate by whichever broker
//! leads the shard then.
//!
//! `acks` decides what the answer waits for. `1` answers once the batch is
//! written as the stream's fsync policy says; `-1` (all) also waits for the
//! stream's consistency, which is a majority of the replica set on a `Quorum`
//! stream and nothing more on a `Leader` one. `0` writes and answers nothing.
//!
//! A record's key and headers are stored with it when the listener opts in
//! (`FELIX_KAFKA_RECORD_HEADERS`) and the broker accepts record headers, and
//! dropped (and counted) otherwise, so a fleet that has not finalized them
//! still takes Kafka produces. Producer timestamps are
//! always dropped: a Felix record's time is the broker's append time.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use felix_broker::{BrokerError, IdempotentOutcome, PublishOutcome, StreamHandle};
use futures::future::{BoxFuture, join_all};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;

use super::partition::resolve_write;
use super::producer_id::{felix_producer, producer_epochs, producer_family};
use crate::cluster::{Principal, Produce, ProduceError, ShardRef, WriteError, WritePermit};
use crate::records::decode::{self, Batch, Refused};
use crate::service::Shared;

/// A produce whose batches hold their places in their partitions' order.
pub(super) struct Claimed<'a> {
    /// How long the connection must stay muted to keep the tenant within its
    /// publish quota.
    pub(super) throttle: Duration,
    /// Resolves once every batch has what `acks` asks for, to the body, or
    /// `None` for `acks=0`, which a Kafka broker never answers, and the error
    /// it is counted under.
    pub(super) answer: BoxFuture<'a, Result<(Option<Bytes>, i16)>>,
}

/// Claim a produce's place: every batch is checked and takes its offsets,
/// or is handed to the shard's Raft group, before this returns. The commit
/// waits are left to [`Claimed::answer`], so the connection can claim the
/// next request while they run.
pub(super) async fn claim<'a>(
    shared: &'a Shared,
    principal: Option<&Principal>,
    request: ProduceRequest,
    version: i16,
) -> Claimed<'a> {
    let transactional = request
        .transactional_id
        .as_ref()
        .is_some_and(|id| !id.is_empty());
    let wait_for_consistency = request.acks == -1;
    let mut throttled = Duration::ZERO;
    let mut topics = Vec::with_capacity(request.topic_data.len());
    for topic in request.topic_data {
        let mut partitions = Vec::with_capacity(topic.partition_data.len());
        for partition in topic.partition_data {
            let written = if transactional {
                Err(Failure::from(Refused::Transactional))
            } else {
                write(
                    shared,
                    principal,
                    topic.name.as_str(),
                    partition.index,
                    partition.records.unwrap_or_default(),
                    wait_for_consistency,
                    &mut throttled,
                )
                .await
            };
            let index = partition.index;
            partitions.push(async move {
                let written = match written {
                    Ok(pending) => pending.await,
                    Err(failure) => Err(failure),
                };
                respond(index, written, version)
            });
        }
        topics.push(async move {
            TopicProduceResponse::default()
                .with_name(topic.name)
                .with_partition_responses(join_all(partitions).await)
        });
    }

    let acks = request.acks;
    let answer = Box::pin(async move {
        // Every partition's wait at once: on the hybrid, group commit can
        // only coalesce flushes that are in flight together.
        let topics = join_all(topics).await;
        let error = super::first_error(
            topics
                .iter()
                .flat_map(|topic| topic.partition_responses.iter())
                .map(|partition| partition.error_code),
        );
        if acks == 0 {
            return Ok((None, error));
        }
        let throttle_time_ms = i32::try_from(throttled.as_millis()).unwrap_or(i32::MAX);
        let (body, error) = super::encode(
            &ProduceResponse::default()
                .with_responses(topics)
                .with_throttle_time_ms(throttle_time_ms),
            version,
            error,
        )?;
        Ok((Some(body), error))
    });
    Claimed {
        throttle: throttled,
        answer,
    }
}

/// A claimed partition or batch, resolving once it is durable and has what
/// its stream's consistency asks for.
type Pending<'a, T> = BoxFuture<'a, Result<T, Failure>>;

/// Where a partition's records landed.
struct Written {
    /// `None` when the request carried no records.
    base_offset: Option<u64>,
    log_start_offset: u64,
}

/// Why they did not, with a sentence for clients new enough to show one.
struct Failure {
    error: ResponseError,
    message: Option<String>,
}

impl From<ResponseError> for Failure {
    fn from(error: ResponseError) -> Self {
        Self {
            error,
            message: None,
        }
    }
}

impl From<Refused> for Failure {
    fn from(refused: Refused) -> Self {
        let (error, message) = match refused {
            Refused::LegacyFormat => (
                ResponseError::UnsupportedForMessageFormat,
                "Felix takes v2 record batches only; use a client that writes them".to_string(),
            ),
            Refused::Transactional => (
                super::transactions::REFUSED,
                super::transactions::MESSAGE.to_string(),
            ),
            Refused::TooLarge => (
                ResponseError::MessageTooLarge,
                format!(
                    "a record batch may decompress to at most {} bytes",
                    decode::MAX_BATCH_BYTES
                ),
            ),
            Refused::Invalid(reason) => (ResponseError::InvalidRecord, reason),
            Refused::Corrupt(reason) => (ResponseError::CorruptMessage, reason),
        };
        Self {
            error,
            message: Some(message),
        }
    }
}

impl From<WriteError> for Failure {
    fn from(error: WriteError) -> Self {
        match error {
            // Both send the client to Metadata and the shard's leader, where
            // an idempotent re-send is answered from the log.
            WriteError::NotLeader | WriteError::LeadershipLost => {
                ResponseError::NotLeaderOrFollower.into()
            }
            WriteError::QuorumTimeout => Self {
                error: ResponseError::RequestTimedOut,
                message: Some(
                    "written on the leader, but a majority of replicas did not confirm it in time"
                        .to_string(),
                ),
            },
        }
    }
}

async fn write<'a>(
    shared: &'a Shared,
    principal: Option<&Principal>,
    topic: &str,
    partition: i32,
    records: Bytes,
    wait_for_consistency: bool,
    throttled: &mut Duration,
) -> Result<Pending<'a, Written>, Failure> {
    let located = resolve_write(shared, principal, topic, partition).await?;
    let keep_headers = shared.settings.record_headers && shared.broker.records_headers();
    let batches = decode::decode(records, keep_headers)?;
    // Before the quota is charged: nothing is written.
    if batches.iter().any(|batch| batch.blocks.is_some())
        && let Some(feature) = shared
            .cluster
            .refuses_record_headers(&shared.broker, &located.shard_ref())
    {
        crate::metrics::produce_refused_for("record_headers_not_finalized");
        return Err(Failure {
            error: ResponseError::InvalidRecord,
            message: Some(format!(
                "this partition stores record keys and headers once the cluster finalizes {feature}"
            )),
        });
    }
    // A kept block is charged with its payload; both are bytes the broker stores.
    let (count, bytes) = batches.iter().fold((0u64, 0u64), |(count, bytes), batch| {
        let blocks = batch.blocks.iter().flatten().flatten().map(Bytes::len);
        (
            count + batch.values.len() as u64,
            bytes
                + batch
                    .values
                    .iter()
                    .map(Bytes::len)
                    .chain(blocks)
                    .sum::<usize>() as u64,
        )
    });
    // Each charge reports the tenant's whole debt, so the last one is the
    // longest.
    let hold = shared
        .cluster
        .admit_produce(&located.tenant_id, count, bytes);
    *throttled = (*throttled).max(hold);
    let permit = shared.cluster.admit_write(&located.shard_ref()).await?;
    let handle = located.handle(shared).await?;

    let mut claimed = Vec::with_capacity(batches.len());
    let mut refused = None;
    for batch in &batches {
        match publish(shared, &located.shard_ref(), &handle, &permit, batch).await {
            Ok(pending) => claimed.push(pending),
            Err(failure) => {
                refused = Some(failure);
                break;
            }
        }
    }
    Ok(Box::pin(async move {
        let generation = permit.generation();
        let outcomes = join_all(claimed).await;
        // Held until the records are durable, so a move waits for them.
        drop(permit);
        let mut written: Option<(u64, u64)> = None;
        // The first failure in batch order is the answer: a later batch's
        // refusal can follow from it.
        for outcome in outcomes {
            if let Some((first, last)) = outcome?.offsets {
                written = Some((written.map_or(first, |(start, _)| start), last));
            }
        }
        if let Some(refused) = refused {
            return Err(refused);
        }

        if wait_for_consistency && written.is_some() {
            let outcome = PublishOutcome {
                subscribers: 0,
                offsets: written,
            };
            shared
                .cluster
                .await_consistency(&located.shard_ref(), &handle, &outcome, generation)
                .await?;
        }
        Ok(Written {
            base_offset: written.map(|(first, _)| first),
            log_start_offset: handle.log().map_or(0, |log| log.readable_base()),
        })
    }))
}

/// Claim one batch's place, returning its commit wait.
async fn publish<'a>(
    shared: &'a Shared,
    shard: &ShardRef<'_>,
    handle: &StreamHandle,
    permit: &WritePermit,
    batch: &Batch,
) -> Result<Pending<'a, PublishOutcome>, Failure> {
    // Blocks are built only when the listener opted in and the broker accepts
    // them, and any key or header makes one, so none means these were not kept.
    if batch.blocks.is_none() {
        crate::metrics::dropped(batch.keyed, batch.with_headers);
    }
    let headers = batch.blocks.as_deref();
    // Held until this batch is on the log, so no other epoch of this
    // producer is checked or written between the fence check and it.
    let mut _family = None;
    let producer = if batch.producer_id >= 0 {
        let invalid = |message: &str| Failure {
            error: ResponseError::InvalidRecord,
            message: Some(message.to_string()),
        };
        let Ok(sequence) = u64::try_from(batch.base_sequence) else {
            return Err(invalid("an idempotent batch needs a base sequence"));
        };
        if batch.producer_epoch < 0 {
            return Err(invalid("an idempotent batch needs a producer epoch"));
        }
        let id = felix_producer(batch.producer_id, batch.producer_epoch);
        _family = Some(
            shared
                .broker
                .producer_turn(handle, producer_family(batch.producer_id))
                .await,
        );
        let newest = handle
            .log()
            .and_then(|log| log.producer_highest_known(producer_epochs(batch.producer_id)));
        if newest.is_some_and(|newest| newest > id) {
            return Err(Failure {
                error: ResponseError::InvalidProducerEpoch,
                message: Some(
                    "this partition holds records from a later epoch of this producer".to_string(),
                ),
            });
        }
        Some((id, sequence))
    } else {
        None
    };
    let produce = Produce {
        payloads: &batch.values,
        headers,
        producer,
    };
    let produced = shared
        .cluster
        .produce(&shared.broker, shard, handle, permit, produce)
        .await;
    let pending: Pending<'static, IdempotentOutcome> = if let Some(produced) = produced {
        let proposed = match produced {
            Ok(proposed) => proposed,
            Err(ProduceError::Broker(err)) => return Err(publish_failure(&err)),
            Err(ProduceError::Write(err)) => return Err(err.into()),
        };
        // A Raft group's log learns a record when it writes it, not when it
        // is handed over. The producer's turn comes round once the last one
        // is written here or given up, and only then may another epoch be
        // checked against the log.
        if let Some((id, _)) = producer {
            drop(shared.broker.producer_turn(handle, id).await);
        }
        Box::pin(async move {
            proposed.await.map_err(|err| match err {
                ProduceError::Broker(err) => publish_failure(&err),
                ProduceError::Write(err) => err.into(),
            })
        })
    } else if let Some((producer_id, sequence)) = producer {
        let claim = shared
            .broker
            .claim_records_idempotent(handle, producer_id, sequence, &batch.values, headers)
            .await
            .map_err(|err| publish_failure(&err))?;
        let broker = Arc::clone(&shared.broker);
        let handle = handle.clone();
        finishing(async move {
            broker
                .complete_records_idempotent(&handle, claim)
                .await
                .map_err(|err| publish_failure(&err))
        })
    } else {
        let claimed = shared
            .broker
            .claim_publish(handle, &batch.values, None, headers)
            .await
            .map_err(|err| publish_failure(&err))?;
        let broker = Arc::clone(&shared.broker);
        finishing(async move {
            broker
                .complete_publish(claimed)
                .await
                .map(|outcome| IdempotentOutcome {
                    outcome,
                    duplicate: false,
                })
                .map_err(|err| publish_failure(&err))
        })
    };
    let records = batch.values.len() as u64;
    let bytes = batch.values.iter().map(Bytes::len).sum::<usize>() as u64;
    Ok(Box::pin(async move {
        let IdempotentOutcome { outcome, duplicate } = pending.await?;
        if duplicate {
            crate::metrics::duplicate(records);
        } else {
            crate::metrics::produced(records, bytes);
        }
        Ok(outcome)
    }))
}

/// `completion` as a pending batch that runs to the end even if the
/// connection goes away before awaiting it. A claim's offsets are taken, so
/// its records exist; dropped unfinished, they would never be fanned out.
fn finishing<F>(completion: F) -> Pending<'static, IdempotentOutcome>
where
    F: Future<Output = Result<IdempotentOutcome, Failure>> + Send + 'static,
{
    struct Finishing<F: Future + Send + 'static>(Option<Pin<Box<F>>>);

    impl<F: Future + Send + 'static> Future for Finishing<F> {
        type Output = F::Output;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
            let inner = self.0.as_mut().expect("polled after it finished");
            let done = ready!(inner.as_mut().poll(cx));
            self.0 = None;
            Poll::Ready(done)
        }
    }

    impl<F: Future + Send + 'static> Drop for Finishing<F> {
        fn drop(&mut self) {
            if let Some(inner) = self.0.take()
                && let Ok(runtime) = tokio::runtime::Handle::try_current()
            {
                runtime.spawn(async move {
                    let _ = inner.await;
                });
            }
        }
    }

    Box::pin(Finishing(Some(Box::pin(completion))))
}

fn publish_failure(err: &BrokerError) -> Failure {
    let message = match err {
        BrokerError::SequenceGap { expected } => Some(format!(
            "sequence gap: this partition expected record sequence {expected} next"
        )),
        BrokerError::UnknownProducer { .. } => {
            Some("this partition's log holds no records from this producer id".to_string())
        }
        BrokerError::SequenceExpired { .. } => {
            Some("the batch was already written, too long ago to say where".to_string())
        }
        _ => None,
    };
    Failure {
        error: crate::errors::from_broker(err),
        message,
    }
}

fn respond(
    index: i32,
    written: Result<Written, Failure>,
    version: i16,
) -> PartitionProduceResponse {
    let answer = PartitionProduceResponse::default().with_index(index);
    match written {
        Ok(written) => answer
            .with_base_offset(written.base_offset.map_or(-1, |offset| offset as i64))
            // A record's timestamp is the broker's append time; saying so
            // tells the client the one it sent was not kept.
            .with_log_append_time_ms(now_ms())
            .with_log_start_offset(written.log_start_offset as i64),
        Err(Failure { error, message }) => {
            crate::metrics::produce_refused(error.code());
            let answer = answer
                .with_error_code(error.code())
                .with_base_offset(-1)
                .with_log_append_time_ms(-1)
                .with_log_start_offset(-1);
            // The message field exists from v8.
            match message {
                Some(message) if version >= 8 => {
                    answer.with_error_message(Some(StrBytes::from_string(message)))
                }
                _ => answer,
            }
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(-1, |since| since.as_millis() as i64)
}

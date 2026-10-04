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
//! Keys, headers and producer timestamps are dropped: a Felix record is a
//! payload and the broker's append time. The key has already done its job by
//! then, choosing the partition.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use felix_broker::{BrokerError, IdempotentOutcome, PublishOutcome, StreamHandle};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;

use super::partition::resolve_write;
use crate::cluster::{Principal, WriteError};
use crate::records::decode::{self, Batch, Refused};
use crate::service::Shared;

/// Answer a produce: the body, or `None` for `acks=0`, which a Kafka broker
/// never answers; the error it is counted under; and how long the connection
/// must stay muted to keep the tenant within its publish quota.
pub(super) async fn answer(
    shared: &Shared,
    principal: Option<&Principal>,
    request: ProduceRequest,
    version: i16,
) -> Result<(Option<Bytes>, i16, Duration)> {
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
            partitions.push(respond(partition.index, written, version));
        }
        topics.push(
            TopicProduceResponse::default()
                .with_name(topic.name)
                .with_partition_responses(partitions),
        );
    }

    let error = super::first_error(
        topics
            .iter()
            .flat_map(|topic| topic.partition_responses.iter())
            .map(|partition| partition.error_code),
    );
    if request.acks == 0 {
        return Ok((None, error, throttled));
    }
    let throttle_time_ms = i32::try_from(throttled.as_millis()).unwrap_or(i32::MAX);
    let (body, error) = super::encode(
        &ProduceResponse::default()
            .with_responses(topics)
            .with_throttle_time_ms(throttle_time_ms),
        version,
        error,
    )?;
    Ok((Some(body), error, throttled))
}

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

async fn write(
    shared: &Shared,
    principal: Option<&Principal>,
    topic: &str,
    partition: i32,
    records: Bytes,
    wait_for_consistency: bool,
    throttled: &mut Duration,
) -> Result<Written, Failure> {
    let located = resolve_write(shared, principal, topic, partition).await?;
    let batches = decode::decode(records)?;
    let (count, bytes) = batches.iter().fold((0u64, 0u64), |(count, bytes), batch| {
        (
            count + batch.values.len() as u64,
            bytes + batch.values.iter().map(Bytes::len).sum::<usize>() as u64,
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

    let mut written: Option<(u64, u64)> = None;
    for batch in &batches {
        let outcome = publish(shared, &handle, batch).await?;
        if let Some((first, last)) = outcome.offsets {
            written = Some((written.map_or(first, |(start, _)| start), last));
        }
    }
    // Held until the records are durable, so a move waits for them.
    drop(permit);

    if wait_for_consistency && written.is_some() {
        let outcome = PublishOutcome {
            subscribers: 0,
            offsets: written,
        };
        shared
            .cluster
            .await_consistency(&located.shard_ref(), &handle, &outcome)
            .await?;
    }
    Ok(Written {
        base_offset: written.map(|(first, _)| first),
        log_start_offset: handle.log().map_or(0, |log| log.base_offset()),
    })
}

async fn publish(
    shared: &Shared,
    handle: &StreamHandle,
    batch: &Batch,
) -> Result<PublishOutcome, Failure> {
    crate::metrics::dropped(batch.keyed, batch.with_headers);
    let result = if batch.producer_id >= 0 {
        let Ok(sequence) = u64::try_from(batch.base_sequence) else {
            return Err(Failure {
                error: ResponseError::InvalidRecord,
                message: Some("an idempotent batch needs a base sequence".to_string()),
            });
        };
        shared
            .broker
            .publish_records_idempotent(handle, batch.producer_id as u64, sequence, &batch.values)
            .await
            .map(|IdempotentOutcome { outcome, duplicate }| (outcome, duplicate))
    } else {
        shared
            .broker
            .publish_batch_with_outcome(handle, &batch.values, None)
            .await
            .map(|outcome| (outcome, false))
    };
    match result {
        Ok((outcome, true)) => {
            crate::metrics::duplicate(batch.values.len() as u64);
            Ok(outcome)
        }
        Ok((outcome, false)) => {
            let bytes = batch.values.iter().map(Bytes::len).sum::<usize>();
            crate::metrics::produced(batch.values.len() as u64, bytes as u64);
            Ok(outcome)
        }
        Err(err) => Err(publish_failure(&err)),
    }
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

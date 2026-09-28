//! `Fetch`: records from each partition's offset, long-polling when there are
//! too few.
//!
//! A fetch that finds fewer than `min_bytes` waits up to `max_wait_ms` for
//! more. The wait is on the shards' append notifications, not on a timer, so a
//! record published during the wait is returned as soon as it commits. Each
//! notification is armed before the log is read: a publish that lands between
//! the read and the wait still wakes it.
//!
//! Fetch sessions (KIP-227) are not kept. Every answer carries session id 0,
//! which tells the client to keep sending full fetches.

use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse, TopicName};
use tokio::sync::futures::OwnedNotified;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::partition::Readable;
use crate::cluster::Principal;
use crate::service::Shared;

/// The longest a fetch waits, whatever the client asked for. Clients ask for
/// hundreds of milliseconds; this only stops a hostile value from parking a
/// connection indefinitely.
const MAX_WAIT: Duration = Duration::from_secs(30);

/// One requested partition.
struct Slot {
    topic: usize,
    partition: i32,
    fetch_offset: i64,
    max_bytes: usize,
    target: Result<Readable, ResponseError>,
}

/// One pass over every partition.
struct Pass {
    partitions: Vec<PartitionData>,
    bytes: usize,
    records: u64,
    errored: bool,
}

pub(super) async fn answer(
    shared: &Shared,
    principal: Option<&Principal>,
    request: FetchRequest,
    version: i16,
    shutdown: &CancellationToken,
) -> Result<(Bytes, i16)> {
    if request.session_id != 0 {
        // A session this broker never created: the client falls back to a
        // full fetch.
        let response =
            FetchResponse::default().with_error_code(ResponseError::FetchSessionIdNotFound.code());
        return super::encode(&response, version, response.error_code);
    }

    let names: Vec<TopicName> = request.topics.iter().map(|t| t.topic.clone()).collect();
    let mut slots = Vec::new();
    for (topic, requested) in request.topics.iter().enumerate() {
        for partition in &requested.partitions {
            slots.push(Slot {
                topic,
                partition: partition.partition,
                fetch_offset: partition.fetch_offset,
                max_bytes: partition.partition_max_bytes.max(0) as usize,
                target: super::partition::resolve(
                    shared,
                    principal,
                    requested.topic.as_str(),
                    partition.partition,
                )
                .await,
            });
        }
    }

    let min_bytes = request.min_bytes.max(0) as usize;
    let max_bytes = usize::try_from(request.max_bytes)
        .ok()
        .filter(|max| *max > 0)
        .unwrap_or(usize::MAX);
    let started = Instant::now();
    let deadline = started + Duration::from_millis(request.max_wait_ms.max(0) as u64).min(MAX_WAIT);
    let mut waited = false;
    let pass = loop {
        let mut woken: Vec<Pin<Box<OwnedNotified>>> = slots
            .iter()
            .filter_map(|slot| slot.target.as_ref().ok())
            .map(|readable| Box::pin(readable.appended.clone().notified_owned()))
            .collect();
        for notified in &mut woken {
            notified.as_mut().enable();
        }

        let pass = read(&slots, max_bytes).await;
        if pass.bytes >= min_bytes
            || pass.errored
            || woken.is_empty()
            || Instant::now() >= deadline
            || shutdown.is_cancelled()
        {
            break pass;
        }
        waited = true;
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = tokio::time::sleep_until(deadline) => {}
            _ = futures::future::select_all(woken) => {}
        }
    };

    if waited {
        let outcome = if pass.bytes > 0 { "data" } else { "timeout" };
        crate::metrics::long_poll(outcome, started.elapsed());
    }
    crate::metrics::fetched(pass.records, pass.bytes as u64);

    let error = super::first_error(pass.partitions.iter().map(|p| p.error_code));
    let mut topics: Vec<FetchableTopicResponse> = names
        .into_iter()
        .map(|name| FetchableTopicResponse::default().with_topic(name))
        .collect();
    for (slot, partition) in slots.iter().zip(pass.partitions) {
        topics[slot.topic].partitions.push(partition);
    }
    let response = FetchResponse::default().with_responses(topics);
    super::encode(&response, version, error)
}

/// Read every partition once, within the request's byte limits.
///
/// The first partition with records returns at least one even past the
/// limits, as Kafka does, so a record larger than `max_bytes` cannot stall a
/// consumer forever.
async fn read(slots: &[Slot], max_bytes: usize) -> Pass {
    let mut pass = Pass {
        partitions: Vec::with_capacity(slots.len()),
        bytes: 0,
        records: 0,
        errored: false,
    };
    for slot in slots {
        let answer = PartitionData::default().with_partition_index(slot.partition);
        let answer = match &slot.target {
            Err(error) => answer.with_error_code(error.code()).with_high_watermark(-1),
            Ok(readable) => {
                let remaining = max_bytes.saturating_sub(pass.bytes);
                let budget = slot.max_bytes.min(remaining);
                let budget = if pass.bytes == 0 {
                    budget.max(1)
                } else {
                    budget
                };
                read_partition(readable, slot.fetch_offset, budget, &mut pass, answer).await
            }
        };
        pass.errored |= answer.error_code != 0;
        pass.partitions.push(answer);
    }
    pass
}

async fn read_partition(
    readable: &Readable,
    fetch_offset: i64,
    budget: usize,
    pass: &mut Pass,
    answer: PartitionData,
) -> PartitionData {
    let log = &readable.log;
    let base = log.base_offset();
    let tail = match log.tail_offset().await {
        Ok(tail) => tail,
        Err(err) => return answer.with_error_code(crate::errors::from_broker(&err).code()),
    };
    // Consumers read to the high watermark, which on a `Quorum` shard is the
    // commit point rather than the log's end. An offset between the two is
    // not out of range, only not readable yet, so it is answered empty.
    let high_watermark = readable.high_watermark(tail);
    let answer = answer
        .with_high_watermark(high_watermark as i64)
        .with_last_stable_offset(high_watermark as i64)
        .with_log_start_offset(base as i64);
    let Ok(offset) = u64::try_from(fetch_offset) else {
        return answer.with_error_code(ResponseError::OffsetOutOfRange.code());
    };
    if offset < base || offset > tail {
        return answer.with_error_code(ResponseError::OffsetOutOfRange.code());
    }
    if offset >= high_watermark || budget == 0 {
        return answer;
    }
    // The raw log, generation-start records included: `encode_page` needs to
    // see a trailing run of them to move the consumer past it.
    let mut records = match log.read_log_from(offset, budget).await {
        Ok(records) => records,
        Err(err) => return answer.with_error_code(crate::errors::from_broker(&err).code()),
    };
    records.retain(|record| record.offset < high_watermark);
    match crate::records::encode_page(&records) {
        Ok((batch, visible)) => {
            pass.bytes += batch.len();
            pass.records += visible as u64;
            answer.with_records(Some(batch))
        }
        Err(err) => {
            tracing::warn!(error = %err, "could not encode a kafka record batch");
            answer.with_error_code(ResponseError::UnknownServerError.code())
        }
    }
}

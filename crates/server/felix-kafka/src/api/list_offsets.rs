//! `ListOffsets`: earliest, latest, latest record, and offset for a time.

use anyhow::Result;
use bytes::Bytes;
use felix_broker::StreamLog;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::ListOffsetsRequest;
use kafka_protocol::messages::ListOffsetsResponse;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};

use crate::cluster::Principal;
use crate::service::Shared;

/// The special timestamps a request may ask for instead of a time.
const LATEST: i64 = -1;
const EARLIEST: i64 = -2;
const MAX_TIMESTAMP: i64 = -3;

pub(super) async fn answer(
    shared: &Shared,
    principal: Option<&Principal>,
    request: ListOffsetsRequest,
    version: i16,
) -> Result<(Bytes, i16)> {
    let mut codes = Vec::new();
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let mut answer = ListOffsetsPartitionResponse::default()
                .with_partition_index(partition.partition_index);
            let found = match super::partition::resolve(
                shared,
                principal,
                topic.name.as_str(),
                partition.partition_index,
            )
            .await
            {
                Ok(readable) => offset_for(&readable, partition.timestamp).await,
                Err(error) => Err(error),
            };
            match found {
                Ok((timestamp, offset)) => {
                    answer.timestamp = timestamp;
                    answer.offset = offset;
                }
                Err(error) => answer.error_code = error.code(),
            }
            codes.push(answer.error_code);
            partitions.push(answer);
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic.name)
                .with_partitions(partitions),
        );
    }
    let response = ListOffsetsResponse::default().with_topics(topics);
    super::encode(&response, version, super::first_error(codes))
}

/// `(timestamp, offset)` for one partition. The timestamp is -1 except where
/// a record's own time is the answer.
async fn offset_for(
    readable: &super::partition::Readable,
    timestamp: i64,
) -> Result<(i64, i64), ResponseError> {
    let log = &readable.log;
    let storage = |err: felix_broker::BrokerError| crate::errors::from_broker(&err);
    let base = log.base_offset();
    // The high watermark, not the log end: an offset past the commit point is
    // not one a consumer may start from.
    let tail = readable.high_watermark(log.tail_offset().await.map_err(storage)?);
    match timestamp {
        LATEST => Ok((-1, tail as i64)),
        EARLIEST => Ok((-1, base as i64)),
        MAX_TIMESTAMP => {
            if tail == base {
                return Ok((-1, -1));
            }
            let last = record_at(log, tail - 1).await.map_err(storage)?;
            Ok(last.map_or((-1, -1), |(time, offset)| (time, offset as i64)))
        }
        at if at >= 0 => {
            // The first record at or after `at`. Timestamps are append times,
            // so they rise with the offset and a binary search finds it.
            let (mut low, mut high) = (base, tail);
            while low < high {
                let mid = low + (high - low) / 2;
                match record_at(log, mid).await.map_err(storage)? {
                    Some((time, _)) if time < at => low = mid + 1,
                    _ => high = mid,
                }
            }
            if low == tail {
                // Nothing that recent: Kafka answers "no offset".
                return Ok((-1, -1));
            }
            let found = record_at(log, low).await.map_err(storage)?;
            Ok(found.map_or((-1, -1), |(time, offset)| (time, offset as i64)))
        }
        _ => Err(ResponseError::InvalidRequest),
    }
}

/// The record at or just after `offset`: its time and its offset.
async fn record_at(log: &StreamLog, offset: u64) -> felix_broker::Result<Option<(i64, u64)>> {
    // One byte asks for a single record; the log returns the first whatever
    // its size.
    let records = log.read_from(offset, 1).await?;
    Ok(records
        .first()
        .map(|record| (crate::records::timestamp_ms(record), record.offset)))
}

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

use crate::cluster::{Principal, Unreadable};
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
                Ok(readable) => offset_for(&readable, partition.timestamp, version).await,
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
    version: i16,
) -> Result<(i64, i64), ResponseError> {
    let log = &readable.log;
    let storage = |err: felix_broker::BrokerError| crate::errors::from_broker(&err);
    let base = log.readable_base();
    // The high watermark, not the log end: an offset past the commit point is
    // not one a consumer may start from.
    let end = log.tail_offset().await.map_err(storage)?;
    let tail = match readable.high_watermark(end) {
        Ok(tail) => tail,
        Err(Unreadable::NotServed) => return Err(ResponseError::NotLeaderOrFollower),
        // A new leader with no mark yet has no `latest` to give, but the
        // other queries are answered with the log start as their bound.
        Err(Unreadable::Settling) if timestamp == LATEST => return Err(settling(version)),
        Err(Unreadable::Settling) => readable.floor(end),
    };
    match timestamp {
        LATEST => Ok((-1, tail as i64)),
        EARLIEST => Ok((-1, base as i64)),
        MAX_TIMESTAMP => {
            if tail == base {
                return Ok((-1, -1));
            }
            let last = last_record_before(log, base, tail).await.map_err(storage)?;
            Ok(last.map_or((-1, -1), |(time, offset)| (time, offset as i64)))
        }
        at if at >= 0 => {
            // Kafka asks in milliseconds; the first record at or after `at`
            // ms is the first at or after `at * 1000` µs.
            let found = log
                .offset_for_time((at as u64).saturating_mul(1_000), tail)
                .await
                .map_err(storage)?;
            // Nothing that recent: Kafka answers "no offset".
            Ok(found.map_or((-1, -1), |(offset, micros)| {
                ((micros / 1_000) as i64, offset as i64)
            }))
        }
        _ => Err(ResponseError::InvalidRequest),
    }
}

/// The refusal for `latest` while a new leader has no mark, as Kafka answers
/// it while a new leader's high watermark catches up: `OFFSET_NOT_AVAILABLE`
/// from v5, which older clients do not know, and `LEADER_NOT_AVAILABLE`
/// before it. Both are retriable.
fn settling(version: i16) -> ResponseError {
    if version >= 5 {
        ResponseError::OffsetNotAvailable
    } else {
        ResponseError::LeaderNotAvailable
    }
}

/// The last client record below `tail`, stepping back over any records
/// that are not data at the end of the log.
async fn last_record_before(
    log: &StreamLog,
    base: u64,
    tail: u64,
) -> felix_broker::Result<Option<(i64, u64)>> {
    let mut at = tail;
    while at > base {
        at -= 1;
        // Every record, so a trim record is seen at its offset rather than
        // read past. `at` is at or above the readable base.
        let records = log.read_replicated_from(at, 1).await?;
        match records.first() {
            Some(record) if record.offset == at && record.mark.is_data() => {
                return Ok(Some((crate::records::timestamp_ms(record), record.offset)));
            }
            Some(record) if record.offset == at => {}
            _ => return Ok(None),
        }
    }
    Ok(None)
}

//! Felix log records as a Kafka v2 record batch, and a producer's batches as
//! Felix payloads (`decode`).
//!
//! Offsets are not translated: a Felix shard's offsets start at zero, which is
//! Kafka's model already. They are contiguous in the log but not in what a
//! consumer sees, since a generation-start record holds an offset and is never
//! returned; Kafka allows that gap, as it does for its own control records.
//! Timestamps are the broker's append time in milliseconds. Felix records have
//! no key and no headers.

pub(crate) mod decode;

use bytes::{Bytes, BytesMut};
use felix_storage::log::LogRecord;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};

/// Encode a page read straight from the log: its client records as one
/// batch, then an empty batch over any generation-start records it ends with.
///
/// A gap before or between client records needs nothing, since each record
/// carries its own offset delta. A trailing run does: a consumer would keep
/// fetching from it and get nothing back, sitting one behind the high
/// watermark until someone publishes. An empty batch is how Kafka's own
/// compacted logs move a consumer past offsets that hold nothing.
pub(crate) fn encode_page(records: &[LogRecord]) -> anyhow::Result<(Bytes, usize)> {
    let visible: Vec<LogRecord> = records
        .iter()
        .filter(|record| !record.mark.is_generation_start())
        .cloned()
        .collect();
    let trailing = records
        .iter()
        .rev()
        .take_while(|record| record.mark.is_generation_start())
        .count();
    let mut out = BytesMut::from(encode_batch(&visible)?.as_ref());
    if trailing > 0 {
        let run = &records[records.len() - trailing..];
        encode_empty_batch(
            &mut out,
            run[0].offset,
            run[trailing - 1].offset,
            timestamp_ms(&run[0]),
        );
    }
    Ok((out.freeze(), visible.len()))
}

/// A v2 record batch holding no records that covers `first..=last`.
fn encode_empty_batch(out: &mut BytesMut, first: u64, last: u64, timestamp: i64) {
    // Everything after the CRC field, which the CRC-32C covers.
    let mut body = Vec::with_capacity(40);
    body.extend_from_slice(&0i16.to_be_bytes()); // attributes
    body.extend_from_slice(&((last - first) as i32).to_be_bytes()); // last offset delta
    body.extend_from_slice(&timestamp.to_be_bytes()); // first timestamp
    body.extend_from_slice(&timestamp.to_be_bytes()); // max timestamp
    body.extend_from_slice(&NO_PRODUCER_ID.to_be_bytes());
    body.extend_from_slice(&NO_PRODUCER_EPOCH.to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes()); // base sequence
    body.extend_from_slice(&0i32.to_be_bytes()); // record count
    let batch_len = 4 + 1 + 4 + body.len();
    out.extend_from_slice(&(first as i64).to_be_bytes());
    out.extend_from_slice(&(batch_len as i32).to_be_bytes());
    out.extend_from_slice(&NO_PARTITION_LEADER_EPOCH.to_be_bytes());
    out.extend_from_slice(&[2u8]); // magic
    out.extend_from_slice(&crc32c::crc32c(&body).to_be_bytes());
    out.extend_from_slice(&body);
}

/// Encode `records` as one uncompressed batch.
///
/// The encoder starts a new batch wherever `offset - sequence` changes, so
/// each record's sequence is set to keep that constant. With no producer id
/// the batch's base sequence comes out as -1, which is what a client expects
/// from a non-idempotent batch.
pub(crate) fn encode_batch(records: &[LogRecord]) -> anyhow::Result<Bytes> {
    let Some(first) = records.first() else {
        return Ok(Bytes::new());
    };
    let first_offset = first.offset;
    let converted: Vec<Record> = records
        .iter()
        .map(|record| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: record.offset as i64,
            sequence: (record.offset - first_offset) as i32 - 1,
            timestamp: timestamp_ms(record),
            key: None,
            value: Some(record.payload.clone()),
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &converted,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(buf.freeze())
}

/// A record's timestamp as Kafka carries it: milliseconds since the epoch.
pub(crate) fn timestamp_ms(record: &LogRecord) -> i64 {
    (record.timestamp_micros / 1_000) as i64
}

#[cfg(test)]
mod tests;

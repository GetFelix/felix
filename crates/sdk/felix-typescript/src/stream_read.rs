//! Reading a stream without subscribing, and batch publish reports.

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::types::or_null;

/// One committed record from `read`.
#[napi(object)]
pub struct StreamRecord {
    pub offset: BigInt,
    pub payload: Buffer,
    /// The principal that published it, when the broker recorded one.
    pub publisher: Either<String, Null>,
    /// When the record was appended, in microseconds since the Unix epoch.
    pub timestamp_micros: BigInt,
}

/// One page of a shard's committed records.
#[napi(object)]
pub struct StreamPage {
    pub records: Vec<StreamRecord>,
    /// Where the next page starts. Offsets that hold no record are passed
    /// over, so this is not always the last offset plus one.
    pub next_offset: BigInt,
}

impl From<felix_client::StreamPage> for StreamPage {
    fn from(page: felix_client::StreamPage) -> Self {
        Self {
            records: page
                .records
                .into_iter()
                .map(|r| StreamRecord {
                    offset: BigInt::from(r.offset),
                    payload: r.payload.to_vec().into(),
                    publisher: or_null(r.publisher),
                    timestamp_micros: BigInt::from(r.timestamp_micros),
                })
                .collect(),
            next_offset: BigInt::from(page.next_offset),
        }
    }
}

/// A record for `publishRecords`: the routing key it hashes by and its payload.
#[napi(object)]
pub struct KeyedRecord {
    pub key: Buffer,
    pub payload: Buffer,
}

/// How one shard's piece of a `publishRecords` call fared.
#[napi(object)]
pub struct ShardPublish {
    pub shard: u32,
    /// Positions, in the order passed, of the records in this piece.
    pub records: Vec<u32>,
    /// The log offset of the piece's first record, when the broker reported
    /// one.
    pub offset: Either<BigInt, Null>,
    /// Why the piece was not accepted, as a typed `FelixError`. A piece is all
    /// or nothing.
    pub error: Either<String, Null>,
}

/// What `publishRecords` did, shard by shard. Every record is in exactly one
/// entry.
#[napi(object)]
pub struct BatchReport {
    pub shards: Vec<ShardPublish>,
    /// Whether every piece was accepted.
    pub ok: bool,
    /// Positions of the records in failed pieces, ascending.
    pub failed_records: Vec<u32>,
}

impl BatchReport {
    pub(crate) fn from_report(report: felix_client::BatchReport) -> Self {
        let ok = report.is_ok();
        let failed_records = report
            .failed_records()
            .into_iter()
            .map(|position| position as u32)
            .collect();
        let shards = report
            .shards
            .into_iter()
            .map(|piece| {
                let (offset, error) = match piece.outcome {
                    Ok(offset) => (or_null(offset.map(BigInt::from)), Either::B(Null)),
                    Err(err) => (Either::B(Null), Either::A(crate::errors::encode(&err))),
                };
                ShardPublish {
                    shard: piece.shard,
                    records: piece.records.into_iter().map(|p| p as u32).collect(),
                    offset,
                    error,
                }
            })
            .collect();
        Self {
            shards,
            ok,
            failed_records,
        }
    }
}

/// One broker's connection count, from `connectionsPerNode`.
#[napi(object)]
pub struct ConnectionCount {
    pub addr: String,
    pub connections: u32,
}

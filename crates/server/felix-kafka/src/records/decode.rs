//! A producer's record batches, as Felix payloads.
//!
//! Only v2 record batches are taken. The v0 and v1 message sets predate
//! idempotence and every client this listener supports writes v2, so a
//! legacy set is refused rather than half-supported.
//!
//! A record's key and headers become its Felix block when the caller keeps
//! them (see `felix_wire::RecordHeaders`); a record with neither has no block.
//! A null header value becomes an empty one, since Felix values are not
//! nullable. Kafka's decoder keeps one value per header name, the last.
//!
//! Decompression is bounded. A batch arrives inside a request of at most
//! 8 MiB, but a few megabytes of zstd can expand to far more memory than a
//! broker has; each batch stops inflating at [`MAX_BATCH_BYTES`].

use std::io::Read;

use anyhow::Context;
use bytes::{Buf, Bytes};
use felix_wire::{RESERVED_HEADER_PREFIX, RecordHeaders};
use kafka_protocol::records::{BatchDecodeInfo, Compression, Record, RecordBatchDecoder};

/// The most a batch may decompress to.
pub(crate) const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// One producer batch, decoded.
#[derive(Debug)]
pub(crate) struct Batch {
    /// -1 for a producer that is not idempotent.
    pub(crate) producer_id: i64,
    pub(crate) base_sequence: i32,
    /// The record values. A null value is stored as an empty payload.
    pub(crate) values: Vec<Bytes>,
    /// Each record's key and headers block, one per value; `None` when the
    /// caller did not keep them or no record has a key or headers.
    pub(crate) blocks: Option<Vec<Option<Bytes>>>,
    /// Records that carried a key, and records that carried headers. When the
    /// blocks are not kept, these say how many were dropped.
    pub(crate) keyed: u64,
    pub(crate) with_headers: u64,
}

/// Why a partition's records were not taken.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    /// A v0 or v1 message set.
    LegacyFormat,
    /// Part of a transaction, or a transaction marker.
    Transactional,
    /// Decompressed past [`MAX_BATCH_BYTES`].
    TooLarge,
    /// A record Kafka would call invalid: a header name Felix reserves or
    /// cannot store.
    Invalid(String),
    /// Truncated, a bad CRC, an unknown codec, or records that do not parse.
    Corrupt(String),
}

/// Decode every batch in a partition's `records`. With `keep_headers`, each
/// record's key and headers are built into blocks and checked; without, they
/// are only counted.
pub(crate) fn decode(records: Bytes, keep_headers: bool) -> Result<Vec<Batch>, Refused> {
    let mut headers_view = records.clone();
    let infos = RecordBatchDecoder::decode_batch_info(&mut headers_view)
        .map_err(|err| Refused::Corrupt(format!("{err:#}")))?;
    // The header pass stops at the first batch that is not v2.
    if headers_view.has_remaining() {
        return Err(match magic(&headers_view) {
            Some(0 | 1) => Refused::LegacyFormat,
            _ => Refused::Corrupt("a record batch is not in the v2 format".to_string()),
        });
    }

    let mut rest = records;
    let mut batches = Vec::with_capacity(infos.len());
    for info in infos {
        if info.transactional || info.control {
            return Err(Refused::Transactional);
        }
        let set = RecordBatchDecoder::decode_with_custom_compression(
            &mut rest,
            Some(|data: &mut Bytes, compression| inflate(data, compression)),
        )
        .map_err(|err| {
            if err.downcast_ref::<TooLarge>().is_some() {
                Refused::TooLarge
            } else {
                Refused::Corrupt(format!("{err:#}"))
            }
        })?;
        batches.push(batch(&info, set.records, keep_headers)?);
    }
    Ok(batches)
}

fn batch(
    info: &BatchDecodeInfo,
    records: Vec<Record>,
    keep_headers: bool,
) -> Result<Batch, Refused> {
    let mut keyed = 0;
    let mut with_headers = 0;
    let mut values = Vec::with_capacity(records.len());
    let mut blocks = Vec::with_capacity(if keep_headers { records.len() } else { 0 });
    for record in records {
        keyed += u64::from(record.key.is_some());
        with_headers += u64::from(!record.headers.is_empty());
        if keep_headers {
            blocks.push(block(&record)?);
        }
        values.push(record.value.unwrap_or_default());
    }
    Ok(Batch {
        producer_id: info.producer_id,
        base_sequence: info.base_sequence,
        values,
        blocks: Some(blocks).filter(|blocks| blocks.iter().any(Option::is_some)),
        keyed,
        with_headers,
    })
}

/// The record's key and headers as a block, or `None` when it has neither.
fn block(record: &Record) -> Result<Option<Bytes>, Refused> {
    let mut headers = Vec::with_capacity(record.headers.len());
    for (name, value) in &record.headers {
        if name.starts_with(RESERVED_HEADER_PREFIX) {
            return Err(Refused::Invalid(format!(
                "header names starting with \"{RESERVED_HEADER_PREFIX}\" are reserved for Felix"
            )));
        }
        headers.push((name.to_string(), value.clone().unwrap_or_default()));
    }
    let block = RecordHeaders {
        key: record.key.clone(),
        headers,
    };
    if block.is_empty() {
        return Ok(None);
    }
    block.encode().map(Some).map_err(|err| {
        Refused::Invalid(format!("a record's key or headers cannot be stored: {err}"))
    })
}

/// The magic byte of the batch at the front of `buf`.
fn magic(buf: &Bytes) -> Option<i8> {
    buf.get(16).map(|byte| *byte as i8)
}

#[derive(Debug)]
struct TooLarge;

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a record batch decompresses to more than {MAX_BATCH_BYTES} bytes"
        )
    }
}

impl std::error::Error for TooLarge {}

fn inflate(data: &mut Bytes, compression: Compression) -> anyhow::Result<Bytes> {
    let data = data.split_to(data.len());
    match compression {
        Compression::None => Ok(data),
        Compression::Gzip => read_bounded(flate2::read::GzDecoder::new(data.reader())),
        Compression::Snappy => snappy(&data),
        Compression::Lz4 => read_bounded(lz4::Decoder::new(data.reader()).context("lz4 frame")?),
        Compression::Zstd => {
            read_bounded(zstd::stream::read::Decoder::new(data.reader()).context("zstd frame")?)
        }
    }
}

fn read_bounded(reader: impl Read) -> anyhow::Result<Bytes> {
    let mut out = Vec::new();
    reader
        .take(MAX_BATCH_BYTES as u64 + 1)
        .read_to_end(&mut out)
        .context("decompress record batch")?;
    if out.len() > MAX_BATCH_BYTES {
        return Err(TooLarge.into());
    }
    Ok(out.into())
}

/// The Java client frames snappy the way snappy-java does (a magic header,
/// then length-prefixed blocks); librdkafka sends raw snappy. Both are taken.
fn snappy(data: &[u8]) -> anyhow::Result<Bytes> {
    const XERIAL: &[u8; 8] = b"\x82SNAPPY\x00";
    const XERIAL_HEADER: usize = 16;
    let mut blocks: Vec<&[u8]> = Vec::new();
    if data.starts_with(XERIAL) {
        let mut rest = data.get(XERIAL_HEADER..).context("snappy header")?;
        while !rest.is_empty() {
            let (len, tail) = rest
                .split_first_chunk::<4>()
                .context("snappy block length")?;
            let len = u32::from_be_bytes(*len) as usize;
            let block = tail.get(..len).context("snappy block")?;
            blocks.push(block);
            rest = &tail[len..];
        }
    } else {
        blocks.push(data);
    }
    let mut total = 0usize;
    for block in &blocks {
        total += snap::raw::decompress_len(block).context("snappy length")?;
        if total > MAX_BATCH_BYTES {
            return Err(TooLarge.into());
        }
    }
    let mut out = vec![0u8; total];
    let mut at = 0;
    for block in blocks {
        at += snap::raw::Decoder::new()
            .decompress(block, &mut out[at..])
            .context("snappy block")?;
    }
    out.truncate(at);
    Ok(out.into())
}

#[cfg(test)]
mod tests;

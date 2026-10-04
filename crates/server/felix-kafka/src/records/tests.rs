use bytes::Bytes;
use felix_storage::log::{LogRecord, RecordMark};
use kafka_protocol::records::RecordBatchDecoder;

use super::encode_batch;

fn record(offset: u64, value: &'static str) -> LogRecord {
    LogRecord {
        offset,
        timestamp_micros: 1_700_000_000_000_000 + offset * 1_000,
        checksum: 0,
        payload: Bytes::from_static(value.as_bytes()),
        mark: RecordMark::None,
        publisher: None,
    }
}

#[test]
fn records_encode_as_one_batch_a_client_can_decode() {
    let encoded =
        encode_batch(&[record(5, "a"), record(6, "bb"), record(7, "ccc")]).expect("encode");

    let batches = RecordBatchDecoder::decode_batch_info(&mut encoded.clone()).expect("info");
    assert_eq!(batches.len(), 1, "one batch, not one per record");
    assert_eq!(batches[0].record_count, 3);
    assert_eq!(batches[0].min_offset, 5);
    assert_eq!(batches[0].base_sequence, -1);
    assert_eq!(batches[0].producer_id, -1);

    // The decoder checks the CRC-32C, so this also proves the checksum.
    let decoded = RecordBatchDecoder::decode(&mut encoded.clone()).expect("decode");
    let seen: Vec<(i64, i64, Option<Bytes>)> = decoded
        .records
        .into_iter()
        .map(|record| (record.offset, record.timestamp, record.value))
        .collect();
    assert_eq!(
        seen,
        vec![
            (5, 1_700_000_000_005, Some(Bytes::from_static(b"a"))),
            (6, 1_700_000_000_006, Some(Bytes::from_static(b"bb"))),
            (7, 1_700_000_000_007, Some(Bytes::from_static(b"ccc"))),
        ]
    );
}

#[test]
fn no_records_encode_as_nothing() {
    assert!(encode_batch(&[]).expect("encode").is_empty());
}

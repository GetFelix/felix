use bytes::Bytes;
use felix_storage::log::{LogRecord, RecordMark};
use kafka_protocol::records::RecordBatchDecoder;

use super::{encode_batch, encode_page};

fn record(offset: u64, value: &'static str) -> LogRecord {
    LogRecord {
        generation: None,
        offset,
        timestamp_micros: 1_700_000_000_000_000 + offset * 1_000,
        checksum: 0,
        payload: Bytes::from_static(value.as_bytes()),
        mark: RecordMark::None,
        publisher: None,
        headers: None,
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

#[test]
fn a_records_key_and_headers_come_back_in_order_and_a_bare_record_has_none() {
    let mut keyed = record(0, "a");
    keyed.headers = Some(Box::new(
        felix_wire::RecordHeaders {
            key: Some(Bytes::from_static(b"k")),
            headers: vec![
                ("trace".to_string(), Bytes::from_static(b"1")),
                ("empty".to_string(), Bytes::new()),
            ],
        }
        .encode()
        .expect("block"),
    ));
    let encoded = encode_batch(&[keyed, record(1, "b")]).expect("encode");

    let decoded = RecordBatchDecoder::decode(&mut encoded.clone()).expect("decode");
    let [first, second] = decoded.records.as_slice() else {
        panic!("two records");
    };
    assert_eq!(first.key, Some(Bytes::from_static(b"k")));
    let headers: Vec<_> = first.headers.iter().collect();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].0.as_str(), "trace");
    assert_eq!(headers[0].1, &Some(Bytes::from_static(b"1")));
    assert_eq!(headers[1].0.as_str(), "empty");
    assert_eq!(headers[1].1, &Some(Bytes::new()));
    assert_eq!((second.key.clone(), second.headers.len()), (None, 0));
}

#[test]
fn a_block_that_does_not_parse_fails_the_page_rather_than_the_record_being_sent_bare() {
    let mut broken = record(0, "a");
    broken.headers = Some(Box::new(Bytes::from_static(&[0xff])));
    assert!(encode_batch(&[broken]).is_err());
}

/// A page never encodes a membership or counter record. The client records
/// keep their offsets, and the trailing run of other kinds becomes an empty
/// batch, so the consumer fetches next from the tail.
#[test]
fn a_page_leaves_out_membership_and_counter_records() {
    let marked = |offset, mark| LogRecord {
        mark,
        ..record(offset, "not an event")
    };
    let page = [
        record(0, "a"),
        marked(1, RecordMark::Membership),
        marked(2, RecordMark::CounterDelta),
        record(3, "b"),
        marked(4, RecordMark::CounterCheckpoint),
    ];
    let (encoded, visible) = encode_page(&page, 0, 5).expect("encode");
    assert_eq!(visible, 2);
    let batches = RecordBatchDecoder::decode_batch_info(&mut encoded.clone()).expect("info");
    assert_eq!(batches.len(), 2);
    assert_eq!((batches[1].min_offset, batches[1].record_count), (4, 0));
    let decoded = RecordBatchDecoder::decode(&mut encoded.clone()).expect("decode");
    let seen: Vec<(i64, Option<Bytes>)> = decoded
        .records
        .into_iter()
        .map(|record| (record.offset, record.value))
        .collect();
    assert_eq!(
        seen,
        vec![
            (0, Some(Bytes::from_static(b"a"))),
            (3, Some(Bytes::from_static(b"b"))),
        ]
    );
}

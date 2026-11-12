use bytes::Bytes;

use super::*;
use crate::log::RecordMark;

fn record(offset: Offset, len: usize) -> LogRecord {
    LogRecord {
        offset,
        timestamp_micros: 0,
        checksum: 0,
        payload: Bytes::from(vec![b'x'; len]),
        mark: RecordMark::None,
        publisher: None,
        headers: None,
        generation: Some(1),
    }
}

fn offsets(records: Option<Vec<LogRecord>>) -> Option<Vec<Offset>> {
    records.map(|records| records.iter().map(|record| record.offset).collect())
}

#[test]
fn a_range_is_served_only_from_its_start() {
    let mut cache = TailCache::default();
    cache.extend(7, (10..15).map(|offset| record(offset, 8)));

    assert_eq!(
        offsets(cache.range(7, 10, 15)),
        Some(vec![10, 11, 12, 13, 14])
    );
    assert_eq!(offsets(cache.range(7, 12, 14)), Some(vec![12, 13]));
    assert_eq!(offsets(cache.range(7, 13, 99)), Some(vec![13, 14]));
    assert_eq!(offsets(cache.range(7, 9, 15)), None, "starts below");
    assert_eq!(offsets(cache.range(7, 15, 16)), None, "starts past");
}

#[test]
fn a_cut_log_is_served_nothing() {
    let mut cache = TailCache::default();
    cache.extend(7, (10..15).map(|offset| record(offset, 8)));
    assert_eq!(offsets(cache.range(8, 10, 15)), None);

    // The next append is after the cut, and what was held goes.
    cache.extend(8, [record(12, 8)]);
    assert_eq!(offsets(cache.range(8, 10, 15)), None);
    assert_eq!(offsets(cache.range(8, 12, 15)), Some(vec![12]));
}

#[test]
fn records_that_do_not_follow_replace_what_is_held() {
    let mut cache = TailCache::default();
    cache.extend(7, (10..15).map(|offset| record(offset, 8)));
    cache.extend(7, [record(20, 8), record(21, 8)]);
    assert_eq!(offsets(cache.range(7, 10, 30)), None);
    assert_eq!(offsets(cache.range(7, 20, 30)), Some(vec![20, 21]));
}

#[test]
fn the_oldest_records_go_first_at_capacity() {
    let mut cache = TailCache::default();
    let each = size_of_record(&record(0, 100));
    let fit = CAPACITY_BYTES / each;
    cache.extend(7, (0..fit + 3).map(|offset| record(offset, 100)));

    assert_eq!(offsets(cache.range(7, 2, 4)), None);
    assert_eq!(offsets(cache.range(7, 3, 5)), Some(vec![3, 4]));
    assert!(cache.bytes <= CAPACITY_BYTES);

    // One record over the capacity is not kept.
    cache.extend(7, [record(fit + 3, CAPACITY_BYTES as usize)]);
    assert_eq!(offsets(cache.range(7, fit + 3, fit + 4)), None);
    assert_eq!(cache.bytes, 0);
}

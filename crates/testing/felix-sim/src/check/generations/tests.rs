use bytes::Bytes;
use felix_storage::log::{LogRecord, RecordMark};

use super::*;

fn record(offset: u64, payload: &'static str, generation: Option<u64>) -> LogRecord {
    LogRecord {
        offset,
        timestamp_micros: 0,
        checksum: payload.len() as u32,
        payload: Bytes::from_static(payload.as_bytes()),
        mark: RecordMark::default(),
        publisher: None,
        headers: None,
        generation,
    }
}

fn logs(a: Vec<LogRecord>, b: Vec<LogRecord>) -> Vec<(String, Vec<LogRecord>)> {
    vec![("b1".to_string(), a), ("b2".to_string(), b)]
}

#[test]
fn replicas_that_name_the_same_writer_agree() {
    let held = logs(
        vec![record(0, "a", Some(3)), record(1, "b", Some(4))],
        vec![record(0, "a", Some(3)), record(1, "b", Some(4))],
    );
    assert!(disagreements(&held, &mut Tally::default()).is_empty());
}

#[test]
fn two_known_writers_for_one_record_disagree() {
    let held = logs(vec![record(0, "a", Some(3))], vec![record(0, "a", Some(2))]);
    let found = disagreements(&held, &mut Tally::default());
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].0, 0);
}

#[test]
fn an_unknown_writer_or_no_stamp_agrees_with_anything() {
    let held = logs(
        vec![record(0, "a", Some(UNKNOWN_WRITER)), record(1, "b", None)],
        vec![record(0, "a", Some(5)), record(1, "b", Some(6))],
    );
    assert!(disagreements(&held, &mut Tally::default()).is_empty());
}

#[test]
fn different_payloads_at_an_offset_are_not_compared() {
    let held = logs(
        vec![record(0, "a", Some(3))],
        vec![record(0, "bb", Some(2))],
    );
    assert!(disagreements(&held, &mut Tally::default()).is_empty());
}

#[test]
fn a_record_held_once_is_not_compared() {
    let held = logs(vec![record(0, "a", Some(3))], Vec::new());
    assert!(disagreements(&held, &mut Tally::default()).is_empty());
}

#[test]
fn the_tally_counts_compared_records_and_reads_back() {
    let held = logs(
        vec![
            record(0, "a", Some(3)),
            record(1, "b", None),
            record(2, "c", Some(1)),
        ],
        vec![record(0, "a", Some(3)), record(1, "b", Some(2))],
    );
    let mut tally = Tally::default();
    assert!(disagreements(&held, &mut tally).is_empty());
    assert_eq!(
        tally,
        Tally {
            compared: 2,
            stamped: 1
        }
    );
    assert_eq!(
        Tally::parse(&tally.summary()),
        Some(Tally {
            compared: 2,
            stamped: 1
        })
    );
}

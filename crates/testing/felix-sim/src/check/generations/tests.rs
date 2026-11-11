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
            stamped: 1,
            unread: 0
        }
    );
    assert_eq!(
        Tally::parse(&tally.summary()),
        Some(Tally {
            compared: 2,
            stamped: 1,
            unread: 0
        })
    );
}

#[test]
fn writers_4_and_5_for_the_same_payload_are_reported_and_0_and_5_are_not() {
    let held = logs(
        vec![
            record(0, "same", Some(4)),
            record(1, "same", Some(UNKNOWN_WRITER)),
        ],
        vec![record(0, "same", Some(5)), record(1, "same", Some(5))],
    );
    let found = disagreements(&held, &mut Tally::default());
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].0, 0);
    assert_eq!(found[0].1, vec![("b1", 4), ("b2", 5)]);
}

#[tokio::test]
async fn a_head_trimmed_log_is_read_from_its_first_offset() {
    let held = [record(5, "a", Some(3)), record(6, "b", Some(3))];
    let read = read_pages(|next| {
        let page: Vec<_> = held.iter().filter(|r| r.offset >= next).cloned().collect();
        async move {
            if next < 5 {
                Err(BrokerError::CursorTooOld {
                    requested: next,
                    oldest: 5,
                    trim_headers: None,
                })
            } else {
                Ok(page)
            }
        }
    })
    .await;
    assert_eq!(read.unwrap().len(), 2);
}

#[tokio::test]
async fn a_read_that_fails_is_an_error_not_an_empty_log() {
    let read = read_pages(|_| async { Err(BrokerError::Storage("disk".into())) }).await;
    assert!(read.is_err());
    let tally = Tally {
        compared: 3,
        stamped: 1,
        unread: 1,
    };
    assert_eq!(Tally::parse(&tally.summary()), Some(tally));
}

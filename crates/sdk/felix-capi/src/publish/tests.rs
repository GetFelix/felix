use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use super::*;
use crate::status::{FELIX_STATUS_AUTH, FELIX_STATUS_INVALID_ARGUMENT};

#[test]
fn records_laid_end_to_end_are_split_by_their_lengths() {
    let data = b"abcdef";
    let lens = [1usize, 0, 2, 3];
    // SAFETY: both point at live arrays of the stated sizes.
    let records =
        unsafe { laid_end_to_end(data.as_ptr(), lens.as_ptr(), lens.len(), "data", "lens") }
            .expect("split");
    assert_eq!(
        records,
        [b"a".to_vec(), Vec::new(), b"bc".to_vec(), b"def".to_vec()]
    );
}

#[test]
fn empty_records_need_no_buffer() {
    let lens = [0usize, 0];
    // SAFETY: `lens` is live; `data` is never read for a zero total.
    let records =
        unsafe { laid_end_to_end(ptr::null(), lens.as_ptr(), 2, "data", "lens") }.expect("split");
    assert_eq!(records, [Vec::<u8>::new(), Vec::new()]);
    // SAFETY: nothing is read for a count of zero.
    let none = unsafe { laid_end_to_end(ptr::null(), ptr::null(), 0, "data", "lens") };
    assert!(none.expect("empty").is_empty());
}

#[test]
fn missing_buffers_are_named() {
    let lens = [3usize];
    // SAFETY: nothing behind a null pointer is read.
    let failure =
        unsafe { laid_end_to_end(ptr::null(), lens.as_ptr(), 1, "keys", "key_lens") }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains("keys"), "{}", failure.message);
    // SAFETY: as above.
    let failure =
        unsafe { laid_end_to_end(b"x".as_ptr(), ptr::null(), 1, "data", "lens") }.unwrap_err();
    assert!(
        failure.message.contains("lens is null"),
        "{}",
        failure.message
    );
}

#[test]
fn lengths_that_overflow_are_refused() {
    let lens = [usize::MAX, 1];
    // SAFETY: the lengths are rejected before `data` is read.
    let failure =
        unsafe { laid_end_to_end(b"x".as_ptr(), lens.as_ptr(), 2, "data", "lens") }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

/// Null is no key; non-null with a zero length is the empty key.
#[test]
fn a_null_key_is_no_key_and_an_empty_one_is_a_key() {
    // SAFETY: null, and a live one-byte buffer.
    unsafe {
        assert_eq!(routing_key(ptr::null(), 0).unwrap(), None);
        assert_eq!(routing_key(b"k".as_ptr(), 0).unwrap(), Some(Bytes::new()));
        assert_eq!(
            routing_key(b"k".as_ptr(), 1).unwrap(),
            Some(Bytes::from_static(b"k"))
        );
        assert!(routing_key(ptr::null(), 1).is_err());
    }
}

#[test]
fn unknown_modes_are_refused() {
    assert!(parse_ack(3).is_err());
    assert!(parse_delivery(2).is_err());
    assert!(parse_delivery(FELIX_DELIVERY_AT_LEAST_ONCE).unwrap());
}

#[derive(Debug, PartialEq)]
struct Row {
    status: FelixStatus,
    offset: Option<u64>,
    message: Option<String>,
}

#[derive(Debug, PartialEq)]
struct Seen {
    status: FelixStatus,
    message: Option<String>,
    rows: Option<Vec<Row>>,
}

fn text(message: *const c_char) -> Option<String> {
    // SAFETY: null or valid for the duration of the callback.
    (!message.is_null()).then(|| {
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    })
}

unsafe extern "C" fn record(
    user_data: *mut c_void,
    status: FelixStatus,
    message: *const c_char,
    results: *const FelixRecordResult,
    count: usize,
) {
    // SAFETY: every test passes a live `Mutex<Vec<Seen>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<Seen>>>() };
    let rows = (!results.is_null()).then(|| {
        // SAFETY: `count` entries, valid while the callback runs.
        unsafe { std::slice::from_raw_parts(results, count) }
            .iter()
            .map(|r| Row {
                status: r.status,
                offset: r.has_offset.then_some(r.offset),
                message: text(r.message),
            })
            .collect()
    });
    calls.lock().unwrap().push(Seen {
        status,
        message: text(message),
        rows,
    });
}

fn deliver(recorded: Recorded) -> Seen {
    let calls = Mutex::new(Vec::new());
    let done =
        Completion::<Recorded>::new(Some(record), (&raw const calls).cast_mut().cast::<c_void>())
            .expect("callback is set");
    recorded.report(done);
    let mut calls = calls.into_inner().unwrap();
    assert_eq!(calls.len(), 1);
    calls.pop().unwrap()
}

fn ok(offset: u64) -> Row {
    Row {
        status: FELIX_STATUS_OK,
        offset: Some(offset),
        message: None,
    }
}

fn denied() -> Failure {
    Failure {
        status: FELIX_STATUS_AUTH,
        message: "denied".to_string(),
    }
}

/// A piece is appended contiguously, so each record's offset follows from
/// the piece's first.
#[test]
fn each_record_gets_its_own_offset() {
    let seen = deliver(Recorded::whole(3, Ok(Some(40))));
    assert_eq!(seen.status, FELIX_STATUS_OK);
    assert_eq!(seen.rows, Some(vec![ok(40), ok(41), ok(42)]));
}

#[test]
fn a_failed_batch_fails_every_record() {
    let seen = deliver(Recorded::whole(2, Err(denied())));
    assert_eq!(seen.status, FELIX_STATUS_AUTH);
    assert_eq!(seen.message.as_deref(), Some("denied"));
    let row = || Row {
        status: FELIX_STATUS_AUTH,
        offset: None,
        message: Some("denied".to_string()),
    };
    assert_eq!(seen.rows, Some(vec![row(), row()]));
}

/// Pieces interleave positions; offsets follow each record's place in its
/// own piece, and the call reports the first record that failed.
#[test]
fn pieces_report_per_record() {
    let recorded = Recorded {
        count: 4,
        pieces: vec![
            Piece {
                records: vec![1, 3],
                outcome: Err(denied()),
            },
            Piece {
                records: vec![0, 2],
                outcome: Ok(Some(7)),
            },
        ],
    };
    let seen = deliver(recorded);
    assert_eq!(seen.status, FELIX_STATUS_AUTH);
    let failed = || Row {
        status: FELIX_STATUS_AUTH,
        offset: None,
        message: Some("denied".to_string()),
    };
    assert_eq!(seen.rows, Some(vec![ok(7), failed(), ok(8), failed()]));
}

#[test]
fn an_accepted_record_without_an_offset_has_none() {
    let seen = deliver(Recorded::whole(1, Ok(None)));
    assert_eq!(
        seen.rows,
        Some(vec![Row {
            status: FELIX_STATUS_OK,
            offset: None,
            message: None,
        }])
    );
}

/// A record no piece mentions is a failure, never a silent success.
#[test]
fn a_record_with_no_outcome_is_an_error() {
    let recorded = Recorded {
        count: 2,
        pieces: vec![Piece {
            records: vec![0],
            outcome: Ok(Some(1)),
        }],
    };
    let seen = deliver(recorded);
    assert_eq!(seen.status, FELIX_STATUS_ERROR);
    let rows = seen.rows.expect("rows");
    assert_eq!(rows[0], ok(1));
    assert_eq!(rows[1].status, FELIX_STATUS_ERROR);
}

#[test]
fn an_empty_batch_has_no_results() {
    let seen = deliver(Recorded::whole(0, Ok(None)));
    assert_eq!(
        seen,
        Seen {
            status: FELIX_STATUS_OK,
            message: None,
            rows: None,
        }
    );
}

/// A call that ended without an outcome per record, such as a cancel, passes
/// no results.
#[test]
fn a_cancelled_batch_has_no_results() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let done =
        Completion::<Recorded>::new(Some(record), (&raw const calls).cast_mut().cast::<c_void>())
            .expect("callback is set");
    done.status(FELIX_STATUS_CANCELLED);
    let calls = calls.into_inner().unwrap();
    assert_eq!(calls[0].rows, None);
    assert_eq!(calls[0].status, FELIX_STATUS_CANCELLED);
}

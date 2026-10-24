use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use super::*;
use crate::status::{FELIX_STATUS_AUTH, FELIX_STATUS_INVALID_ARGUMENT};

#[test]
fn reclaim_needs_a_consumer() {
    let failure = poll_options(None, true, 0, 0).unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains("reclaim"), "{}", failure.message);
}

#[test]
fn consumer_names_are_one_to_128_bytes() {
    assert!(poll_options(Some(""), false, 0, 0).is_err());
    assert!(poll_options(Some(&"x".repeat(129)), false, 0, 0).is_err());
    let options = poll_options(Some(&"x".repeat(128)), true, 0, 0).unwrap();
    let member = options.member.expect("a member");
    assert!(member.reclaim);
    assert_eq!(member.consumer.len(), 128);
}

#[test]
fn zero_visibility_is_the_brokers_own() {
    let options = poll_options(None, false, 250, 0).unwrap();
    assert_eq!(options.member, None);
    assert_eq!(options.wait, Duration::from_millis(250));
    assert_eq!(options.visibility, None);
    let options = poll_options(None, false, 0, 1500).unwrap();
    assert_eq!(options.visibility, Some(Duration::from_millis(1500)));
}

/// A settle carries only what the broker checks.
#[test]
fn a_claim_is_the_offset_and_the_attempt_count() {
    let record = claim(42, 3);
    assert_eq!((record.offset, record.attempts), (42, 3));
    assert!(record.payload.is_empty());
    assert_eq!(record.skipped_before, 0);
}

#[test]
fn durations_saturate_instead_of_wrapping() {
    assert_eq!(millis(Duration::from_millis(1500)), 1500);
    assert_eq!(millis(Duration::MAX), u64::MAX);
}

type Row = (u64, u32, u64, Vec<u8>);

#[derive(Debug, PartialEq)]
struct Seen {
    status: FelixStatus,
    message: Option<String>,
    records: Option<Vec<Row>>,
}

unsafe extern "C" fn polled(
    user_data: *mut c_void,
    status: FelixStatus,
    message: *const c_char,
    records: *const FelixGroupRecord,
    count: usize,
) {
    // SAFETY: every test passes a live `Mutex<Vec<Seen>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<Seen>>>() };
    let records = (!records.is_null()).then(|| {
        // SAFETY: `count` entries, valid while the callback runs.
        unsafe { std::slice::from_raw_parts(records, count) }
            .iter()
            .map(|r| {
                let payload = if r.payload.is_null() {
                    Vec::new()
                } else {
                    // SAFETY: `payload_len` bytes, valid during the callback.
                    unsafe { std::slice::from_raw_parts(r.payload, r.payload_len) }.to_vec()
                };
                (r.offset, r.attempts, r.skipped_before, payload)
            })
            .collect()
    });
    let message = (!message.is_null()).then(|| {
        // SAFETY: NUL-terminated, valid during the callback.
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    });
    calls.lock().unwrap().push(Seen {
        status,
        message,
        records,
    });
}

fn poll_record(offset: u64, attempts: u32, skipped: u64, payload: &'static [u8]) -> GroupRecord {
    GroupRecord {
        skipped_before: skipped,
        payload: Bytes::from_static(payload),
        ..claim(offset, attempts)
    }
}

#[test]
fn polled_records_keep_their_fields_and_payloads() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let done = Completion::<Polled>::new(Some(polled), (&raw const calls).cast_mut().cast())
        .expect("callback is set");
    done.ok(Polled(vec![
        poll_record(5, 1, 0, b"abc"),
        poll_record(9, 2, 3, b""),
    ]));
    let calls = calls.into_inner().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, FELIX_STATUS_OK);
    assert_eq!(
        calls[0].records,
        Some(vec![(5, 1, 0, b"abc".to_vec()), (9, 2, 3, Vec::new())])
    );
}

/// An empty poll is a success with a count of zero, not a failure.
#[test]
fn an_empty_poll_is_ok_and_a_failure_has_no_records() {
    let calls = Mutex::new(Vec::<Seen>::new());
    let data = (&raw const calls).cast_mut().cast::<c_void>();
    Completion::<Polled>::new(Some(polled), data)
        .unwrap()
        .ok(Polled(Vec::new()));
    Completion::<Polled>::new(Some(polled), data)
        .unwrap()
        .fail(Failure {
            status: FELIX_STATUS_AUTH,
            message: "denied".to_string(),
        });
    let calls = calls.into_inner().unwrap();
    assert_eq!(calls[0].status, FELIX_STATUS_OK);
    assert_eq!(calls[0].records, Some(Vec::new()));
    assert_eq!(calls[1].status, FELIX_STATUS_AUTH);
    assert_eq!(calls[1].message.as_deref(), Some("denied"));
    assert_eq!(calls[1].records, None);
}

#[test]
fn info_rows_say_whether_there_is_a_cursor() {
    let info = |committed| {
        Described(GroupInfo {
            committed,
            tail: 10,
            in_flight: 2,
            owed: 1,
            dead_letters: 4,
        })
        .row()
    };
    let none = info(None);
    assert!(!none.has_committed);
    assert_eq!(none.committed, 0);
    let some = info(Some(7));
    assert!(some.has_committed);
    assert_eq!(
        (
            some.committed,
            some.tail,
            some.in_flight,
            some.owed,
            some.dead_letters
        ),
        (7, 10, 2, 1, 4)
    );
}

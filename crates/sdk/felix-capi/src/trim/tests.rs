use std::sync::Mutex;

use super::*;
use crate::status::{FELIX_STATUS_ERROR, FELIX_STATUS_INVALID_ARGUMENT};

#[derive(Debug, PartialEq)]
struct Seen {
    trimmed: bool,
    has_offset: bool,
    offset: u64,
    oldest: u64,
    tail: u64,
}

type Log = Mutex<Vec<(FelixStatus, Option<Seen>)>>;

unsafe extern "C" fn on_trim(
    user_data: *mut c_void,
    status: FelixStatus,
    _message: *const c_char,
    outcome: *const FelixTrimOutcome,
) {
    // SAFETY: tests pass a pointer to a live `Mutex` that outlives the call.
    let log: &Log = unsafe { &*user_data.cast() };
    // SAFETY: null or valid while the callback runs.
    let seen = unsafe { outcome.as_ref() }.map(|o| Seen {
        trimmed: o.trimmed,
        has_offset: o.has_offset,
        offset: o.offset,
        oldest: o.oldest,
        tail: o.tail,
    });
    log.lock().unwrap().push((status, seen));
}

fn deliver(seen: &Log, status: FelixStatus, value: Option<ConditionalTrim>) {
    // SAFETY: the callback is the one this type delivers to.
    unsafe {
        Trimmed::deliver(
            Some(on_trim),
            ptr::from_ref(seen).cast_mut().cast(),
            status,
            ptr::null(),
            value.map(Trimmed),
        );
    }
}

#[test]
fn a_trim_reports_where_it_landed_and_the_floor() {
    let seen = Mutex::new(Vec::new());
    deliver(
        &seen,
        FELIX_STATUS_OK,
        Some(ConditionalTrim::Trimmed {
            offset: Some(12),
            oldest: 9,
        }),
    );
    assert_eq!(
        seen.into_inner().unwrap(),
        [(
            FELIX_STATUS_OK,
            Some(Seen {
                trimmed: true,
                has_offset: true,
                offset: 12,
                oldest: 9,
                tail: 0,
            })
        )]
    );
}

#[test]
fn a_trim_that_wrote_nothing_has_no_offset() {
    let seen = Mutex::new(Vec::new());
    deliver(
        &seen,
        FELIX_STATUS_OK,
        Some(ConditionalTrim::Trimmed {
            offset: None,
            oldest: 4,
        }),
    );
    let (_, outcome) = seen.into_inner().unwrap().remove(0);
    let outcome = outcome.expect("an outcome");
    assert!(outcome.trimmed && !outcome.has_offset);
    assert_eq!(outcome.oldest, 4);
}

#[test]
fn a_stale_tail_is_an_answer_with_the_tail() {
    let seen = Mutex::new(Vec::new());
    deliver(
        &seen,
        FELIX_STATUS_OK,
        Some(ConditionalTrim::Refused { tail: 31 }),
    );
    assert_eq!(
        seen.into_inner().unwrap(),
        [(
            FELIX_STATUS_OK,
            Some(Seen {
                trimmed: false,
                has_offset: false,
                offset: 0,
                oldest: 0,
                tail: 31,
            })
        )]
    );
}

#[test]
fn a_failure_has_no_outcome() {
    let seen = Mutex::new(Vec::new());
    deliver(&seen, FELIX_STATUS_ERROR, None);
    assert_eq!(seen.into_inner().unwrap(), [(FELIX_STATUS_ERROR, None)]);
}

#[test]
fn headers_are_copied_and_a_null_pointer_is_none() {
    // SAFETY: a null pointer with a zero length is the case.
    assert_eq!(unsafe { optional_headers(ptr::null(), 0) }.unwrap(), None);
    let live = *b"who";
    // SAFETY: three live bytes.
    let copy = unsafe { optional_headers(live.as_ptr(), 3) }.unwrap();
    assert_eq!(copy.as_deref(), Some(&b"who"[..]));
    // SAFETY: a present but empty header is not none.
    let empty = unsafe { optional_headers(live.as_ptr(), 0) }.unwrap();
    assert_eq!(empty, Some(Vec::new()));
}

#[test]
fn headers_that_lie_about_their_length_are_refused() {
    // SAFETY: a null pointer is rejected before it is read.
    let failure = unsafe { optional_headers(ptr::null(), 3) }.unwrap_err();
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
    assert!(failure.message.contains("headers"), "{}", failure.message);
}

#[test]
fn a_call_without_a_client_is_refused_before_anything_starts() {
    let s = c"s";
    // SAFETY: the strings are NUL-terminated; the null client is the case.
    let status = unsafe {
        felix_client_trim_before(
            ptr::null(),
            s.as_ptr(),
            s.as_ptr(),
            s.as_ptr(),
            0,
            5,
            5,
            ptr::null(),
            0,
            ptr::null(),
            None,
            ptr::null_mut(),
        )
    };
    assert_eq!(status, FELIX_STATUS_INVALID_ARGUMENT);
}

use std::ffi::{CStr, c_char, c_void};
use std::sync::Mutex;

use super::*;
use crate::publish::{FelixPublishCallback, Published};
use crate::status::{FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_NOT_FOUND};

/// What one callback saw.
#[derive(Debug, PartialEq)]
struct Seen {
    status: FelixStatus,
    message: Option<String>,
    offset: Option<u64>,
}

unsafe extern "C" fn record(
    user_data: *mut c_void,
    status: FelixStatus,
    message: *const c_char,
    has_offset: bool,
    offset: u64,
) {
    // SAFETY: every test passes a live `Mutex<Vec<Seen>>`.
    let calls = unsafe { &*user_data.cast::<Mutex<Vec<Seen>>>() };
    // SAFETY: null or valid for the duration of the callback.
    let message = (!message.is_null()).then(|| {
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    });
    calls.lock().unwrap().push(Seen {
        status,
        message,
        offset: has_offset.then_some(offset),
    });
}

const RECORD: FelixPublishCallback = Some(record);

fn completion(calls: &Mutex<Vec<Seen>>) -> Completion<Published> {
    Completion::new(RECORD, std::ptr::from_ref(calls).cast_mut().cast()).unwrap()
}

#[test]
fn success_calls_back_once_without_a_message() {
    let calls = Mutex::new(Vec::new());
    completion(&calls).ok(Published(Some(7)));
    assert_eq!(
        *calls.lock().unwrap(),
        [Seen {
            status: FELIX_STATUS_OK,
            message: None,
            offset: Some(7)
        }]
    );
}

#[test]
fn a_failure_carries_its_own_message() {
    let calls = Mutex::new(Vec::new());
    completion(&calls).fail(Failure {
        status: FELIX_STATUS_NOT_FOUND,
        message: "unknown stream".to_string(),
        cursor: None,
    });
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, FELIX_STATUS_NOT_FOUND);
    assert_eq!(calls[0].message.as_deref(), Some("unknown stream"));
}

/// A task the runtime drops before it finishes still calls back.
#[test]
fn dropping_an_unfinished_call_reports_it_cancelled() {
    let calls = Mutex::new(Vec::new());
    drop(completion(&calls));
    assert_eq!(
        *calls.lock().unwrap(),
        [Seen {
            status: FELIX_STATUS_CANCELLED,
            message: None,
            offset: None
        }]
    );
}

#[test]
fn a_panic_in_the_task_reports_a_panic() {
    let calls = Mutex::new(Vec::new());
    let done = completion(&calls);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _done = done;
        panic!("boom");
    }));
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, FELIX_STATUS_PANIC);
    assert!(calls[0].message.is_some());
}

/// The same, through a real task: tokio catches the panic and drops the
/// future while unwinding.
#[tokio::test]
async fn a_panicking_task_reports_a_panic_once() {
    let calls: &'static Mutex<Vec<Seen>> = Box::leak(Box::new(Mutex::new(Vec::new())));
    let done = completion(calls);
    let joined = tokio::spawn(async move {
        let _done = done;
        panic!("boom");
    })
    .await;
    assert!(joined.unwrap_err().is_panic());
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, FELIX_STATUS_PANIC);
}

#[test]
fn a_null_callback_is_refused_and_never_called() {
    let failure = Completion::<Published>::new(None, std::ptr::null_mut())
        .err()
        .expect("refused");
    assert_eq!(failure.status, FELIX_STATUS_INVALID_ARGUMENT);
}

#[tokio::test]
async fn either_token_cancels() {
    let handle = CancellationToken::new();
    let user = CancellationToken::new();
    assert!(!is_cancelled(&handle, Some(&user)));
    user.cancel();
    assert!(is_cancelled(&handle, Some(&user)));
    cancelled(&handle, Some(&user)).await;
    assert!(!is_cancelled(&handle, None));
}

type CursorSeen = Vec<Option<(u32, u64, u64, Option<Vec<u8>>)>>;

unsafe extern "C" fn record_cursor(
    user_data: *mut c_void,
    _status: FelixStatus,
    _message: *const c_char,
    _has_offset: bool,
    _offset: u64,
) {
    // SAFETY: the test passes a live `Mutex<CursorSeen>`.
    let seen = unsafe { &*user_data.cast::<Mutex<CursorSeen>>() };
    let mut out = std::mem::MaybeUninit::<crate::error::FelixCursorError>::uninit();
    // SAFETY: `out` is writable.
    let found = unsafe { crate::error::felix_last_cursor_error(out.as_mut_ptr()) };
    seen.lock().unwrap().push(found.then(|| {
        // SAFETY: written because `found`.
        let c = unsafe { out.assume_init() };
        let headers = c.has_trim_headers.then(|| {
            // SAFETY: valid for `trim_headers_len` until the callback returns.
            unsafe { std::slice::from_raw_parts(c.trim_headers, c.trim_headers_len) }.to_vec()
        });
        (c.reason, c.requested, c.available, headers)
    }));
}

/// A refused cursor's details are readable inside its callback, and only there.
#[test]
fn a_cursor_failure_exposes_its_details_to_the_callback_only() {
    let seen: Mutex<CursorSeen> = Mutex::new(Vec::new());
    let new = || {
        Completion::<Published>::new(
            Some(record_cursor),
            std::ptr::from_ref(&seen).cast_mut().cast(),
        )
        .unwrap()
    };
    let err = anyhow::Error::new(felix_client::SubscribeCursorError {
        reason: felix_client::CursorErrorReason::TooOld,
        requested: 3,
        available: 9,
        trim_headers: Some(bytes::Bytes::from_static(b"who")),
    });
    new().fail(Failure::from(err));
    new().fail(Failure::from(anyhow::anyhow!("unknown stream")));
    assert_eq!(
        *seen.lock().unwrap(),
        [
            Some((
                crate::error::FELIX_CURSOR_TOO_OLD,
                3,
                9,
                Some(b"who".to_vec())
            )),
            None
        ]
    );
    // Not left behind for the next call on this thread.
    let mut out = std::mem::MaybeUninit::<crate::error::FelixCursorError>::uninit();
    // SAFETY: `out` is writable.
    assert!(!unsafe { crate::error::felix_last_cursor_error(out.as_mut_ptr()) });
}

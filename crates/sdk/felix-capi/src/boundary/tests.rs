use std::ffi::CStr;

use super::*;
use crate::error::felix_last_error_message;
use crate::status::{FELIX_STATUS_NOT_FOUND, FELIX_STATUS_OK};

fn last_error() -> Option<String> {
    let ptr = felix_last_error_message();
    // SAFETY: the library hands back null or a string it owns until the next
    // call on this thread.
    (!ptr.is_null()).then(|| {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    })
}

#[test]
fn a_panic_becomes_a_status_and_a_message() {
    let status = guard(|| panic!("boom"));
    assert_eq!(status, FELIX_STATUS_PANIC);
    assert_eq!(last_error().as_deref(), Some("felix panicked: boom"));
}

#[test]
fn a_failure_records_its_message_and_success_clears_it() {
    let status = guard(|| Err(anyhow::anyhow!("unknown stream t1/ns/s").into()));
    assert_eq!(status, FELIX_STATUS_NOT_FOUND);
    assert!(last_error().unwrap().contains("unknown stream"));

    assert_eq!(guard(|| Ok(FELIX_STATUS_OK)), FELIX_STATUS_OK);
    assert_eq!(last_error(), None);
}

#[test]
fn the_last_error_belongs_to_its_thread() {
    let _ = guard(|| Err(Failure::invalid("here")));
    std::thread::spawn(|| assert_eq!(last_error(), None))
        .join()
        .unwrap();
    assert_eq!(last_error().as_deref(), Some("here"));
}

#[test]
fn strings_are_checked_before_use() {
    // SAFETY: null is allowed, and the literal is NUL-terminated and static.
    unsafe {
        assert_eq!(
            required_str(std::ptr::null(), "stream").unwrap_err().status,
            FELIX_STATUS_INVALID_ARGUMENT
        );
        assert_eq!(optional_str(std::ptr::null(), "ca_file").unwrap(), None);
        let bad = c"\xff";
        assert!(required_str(bad.as_ptr(), "stream").is_err());
        assert_eq!(required_str(c"ok".as_ptr(), "stream").unwrap(), "ok");
    }
}

#[test]
fn a_cursor_failure_is_readable_until_the_next_call() {
    let err = anyhow::Error::new(felix_client::SubscribeCursorError {
        reason: felix_client::CursorErrorReason::InFuture,
        requested: 50,
        available: 12,
        trim_headers: None,
    });
    assert_eq!(
        guard(|| Err(err.into())),
        crate::status::FELIX_STATUS_CURSOR
    );
    let mut out = std::mem::MaybeUninit::<crate::error::FelixCursorError>::uninit();
    // SAFETY: `out` is writable.
    assert!(unsafe { crate::error::felix_last_cursor_error(out.as_mut_ptr()) });
    // SAFETY: written because the call returned true.
    let cursor = unsafe { out.assume_init() };
    assert_eq!(cursor.reason, crate::error::FELIX_CURSOR_IN_FUTURE);
    assert_eq!((cursor.requested, cursor.available), (50, 12));
    assert!(!cursor.has_trim_headers && cursor.trim_headers.is_null());

    assert_eq!(guard(|| Ok(FELIX_STATUS_OK)), FELIX_STATUS_OK);
    let mut again = std::mem::MaybeUninit::<crate::error::FelixCursorError>::uninit();
    // SAFETY: `again` is writable.
    assert!(!unsafe { crate::error::felix_last_cursor_error(again.as_mut_ptr()) });
}

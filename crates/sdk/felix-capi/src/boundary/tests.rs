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

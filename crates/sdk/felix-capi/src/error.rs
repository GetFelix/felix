//! The last error, per thread.
//!
//! Thread-local so two threads failing at once each read their own message,
//! and so reading it takes no lock.

use std::cell::RefCell;
use std::ffi::{CString, c_char};
use std::ptr;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// The message for the last call on this thread that did not return
/// `FELIX_STATUS_OK`, or null if that call succeeded.
///
/// The string is owned by the library and stays valid until the next Felix
/// call on the same thread. Copy it to keep it. Do not free it.
#[unsafe(no_mangle)]
pub extern "C" fn felix_last_error_message() -> *const c_char {
    LAST_ERROR
        .try_with(|slot| {
            slot.borrow()
                .as_ref()
                .map_or(ptr::null(), |message| message.as_ptr())
        })
        .unwrap_or(ptr::null())
}

pub(crate) fn set_last_error(message: String) {
    // An interior NUL would truncate the C string; replace it rather than
    // lose the message.
    let message = CString::new(message.replace('\0', "\u{FFFD}")).unwrap_or_default();
    let _ = LAST_ERROR.try_with(|slot| *slot.borrow_mut() = Some(message));
}

pub(crate) fn clear_last_error() {
    let _ = LAST_ERROR.try_with(|slot| *slot.borrow_mut() = None);
}

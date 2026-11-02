//! The last error, per thread.
//!
//! Thread-local so two threads failing at once each read their own message,
//! and so reading it takes no lock.

use std::cell::RefCell;
use std::ffi::{CString, c_char};
use std::ptr;

use felix_client::{CursorErrorReason, SubscribeCursorError};

/// The offset was discarded by retention or a head trim.
pub const FELIX_CURSOR_TOO_OLD: u32 = 0;
/// The offset is past the end of the stream.
pub const FELIX_CURSOR_IN_FUTURE: u32 = 1;

/// Why a subscription start was refused, for the thread that sees
/// `FELIX_STATUS_CURSOR`. See `felix_last_cursor_error`.
#[repr(C)]
pub struct FelixCursorError {
    /// `FELIX_CURSOR_TOO_OLD` or `FELIX_CURSOR_IN_FUTURE`.
    pub reason: u32,
    /// The offset that was asked for.
    pub requested: u64,
    /// The nearest offset that would have worked: the oldest retained for
    /// `FELIX_CURSOR_TOO_OLD`, the tail for `FELIX_CURSOR_IN_FUTURE`.
    pub available: u64,
    /// Whether the floor is a head trim's, so `trim_headers` is meaningful. A
    /// trim written with no headers has this set and a zero length.
    pub has_trim_headers: bool,
    /// The headers the head trim was written with, `trim_headers_len` bytes.
    /// Null when `has_trim_headers` is false. Owned by the library, valid as
    /// long as the message from `felix_last_error_message` is: until the next
    /// Felix call on the thread, or until the callback returns.
    pub trim_headers: *const u8,
    pub trim_headers_len: usize,
}

/// A refused cursor, held until the thread's next Felix call.
#[derive(Debug, Clone)]
pub(crate) struct CursorDetail {
    reason: u32,
    requested: u64,
    available: u64,
    trim_headers: Option<Vec<u8>>,
}

impl From<&SubscribeCursorError> for CursorDetail {
    fn from(err: &SubscribeCursorError) -> Self {
        Self {
            reason: match err.reason {
                CursorErrorReason::TooOld => FELIX_CURSOR_TOO_OLD,
                CursorErrorReason::InFuture => FELIX_CURSOR_IN_FUTURE,
            },
            requested: err.requested,
            available: err.available,
            trim_headers: err.trim_headers.as_ref().map(|bytes| bytes.to_vec()),
        }
    }
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
    static LAST_CURSOR: RefCell<Option<CursorDetail>> = const { RefCell::new(None) };
}

/// Fill `out` with why the last call on this thread failed with
/// `FELIX_STATUS_CURSOR`, and return true. Returns false, leaving `out`
/// untouched, when the last call did not fail that way.
///
/// Call it where the status is seen: right after a call that returned
/// `FELIX_STATUS_CURSOR`, or inside the callback that received it. Anywhere
/// else, such as another thread, there is nothing to read. `out`'s
/// `trim_headers` is valid until the next Felix call on the thread, or until
/// the callback returns; copy it to keep it.
///
/// # Safety
///
/// `out` is null or valid for a write of a `FelixCursorError`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_last_cursor_error(out: *mut FelixCursorError) -> bool {
    if out.is_null() {
        return false;
    }
    LAST_CURSOR
        .try_with(|slot| {
            let slot = slot.borrow();
            let Some(cursor) = slot.as_ref() else {
                return false;
            };
            let (has_trim_headers, trim_headers, trim_headers_len) = match &cursor.trim_headers {
                Some(bytes) => (true, bytes.as_ptr(), bytes.len()),
                None => (false, ptr::null(), 0),
            };
            // SAFETY: non-null, and the caller promises it is writable.
            unsafe {
                out.write(FelixCursorError {
                    reason: cursor.reason,
                    requested: cursor.requested,
                    available: cursor.available,
                    has_trim_headers,
                    trim_headers,
                    trim_headers_len,
                });
            }
            true
        })
        .unwrap_or(false)
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
    set_last_cursor(None);
}

pub(crate) fn set_last_cursor(cursor: Option<CursorDetail>) {
    let _ = LAST_CURSOR.try_with(|slot| *slot.borrow_mut() = cursor);
}

//! What every exported function does at the edge: catch panics, record the
//! error, and read C arguments into Rust values.

use std::any::Any;
use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};

use felix_client::SubscribeCursorError;

use crate::error::{CursorDetail, clear_last_error, set_last_cursor, set_last_error};
use crate::status::{
    FELIX_STATUS_CURSOR, FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_PANIC, FelixStatus, classify,
};

/// Why a call failed: the status to return and the message to record, and
/// for a refused cursor where the reader can resume.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) status: FelixStatus,
    pub(crate) message: String,
    pub(crate) cursor: Option<CursorDetail>,
}

impl Failure {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: FELIX_STATUS_INVALID_ARGUMENT,
            message: message.into(),
            cursor: None,
        }
    }
}

impl From<anyhow::Error> for Failure {
    fn from(err: anyhow::Error) -> Self {
        let status = classify(&err);
        // Only when the status says so: a coded broker error wins over a
        // cursor error elsewhere in the chain.
        let cursor = (status == FELIX_STATUS_CURSOR)
            .then(|| {
                err.chain()
                    .find_map(|e| e.downcast_ref::<SubscribeCursorError>())
                    .map(CursorDetail::from)
            })
            .flatten();
        Self {
            status,
            message: format!("{err:#}"),
            cursor,
        }
    }
}

/// Run the body of an exported function.
///
/// The body returns the status for a call that did not fail (OK, or a
/// timeout or end of stream), or a [`Failure`]. A panic is caught here so it
/// never unwinds into C, which would be undefined behaviour.
pub(crate) fn guard(body: impl FnOnce() -> Result<FelixStatus, Failure>) -> FelixStatus {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(status)) => {
            clear_last_error();
            status
        }
        Ok(Err(failure)) => {
            set_last_error(failure.message);
            set_last_cursor(failure.cursor);
            failure.status
        }
        Err(payload) => {
            set_last_error(format!("felix panicked: {}", panic_text(&*payload)));
            set_last_cursor(None);
            FELIX_STATUS_PANIC
        }
    }
}

/// [`guard`] for a function with nothing to report, such as a free.
pub(crate) fn guard_void(body: impl FnOnce()) {
    let _ = guard(|| {
        body();
        Ok(crate::status::FELIX_STATUS_OK)
    });
}

/// A required C string, as UTF-8.
///
/// # Safety
///
/// `ptr` is null or points to a NUL-terminated string that outlives `'a`.
pub(crate) unsafe fn required_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, Failure> {
    // SAFETY: the caller's contract, passed through.
    unsafe { optional_str(ptr, name) }?.ok_or_else(|| Failure::invalid(format!("{name} is null")))
}

/// An optional C string, as UTF-8. Null is `None`.
///
/// # Safety
///
/// `ptr` is null or points to a NUL-terminated string that outlives `'a`.
pub(crate) unsafe fn optional_str<'a>(
    ptr: *const c_char,
    name: &str,
) -> Result<Option<&'a str>, Failure> {
    if ptr.is_null() {
        return Ok(None);
    }
    // SAFETY: non-null, and the caller promises a NUL-terminated string.
    let raw = unsafe { CStr::from_ptr(ptr) };
    raw.to_str()
        .map(Some)
        .map_err(|_| Failure::invalid(format!("{name} is not valid UTF-8")))
}

/// Write `value` through an out-pointer when the caller passed one.
///
/// # Safety
///
/// `out` is null or valid for a write of `T`.
pub(crate) unsafe fn write_optional<T>(out: *mut T, value: T) {
    if !out.is_null() {
        // SAFETY: non-null, and the caller promises it is writable.
        unsafe { out.write(value) };
    }
}

/// A field of a caller's size-gated struct, or `None` when `size` does not
/// reach all of it. Read unaligned: nothing checks the caller's alignment.
///
/// # Safety
///
/// `base` is valid for reads of `size` bytes, and `offset` is where a `T`
/// would lie in the struct.
pub(crate) unsafe fn read_field<T: Copy>(base: *const u8, size: usize, offset: usize) -> Option<T> {
    // SAFETY: `size` covers the field, so the address is in bounds.
    (size >= offset + size_of::<T>())
        .then(|| unsafe { base.add(offset).cast::<T>().read_unaligned() })
}

/// A `bool` field of a caller's struct, false when `size` does not reach it.
/// Read as a byte, since C may hold any value there.
///
/// # Safety
///
/// As for [`read_field`].
pub(crate) unsafe fn read_flag(base: *const u8, size: usize, offset: usize) -> bool {
    // SAFETY: the caller's contract, passed through.
    unsafe { read_field::<u8>(base, size, offset) }.is_some_and(|byte| byte != 0)
}

fn panic_text(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic with no message")
}

#[cfg(test)]
mod tests;

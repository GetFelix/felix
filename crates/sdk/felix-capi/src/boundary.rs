//! What every exported function does at the edge: catch panics, record the
//! error, and read C arguments into Rust values.

use std::any::Any;
use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::error::{clear_last_error, set_last_error};
use crate::status::{FELIX_STATUS_INVALID_ARGUMENT, FELIX_STATUS_PANIC, FelixStatus, classify};

/// Why a call failed: the status to return and the message to record.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) status: FelixStatus,
    pub(crate) message: String,
}

impl Failure {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: FELIX_STATUS_INVALID_ARGUMENT,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for Failure {
    fn from(err: anyhow::Error) -> Self {
        Self {
            status: classify(&err),
            message: format!("{err:#}"),
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
            failure.status
        }
        Err(payload) => {
            set_last_error(format!("felix panicked: {}", panic_text(&*payload)));
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

fn panic_text(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a panic with no message")
}

#[cfg(test)]
mod tests;

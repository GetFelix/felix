//! Running a call on the client's runtime and reporting it to a C callback.
//!
//! Every accepted call ends in exactly one callback. [`Completion`] makes that
//! hold by construction: finishing it calls back, and dropping it unfinished
//! (the task panicked, or the runtime shut down under it) calls back too.

use std::ffi::{CString, c_char, c_void};
use std::ptr;

use tokio_util::sync::CancellationToken;

use crate::boundary::Failure;
use crate::status::{FELIX_STATUS_CANCELLED, FELIX_STATUS_OK, FELIX_STATUS_PANIC, FelixStatus};

/// A result type a callback can carry, and how it is handed to C.
pub(crate) trait Deliver: Sized + Send + 'static {
    /// The nullable C function pointer type.
    type Callback: Copy + Send + 'static;

    fn is_null(callback: Self::Callback) -> bool;

    /// # Safety
    ///
    /// `callback` is non-null, and `message` is null or a NUL-terminated
    /// string that outlives the call.
    unsafe fn deliver(
        callback: Self::Callback,
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        value: Option<Self>,
    );
}

/// The caller's `user_data`, passed back untouched on whichever thread the
/// callback runs. The header tells the caller it may be any thread.
struct UserData(*mut c_void);

// SAFETY: the library never dereferences it; it only hands it back.
unsafe impl Send for UserData {}

/// The pending callback of one accepted call.
pub(crate) struct Completion<V: Deliver> {
    callback: V::Callback,
    user_data: UserData,
    done: bool,
}

impl<V: Deliver> Completion<V> {
    /// Fails, without arming anything, when `callback` is null.
    ///
    /// Make this the last fallible step before spawning: once it exists, a
    /// panic that drops it calls back, and the call must then not also
    /// report the panic as its own status.
    pub(crate) fn new(callback: V::Callback, user_data: *mut c_void) -> Result<Self, Failure> {
        if V::is_null(callback) {
            return Err(Failure::invalid("callback is null"));
        }
        Ok(Self {
            callback,
            user_data: UserData(user_data),
            done: false,
        })
    }

    pub(crate) fn ok(self, value: V) {
        self.finish(FELIX_STATUS_OK, None, Some(value));
    }

    /// An outcome that is not a failure and carries no value, such as a
    /// timeout or the end of a stream.
    pub(crate) fn status(self, status: FelixStatus) {
        self.finish(status, None, None);
    }

    pub(crate) fn fail(self, failure: Failure) {
        self.finish(failure.status, Some(failure.message), None);
    }

    /// A value that carries its own outcome, such as a batch in which some
    /// records failed.
    pub(crate) fn with_status(self, status: FelixStatus, message: Option<String>, value: V) {
        self.finish(status, message, Some(value));
    }

    pub(crate) fn result(self, result: Result<V, Failure>) {
        match result {
            Ok(value) => self.ok(value),
            Err(failure) => self.fail(failure),
        }
    }

    fn finish(mut self, status: FelixStatus, message: Option<String>, value: Option<V>) {
        self.done = true;
        call(self.callback, &self.user_data, status, message, value);
    }
}

impl<V: Deliver> Drop for Completion<V> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // Dropped while unwinding means the task panicked; otherwise the
        // runtime dropped the task before it finished.
        let (status, message) = if std::thread::panicking() {
            (
                FELIX_STATUS_PANIC,
                Some("felix panicked while running the call".to_string()),
            )
        } else {
            (FELIX_STATUS_CANCELLED, None)
        };
        call::<V>(self.callback, &self.user_data, status, message, None);
    }
}

fn call<V: Deliver>(
    callback: V::Callback,
    user_data: &UserData,
    status: FelixStatus,
    message: Option<String>,
    value: Option<V>,
) {
    let message = message.map(c_message);
    let message_ptr = message.as_ref().map_or(ptr::null(), |m| m.as_ptr());
    // SAFETY: `new` refused a null callback, and the message lives until the
    // callback returns.
    unsafe { V::deliver(callback, user_data.0, status, message_ptr, value) };
}

/// A message as a C string. An interior NUL would truncate it, so it is
/// replaced instead.
pub(crate) fn c_message(message: String) -> CString {
    CString::new(message.replace('\0', "\u{FFFD}")).unwrap_or_default()
}

/// Resolves once `handle` or the caller's own token is cancelled.
pub(crate) async fn cancelled(handle: &CancellationToken, user: Option<&CancellationToken>) {
    match user {
        Some(user) => tokio::select! {
            () = handle.cancelled() => {}
            () = user.cancelled() => {}
        },
        None => handle.cancelled().await,
    }
}

/// Resolves once the caller's token is cancelled; never without one.
pub(crate) async fn user_cancelled(user: Option<&CancellationToken>) {
    match user {
        Some(user) => user.cancelled().await,
        None => std::future::pending().await,
    }
}

pub(crate) fn is_cancelled(handle: &CancellationToken, user: Option<&CancellationToken>) -> bool {
    handle.is_cancelled() || user.is_some_and(CancellationToken::is_cancelled)
}

#[cfg(test)]
mod tests;

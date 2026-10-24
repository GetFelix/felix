//! Cancellation the caller controls.

use tokio_util::sync::CancellationToken;

use crate::boundary::guard_void;

/// A token a caller cancels to stop the calls it was passed to.
///
/// One token may be passed to any number of calls. It can be freed while
/// calls still hold it; they keep their own reference.
pub struct FelixCancelToken {
    token: CancellationToken,
}

/// A new, uncancelled token. Free it with `felix_cancel_token_free`.
#[unsafe(no_mangle)]
pub extern "C" fn felix_cancel_token_new() -> *mut FelixCancelToken {
    Box::into_raw(Box::new(FelixCancelToken {
        token: CancellationToken::new(),
    }))
}

/// Cancel every call holding this token, and any later call passed it.
/// Null is ignored.
///
/// # Safety
///
/// `token` is null or a live token.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_cancel_token_cancel(token: *const FelixCancelToken) {
    guard_void(|| {
        // SAFETY: null or a live token, per the contract above.
        if let Some(token) = unsafe { token.as_ref() } {
            token.token.cancel();
        }
    });
}

/// Free a token. Null is ignored. Calls holding it are not cancelled.
///
/// # Safety
///
/// `token` is null or a pointer from `felix_cancel_token_new` that has not
/// been freed, and no other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_cancel_token_free(token: *mut FelixCancelToken) {
    guard_void(|| {
        if !token.is_null() {
            // SAFETY: the pointer came from `Box::into_raw` in
            // `felix_cancel_token_new` and is freed exactly once.
            drop(unsafe { Box::from_raw(token) });
        }
    });
}

/// The token a call holds, if the caller passed one.
///
/// # Safety
///
/// `token` is null or a live token.
pub(crate) unsafe fn user_token(token: *const FelixCancelToken) -> Option<CancellationToken> {
    // SAFETY: the caller's contract, passed through.
    unsafe { token.as_ref() }.map(|token| token.token.clone())
}

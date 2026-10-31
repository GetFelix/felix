//! Conditional head trims.
//!
//! A write like `felix_client_publish_if`: it starts through
//! [`spawn_publish`], so a trim cancelled after it may have been sent reports
//! `FELIX_STATUS_OUTCOME_UNKNOWN`, and a stale tail is an answer rather than a
//! failure.

use std::ffi::{c_char, c_void};
use std::ptr;

use felix_client::ConditionalTrim;

use crate::boundary::{Failure, guard, required_str};
use crate::cancel::{FelixCancelToken, user_token};
use crate::client::FelixClient;
use crate::commit::{Scope, scope};
use crate::completion::{Completion, Deliver};
use crate::publish::{bytes, spawn_publish};
use crate::status::{FELIX_STATUS_OK, FelixStatus};

/// What a trim did. Valid only while the callback runs.
#[repr(C)]
pub struct FelixTrimOutcome {
    /// The trim is durable. False when the shard's next offset was not
    /// `expected_offset`: nothing was written, and `tail` is that offset.
    pub trimmed: bool,
    /// Whether the trim wrote a record. False when the floor already stood
    /// at or past `before`.
    pub has_offset: bool,
    /// Where the trim record landed; zero unless `has_offset`.
    pub offset: u64,
    /// The oldest offset reads of the shard start at now; zero unless
    /// `trimmed`.
    pub oldest: u64,
    /// The shard's next offset; zero unless `trimmed` is false.
    pub tail: u64,
}

/// Called once when `felix_client_trim_before` finishes. `outcome` is null
/// unless the status is `FELIX_STATUS_OK`, and is valid only while the
/// callback runs.
pub type FelixTrimCallback = Option<
    unsafe extern "C" fn(
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        outcome: *const FelixTrimOutcome,
    ),
>;

pub(crate) struct Trimmed(ConditionalTrim);

impl Trimmed {
    fn view(&self) -> FelixTrimOutcome {
        match self.0 {
            ConditionalTrim::Trimmed { offset, oldest } => FelixTrimOutcome {
                trimmed: true,
                has_offset: offset.is_some(),
                offset: offset.unwrap_or(0),
                oldest,
                tail: 0,
            },
            ConditionalTrim::Refused { tail } => FelixTrimOutcome {
                trimmed: false,
                has_offset: false,
                offset: 0,
                oldest: 0,
                tail,
            },
        }
    }
}

impl Deliver for Trimmed {
    type Callback = FelixTrimCallback;

    fn is_null(callback: Self::Callback) -> bool {
        callback.is_none()
    }

    unsafe fn deliver(
        callback: Self::Callback,
        user_data: *mut c_void,
        status: FelixStatus,
        message: *const c_char,
        value: Option<Self>,
    ) {
        let Some(callback) = callback else { return };
        let view = value.as_ref().map(Trimmed::view);
        let outcome = view.as_ref().map_or(ptr::null(), ptr::from_ref);
        // SAFETY: `view` lives until the callback returns.
        unsafe { callback(user_data, status, message, outcome) };
    }
}

/// The trim's headers, or `None` for a null pointer. Copied.
///
/// # Safety
///
/// `headers` is null or valid for `len` bytes.
unsafe fn optional_headers(headers: *const u8, len: usize) -> Result<Option<Vec<u8>>, Failure> {
    if headers.is_null() {
        if len != 0 {
            return Err(Failure::invalid(
                "headers is null but headers_len is not zero",
            ));
        }
        return Ok(None);
    }
    // SAFETY: the caller's contract, passed through.
    Ok(Some(unsafe { bytes(headers, len, "headers") }?.to_vec()))
}

/// Hide the records of `shard` below `before`, only if the shard's next offset
/// is exactly `expected_offset`. Reads below the floor are then refused as
/// `FELIX_STATUS_CURSOR`.
///
/// The trim is a record of its own and lands at `expected_offset`, so `before`
/// can be at most that. `headers` (`headers_len` bytes, null for none) are kept
/// with the trim and handed back to a reader refused below it. A stale tail is
/// an answer: `FELIX_STATUS_OK` with `trimmed` false and the shard's `tail`.
/// A `before` at or below the current floor writes nothing and answers
/// `trimmed` true with `has_offset` false, but only when the tail matches.
///
/// Needs the `stream.trim` permission (`FELIX_STATUS_AUTH` without it) and a
/// durable stream without atomic commits. A broker that does not advertise
/// head trims completes the call with `FELIX_STATUS_UNSUPPORTED`, sending
/// nothing. Other refusals, such as a fleet that has not finalized head trims,
/// are `FELIX_STATUS_ERROR` with the broker's reason in the message.
///
/// A write: cancelled after it may have been sent, or when the broker stopped
/// waiting for it, it completes with `FELIX_STATUS_OUTCOME_UNKNOWN`. The trim
/// may have landed; send the call again and, if it did, the answer is a
/// refusal with the current tail.
///
/// # Safety
///
/// `client` is a live client, the strings are NUL-terminated, `headers` is
/// null or valid for `headers_len` bytes, `cancel` is null or a live token,
/// and `callback` is safe to call with `user_data` from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn felix_client_trim_before(
    client: *const FelixClient,
    tenant_id: *const c_char,
    ns: *const c_char,
    stream: *const c_char,
    shard: u32,
    before: u64,
    expected_offset: u64,
    headers: *const u8,
    headers_len: usize,
    cancel: *const FelixCancelToken,
    callback: FelixTrimCallback,
    user_data: *mut c_void,
) -> FelixStatus {
    guard(|| {
        // SAFETY: the pointers meet the contract above.
        let (client, scope) = unsafe { scope(client, tenant_id, ns) }?;
        // SAFETY: as above.
        let stream = unsafe { required_str(stream, "stream") }?.to_string();
        // SAFETY: as above.
        let headers = unsafe { optional_headers(headers, headers_len) }?;
        // SAFETY: null or a live token, per the contract above.
        let user = unsafe { user_token(cancel) };
        let done = Completion::<Trimmed>::new(callback, user_data)?;
        let ask = async move {
            let Scope {
                inner,
                tenant_id,
                ns,
            } = scope;
            inner
                .trim_before(
                    &tenant_id,
                    &ns,
                    &stream,
                    shard,
                    before,
                    expected_offset,
                    headers.as_deref(),
                )
                .await
        };
        spawn_publish(client, user, done, ask, |done, outcome| {
            done.result(outcome.map(Trimmed).map_err(Failure::from));
        });
        Ok(FELIX_STATUS_OK)
    })
}

#[cfg(test)]
mod tests;

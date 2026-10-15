//! Status codes, and which one an error from the Rust client becomes.
//!
//! An `int32_t` with named constants rather than a C enum: the width is then
//! fixed for every compiler and for cgo and P/Invoke, and a code added later
//! cannot be undefined behaviour in a caller that passes one back.
//!
//! The error classes follow the Python and Node bindings, split by what the
//! caller does next: retry, give up, or resend only if idempotent.

use felix_client::{
    BrokerError, NotLeaderError, RetryClass, SubscribeCursorError, SubscriptionLost,
};

/// The result of every fallible call. `FELIX_STATUS_OK` is zero; every other
/// value says why the call did not do what it was asked.
pub type FelixStatus = i32;

/// The call did what it was asked.
pub const FELIX_STATUS_OK: FelixStatus = 0;
/// A bounded wait ended with nothing to return. The handle is still usable.
pub const FELIX_STATUS_TIMEOUT: FelixStatus = 1;
/// The broker ended the subscription. No more events will arrive on it.
pub const FELIX_STATUS_CLOSED: FelixStatus = 2;
/// A required pointer was null, a string was not UTF-8, or a value was out
/// of range. Nothing was sent.
pub const FELIX_STATUS_INVALID_ARGUMENT: FelixStatus = 10;
/// The library panicked. The call did not complete; the handle may still be
/// usable, but the failure is a bug worth reporting.
pub const FELIX_STATUS_PANIC: FelixStatus = 11;
/// A failure with no more specific class.
pub const FELIX_STATUS_ERROR: FelixStatus = 20;
/// The broker could not be reached, the connection was lost mid-call, or the
/// broker is shutting down. Worth retrying.
pub const FELIX_STATUS_CONNECTION: FelixStatus = 21;
/// The token was rejected or lacks the permission the call needs. Retrying
/// will not help.
pub const FELIX_STATUS_AUTH: FelixStatus = 22;
/// The tenant, namespace, stream or cache does not exist.
pub const FELIX_STATUS_NOT_FOUND: FelixStatus = 23;
/// The requested start offset is no longer retained.
pub const FELIX_STATUS_CURSOR: FelixStatus = 24;
/// No broker can serve the shard right now, usually while it moves. Nothing
/// was applied; retrying is safe.
pub const FELIX_STATUS_SHARD_UNAVAILABLE: FelixStatus = 25;
/// The broker is shedding load. Nothing was applied; retry after a pause.
pub const FELIX_STATUS_OVERLOADED: FelixStatus = 26;
/// The write may or may not have been applied. Resend only if it is
/// idempotent.
pub const FELIX_STATUS_OUTCOME_UNKNOWN: FelixStatus = 27;

/// The status for an error from the Rust client.
///
/// A broker's error code wins over the message. Without one the message is
/// read, conservatively: a wrong class is worse than the general one.
pub(crate) fn classify(err: &anyhow::Error) -> FelixStatus {
    if let Some(broker) = err.chain().find_map(|e| e.downcast_ref::<BrokerError>()) {
        return status_for_code(broker.code.as_str(), broker.retry);
    }
    // A redirect the client could not follow says what `not_leader` says.
    if err.chain().any(|e| e.is::<NotLeaderError>()) {
        return FELIX_STATUS_SHARD_UNAVAILABLE;
    }
    if err.chain().any(|e| e.is::<SubscribeCursorError>()) {
        return FELIX_STATUS_CURSOR;
    }
    if err.chain().any(|e| e.is::<SubscriptionLost>()) {
        return FELIX_STATUS_CONNECTION;
    }
    status_from_text(&format!("{err:#}").to_ascii_lowercase())
}

/// The status for a broker error code. `outcome_unknown` wins over the code,
/// because "this may have been written" decides what the caller does next.
pub(crate) fn status_for_code(code: &str, retry: RetryClass) -> FelixStatus {
    if retry == RetryClass::OutcomeUnknown {
        return FELIX_STATUS_OUTCOME_UNKNOWN;
    }
    match code {
        "unauthenticated" | "forbidden" => FELIX_STATUS_AUTH,
        "not_found" => FELIX_STATUS_NOT_FOUND,
        "shard_unavailable" | "not_leader" => FELIX_STATUS_SHARD_UNAVAILABLE,
        "overloaded" => FELIX_STATUS_OVERLOADED,
        "draining" => FELIX_STATUS_CONNECTION,
        _ => FELIX_STATUS_ERROR,
    }
}

/// The fallback for an error without a code, matching the other bindings.
fn status_from_text(lower: &str) -> FelixStatus {
    let any = |needles: &[&str]| needles.iter().any(|needle| lower.contains(needle));
    if any(&["unauthorized", "permission", "forbidden", "token"]) {
        FELIX_STATUS_AUTH
    } else if any(&[
        "unknown tenant",
        "unknown stream",
        "unknown cache",
        "not found",
    ]) {
        FELIX_STATUS_NOT_FOUND
    } else if any(&["cursor", "trimmed", "too old"]) {
        FELIX_STATUS_CURSOR
    } else if any(&["connect", "timed out", "timeout", "no broker", "not leader"]) {
        FELIX_STATUS_CONNECTION
    } else {
        FELIX_STATUS_ERROR
    }
}

#[cfg(test)]
mod tests;

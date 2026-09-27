//! Typed errors, so JavaScript can branch on *why* something failed.
//!
//! A single error carrying prose would push callers into matching on message
//! text, which breaks the moment a message is reworded. The split here mirrors
//! the one the Python binding makes, and the one that actually changes what an
//! application does: a connection failure or an unavailable shard is worth
//! retrying, an authorization failure is not, and a write whose outcome is
//! unknown is safe to resend only when it is idempotent. A broker that
//! negotiated error codes says which it is; only an error without a code falls
//! back to reading the message.
//!
//! A napi error carries only a status from napi's own fixed `Status` enum and a
//! message, so none of this can be set as a property from here. The kind
//! travels as a message prefix and `errors.js` lifts it onto a typed
//! `FelixError` (as `kind`), along with the broker's code, retry class and
//! detail when there are any:
//!
//! ```text
//! FELIX_AUTH: <text>                               no broker code
//! FELIX_SHARD_UNAVAILABLE {"code":...}\n<text>     with one
//! ```
//!
//! Compact JSON never holds a raw newline, which is what makes the second form
//! safe to split. Both halves ship as one package, so this is an internal
//! detail rather than something a caller parses.

use felix_client::{BrokerError, NotLeaderError, SubscribeCursorError, SubscriptionLost};
use felix_wire::RetryClass;
use napi::{Error, Status};

/// The separator between the kind and the text when there is no broker code.
pub(crate) const KIND_SEPARATOR: &str = ": ";

/// The broker rejected the token, or it lacks the permission this call needs.
/// Not retryable.
pub(crate) const KIND_AUTH: &str = "FELIX_AUTH";
/// The tenant, namespace, stream or cache does not exist.
pub(crate) const KIND_NOT_FOUND: &str = "FELIX_NOT_FOUND";
/// The requested start offset is gone — retention discarded it. Recover by
/// restarting from `earliest` and accepting the gap.
pub(crate) const KIND_CURSOR: &str = "FELIX_CURSOR";
/// The broker could not be reached, the connection was lost mid-call, or the
/// broker is shutting down. Worth retrying, and against a different broker.
pub(crate) const KIND_CONNECTION: &str = "FELIX_CONNECTION";
/// Nobody can serve the shard right now, typically while it moves. Nothing was
/// applied.
pub(crate) const KIND_SHARD_UNAVAILABLE: &str = "FELIX_SHARD_UNAVAILABLE";
/// The broker is shedding load. Nothing was applied.
pub(crate) const KIND_OVERLOADED: &str = "FELIX_OVERLOADED";
/// The write may have been applied.
pub(crate) const KIND_OUTCOME_UNKNOWN: &str = "FELIX_OUTCOME_UNKNOWN";
/// Something this binding could not classify. Deliberately not a category.
pub(crate) const KIND_GENERIC: &str = "FELIX_ERROR";
/// A bad argument to this binding, rather than a failure of the call.
pub(crate) const KIND_INVALID: &str = "FELIX_INVALID";

/// Classify an error from the Rust client into a typed one.
pub(crate) fn classify(err: impl Into<anyhow::Error>) -> Error {
    Error::new(Status::GenericFailure, encode(&err.into()))
}

/// An error raised by this binding rather than by the client beneath it.
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::new(
        Status::InvalidArg,
        format!("{KIND_INVALID}{KIND_SEPARATOR}{}", message.into()),
    )
}

/// The message `index.js` decodes.
///
/// A code from the broker wins over the message. Without one, classification
/// is deliberately conservative: anything unrecognised gets `FELIX_ERROR`,
/// because a wrong category is worse than a general one.
pub(crate) fn encode(err: &anyhow::Error) -> String {
    let text = format!("{err:#}");

    if let Some(broker) = err.chain().find_map(|e| e.downcast_ref::<BrokerError>()) {
        let mut meta = serde_json::json!({
            "code": broker.code.as_str(),
            "retry": broker.retry.as_str(),
        });
        if let Some(detail) = &broker.detail {
            meta["detail"] = serde_json::to_value(detail).unwrap_or_default();
        }
        let kind = kind_for_code(broker.code.as_str(), broker.retry);
        return format!("{kind} {meta}\n{text}");
    }
    // A redirect the client could not follow: the same fact the `not_leader`
    // code carries, so it reads the same way.
    if err
        .chain()
        .any(|e| e.downcast_ref::<NotLeaderError>().is_some())
    {
        let meta = serde_json::json!({
            "code": "not_leader",
            "retry": RetryClass::Redirect.as_str(),
        });
        return format!("{KIND_SHARD_UNAVAILABLE} {meta}\n{text}");
    }
    let kind = if err
        .chain()
        .any(|e| e.downcast_ref::<SubscribeCursorError>().is_some())
    {
        KIND_CURSOR
    } else if err
        .chain()
        .any(|e| e.downcast_ref::<SubscriptionLost>().is_some())
    {
        KIND_CONNECTION
    } else {
        kind_from_text(&text.to_ascii_lowercase())
    };
    format!("{kind}{KIND_SEPARATOR}{text}")
}

/// The kind for a broker code.
///
/// `outcome_unknown` wins over the code: whatever went wrong, the caller's
/// next step is decided by "this may have been written". A code this version
/// does not know is a plain `FelixError`, with its retry class still on it.
pub(crate) fn kind_for_code(code: &str, retry: RetryClass) -> &'static str {
    if retry == RetryClass::OutcomeUnknown {
        return KIND_OUTCOME_UNKNOWN;
    }
    match code {
        "unauthenticated" | "forbidden" => KIND_AUTH,
        "not_found" => KIND_NOT_FOUND,
        "shard_unavailable" | "not_leader" => KIND_SHARD_UNAVAILABLE,
        "overloaded" => KIND_OVERLOADED,
        // This broker is going away; another one will take the request.
        "draining" => KIND_CONNECTION,
        _ => KIND_GENERIC,
    }
}

/// The fallback for an error without a code.
fn kind_from_text(lower: &str) -> &'static str {
    if lower.contains("unauthorized")
        || lower.contains("permission")
        || lower.contains("forbidden")
        || lower.contains("token")
    {
        KIND_AUTH
    } else if lower.contains("unknown tenant")
        || lower.contains("unknown stream")
        || lower.contains("unknown cache")
        || lower.contains("unknown namespace")
        || lower.contains("not found")
    {
        KIND_NOT_FOUND
    } else if lower.contains("cursor") || lower.contains("trimmed") || lower.contains("too old") {
        KIND_CURSOR
    } else if lower.contains("connect")
        || lower.contains("connection")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("no broker")
        || lower.contains("not leader")
    {
        KIND_CONNECTION
    } else {
        KIND_GENERIC
    }
}

#[cfg(test)]
mod tests;

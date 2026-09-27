//! Typed exceptions, so Python can branch on *why* something failed.
//!
//! A single `FelixError` carrying prose would push callers into matching on
//! message text, which breaks the moment a message is reworded. The split here
//! follows what actually changes an application's next step: a connection
//! failure or an unavailable shard is worth retrying, an authorization failure
//! is not, and a write whose outcome is unknown is safe to resend only when it
//! is idempotent.
//!
//! A broker that negotiated error codes says which it is, and the class is
//! chosen from that. Only an error without a code (an older broker, or a
//! failure inside the client) falls back to reading the message.
use felix_client::{BrokerError, NotLeaderError, SubscribeCursorError, SubscriptionLost};
use felix_wire::RetryClass;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;

create_exception!(
    _felix,
    FelixError,
    PyException,
    "Base class for every Felix error."
);
create_exception!(
    _felix,
    ConnectionError,
    FelixError,
    "The broker could not be reached, the connection was lost mid-call, or the broker is shutting down."
);
create_exception!(
    _felix,
    AuthError,
    FelixError,
    "The token was rejected, or it does not carry the permission this call needs."
);
create_exception!(
    _felix,
    NotFoundError,
    FelixError,
    "The tenant, namespace, stream, or cache does not exist on the broker."
);
create_exception!(
    _felix,
    CursorError,
    FelixError,
    "The requested start offset is no longer available (retention discarded it)."
);
create_exception!(
    _felix,
    ShardUnavailableError,
    FelixError,
    "No broker can serve the shard right now, typically while it moves. Nothing was applied; retrying is safe."
);
create_exception!(
    _felix,
    OverloadedError,
    FelixError,
    "The broker is shedding load. Nothing was applied; retry after a pause."
);
create_exception!(
    _felix,
    OutcomeUnknownError,
    FelixError,
    "The write may or may not have been applied. Only an idempotent request is safe to resend."
);

/// Which exception class an error becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Generic,
    Connection,
    Auth,
    NotFound,
    Cursor,
    ShardUnavailable,
    Overloaded,
    OutcomeUnknown,
}

/// An error from the Rust client, decided but not yet a Python object.
#[derive(Debug)]
pub(crate) struct Classified {
    pub(crate) kind: Kind,
    pub(crate) text: String,
    pub(crate) code: Option<String>,
    pub(crate) retry: Option<&'static str>,
    pub(crate) reason: Option<String>,
    pub(crate) retry_after_ms: Option<u64>,
}

/// Decide what an error from the Rust client is.
///
/// A code from the broker wins over the message. Without one, classification
/// is deliberately conservative: anything unrecognised is the base
/// `FelixError`, because a wrong category is worse than a general one.
pub(crate) fn classify(err: &anyhow::Error) -> Classified {
    let text = format!("{err:#}");
    let mut out = Classified {
        kind: Kind::Generic,
        text,
        code: None,
        retry: None,
        reason: None,
        retry_after_ms: None,
    };

    if let Some(broker) = err.chain().find_map(|e| e.downcast_ref::<BrokerError>()) {
        out.kind = kind_for_code(broker.code.as_str(), broker.retry);
        out.code = Some(broker.code.as_str().to_string());
        out.retry = Some(broker.retry.as_str());
        if let Some(detail) = &broker.detail {
            out.reason = detail.reason.clone();
            out.retry_after_ms = detail.retry_after_ms;
        }
        return out;
    }
    // A redirect the client could not follow: the same fact the `not_leader`
    // code carries, so it reads the same way.
    if err
        .chain()
        .any(|e| e.downcast_ref::<NotLeaderError>().is_some())
    {
        out.kind = Kind::ShardUnavailable;
        out.code = Some("not_leader".to_string());
        out.retry = Some(RetryClass::Redirect.as_str());
        return out;
    }
    if err
        .chain()
        .any(|e| e.downcast_ref::<SubscribeCursorError>().is_some())
    {
        out.kind = Kind::Cursor;
        return out;
    }
    // A subscription whose broker went away, which a caller retries against
    // another broker rather than treating as the end of the stream.
    if err
        .chain()
        .any(|e| e.downcast_ref::<SubscriptionLost>().is_some())
    {
        out.kind = Kind::Connection;
        return out;
    }
    out.kind = kind_from_text(&out.text.to_ascii_lowercase());
    out
}

/// The class for a broker code.
///
/// `outcome_unknown` wins over the code: whatever went wrong, the caller's
/// next step is decided by "this may have been written". A code this version
/// does not know is a plain `FelixError`, with its retry class still on it.
pub(crate) fn kind_for_code(code: &str, retry: RetryClass) -> Kind {
    if retry == RetryClass::OutcomeUnknown {
        return Kind::OutcomeUnknown;
    }
    match code {
        "unauthenticated" | "forbidden" => Kind::Auth,
        "not_found" => Kind::NotFound,
        "shard_unavailable" | "not_leader" => Kind::ShardUnavailable,
        "overloaded" => Kind::Overloaded,
        // This broker is going away; another one will take the request.
        "draining" => Kind::Connection,
        _ => Kind::Generic,
    }
}

/// Turn an error from the Rust client into the Python exception for it.
///
/// Every instance carries `code`, `retry` and `detail`, `None` when the broker
/// sent no code, so a caller can read them without `getattr`.
pub(crate) fn to_py_err(err: anyhow::Error) -> PyErr {
    let classified = classify(&err);
    let text = classified.text.clone();
    let py_err = match classified.kind {
        Kind::Generic => FelixError::new_err(text),
        Kind::Connection => ConnectionError::new_err(text),
        Kind::Auth => AuthError::new_err(text),
        Kind::NotFound => NotFoundError::new_err(text),
        Kind::Cursor => CursorError::new_err(text),
        Kind::ShardUnavailable => ShardUnavailableError::new_err(text),
        Kind::Overloaded => OverloadedError::new_err(text),
        Kind::OutcomeUnknown => OutcomeUnknownError::new_err(text),
    };
    if classified.code.is_some() {
        Python::attach(|py| {
            // Best effort: without the attributes it is still the right class,
            // and the class-level `None`s keep them readable.
            let _ = annotate(py, &py_err, &classified);
        });
    }
    py_err
}

/// Register the exception types on the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("FelixError", py.get_type::<FelixError>())?;
    module.add("ConnectionError", py.get_type::<ConnectionError>())?;
    module.add("AuthError", py.get_type::<AuthError>())?;
    module.add("NotFoundError", py.get_type::<NotFoundError>())?;
    module.add("CursorError", py.get_type::<CursorError>())?;
    module.add(
        "ShardUnavailableError",
        py.get_type::<ShardUnavailableError>(),
    )?;
    module.add("OverloadedError", py.get_type::<OverloadedError>())?;
    module.add("OutcomeUnknownError", py.get_type::<OutcomeUnknownError>())?;
    // Class-level defaults, so an error without a code still answers `None`.
    let base = py.get_type::<FelixError>();
    for name in ["code", "retry", "detail"] {
        base.setattr(name, py.None())?;
    }
    Ok(())
}

/// The fallback for an error without a code.
fn kind_from_text(lower: &str) -> Kind {
    if lower.contains("unauthorized")
        || lower.contains("permission")
        || lower.contains("forbidden")
        || lower.contains("token")
    {
        return Kind::Auth;
    }
    if lower.contains("unknown tenant")
        || lower.contains("unknown stream")
        || lower.contains("unknown cache")
        || lower.contains("not found")
    {
        return Kind::NotFound;
    }
    if lower.contains("cursor") || lower.contains("trimmed") || lower.contains("too old") {
        return Kind::Cursor;
    }
    if lower.contains("connect")
        || lower.contains("connection")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("no broker")
        || lower.contains("not leader")
    {
        return Kind::Connection;
    }
    Kind::Generic
}

fn annotate(py: Python<'_>, err: &PyErr, classified: &Classified) -> PyResult<()> {
    let value = err.value(py);
    value.setattr("code", classified.code.as_deref())?;
    value.setattr("retry", classified.retry)?;
    let detail = PyDict::new(py);
    if let Some(reason) = &classified.reason {
        detail.set_item("reason", reason)?;
    }
    if let Some(ms) = classified.retry_after_ms {
        detail.set_item("retry_after_ms", ms)?;
    }
    if !detail.is_empty() {
        value.setattr("detail", detail)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

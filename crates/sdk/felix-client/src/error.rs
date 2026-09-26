//! Errors a caller is expected to downcast to and act on.
//!
//! Each is typed rather than formatted into a string because the caller's
//! next step depends on its fields, not its message.

use felix_wire::{CursorErrorReason, ErrorCode, ErrorDetail, PublishRefusalReason, RetryClass};

/// The broker would not append an idempotent publish, and said why.
///
/// Carried as a typed error so a producer can act on the reason rather than
/// parse the message: a sequence gap means stop, an unknown producer means
/// start again under a new id, and neither is a transport failure to retry.
/// Recover it from an `anyhow::Error` with `downcast_ref`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRefused {
    /// Why the broker refused.
    pub reason: PublishRefusalReason,
    /// The broker's explanation, for logs.
    pub message: String,
}

impl std::fmt::Display for PublishRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "publish refused ({:?}): {}", self.reason, self.message)
    }
}

impl std::error::Error for PublishRefused {}

/// The broker does not own this shard, and names the one that does.
///
/// Typed rather than a formatted string because it is an instruction, not a
/// failure: the subscription is available, just somewhere else, and a caller
/// that can reach the named broker can simply go there. [`ClusterClient`] does
/// exactly that.
///
/// [`ClusterClient`]: crate::ClusterClient
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct NotLeaderError {
    /// The broker that owns the shard.
    pub node_id: String,
    /// Where clients reach it, when the cluster has been told. Absent means the
    /// owner is known by name only, and a caller has to resolve it some other
    /// way -- discovery, or its own configuration.
    pub addr: Option<String>,
    /// The assignment epoch this answer describes.
    pub generation: u64,
}

impl std::fmt::Display for NotLeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.addr {
            Some(addr) => write!(
                f,
                "shard is owned by {} at {addr} (generation {})",
                self.node_id, self.generation
            ),
            None => write!(
                f,
                "shard is owned by {}, whose client address is not published (generation {})",
                self.node_id, self.generation
            ),
        }
    }
}

/// A resume could not start where it asked to.
///
/// Typed rather than a formatted string so callers can branch: `TooOld` means
/// retention has passed the requested offset and the application must decide
/// whether to accept the gap or start from `earliest`; `InFuture` means the
/// offset does not exist yet, which usually means a checkpoint was written from
/// a different stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub struct SubscribeCursorError {
    /// Whether the offset was too old or not yet written.
    pub reason: CursorErrorReason,
    /// The offset that was asked for.
    pub requested: u64,
    /// The nearest offset that would have worked: the oldest retained for
    /// `TooOld`, the current tail for `InFuture`.
    pub available: u64,
}

impl std::fmt::Display for SubscribeCursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            CursorErrorReason::TooOld => write!(
                f,
                "offset {} is no longer retained; oldest available is {}",
                self.requested, self.available
            ),
            CursorErrorReason::InFuture => write!(
                f,
                "offset {} is past the end of the stream; tail is {}",
                self.requested, self.available
            ),
        }
    }
}

/// A subscription's event stream broke before the broker ended it.
///
/// The connection dropped, timed out or was reset. A clean end from the
/// broker is `Ok(None)` instead, so a consumer loop that stops on `None` does
/// not mistake a dead broker for a finished stream. On a durable stream,
/// resubscribe from the last delivered offset plus one;
/// [`crate::ClusterSubscription`] does that itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("subscription connection lost: {reason}")]
pub struct SubscriptionLost {
    /// What the transport reported.
    pub reason: String,
}

/// The broker refused a request and sent a typed code with it.
///
/// Only a broker that advertised `FEATURE_ERROR_CODES` sends one; from any
/// other the same failure stays a plain error with the same text. The retry
/// class is the broker's statement of whether the request may have been
/// applied, and is the field to act on: a code this client does not know still
/// carries one. Recover it from an `anyhow::Error` with `downcast_ref`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerError {
    /// What went wrong.
    pub code: ErrorCode,
    /// What the caller may do about it.
    pub retry: RetryClass,
    /// Extra facts, such as why a shard is unavailable.
    pub detail: Option<ErrorDetail>,
    /// The broker's explanation, for logs.
    pub message: String,
    context: &'static str,
}

impl BrokerError {
    /// Why a `shard_unavailable` shard cannot be served, when the broker said.
    pub fn reason(&self) -> Option<&str> {
        self.detail.as_ref()?.reason.as_deref()
    }
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.context, self.message)
    }
}

impl std::error::Error for BrokerError {}

/// A refusal as the caller sees it: typed when the broker sent a code, and
/// otherwise the `"{context}: {message}"` text it has always been.
pub(crate) fn refused(
    context: &'static str,
    message: String,
    code: Option<ErrorCode>,
    retry: Option<RetryClass>,
    detail: Option<ErrorDetail>,
) -> anyhow::Error {
    match code {
        Some(code) => BrokerError {
            retry: retry.unwrap_or_else(|| code.default_retry()),
            code,
            detail,
            message,
            context,
        }
        .into(),
        None => anyhow::anyhow!("{context}: {message}"),
    }
}

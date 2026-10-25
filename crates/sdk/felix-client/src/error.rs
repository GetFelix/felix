//! Errors a caller is expected to downcast to and act on.
//!
//! Each is typed rather than formatted into a string because the caller's
//! next step depends on its fields, not its message.

use felix_wire::{CursorErrorReason, ErrorCode, ErrorDetail, PublishRefusalReason, RetryClass};

/// Authentication failed, and retrying will not change that.
///
/// Either the broker refused the credential ([`AuthErrorKind::Refused`]) or
/// the token source could not produce one: the provider failed, answered
/// with something unusable, panicked, or took longer than
/// [`FETCH_TIMEOUT`](crate::FETCH_TIMEOUT) ([`AuthErrorKind::Token`]). The
/// message is the provider's or broker's own, unchanged.
///
/// Typed so the client and its bindings classify an auth failure by this
/// marker rather than by words in the message. A cluster client does not
/// retry it: a provider call can rotate a refresh token, so repeating a
/// failed one costs a rotation each time. Recover it from an
/// `anyhow::Error` with `downcast_ref`, including when it is wrapped in context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthError {
    /// Whether the broker or the token source failed.
    pub kind: AuthErrorKind,
    /// The provider's or broker's message.
    pub message: String,
}

/// Which side of authentication failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthErrorKind {
    /// The broker refused the credential.
    Refused,
    /// The token source failed to produce a token.
    Token,
}

impl AuthError {
    pub(crate) fn refused(message: impl Into<String>) -> Self {
        Self {
            kind: AuthErrorKind::Refused,
            message: message.into(),
        }
    }

    pub(crate) fn token(message: impl Into<String>) -> Self {
        Self {
            kind: AuthErrorKind::Token,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            AuthErrorKind::Refused => write!(f, "auth rejected: {}", self.message),
            AuthErrorKind::Token => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for AuthError {}

/// Whether the token source, not the broker, failed.
pub(crate) fn is_token_failure(err: &anyhow::Error) -> bool {
    err.downcast_ref::<AuthError>()
        .is_some_and(|a| a.kind == AuthErrorKind::Token)
}

/// Mark a token source's failure as an [`AuthError`], keeping its text. An
/// error that already is one passes through.
pub(crate) fn token_failure(err: anyhow::Error) -> anyhow::Error {
    if err.is::<AuthError>() {
        err
    } else {
        AuthError::token(format!("{err:#}")).into()
    }
}

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

/// A durable-stream subscription fell behind and records were dropped,
/// by the broker's queue for it or by this client's own.
///
/// The last thing the subscription yields. Nothing at or above `resume_from`
/// came before it, but a record below it may not have either: the broker's
/// connection writer can drop a frame for a slow subscription without
/// reporting it. To carry on without a gap, resubscribe after the last event
/// received, or from `resume_from` if none was;
/// [`crate::ClusterSubscription`] and [`crate::ShardedSubscription`] do that
/// themselves. Only a broker that advertised
/// `FEATURE_SUBSCRIPTION_LAGGED` reports its own drops this way; an older one
/// keeps the subscription going and a drop shows as a jump in offsets.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("subscription fell behind; records from offset {resume_from} were dropped")]
pub struct SubscriptionLagged {
    /// The first dropped offset.
    pub resume_from: u64,
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

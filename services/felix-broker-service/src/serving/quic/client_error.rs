//! What a client is told when its request fails.
//!
//! Every failure site states a code through [`ClientError`], and the writer
//! decides per control stream whether the code reaches the wire: only a client
//! that offered `FEATURE_ERROR_CODES` gets it, so every other client sees the
//! same frames it always did. See the error-code table in `docs/protocol.md`.

use std::sync::atomic::{AtomicBool, Ordering};

use felix_wire::{ErrorCode, ErrorDetail, Message, RetryClass};

use crate::serving::forward::ForwardError;
use crate::shards::lifecycle::fence::Fenced;
use crate::shards::routing::Reason;

/// What a `moving` refusal suggests waiting. A switch-over is normally tens of
/// milliseconds; a move still going after the hold window is not one that the
/// next millisecond will finish.
const MOVING_RETRY_AFTER_MS: u64 = 100;
/// `detail.reason` on an `overloaded` refusal caused by the tenant's quota
/// rather than the broker's own load.
pub(crate) const TENANT_QUOTA_REASON: &str = "tenant_quota";
/// `detail.reason` on an `overloaded` refusal because the publish queue had
/// no room for the tenant.
pub(crate) const QUEUE_FULL_REASON: &str = "publish_queue_full";
/// What a publish refused for a full queue is told to wait. Room comes back as
/// fast as claims run, so the hint is short; it is there so a client backs off
/// at all rather than resending into the same full queue.
const QUEUE_FULL_RETRY_AFTER_MS: u64 = 10;

/// A failed request, as the client will see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClientError {
    code: ErrorCode,
    retry: RetryClass,
    detail: Option<ErrorDetail>,
    message: String,
}

impl ClientError {
    /// An error with the code's default retry class.
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            retry: code.default_retry(),
            code,
            detail: None,
            message: message.into(),
        }
    }

    pub(crate) fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, message)
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, message)
    }

    pub(crate) fn overloaded(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Overloaded, message)
    }

    pub(crate) fn limit_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::LimitExceeded, message)
    }

    pub(crate) fn draining(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Draining, message)
    }

    /// The tenant is over its publish quota; nothing was queued. `overloaded`
    /// because that is what an older client already backs off on, with a
    /// reason and a wait for one that reads them.
    pub(crate) fn tenant_quota(wait: std::time::Duration) -> Self {
        let millis = (wait.as_millis() as u64).max(1);
        Self::overloaded(format!(
            "tenant publish quota exceeded; retry in {millis} ms"
        ))
        .with_reason(TENANT_QUOTA_REASON)
        .with_retry_after(millis)
    }

    /// The publish queue had no room for the tenant; nothing was queued.
    /// `overloaded`, which every client already backs off on and retries.
    /// Keeps the words "publish queue full" for clients that match on text.
    pub(crate) fn queue_full() -> Self {
        Self::overloaded(format!(
            "publish queue full; retry in {QUEUE_FULL_RETRY_AFTER_MS} ms"
        ))
        .with_reason(QUEUE_FULL_REASON)
        .with_retry_after(QUEUE_FULL_RETRY_AFTER_MS)
    }

    /// A failure inside the broker whose effect on the request is unknown.
    /// Sites where nothing could have been applied say so with
    /// [`Self::with_retry`].
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    /// The shard cannot be served right now, and why.
    ///
    /// A shard that is moving has already been waited on for the hold window,
    /// so the client is told to pause briefly rather than retry at once.
    pub(crate) fn unavailable(reason: &Reason, message: impl Into<String>) -> Self {
        let error = Self::new(ErrorCode::ShardUnavailable, message).with_reason(reason.wire_name());
        match reason {
            Reason::Moving => error.with_retry_after(MOVING_RETRY_AFTER_MS),
            _ => error,
        }
    }

    /// Put `context` in front of the message, as `"{context}: {message}"`.
    pub(crate) fn prefixed(mut self, context: &str) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }

    /// This broker's authority over the shard lapsed before it wrote anything.
    pub(crate) fn fenced(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ShardUnavailable, message)
            .with_reason(felix_wire::shard_unavailable_reason::FENCED)
    }

    /// Keep the code, say it in `message`.
    pub(crate) fn reworded(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub(crate) fn with_retry(mut self, retry: RetryClass) -> Self {
        self.retry = retry;
        self
    }

    fn with_reason(mut self, reason: &str) -> Self {
        self.detail.get_or_insert_with(ErrorDetail::default).reason = Some(reason.to_string());
        self
    }

    fn with_retry_after(mut self, millis: u64) -> Self {
        self.detail
            .get_or_insert_with(ErrorDetail::default)
            .retry_after_ms = Some(millis);
        self
    }

    pub(crate) fn code(&self) -> &ErrorCode {
        &self.code
    }

    pub(crate) fn retry(&self) -> RetryClass {
        self.retry
    }

    pub(crate) fn detail(&self) -> Option<&ErrorDetail> {
        self.detail.as_ref()
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    /// Classify an error that came up from below the serving layer.
    ///
    /// Looks through the whole chain for a type that says what happened; what
    /// matches nothing is `internal`, whose retry class admits the request may
    /// have been applied. The message is always the error's own text, so a
    /// client without codes sees exactly what it saw before.
    pub(crate) fn from_anyhow(err: &anyhow::Error) -> Self {
        let message = err.to_string();
        for cause in err.chain() {
            if let Some(known) = cause.downcast_ref::<ClientError>() {
                return known.clone();
            }
            if let Some(refused) = cause.downcast_ref::<Fenced>() {
                return Self::from(*refused).reworded(message);
            }
            if let Some(broker) = cause.downcast_ref::<felix_broker::BrokerError>() {
                return Self::classify_broker(broker, message);
            }
            if let Some(quorum) = cause.downcast_ref::<felix_replication::quorum::QuorumError>() {
                let code = match quorum {
                    felix_replication::quorum::QuorumError::TimedOut { .. } => {
                        ErrorCode::QuorumTimeout
                    }
                    felix_replication::quorum::QuorumError::LeadershipLost { .. } => {
                        ErrorCode::LeadershipLost
                    }
                };
                return Self::new(code, message);
            }
            if let Some(forward) = cause.downcast_ref::<ForwardError>() {
                return Self::classify_forward(forward, message);
            }
        }
        Self::internal(message)
    }

    /// A publish that never reached the queue: the fence refused it at
    /// admission, or the queue had no room.
    pub(crate) fn not_enqueued(err: &anyhow::Error) -> Self {
        if let Some(known) = err.downcast_ref::<ClientError>() {
            return known.clone();
        }
        match err.downcast_ref::<Fenced>() {
            Some(refused) => Self::from(*refused),
            None => Self::overloaded(err.to_string()),
        }
    }

    /// Classify a broker-core error, keeping the text the caller already chose.
    pub(crate) fn from_broker(err: &felix_broker::BrokerError, message: impl Into<String>) -> Self {
        Self::classify_broker(err, message.into())
    }

    fn classify_broker(err: &felix_broker::BrokerError, message: String) -> Self {
        use felix_broker::BrokerError as E;
        match err {
            E::StreamNotFound { .. }
            | E::TenantNotFound(_)
            | E::NamespaceNotFound { .. }
            // The stream was removed or replaced under a cached handle; looking
            // it up again is what the client's retry does.
            | E::StreamHandleInactive(_) => Self::not_found(message),
            E::CapacityTooLarge
            | E::CursorTooOld { .. }
            | E::CursorInFuture { .. }
            | E::DurabilityChangeRequiresRecreate { .. }
            | E::StreamNotDurable { .. }
            | E::SequenceGap { .. }
            | E::UnknownProducer { .. }
            | E::SequenceExpired { .. }
            | E::SequenceReused { .. }
            | E::GroupOffsetNotHandedOut { .. }
            | E::CommitNeedsDurableStream => Self::invalid(message),
            E::MalformedCommit(_) => Self::internal(message),
            E::StateViewBusy => Self::overloaded(message),
            // A broker configured without the storage the stream needs will
            // answer the same way until someone changes its configuration.
            E::DurableStorageNotConfigured { .. } => {
                Self::internal(message).with_retry(RetryClass::Fatal)
            }
            E::Storage(_) => Self::new(ErrorCode::Storage, message),
            // Written to a log that was then reset: it may or may not survive.
            E::PublishSuperseded { .. } => Self::new(ErrorCode::Unacknowledged, message),
            // Nothing was read; the shard is served again here once a mark or
            // a lease comes back, or by its new owner.
            E::NotReadable { reason, .. } | E::StateNotReadable(reason) => {
                use felix_wire::shard_unavailable_reason as wire;
                let why = match reason {
                    felix_broker::NotReadable::Settling => wire::NOT_READY,
                    felix_broker::NotReadable::Refused => wire::FENCED,
                };
                Self::new(ErrorCode::ShardUnavailable, message).with_reason(why)
            }
        }
    }

    fn classify_forward(err: &ForwardError, message: String) -> Self {
        use felix_wire::internal::ErrorCode as Internal;
        use felix_wire::shard_unavailable_reason as reason;
        match err {
            ForwardError::Indeterminate { .. } => Self::new(ErrorCode::Unacknowledged, message),
            ForwardError::Refused { code, .. } => match code {
                Some(Internal::Overload) => Self::overloaded(message),
                Some(Internal::StaleRoute) => {
                    Self::new(ErrorCode::ShardUnavailable, message).with_reason(reason::STALE)
                }
                Some(Internal::FencedEpoch) => {
                    Self::new(ErrorCode::ShardUnavailable, message).with_reason(reason::FENCED)
                }
                Some(Internal::Unavailable) => {
                    Self::new(ErrorCode::ShardUnavailable, message).with_reason(reason::NOT_READY)
                }
                // The owner checks the client's own credential on a forward.
                Some(Internal::Unauthorized) => Self::forbidden(message),
                Some(Internal::StorageFailed) => Self::new(ErrorCode::Storage, message),
                Some(
                    Internal::ProtocolVersion
                    | Internal::Malformed
                    | Internal::UnsupportedKind
                    | Internal::LogGap
                    | Internal::LogConflict,
                ) => Self::internal(message).with_retry(RetryClass::Retry),
                // Unreachable, redirected in a loop, or out of budget before
                // anything was sent: the owner could not be reached.
                None => Self::new(ErrorCode::ShardUnavailable, message)
                    .with_reason(reason::OWNER_UNAVAILABLE),
            },
        }
    }

    /// As an `error` message, code included; the writer strips the code for a
    /// client that did not ask for it.
    pub(crate) fn into_message(self) -> Message {
        Message::Error {
            message: self.message,
            code: Some(self.code),
            retry: Some(self.retry),
            detail: self.detail,
        }
    }

    /// As a `publish_error` answering `request_id`.
    pub(crate) fn into_publish_error(self, request_id: u64) -> Message {
        Message::PublishError {
            request_id,
            message: self.message,
            code: Some(self.code),
            retry: Some(self.retry),
            detail: self.detail,
        }
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ClientError {}

/// Nothing was written: the fence refuses before the claim, so the write is
/// safe to send again to the shard's new owner.
impl From<Fenced> for ClientError {
    fn from(refused: Fenced) -> Self {
        Self::fenced(refused.to_string())
    }
}

/// Whether this control stream's client reads error codes.
///
/// Set once when the stream authenticates and read by the writer, which is the
/// one place every answer passes through. The flags are only ever set before
/// the `AuthOk` is queued, and the queue orders everything after it.
#[derive(Debug, Default)]
pub(crate) struct ErrorCodeSupport {
    json: AtomicBool,
    binary_ack: AtomicBool,
    binary_ack_detail: AtomicBool,
    ack_offset: AtomicBool,
}

impl ErrorCodeSupport {
    /// Record what the client offered in `Auth`.
    pub(crate) fn negotiate(&self, peer_features: u32, peer_flags: u16) {
        self.json.store(
            felix_wire::supports_feature(peer_features, felix_wire::FEATURE_ERROR_CODES),
            Ordering::Relaxed,
        );
        self.binary_ack.store(
            felix_wire::supports(peer_flags, felix_wire::FLAG_BINARY_PUBLISH_ACK_CODE),
            Ordering::Relaxed,
        );
        self.binary_ack_detail.store(
            felix_wire::supports(peer_flags, felix_wire::FLAG_BINARY_PUBLISH_ACK_DETAIL),
            Ordering::Relaxed,
        );
        self.ack_offset.store(
            felix_wire::supports(peer_flags, felix_wire::FLAG_BINARY_PUBLISH_ACK_OFFSET),
            Ordering::Relaxed,
        );
    }

    /// The message as this client should receive it.
    pub(crate) fn shape(&self, message: Message) -> Message {
        let message = if self.ack_offset() {
            message
        } else {
            message.without_ack_offset()
        };
        if self.json.load(Ordering::Relaxed) {
            message
        } else {
            message.without_error_code()
        }
    }

    /// Whether a successful ack, binary or JSON, may carry its offset.
    pub(crate) fn ack_offset(&self) -> bool {
        self.ack_offset.load(Ordering::Relaxed)
    }

    /// Whether a failed binary ack may carry its code.
    pub(crate) fn binary_ack(&self) -> bool {
        self.binary_ack.load(Ordering::Relaxed)
    }

    /// Whether a failed binary ack may carry its detail as well as its code.
    pub(crate) fn binary_ack_detail(&self) -> bool {
        self.binary_ack_detail.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;

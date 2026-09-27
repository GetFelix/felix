//! Typed error codes for `error` and `publish_error`.
//!
//! Only sent to a client that offered `FEATURE_ERROR_CODES`. The code says what
//! went wrong and the retry class says what the client may do about it; they
//! travel separately so a client that does not know a code still knows whether
//! retrying is safe. That is also why an unknown code decodes rather than
//! failing, unlike an unknown frame flag: nothing about the frame's layout
//! depends on it.
//!
//! The table in `docs/protocol.md` lists every code, its retry class and when
//! a broker sends it.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// What went wrong, as a stable snake_case name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// The control stream has not authenticated, or the credential was refused.
    Unauthenticated,
    /// The credential does not grant this operation.
    Forbidden,
    /// The tenant, namespace, stream, cache or group does not exist on this
    /// broker. On a cluster it may simply not have arrived yet.
    NotFound,
    /// The request is malformed or asks for something that can never succeed
    /// as sent.
    InvalidRequest,
    /// Nobody can serve the shard right now. `ErrorDetail::reason` says why.
    ShardUnavailable,
    /// Another broker owns the shard. Sent as a code only where `not_leader`
    /// cannot be: that message names the owner, this one does not.
    NotLeader,
    /// The write is durable on the leader but a majority did not confirm it in
    /// time. It may survive; it may not.
    QuorumTimeout,
    /// Leadership of the shard moved after the leader wrote the batch and
    /// before a majority held it. Whether the new leader has it is unknown.
    LeadershipLost,
    /// The broker stopped waiting for the write's outcome: a forwarded batch
    /// whose answer never came, or a commit that outlasted the ack wait.
    Unacknowledged,
    /// The broker is shedding load and did not take the request.
    Overloaded,
    /// The request exceeds a configured limit (payload size, subscriptions per
    /// connection) and will keep doing so.
    LimitExceeded,
    /// The broker is shutting down and takes no new work.
    Draining,
    /// Something failed inside the broker.
    Internal,
    /// The storage layer failed.
    Storage,
    /// A consumer-group ack or nack for a claim this broker does not know:
    /// the group's in-memory state was rebuilt (evicted while idle, or the
    /// shard failed over) after the record was handed out. Nothing was
    /// applied, and the record will be delivered again.
    StaleClaim,
    /// A code this version does not know. Its retry class still applies.
    Unknown(String),
}

impl ErrorCode {
    /// Every code this version defines, for tables and checks.
    pub const ALL: [ErrorCode; 15] = [
        ErrorCode::Unauthenticated,
        ErrorCode::Forbidden,
        ErrorCode::NotFound,
        ErrorCode::InvalidRequest,
        ErrorCode::ShardUnavailable,
        ErrorCode::NotLeader,
        ErrorCode::QuorumTimeout,
        ErrorCode::LeadershipLost,
        ErrorCode::Unacknowledged,
        ErrorCode::Overloaded,
        ErrorCode::LimitExceeded,
        ErrorCode::Draining,
        ErrorCode::Internal,
        ErrorCode::Storage,
        ErrorCode::StaleClaim,
    ];

    /// The wire name.
    pub fn as_str(&self) -> &str {
        match self {
            ErrorCode::Unauthenticated => "unauthenticated",
            ErrorCode::Forbidden => "forbidden",
            ErrorCode::NotFound => "not_found",
            ErrorCode::InvalidRequest => "invalid_request",
            ErrorCode::ShardUnavailable => "shard_unavailable",
            ErrorCode::NotLeader => "not_leader",
            ErrorCode::QuorumTimeout => "quorum_timeout",
            ErrorCode::LeadershipLost => "leadership_lost",
            ErrorCode::Unacknowledged => "unacknowledged",
            ErrorCode::Overloaded => "overloaded",
            ErrorCode::LimitExceeded => "limit_exceeded",
            ErrorCode::Draining => "draining",
            ErrorCode::Internal => "internal",
            ErrorCode::Storage => "storage",
            ErrorCode::StaleClaim => "stale_claim",
            ErrorCode::Unknown(name) => name,
        }
    }

    /// Parse a wire name. Never fails: a name this version does not know is
    /// kept as `Unknown`.
    pub fn parse(name: &str) -> ErrorCode {
        ErrorCode::ALL
            .into_iter()
            .find(|code| code.as_str() == name)
            .unwrap_or_else(|| ErrorCode::Unknown(name.to_string()))
    }

    /// The number the binary publish ack carries. `0` for `Unknown`, which a
    /// broker never sends.
    pub fn to_u16(&self) -> u16 {
        match self {
            ErrorCode::Unauthenticated => 1,
            ErrorCode::Forbidden => 2,
            ErrorCode::NotFound => 3,
            ErrorCode::InvalidRequest => 4,
            ErrorCode::ShardUnavailable => 5,
            ErrorCode::NotLeader => 6,
            ErrorCode::QuorumTimeout => 7,
            ErrorCode::LeadershipLost => 8,
            ErrorCode::Unacknowledged => 9,
            ErrorCode::Overloaded => 10,
            ErrorCode::LimitExceeded => 11,
            ErrorCode::Draining => 12,
            ErrorCode::Internal => 13,
            ErrorCode::Storage => 14,
            ErrorCode::StaleClaim => 15,
            ErrorCode::Unknown(_) => 0,
        }
    }

    /// The code for a binary-ack number. A number this version does not know
    /// becomes `Unknown("code_<n>")`.
    pub fn from_u16(value: u16) -> ErrorCode {
        ErrorCode::ALL
            .into_iter()
            .find(|code| code.to_u16() == value)
            .unwrap_or_else(|| ErrorCode::Unknown(format!("code_{value}")))
    }

    /// The retry class a broker sends with this code unless a site knows
    /// better.
    pub fn default_retry(&self) -> RetryClass {
        match self {
            ErrorCode::Unauthenticated
            | ErrorCode::Forbidden
            | ErrorCode::InvalidRequest
            | ErrorCode::LimitExceeded => RetryClass::Fatal,
            // A broker learns streams from the control plane, so a promoted
            // broker says "not found" for a stream it is about to serve.
            ErrorCode::NotFound | ErrorCode::Overloaded => RetryClass::RetryAfter,
            ErrorCode::ShardUnavailable | ErrorCode::Draining | ErrorCode::StaleClaim => {
                RetryClass::Retry
            }
            ErrorCode::NotLeader => RetryClass::Redirect,
            ErrorCode::QuorumTimeout
            | ErrorCode::LeadershipLost
            | ErrorCode::Unacknowledged
            | ErrorCode::Internal
            | ErrorCode::Storage => RetryClass::OutcomeUnknown,
            ErrorCode::Unknown(_) => RetryClass::Fatal,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(ErrorCode::parse(&name))
    }
}

/// What a client may do after an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryClass {
    /// Nothing was applied; sending the request again is safe.
    Retry,
    /// Nothing was applied; wait before sending again.
    /// `ErrorDetail::retry_after_ms` may say how long.
    RetryAfter,
    /// Nothing was applied; send it to another broker.
    Redirect,
    /// It may have been applied. Only an idempotent request is safe to send
    /// again.
    OutcomeUnknown,
    /// Sending it again will fail the same way.
    Fatal,
}

impl RetryClass {
    /// Every class this version defines.
    pub const ALL: [RetryClass; 5] = [
        RetryClass::Retry,
        RetryClass::RetryAfter,
        RetryClass::Redirect,
        RetryClass::OutcomeUnknown,
        RetryClass::Fatal,
    ];

    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            RetryClass::Retry => "retry",
            RetryClass::RetryAfter => "retry_after",
            RetryClass::Redirect => "redirect",
            RetryClass::OutcomeUnknown => "outcome_unknown",
            RetryClass::Fatal => "fatal",
        }
    }

    /// Parse a wire name. A class this version does not know is read as
    /// `Fatal`: not retrying is the one reading that cannot duplicate a write.
    pub fn parse(name: &str) -> RetryClass {
        RetryClass::ALL
            .into_iter()
            .find(|class| class.as_str() == name)
            .unwrap_or(RetryClass::Fatal)
    }

    /// The byte the binary publish ack carries.
    pub fn to_u8(self) -> u8 {
        match self {
            RetryClass::Retry => 1,
            RetryClass::RetryAfter => 2,
            RetryClass::Redirect => 3,
            RetryClass::OutcomeUnknown => 4,
            RetryClass::Fatal => 5,
        }
    }

    /// The class for a binary-ack byte, `Fatal` when unknown.
    pub fn from_u8(value: u8) -> RetryClass {
        RetryClass::ALL
            .into_iter()
            .find(|class| class.to_u8() == value)
            .unwrap_or(RetryClass::Fatal)
    }
}

impl fmt::Display for RetryClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for RetryClass {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RetryClass {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(RetryClass::parse(&name))
    }
}

/// Extra facts about an error. Every field is optional and unknown fields are
/// ignored, so either side can grow it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// For `shard_unavailable`: `not_assigned`, `owner_unavailable`,
    /// `not_ready`, `stale`, `fenced`, `moving` or `region_not_routable`. A string rather than an enum
    /// so a new reason reaches an old client as text instead of a decode
    /// failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// How long the broker suggests waiting. Always meant for `retry_after`;
    /// sent as a hint with `retry` where the broker has a better guess than
    /// "at once", as it does for a shard that is `moving`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// The `reason` values `shard_unavailable` carries.
pub mod shard_unavailable_reason {
    /// Placement has not assigned the shard.
    pub const NOT_ASSIGNED: &str = "not_assigned";
    /// The owner is known but unreachable or not live.
    pub const OWNER_UNAVAILABLE: &str = "owner_unavailable";
    /// The owner has not finished opening the shard.
    pub const NOT_READY: &str = "not_ready";
    /// This broker's routing view is behind the request's.
    pub const STALE: &str = "stale";
    /// The owner's epoch was superseded; it no longer leads the shard.
    pub const FENCED: &str = "fenced";
    /// The shard is moving to another broker and the move had not cut over
    /// in time. `ErrorDetail::retry_after_ms` may suggest when to try again.
    pub const MOVING: &str = "moving";
    /// The leader is in a region the broker has no bridge to, so it will not
    /// forward the request there. Connecting to the leader directly is not
    /// refused by this.
    pub const REGION_NOT_ROUTABLE: &str = "region_not_routable";
}

#[cfg(test)]
mod tests;

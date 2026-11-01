//! The typed reasons an internal request can be refused.

use crate::error::{Error, Result};

/// Why a forwarded request could not be served.
///
/// Typed because each needs a different response from the requester — see the
/// table in `docs/internal-protocol.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    /// The responder is behind the generation the requester named. It must not
    /// accept the write: it may no longer hold the shard.
    StaleRoute = 1,
    /// The owner cannot serve yet, e.g. still opening the shard.
    Unavailable = 2,
    /// The peer is not permitted. Not retryable.
    Unauthorized = 3,
    /// The owner is shedding load.
    Overload = 4,
    /// The peer spoke a version this broker does not know. Not retryable.
    ProtocolVersion = 5,
    /// The body did not decode. Not retryable.
    Malformed = 6,
    /// The owner accepted the request and the local write failed.
    StorageFailed = 7,
    /// A replication batch starts past the follower's tail. Applying it would
    /// leave a hole, and a log with a hole cannot be read back. The follower
    /// reports the offset it does want; the leader resumes there.
    LogGap = 8,
    /// A replication batch disagrees with bytes the follower has already
    /// stored. Records are never rewritten, so there is no repair for this:
    /// the two logs have diverged and progress stops here.
    LogConflict = 9,
    /// The sender named an epoch older than the responder's. It has been
    /// superseded and is no longer the leader, so its records must not be
    /// stored: it may have written them after losing the shard. Not retryable —
    /// the fence does not lift.
    FencedEpoch = 10,
    /// The responder does not know the kind that was sent, because it predates
    /// it. Not retryable against this peer, and not an error in the request:
    /// the feature simply is not there yet on the other side.
    UnsupportedKind = 11,
    /// The responder counts a newer replica set than the one the sender named;
    /// its generation is in `ReplicateError.expected_offset`. The sender's
    /// view of the shard is out of date, so it must not lead or stand at it.
    /// Not retryable. Sent only in answer to a fence or batch that carries a
    /// set, so only to a peer that offered `ELECTION`.
    StaleSet = 12,
}

impl ErrorCode {
    /// Parse a wire code; an unknown one is an error rather than a guess.
    pub fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(ErrorCode::StaleRoute),
            2 => Ok(ErrorCode::Unavailable),
            3 => Ok(ErrorCode::Unauthorized),
            4 => Ok(ErrorCode::Overload),
            5 => Ok(ErrorCode::ProtocolVersion),
            6 => Ok(ErrorCode::Malformed),
            7 => Ok(ErrorCode::StorageFailed),
            8 => Ok(ErrorCode::LogGap),
            9 => Ok(ErrorCode::LogConflict),
            10 => Ok(ErrorCode::FencedEpoch),
            11 => Ok(ErrorCode::UnsupportedKind),
            12 => Ok(ErrorCode::StaleSet),
            other => Err(Error::UnknownInternalErrorCode(other)),
        }
    }

    /// Whether a requester should try the same peer again.
    ///
    /// `LogGap` is retryable but not by re-sending the same batch: the follower
    /// names the offset it wants, and the leader resumes from there. `LogConflict`
    /// is absent deliberately — divergent logs do not converge by retrying.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::StaleRoute
                | ErrorCode::Unavailable
                | ErrorCode::Overload
                | ErrorCode::LogGap
        )
    }
}

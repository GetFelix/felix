//! Wire-level error type shared by the framing and codec modules.

/// Result of a wire encode or decode.
pub type Result<T> = std::result::Result<T, Error>;

/// Why a frame or message could not be encoded or decoded.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid magic number")]
    InvalidMagic,
    #[error("unsupported version {0}")]
    UnsupportedVersion(u16),
    #[error("frame too large")]
    FrameTooLarge,
    #[error("unsupported frame flags {0:#06x}")]
    UnknownFlags(u16),
    #[error("incomplete frame")]
    Incomplete,
    #[error("failed to serialize message")]
    Serialize(serde_json::Error),
    #[error("failed to deserialize message")]
    Deserialize(serde_json::Error),
    #[error("string field is not valid utf-8")]
    InvalidUtf8,
    #[error("unsupported internal message kind {0}")]
    UnsupportedInternalKind(u16),
    #[error("unknown internal error code {0}")]
    UnknownInternalErrorCode(u16),
    #[error("unknown internal ack mode {0}")]
    UnknownInternalAckMode(u8),
    #[error("unknown internal cache operation {0}")]
    UnknownInternalCacheOp(u8),
    #[error("unknown internal replica log {0}")]
    UnknownInternalReplicaLog(u8),
    #[error("unknown producer mark {0}")]
    UnknownInternalProducerMark(u8),
}

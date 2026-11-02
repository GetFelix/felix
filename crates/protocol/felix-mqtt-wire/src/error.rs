//! What can go wrong decoding or encoding a packet.

use crate::protocol::PacketType;
use crate::reason::ReasonCode;
use crate::text::StringError;
use crate::topic::TopicError;

/// A packet that must be refused. Incomplete input is not an error: decoding
/// returns `Ok(None)` and waits for more bytes.
///
/// Every variant means the connection should be closed. [`DecodeError::reason_code`]
/// gives the 5.0 `DISCONNECT` reason that fits, and the oversized-packet and
/// unsupported-version cases are the ones a server may answer first (with a
/// `CONNACK`) when it has a client to answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    #[error("remaining length is not a minimal variable byte integer of at most four bytes")]
    MalformedLength,
    #[error("packet type {0} is reserved")]
    InvalidPacketType(u8),
    #[error("{packet} has fixed header flags {flags:#06b}")]
    InvalidFlags { packet: PacketType, flags: u8 },
    #[error("packet of {size} bytes exceeds the limit of {max}")]
    PacketTooLarge { size: usize, max: u32 },
    #[error("the first packet is {0}, not CONNECT")]
    FirstPacketNotConnect(PacketType),
    #[error("packet ends inside a field")]
    Truncated,
    #[error("{0} bytes follow the last field")]
    TrailingBytes(usize),
    #[error("invalid string: {0}")]
    InvalidString(#[from] StringError),
    #[error("invalid topic name: {0}")]
    InvalidTopicName(TopicError),
    #[error("invalid topic filter: {0}")]
    InvalidTopicFilter(TopicError),
    #[error("protocol name is not MQTT")]
    InvalidProtocolName,
    #[error("protocol level {0} is not supported")]
    UnsupportedProtocolVersion(u8),
    #[error("invalid CONNECT flags {0:#010b}")]
    InvalidConnectFlags(u8),
    #[error("QoS 3 is not a QoS")]
    InvalidQos,
    #[error("DUP is set on a QoS 0 PUBLISH")]
    DupOnQos0,
    #[error("packet identifier is zero")]
    ZeroPacketId,
    #[error("reason code {code:#04x} is not valid in {packet}")]
    InvalidReasonCode { packet: PacketType, code: u8 },
    #[error("invalid subscription options {0:#04x}")]
    InvalidSubscriptionOptions(u8),
    #[error("{0} must carry at least one entry")]
    EmptyList(PacketType),
    #[error("No Local is set on a shared subscription")]
    NoLocalOnSharedSubscription,
    #[error("unknown property identifier {0:#04x}")]
    UnknownProperty(u32),
    #[error("property {id:#04x} is not allowed in {packet}")]
    PropertyNotAllowed { id: u8, packet: &'static str },
    #[error("property {0:#04x} appears more than once")]
    DuplicateProperty(u8),
    #[error("property {0:#04x} has an invalid value")]
    InvalidPropertyValue(u8),
    #[error("property {0:#04x} is required")]
    MissingProperty(u8),
    #[error("{what} exceeds the limit of {max}")]
    LimitExceeded { what: &'static str, max: usize },
}

impl DecodeError {
    /// The 5.0 reason code that describes this error.
    pub fn reason_code(&self) -> ReasonCode {
        match self {
            DecodeError::PacketTooLarge { .. } => ReasonCode::PacketTooLarge,
            DecodeError::UnsupportedProtocolVersion(_) => ReasonCode::UnsupportedProtocolVersion,
            DecodeError::InvalidTopicName(_) => ReasonCode::TopicNameInvalid,
            DecodeError::InvalidTopicFilter(_) => ReasonCode::TopicFilterInvalid,
            DecodeError::LimitExceeded { .. } => ReasonCode::QuotaExceeded,
            DecodeError::FirstPacketNotConnect(_)
            | DecodeError::InvalidProtocolName
            | DecodeError::InvalidConnectFlags(_)
            | DecodeError::DupOnQos0
            | DecodeError::ZeroPacketId
            | DecodeError::InvalidReasonCode { .. }
            | DecodeError::EmptyList(_)
            | DecodeError::NoLocalOnSharedSubscription
            | DecodeError::PropertyNotAllowed { .. }
            | DecodeError::DuplicateProperty(_)
            | DecodeError::InvalidPropertyValue(_)
            | DecodeError::MissingProperty(_) => ReasonCode::ProtocolError,
            _ => ReasonCode::MalformedPacket,
        }
    }
}

/// A packet that cannot be written as asked. The encoder refuses the same
/// things the decoder does, so a packet it writes is one a peer accepts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EncodeError {
    #[error("packet of {0} bytes is larger than a variable byte integer can describe")]
    PacketTooLarge(usize),
    #[error("{0} does not exist in this protocol version")]
    NotInVersion(&'static str),
    #[error("CONNECT is for a different protocol version than the one asked for")]
    VersionMismatch,
    #[error("reason code {code:#04x} is not valid in {packet}")]
    InvalidReasonCode { packet: PacketType, code: u8 },
    #[error("packet identifier is zero, or present at QoS 0, or missing above it")]
    InvalidPacketId,
    #[error("DUP is set on a QoS 0 PUBLISH")]
    DupOnQos0,
    #[error("invalid topic name: {0}")]
    InvalidTopicName(TopicError),
    #[error("invalid topic filter: {0}")]
    InvalidTopicFilter(TopicError),
    #[error("property {0:#04x} is not allowed here or has an invalid value")]
    InvalidProperty(u8),
    #[error("{0} must carry at least one entry")]
    EmptyList(PacketType),
    #[error("No Local is set on a shared subscription")]
    NoLocalOnSharedSubscription,
    #[error("a password needs a username under 3.1.1")]
    PasswordWithoutUsername,
    #[error("QoS 2 is not a valid maximum QoS")]
    InvalidMaximumQos,
    #[error("string or binary field is longer than 65535 bytes")]
    FieldTooLong,
    #[error("Authentication Method is required in AUTH")]
    MissingAuthenticationMethod,
}

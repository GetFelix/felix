//! The protocol level a connection speaks, the packet types, and QoS.

use std::fmt;

/// An MQTT protocol level. Level 3 (MQTT 3.1) is not supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Version {
    /// MQTT 3.1.1, protocol level 4.
    V311,
    /// MQTT 5.0, protocol level 5.
    V5,
}

impl Version {
    /// The protocol level byte in a `CONNECT` packet.
    pub fn level(self) -> u8 {
        match self {
            Version::V311 => 4,
            Version::V5 => 5,
        }
    }

    pub(crate) fn from_level(level: u8) -> Option<Self> {
        match level {
            4 => Some(Version::V311),
            5 => Some(Version::V5),
            _ => None,
        }
    }

    pub(crate) fn is_v5(self) -> bool {
        self == Version::V5
    }
}

/// Quality of service of a message or a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    pub(crate) fn from_bits(bits: u8) -> Option<Self> {
        match bits {
            0 => Some(QoS::AtMostOnce),
            1 => Some(QoS::AtLeastOnce),
            2 => Some(QoS::ExactlyOnce),
            _ => None,
        }
    }
}

/// The fifteen control packet types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PacketType {
    Connect,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
    Auth,
}

impl PacketType {
    /// The type nibble of the first byte.
    pub fn code(self) -> u8 {
        match self {
            PacketType::Connect => 1,
            PacketType::ConnAck => 2,
            PacketType::Publish => 3,
            PacketType::PubAck => 4,
            PacketType::PubRec => 5,
            PacketType::PubRel => 6,
            PacketType::PubComp => 7,
            PacketType::Subscribe => 8,
            PacketType::SubAck => 9,
            PacketType::Unsubscribe => 10,
            PacketType::UnsubAck => 11,
            PacketType::PingReq => 12,
            PacketType::PingResp => 13,
            PacketType::Disconnect => 14,
            PacketType::Auth => 15,
        }
    }

    /// `None` for the reserved type 0, and for type 15 before 5.0 defined `AUTH`.
    pub(crate) fn from_code(code: u8, version: Option<Version>) -> Option<Self> {
        Some(match code {
            1 => PacketType::Connect,
            2 => PacketType::ConnAck,
            3 => PacketType::Publish,
            4 => PacketType::PubAck,
            5 => PacketType::PubRec,
            6 => PacketType::PubRel,
            7 => PacketType::PubComp,
            8 => PacketType::Subscribe,
            9 => PacketType::SubAck,
            10 => PacketType::Unsubscribe,
            11 => PacketType::UnsubAck,
            12 => PacketType::PingReq,
            13 => PacketType::PingResp,
            14 => PacketType::Disconnect,
            15 if version != Some(Version::V311) => PacketType::Auth,
            _ => return None,
        })
    }

    /// The flag nibble the specification fixes for this type. `PUBLISH` has
    /// none: its flags carry DUP, QoS and RETAIN.
    pub(crate) fn fixed_flags(self) -> Option<u8> {
        match self {
            PacketType::Publish => None,
            PacketType::PubRel | PacketType::Subscribe | PacketType::Unsubscribe => Some(0b0010),
            _ => Some(0),
        }
    }
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PacketType::Connect => "CONNECT",
            PacketType::ConnAck => "CONNACK",
            PacketType::Publish => "PUBLISH",
            PacketType::PubAck => "PUBACK",
            PacketType::PubRec => "PUBREC",
            PacketType::PubRel => "PUBREL",
            PacketType::PubComp => "PUBCOMP",
            PacketType::Subscribe => "SUBSCRIBE",
            PacketType::SubAck => "SUBACK",
            PacketType::Unsubscribe => "UNSUBSCRIBE",
            PacketType::UnsubAck => "UNSUBACK",
            PacketType::PingReq => "PINGREQ",
            PacketType::PingResp => "PINGRESP",
            PacketType::Disconnect => "DISCONNECT",
            PacketType::Auth => "AUTH",
        };
        f.write_str(name)
    }
}

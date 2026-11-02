//! Reason codes, and which packets may carry which.

use std::fmt;

use crate::protocol::PacketType;

macro_rules! reason_codes {
    ($($name:ident = $value:literal,)+) => {
        /// A 5.0 reason code. Several codes share a byte across packets: `0x00` is
        /// `Success`, a normal disconnection and a granted QoS 0 alike, and `0x01`
        /// and `0x02` are the granted QoS levels in a `SUBACK`. 3.1.1's return codes
        /// map onto the same values where the byte is the same, and `CONNACK`'s
        /// 3.1.1 return codes map onto their 5.0 equivalents.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u8)]
        pub enum ReasonCode {
            $($name = $value,)+
        }

        impl ReasonCode {
            /// The byte on the wire.
            pub fn code(self) -> u8 {
                self as u8
            }

            /// `None` for a byte that is not a reason code.
            pub fn from_code(code: u8) -> Option<Self> {
                match code {
                    $($value => Some(ReasonCode::$name),)+
                    _ => None,
                }
            }
        }
    };
}

reason_codes! {
    Success = 0x00,
    GrantedQos1 = 0x01,
    GrantedQos2 = 0x02,
    DisconnectWithWillMessage = 0x04,
    NoMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShuttingDown = 0x8B,
    BadAuthenticationMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QosNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionsNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl ReasonCode {
    /// Whether the code is below `0x80`, which 5.0 defines as success.
    pub fn is_success(self) -> bool {
        self.code() < 0x80
    }

    /// The 3.1.1 `CONNACK` return code, for the reasons 3.1.1 can express.
    pub fn to_v311_connack(self) -> Option<u8> {
        match self {
            ReasonCode::Success => Some(0),
            ReasonCode::UnsupportedProtocolVersion => Some(1),
            ReasonCode::ClientIdentifierNotValid => Some(2),
            ReasonCode::ServerUnavailable => Some(3),
            ReasonCode::BadUserNameOrPassword => Some(4),
            ReasonCode::NotAuthorized => Some(5),
            _ => None,
        }
    }

    pub(crate) fn from_v311_connack(code: u8) -> Option<Self> {
        match code {
            0 => Some(ReasonCode::Success),
            1 => Some(ReasonCode::UnsupportedProtocolVersion),
            2 => Some(ReasonCode::ClientIdentifierNotValid),
            3 => Some(ReasonCode::ServerUnavailable),
            4 => Some(ReasonCode::BadUserNameOrPassword),
            5 => Some(ReasonCode::NotAuthorized),
            _ => None,
        }
    }

    /// The codes the specification lets `packet` carry under 5.0.
    pub(crate) fn permitted_in(self, packet: PacketType) -> bool {
        use ReasonCode::*;
        match packet {
            PacketType::ConnAck => matches!(
                self,
                Success
                    | UnspecifiedError
                    | MalformedPacket
                    | ProtocolError
                    | ImplementationSpecificError
                    | UnsupportedProtocolVersion
                    | ClientIdentifierNotValid
                    | BadUserNameOrPassword
                    | NotAuthorized
                    | ServerUnavailable
                    | ServerBusy
                    | Banned
                    | BadAuthenticationMethod
                    | TopicNameInvalid
                    | PacketTooLarge
                    | QuotaExceeded
                    | PayloadFormatInvalid
                    | RetainNotSupported
                    | QosNotSupported
                    | UseAnotherServer
                    | ServerMoved
                    | ConnectionRateExceeded
            ),
            PacketType::PubAck | PacketType::PubRec => matches!(
                self,
                Success
                    | NoMatchingSubscribers
                    | UnspecifiedError
                    | ImplementationSpecificError
                    | NotAuthorized
                    | TopicNameInvalid
                    | PacketIdentifierInUse
                    | QuotaExceeded
                    | PayloadFormatInvalid
            ),
            PacketType::PubRel | PacketType::PubComp => {
                matches!(self, Success | PacketIdentifierNotFound)
            }
            PacketType::SubAck => matches!(
                self,
                Success
                    | GrantedQos1
                    | GrantedQos2
                    | UnspecifiedError
                    | ImplementationSpecificError
                    | NotAuthorized
                    | TopicFilterInvalid
                    | PacketIdentifierInUse
                    | QuotaExceeded
                    | SharedSubscriptionsNotSupported
                    | SubscriptionIdentifiersNotSupported
                    | WildcardSubscriptionsNotSupported
            ),
            PacketType::UnsubAck => matches!(
                self,
                Success
                    | NoSubscriptionExisted
                    | UnspecifiedError
                    | ImplementationSpecificError
                    | NotAuthorized
                    | TopicFilterInvalid
                    | PacketIdentifierInUse
            ),
            PacketType::Disconnect => matches!(
                self,
                Success
                    | DisconnectWithWillMessage
                    | UnspecifiedError
                    | MalformedPacket
                    | ProtocolError
                    | ImplementationSpecificError
                    | NotAuthorized
                    | ServerBusy
                    | ServerShuttingDown
                    | KeepAliveTimeout
                    | SessionTakenOver
                    | TopicFilterInvalid
                    | TopicNameInvalid
                    | ReceiveMaximumExceeded
                    | TopicAliasInvalid
                    | PacketTooLarge
                    | MessageRateTooHigh
                    | QuotaExceeded
                    | AdministrativeAction
                    | PayloadFormatInvalid
                    | RetainNotSupported
                    | QosNotSupported
                    | UseAnotherServer
                    | ServerMoved
                    | SharedSubscriptionsNotSupported
                    | ConnectionRateExceeded
                    | MaximumConnectTime
                    | SubscriptionIdentifiersNotSupported
                    | WildcardSubscriptionsNotSupported
            ),
            PacketType::Auth => matches!(self, Success | ContinueAuthentication | ReAuthenticate),
            _ => false,
        }
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?} ({:#04x})", self.code())
    }
}

#[cfg(test)]
mod tests;

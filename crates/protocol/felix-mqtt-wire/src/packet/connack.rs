//! `CONNACK`: the server's answer to `CONNECT`.

use bytes::{BufMut, BytesMut};

use super::check_properties_fit;
use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::property::{Properties, Scope};
use crate::protocol::{PacketType, Version};
use crate::reader::Reader;
use crate::reason::ReasonCode;

/// A `CONNACK`. Under 3.1.1 the return code is carried as the 5.0 reason code it
/// corresponds to: `0x01` reads as `UnsupportedProtocolVersion`, `0x02` as
/// `ClientIdentifierNotValid`, `0x03` as `ServerUnavailable`, `0x04` as
/// `BadUserNameOrPassword` and `0x05` as `NotAuthorized`. Encoding for 3.1.1
/// refuses any other reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub reason: ReasonCode,
    /// Empty under 3.1.1.
    pub properties: Properties,
}

impl ConnAck {
    pub(super) fn decode(
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<ConnAck, DecodeError> {
        let flags = r.u8()?;
        if flags & !1 != 0 {
            return Err(DecodeError::InvalidConnectFlags(flags));
        }
        let session_present = flags & 1 != 0;
        let code = r.u8()?;
        let (reason, properties) = if version.is_v5() {
            let reason = ReasonCode::from_code(code)
                .filter(|r| r.permitted_in(PacketType::ConnAck))
                .ok_or(DecodeError::InvalidReasonCode {
                    packet: PacketType::ConnAck,
                    code,
                })?;
            (reason, Properties::decode(r, Scope::ConnAck, limits)?)
        } else {
            let reason =
                ReasonCode::from_v311_connack(code).ok_or(DecodeError::InvalidReasonCode {
                    packet: PacketType::ConnAck,
                    code,
                })?;
            (reason, Properties::default())
        };
        // A refusal has no session to resume.
        if session_present && reason != ReasonCode::Success {
            return Err(DecodeError::InvalidConnectFlags(flags));
        }
        Ok(ConnAck {
            session_present,
            reason,
            properties,
        })
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        check_properties_fit(version, &self.properties)?;
        if self.session_present && self.reason != ReasonCode::Success {
            return Err(EncodeError::InvalidReasonCode {
                packet: PacketType::ConnAck,
                code: self.reason.code(),
            });
        }
        if version.is_v5() {
            if !self.reason.permitted_in(PacketType::ConnAck) {
                return Err(EncodeError::InvalidReasonCode {
                    packet: PacketType::ConnAck,
                    code: self.reason.code(),
                });
            }
            Ok(2 + self.properties.section_len(Scope::ConnAck)?)
        } else {
            if self.reason.to_v311_connack().is_none() {
                return Err(EncodeError::InvalidReasonCode {
                    packet: PacketType::ConnAck,
                    code: self.reason.code(),
                });
            }
            Ok(2)
        }
    }

    pub(super) fn write_body(
        &self,
        version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        dst.put_u8(u8::from(self.session_present));
        if version.is_v5() {
            dst.put_u8(self.reason.code());
            self.properties.write_section(Scope::ConnAck, dst)?;
        } else {
            dst.put_u8(self.reason.to_v311_connack().unwrap_or(0));
        }
        Ok(())
    }
}

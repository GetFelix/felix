//! `AUTH`: extended authentication, 5.0 only.

use bytes::{BufMut, BytesMut};

use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::property::{Properties, Scope};
use crate::protocol::{PacketType, Version};
use crate::reader::Reader;
use crate::reason::ReasonCode;

/// An `AUTH`. The reason is `Success`, `ContinueAuthentication` or
/// `ReAuthenticate`, and the `authentication_method` property is required.
/// Unlike the acknowledgements, the reason and properties are always written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    pub reason: ReasonCode,
    pub properties: Properties,
}

impl Auth {
    pub(super) fn decode(r: &mut Reader, limits: &Limits) -> Result<Auth, DecodeError> {
        let code = r.u8()?;
        let reason = ReasonCode::from_code(code)
            .filter(|reason| reason.permitted_in(PacketType::Auth))
            .ok_or(DecodeError::InvalidReasonCode {
                packet: PacketType::Auth,
                code,
            })?;
        let properties = Properties::decode(r, Scope::Auth, limits)?;
        if properties.authentication_method.is_none() {
            return Err(DecodeError::MissingProperty(0x15));
        }
        Ok(Auth { reason, properties })
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        if !version.is_v5() {
            return Err(EncodeError::NotInVersion("AUTH"));
        }
        if !self.reason.permitted_in(PacketType::Auth) {
            return Err(EncodeError::InvalidReasonCode {
                packet: PacketType::Auth,
                code: self.reason.code(),
            });
        }
        if self.properties.authentication_method.is_none() {
            return Err(EncodeError::MissingAuthenticationMethod);
        }
        Ok(1 + self.properties.section_len(Scope::Auth)?)
    }

    pub(super) fn write_body(
        &self,
        _version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        dst.put_u8(self.reason.code());
        self.properties.write_section(Scope::Auth, dst)
    }
}

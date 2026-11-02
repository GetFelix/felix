//! `DISCONNECT`.

use bytes::{BufMut, BytesMut};

use super::check_properties_fit;
use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::property::{Properties, Scope};
use crate::protocol::{PacketType, Version};
use crate::reader::Reader;
use crate::reason::ReasonCode;

/// A `DISCONNECT`. Under 3.1.1 it is empty: `reason` is `Success` and
/// `properties` is empty. Under 5.0 the encoder leaves out the reason and
/// properties when they are `Success` and empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    pub reason: ReasonCode,
    pub properties: Properties,
}

impl Disconnect {
    pub fn new(reason: ReasonCode) -> Self {
        Disconnect {
            reason,
            properties: Properties::default(),
        }
    }

    pub(super) fn decode(
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<Disconnect, DecodeError> {
        let mut d = Disconnect::new(ReasonCode::Success);
        if !version.is_v5() || r.is_empty() {
            return Ok(d);
        }
        let code = r.u8()?;
        d.reason = ReasonCode::from_code(code)
            .filter(|reason| reason.permitted_in(PacketType::Disconnect))
            .ok_or(DecodeError::InvalidReasonCode {
                packet: PacketType::Disconnect,
                code,
            })?;
        if !r.is_empty() {
            d.properties = Properties::decode(r, Scope::Disconnect, limits)?;
        }
        Ok(d)
    }

    fn is_bare(&self) -> bool {
        self.reason == ReasonCode::Success && self.properties.is_empty()
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        check_properties_fit(version, &self.properties)?;
        if !self.reason.permitted_in(PacketType::Disconnect) {
            return Err(EncodeError::InvalidReasonCode {
                packet: PacketType::Disconnect,
                code: self.reason.code(),
            });
        }
        if !version.is_v5() {
            if self.reason != ReasonCode::Success {
                return Err(EncodeError::NotInVersion("reason code"));
            }
            return Ok(0);
        }
        if self.is_bare() {
            return Ok(0);
        }
        Ok(1 + self.properties.section_len(Scope::Disconnect)?)
    }

    pub(super) fn write_body(
        &self,
        version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        if version.is_v5() && !self.is_bare() {
            dst.put_u8(self.reason.code());
            self.properties.write_section(Scope::Disconnect, dst)?;
        }
        Ok(())
    }
}

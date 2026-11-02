//! `PUBACK`, `PUBREC`, `PUBREL` and `PUBCOMP`: the QoS 1 and 2 handshakes.

use bytes::{BufMut, BytesMut};

use super::{check_packet_id, check_properties_fit, read_packet_id};
use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::property::{Properties, Scope};
use crate::protocol::{PacketType, Version};
use crate::reader::Reader;
use crate::reason::ReasonCode;

/// The body all four share. The [`Packet`](crate::Packet) variant says which one
/// it is.
///
/// 3.1.1 has only the identifier, so under it `reason` is `Success` and
/// `properties` is empty. Under 5.0 the encoder leaves out the reason and
/// properties when they are `Success` and empty, and the decoder accepts every
/// form the specification allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    pub packet_id: u16,
    pub reason: ReasonCode,
    pub properties: Properties,
}

impl Ack {
    pub fn new(packet_id: u16) -> Ack {
        Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }
    }

    pub(super) fn decode(
        kind: PacketType,
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<Ack, DecodeError> {
        let mut ack = Ack::new(read_packet_id(r)?);
        if !version.is_v5() || r.is_empty() {
            return Ok(ack);
        }
        let code = r.u8()?;
        ack.reason = ReasonCode::from_code(code)
            .filter(|reason| reason.permitted_in(kind))
            .ok_or(DecodeError::InvalidReasonCode { packet: kind, code })?;
        if !r.is_empty() {
            ack.properties = Properties::decode(r, Scope::Ack, limits)?;
        }
        Ok(ack)
    }

    /// Whether the encoder can leave out everything after the identifier.
    fn is_bare(&self) -> bool {
        self.reason == ReasonCode::Success && self.properties.is_empty()
    }

    pub(super) fn body_len(
        &self,
        kind: PacketType,
        version: Version,
    ) -> Result<usize, EncodeError> {
        check_packet_id(self.packet_id)?;
        check_properties_fit(version, &self.properties)?;
        if !self.reason.permitted_in(kind) {
            return Err(EncodeError::InvalidReasonCode {
                packet: kind,
                code: self.reason.code(),
            });
        }
        if !version.is_v5() {
            if self.reason != ReasonCode::Success {
                return Err(EncodeError::NotInVersion("reason code"));
            }
            return Ok(2);
        }
        if self.is_bare() {
            return Ok(2);
        }
        Ok(3 + self.properties.section_len(Scope::Ack)?)
    }

    pub(super) fn write_body(
        &self,
        version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        dst.put_u16(self.packet_id);
        if version.is_v5() && !self.is_bare() {
            dst.put_u8(self.reason.code());
            self.properties.write_section(Scope::Ack, dst)?;
        }
        Ok(())
    }
}

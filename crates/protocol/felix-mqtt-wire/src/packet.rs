//! The fifteen control packets, and the entry points that decode and encode them.
//!
//! [`Packet::decode`] reads one packet from the front of a buffer. It checks
//! what makes a packet malformed or a protocol error on its own: the fixed
//! header, field syntax, reason codes, property rules and the limits. What
//! depends on the connection, such as which packet may arrive in which state or
//! which direction, is the session's to decide.

mod ack;
mod auth;
mod connack;
mod connect;
mod disconnect;
mod publish;
mod subscribe;
mod unsubscribe;

pub use ack::Ack;
pub use auth::Auth;
pub use connack::ConnAck;
pub use connect::{Connect, Will};
pub use disconnect::Disconnect;
pub use publish::Publish;
pub use subscribe::{RetainHandling, SubAck, Subscribe, SubscribeFilter};
pub use unsubscribe::{UnsubAck, Unsubscribe};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError};
use crate::limits::{Limits, MAX_VARINT};
use crate::protocol::{PacketType, Version};
use crate::reader::Reader;
use crate::writer::{put_varint, varint_len};

/// One MQTT control packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(Ack),
    PubRec(Ack),
    PubRel(Ack),
    PubComp(Ack),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    Auth(Auth),
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::ConnAck(_) => PacketType::ConnAck,
            Packet::Publish(_) => PacketType::Publish,
            Packet::PubAck(_) => PacketType::PubAck,
            Packet::PubRec(_) => PacketType::PubRec,
            Packet::PubRel(_) => PacketType::PubRel,
            Packet::PubComp(_) => PacketType::PubComp,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::SubAck(_) => PacketType::SubAck,
            Packet::Unsubscribe(_) => PacketType::Unsubscribe,
            Packet::UnsubAck(_) => PacketType::UnsubAck,
            Packet::PingReq => PacketType::PingReq,
            Packet::PingResp => PacketType::PingResp,
            Packet::Disconnect(_) => PacketType::Disconnect,
            Packet::Auth(_) => PacketType::Auth,
        }
    }

    /// Reads one packet from the front of `src` and removes it.
    ///
    /// Returns `Ok(None)` when `src` does not hold a whole packet yet; nothing
    /// is consumed, so call again after reading more. A packet larger than
    /// [`Limits::max_packet_size`] is refused as soon as its fixed header is in,
    /// before its body is waited for.
    ///
    /// `version` is the connection's protocol level once `CONNECT` has been
    /// read, and `None` before that, when only `CONNECT` is accepted.
    ///
    /// Strings, binary fields and the payload of a `PUBLISH` are slices of the
    /// packet's bytes, not copies. A slice keeps the whole buffer allocation
    /// alive until it is dropped, so copy anything that will be held long.
    ///
    /// After an error the stream cannot be resynchronised: close the connection.
    pub fn decode(
        src: &mut BytesMut,
        version: Option<Version>,
        limits: &Limits,
    ) -> Result<Option<Packet>, DecodeError> {
        let Some(header) = FixedHeader::parse(src, version, limits)? else {
            return Ok(None);
        };
        if src.len() < header.total {
            return Ok(None);
        }
        let mut frame = src.split_to(header.total).freeze();
        frame.advance(header.len);
        let mut body = Reader::new(frame);
        let packet = decode_body(&header, &mut body, version, limits)?;
        Ok(Some(packet))
    }

    /// Writes the packet to `dst` for a connection speaking `version`. On error
    /// `dst` is left as it was.
    pub fn encode(&self, version: Version, dst: &mut BytesMut) -> Result<(), EncodeError> {
        let body_len = self.body_len(version)?;
        let start = dst.len();
        dst.reserve(1 + varint_len(body_len) + body_len);
        dst.put_u8(self.first_byte());
        put_varint(dst, body_len);
        if let Err(e) = self.write_body(version, dst) {
            dst.truncate(start);
            return Err(e);
        }
        Ok(())
    }

    /// The size of the whole packet on the wire, for checking a client's
    /// `Maximum Packet Size` before sending.
    pub fn encoded_len(&self, version: Version) -> Result<usize, EncodeError> {
        let body_len = self.body_len(version)?;
        Ok(1 + varint_len(body_len) + body_len)
    }

    /// Encodes into a fresh buffer.
    pub fn to_bytes(&self, version: Version) -> Result<Bytes, EncodeError> {
        let mut dst = BytesMut::new();
        self.encode(version, &mut dst)?;
        Ok(dst.freeze())
    }

    fn first_byte(&self) -> u8 {
        let kind = self.packet_type();
        let flags = match self {
            Packet::Publish(p) => p.flags(),
            _ => kind.fixed_flags().unwrap_or(0),
        };
        kind.code() << 4 | flags
    }

    /// Validates the packet and sizes its body. Every refusal the encoder makes
    /// happens here, so `write_body` only runs on a packet that will encode.
    fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        let len = match self {
            Packet::Connect(p) => p.body_len(version)?,
            Packet::ConnAck(p) => p.body_len(version)?,
            Packet::Publish(p) => p.body_len(version)?,
            Packet::PubAck(p) => p.body_len(PacketType::PubAck, version)?,
            Packet::PubRec(p) => p.body_len(PacketType::PubRec, version)?,
            Packet::PubRel(p) => p.body_len(PacketType::PubRel, version)?,
            Packet::PubComp(p) => p.body_len(PacketType::PubComp, version)?,
            Packet::Subscribe(p) => p.body_len(version)?,
            Packet::SubAck(p) => p.body_len(version)?,
            Packet::Unsubscribe(p) => p.body_len(version)?,
            Packet::UnsubAck(p) => p.body_len(version)?,
            Packet::PingReq | Packet::PingResp => 0,
            Packet::Disconnect(p) => p.body_len(version)?,
            Packet::Auth(p) => p.body_len(version)?,
        };
        if len > MAX_VARINT as usize {
            return Err(EncodeError::PacketTooLarge(len));
        }
        Ok(len)
    }

    fn write_body(&self, version: Version, dst: &mut BytesMut) -> Result<(), EncodeError> {
        match self {
            Packet::Connect(p) => p.write_body(version, dst),
            Packet::ConnAck(p) => p.write_body(version, dst),
            Packet::Publish(p) => p.write_body(version, dst),
            Packet::PubAck(p) => p.write_body(version, dst),
            Packet::PubRec(p) => p.write_body(version, dst),
            Packet::PubRel(p) => p.write_body(version, dst),
            Packet::PubComp(p) => p.write_body(version, dst),
            Packet::Subscribe(p) => p.write_body(version, dst),
            Packet::SubAck(p) => p.write_body(version, dst),
            Packet::Unsubscribe(p) => p.write_body(version, dst),
            Packet::UnsubAck(p) => p.write_body(version, dst),
            Packet::PingReq | Packet::PingResp => Ok(()),
            Packet::Disconnect(p) => p.write_body(version, dst),
            Packet::Auth(p) => p.write_body(version, dst),
        }
    }
}

/// The first byte and the remaining length, which together say how much to wait
/// for.
struct FixedHeader {
    kind: PacketType,
    flags: u8,
    /// Bytes the fixed header takes.
    len: usize,
    /// Bytes of the whole packet.
    total: usize,
}

impl FixedHeader {
    /// `Ok(None)` while the header is incomplete. A reserved type or wrong flags
    /// are refused on the first byte, without waiting for the length.
    fn parse(
        src: &[u8],
        version: Option<Version>,
        limits: &Limits,
    ) -> Result<Option<FixedHeader>, DecodeError> {
        let Some(&first) = src.first() else {
            return Ok(None);
        };
        let kind = PacketType::from_code(first >> 4, version)
            .ok_or(DecodeError::InvalidPacketType(first >> 4))?;
        let flags = first & 0x0F;
        if let Some(fixed) = kind.fixed_flags()
            && flags != fixed
        {
            return Err(DecodeError::InvalidFlags {
                packet: kind,
                flags,
            });
        }
        if version.is_none() && kind != PacketType::Connect {
            return Err(DecodeError::FirstPacketNotConnect(kind));
        }

        let mut remaining = 0usize;
        let mut len = 1;
        loop {
            let Some(&byte) = src.get(len) else {
                return Ok(None);
            };
            remaining |= usize::from(byte & 0x7F) << (7 * (len - 1));
            len += 1;
            if byte & 0x80 == 0 {
                if len > 2 && byte == 0 {
                    return Err(DecodeError::MalformedLength);
                }
                break;
            }
            if len == 5 {
                return Err(DecodeError::MalformedLength);
            }
        }

        let total = len + remaining;
        if total > limits.max_packet_size as usize {
            return Err(DecodeError::PacketTooLarge {
                size: total,
                max: limits.max_packet_size,
            });
        }
        Ok(Some(FixedHeader {
            kind,
            flags,
            len,
            total,
        }))
    }
}

fn decode_body(
    header: &FixedHeader,
    body: &mut Reader,
    version: Option<Version>,
    limits: &Limits,
) -> Result<Packet, DecodeError> {
    // `parse` has already refused every packet but CONNECT while the version
    // is unknown, and CONNECT carries its own.
    let v = version.unwrap_or(Version::V311);
    let packet = match header.kind {
        PacketType::Connect => Packet::Connect(Connect::decode(body, limits)?),
        PacketType::ConnAck => Packet::ConnAck(ConnAck::decode(body, v, limits)?),
        PacketType::Publish => Packet::Publish(Publish::decode(header.flags, body, v, limits)?),
        PacketType::PubAck => Packet::PubAck(Ack::decode(PacketType::PubAck, body, v, limits)?),
        PacketType::PubRec => Packet::PubRec(Ack::decode(PacketType::PubRec, body, v, limits)?),
        PacketType::PubRel => Packet::PubRel(Ack::decode(PacketType::PubRel, body, v, limits)?),
        PacketType::PubComp => Packet::PubComp(Ack::decode(PacketType::PubComp, body, v, limits)?),
        PacketType::Subscribe => Packet::Subscribe(Subscribe::decode(body, v, limits)?),
        PacketType::SubAck => Packet::SubAck(SubAck::decode(body, v, limits)?),
        PacketType::Unsubscribe => Packet::Unsubscribe(Unsubscribe::decode(body, v, limits)?),
        PacketType::UnsubAck => Packet::UnsubAck(UnsubAck::decode(body, v, limits)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect(Disconnect::decode(body, v, limits)?),
        PacketType::Auth => Packet::Auth(Auth::decode(body, limits)?),
    };
    body.finish()?;
    Ok(packet)
}

/// Reads a packet identifier, which is never zero.
fn read_packet_id(r: &mut Reader) -> Result<u16, DecodeError> {
    match r.u16()? {
        0 => Err(DecodeError::ZeroPacketId),
        id => Ok(id),
    }
}

fn check_packet_id(id: u16) -> Result<(), EncodeError> {
    if id == 0 {
        return Err(EncodeError::InvalidPacketId);
    }
    Ok(())
}

/// 5.0's property section is absent from 3.1.1, so a packet that carries
/// properties cannot be encoded for it.
fn check_properties_fit(
    version: Version,
    properties: &crate::property::Properties,
) -> Result<(), EncodeError> {
    if !version.is_v5() && !properties.is_empty() {
        return Err(EncodeError::NotInVersion("properties"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

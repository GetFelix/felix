//! `PUBLISH`: a message, in either direction.

use bytes::{BufMut, Bytes, BytesMut};

use super::{check_packet_id, check_properties_fit, read_packet_id};
use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::property::{Properties, Scope};
use crate::protocol::{QoS, Version};
use crate::reader::Reader;
use crate::text::Utf8;
use crate::topic::{TopicBounds, TopicError, check_topic_name};
use crate::writer::{put_string, varint_len};

const RETAIN: u8 = 0b0001;
const QOS_SHIFT: u8 = 1;
const DUP: u8 = 0b1000;

/// A `PUBLISH`. The payload is a slice of the buffer the packet was read from.
///
/// Under 5.0 the topic may be empty when `properties.topic_alias` names an alias
/// the receiver has seen. The codec cannot know that, so the session resolves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: Utf8,
    /// Present exactly when `qos` is above `AtMostOnce`, and never zero.
    pub packet_id: Option<u16>,
    /// Empty under 3.1.1.
    pub properties: Properties,
    pub payload: Bytes,
}

impl Publish {
    pub(super) fn decode(
        flags: u8,
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<Publish, DecodeError> {
        let qos = QoS::from_bits((flags >> QOS_SHIFT) & 0b11).ok_or(DecodeError::InvalidQos)?;
        let dup = flags & DUP != 0;
        if dup && qos == QoS::AtMostOnce {
            return Err(DecodeError::DupOnQos0);
        }
        let topic = r.string()?;
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(read_packet_id(r)?)
        };
        let properties = if version.is_v5() {
            Properties::decode(r, Scope::Publish, limits)?
        } else {
            Properties::default()
        };

        let bounds = TopicBounds {
            max_bytes: limits.max_topic_bytes,
            max_levels: limits.max_topic_levels,
        };
        match check_topic_name(topic.as_str(), bounds) {
            Ok(()) => {}
            Err(TopicError::Empty) if properties.topic_alias.is_some() => {}
            Err(e) => return Err(DecodeError::InvalidTopicName(e)),
        }

        Ok(Publish {
            dup,
            qos,
            retain: flags & RETAIN != 0,
            topic,
            packet_id,
            properties,
            payload: r.rest(),
        })
    }

    /// The low nibble of the first byte.
    pub(super) fn flags(&self) -> u8 {
        let mut flags = (self.qos as u8) << QOS_SHIFT;
        if self.dup {
            flags |= DUP;
        }
        if self.retain {
            flags |= RETAIN;
        }
        flags
    }

    /// Everything but the payload: topic, identifier and properties.
    fn head_len(&self, version: Version) -> Result<usize, EncodeError> {
        check_properties_fit(version, &self.properties)?;
        if self.dup && self.qos == QoS::AtMostOnce {
            return Err(EncodeError::DupOnQos0);
        }
        match (self.qos, self.packet_id) {
            (QoS::AtMostOnce, None) => {}
            (QoS::AtMostOnce, Some(_)) | (_, None) => return Err(EncodeError::InvalidPacketId),
            (_, Some(id)) => check_packet_id(id)?,
        }
        match check_topic_name(self.topic.as_str(), TopicBounds::FORMAT) {
            Ok(()) => {}
            Err(TopicError::Empty) if version.is_v5() && self.properties.topic_alias.is_some() => {}
            Err(e) => return Err(EncodeError::InvalidTopicName(e)),
        }
        let mut len = 2 + self.topic.len();
        if self.packet_id.is_some() {
            len += 2;
        }
        if version.is_v5() {
            len += self.properties.section_len(Scope::Publish)?;
        }
        Ok(len)
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        Ok(self.head_len(version)? + self.payload.len())
    }

    pub(super) fn write_body(
        &self,
        version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        self.write_head(version, dst)?;
        dst.put_slice(&self.payload);
        Ok(())
    }

    fn write_head(&self, version: Version, dst: &mut BytesMut) -> Result<(), EncodeError> {
        put_string(dst, &self.topic);
        if let Some(id) = self.packet_id {
            dst.put_u16(id);
        }
        if version.is_v5() {
            self.properties.write_section(Scope::Publish, dst)?;
        }
        Ok(())
    }

    /// Writes the fixed header, topic, identifier and properties, but not the
    /// payload, which the caller sends next from wherever it already lives. The
    /// remaining length counts the payload, so the two together are exactly
    /// what [`Packet::encode`](crate::Packet::encode) writes.
    pub fn encode_head(&self, version: Version, dst: &mut BytesMut) -> Result<(), EncodeError> {
        let body_len = self.body_len(version)?;
        if body_len > crate::limits::MAX_VARINT as usize {
            return Err(EncodeError::PacketTooLarge(body_len));
        }
        let start = dst.len();
        dst.reserve(1 + varint_len(body_len) + body_len - self.payload.len());
        dst.put_u8(3 << 4 | self.flags());
        crate::writer::put_varint(dst, body_len);
        if let Err(e) = self.write_head(version, dst) {
            dst.truncate(start);
            return Err(e);
        }
        Ok(())
    }
}

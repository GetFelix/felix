//! `UNSUBSCRIBE` and `UNSUBACK`.

use bytes::{BufMut, BytesMut};

use super::subscribe::{check_reasons, decode_reasons};
use super::{check_packet_id, check_properties_fit, read_packet_id};
use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::property::{Properties, Scope};
use crate::protocol::{PacketType, Version};
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::text::Utf8;
use crate::topic::{TopicBounds, check_topic_filter};
use crate::writer::put_string;

/// An `UNSUBSCRIBE`. It names at least one filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub filters: Vec<Utf8>,
    /// Empty under 3.1.1.
    pub properties: Properties,
}

impl Unsubscribe {
    pub(super) fn decode(
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<Unsubscribe, DecodeError> {
        let packet_id = read_packet_id(r)?;
        let properties = if version.is_v5() {
            Properties::decode(r, Scope::Unsubscribe, limits)?
        } else {
            Properties::default()
        };
        let bounds = TopicBounds {
            max_bytes: limits.max_topic_bytes,
            max_levels: limits.max_topic_levels,
        };
        let mut filters = Vec::new();
        while !r.is_empty() {
            if filters.len() >= limits.max_subscriptions {
                return Err(DecodeError::LimitExceeded {
                    what: "filters in one packet",
                    max: limits.max_subscriptions,
                });
            }
            let filter = r.string()?;
            check_topic_filter(filter.as_str(), version, bounds)
                .map_err(DecodeError::InvalidTopicFilter)?;
            filters.push(filter);
        }
        if filters.is_empty() {
            return Err(DecodeError::EmptyList(PacketType::Unsubscribe));
        }
        Ok(Unsubscribe {
            packet_id,
            filters,
            properties,
        })
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        check_packet_id(self.packet_id)?;
        check_properties_fit(version, &self.properties)?;
        if self.filters.is_empty() {
            return Err(EncodeError::EmptyList(PacketType::Unsubscribe));
        }
        let mut len = 2;
        if version.is_v5() {
            len += self.properties.section_len(Scope::Unsubscribe)?;
        }
        for f in &self.filters {
            check_topic_filter(f.as_str(), version, TopicBounds::FORMAT)
                .map_err(EncodeError::InvalidTopicFilter)?;
            len += 2 + f.len();
        }
        Ok(len)
    }

    pub(super) fn write_body(
        &self,
        version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        dst.put_u16(self.packet_id);
        if version.is_v5() {
            self.properties.write_section(Scope::Unsubscribe, dst)?;
        }
        for f in &self.filters {
            put_string(dst, f);
        }
        Ok(())
    }
}

/// An `UNSUBACK`. Under 3.1.1 it is the identifier alone and `reasons` is empty;
/// under 5.0 there is one reason per filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    pub packet_id: u16,
    pub reasons: Vec<ReasonCode>,
    /// Empty under 3.1.1.
    pub properties: Properties,
}

impl UnsubAck {
    pub(super) fn decode(
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<UnsubAck, DecodeError> {
        let packet_id = read_packet_id(r)?;
        if !version.is_v5() {
            return Ok(UnsubAck {
                packet_id,
                reasons: Vec::new(),
                properties: Properties::default(),
            });
        }
        let properties = Properties::decode(r, Scope::UnsubAck, limits)?;
        let reasons = decode_reasons(r, PacketType::UnsubAck, version, limits)?;
        Ok(UnsubAck {
            packet_id,
            reasons,
            properties,
        })
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        check_packet_id(self.packet_id)?;
        check_properties_fit(version, &self.properties)?;
        if !version.is_v5() {
            if !self.reasons.is_empty() {
                return Err(EncodeError::NotInVersion("UNSUBACK reason codes"));
            }
            return Ok(2);
        }
        check_reasons(&self.reasons, PacketType::UnsubAck, version)?;
        Ok(2 + self.properties.section_len(Scope::UnsubAck)? + self.reasons.len())
    }

    pub(super) fn write_body(
        &self,
        version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        dst.put_u16(self.packet_id);
        if version.is_v5() {
            self.properties.write_section(Scope::UnsubAck, dst)?;
            dst.extend(self.reasons.iter().map(|r| r.code()));
        }
        Ok(())
    }
}

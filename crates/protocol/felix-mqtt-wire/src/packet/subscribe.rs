//! `SUBSCRIBE` and `SUBACK`.

use bytes::{BufMut, BytesMut};

use super::{check_packet_id, check_properties_fit, read_packet_id};
use crate::error::{DecodeError, EncodeError};
use crate::limits::Limits;
use crate::property::{Properties, Scope};
use crate::protocol::{PacketType, QoS, Version};
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::text::Utf8;
use crate::topic::{TopicBounds, check_topic_filter};
use crate::writer::put_string;

const NO_LOCAL: u8 = 0b0000_0100;
const RETAIN_AS_PUBLISHED: u8 = 0b0000_1000;
const RETAIN_HANDLING_SHIFT: u8 = 4;

/// When retained messages are sent for a new subscription (5.0).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetainHandling {
    /// Send them whenever the subscription is made.
    #[default]
    SendAtSubscribe = 0,
    /// Send them only if the subscription did not already exist.
    SendIfNew = 1,
    /// Do not send them.
    DoNotSend = 2,
}

/// One filter and its options. Under 3.1.1 only `qos` exists, and the other
/// options must keep their defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    pub filter: Utf8,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

impl SubscribeFilter {
    pub fn new(filter: Utf8, qos: QoS) -> Self {
        SubscribeFilter {
            filter,
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::default(),
        }
    }

    fn is_shared(&self) -> bool {
        self.filter.as_str().starts_with("$share/")
    }

    fn options(&self) -> u8 {
        let mut options = self.qos as u8 | (self.retain_handling as u8) << RETAIN_HANDLING_SHIFT;
        if self.no_local {
            options |= NO_LOCAL;
        }
        if self.retain_as_published {
            options |= RETAIN_AS_PUBLISHED;
        }
        options
    }
}

/// A `SUBSCRIBE`. It carries at least one filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub packet_id: u16,
    pub filters: Vec<SubscribeFilter>,
    /// Empty under 3.1.1. At most one subscription identifier, and not zero.
    pub properties: Properties,
}

impl Subscribe {
    pub(super) fn decode(
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<Subscribe, DecodeError> {
        let packet_id = read_packet_id(r)?;
        let properties = if version.is_v5() {
            let p = Properties::decode(r, Scope::Subscribe, limits)?;
            if p.subscription_identifiers.len() > 1 {
                return Err(DecodeError::DuplicateProperty(0x0B));
            }
            p
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
                    what: "subscriptions in one packet",
                    max: limits.max_subscriptions,
                });
            }
            let filter = r.string()?;
            check_topic_filter(filter.as_str(), version, bounds)
                .map_err(DecodeError::InvalidTopicFilter)?;
            let options = r.u8()?;
            let f = decode_options(filter, options, version)?;
            // [MQTT-3.8.3-4]
            if f.no_local && f.is_shared() {
                return Err(DecodeError::NoLocalOnSharedSubscription);
            }
            filters.push(f);
        }
        if filters.is_empty() {
            return Err(DecodeError::EmptyList(PacketType::Subscribe));
        }
        Ok(Subscribe {
            packet_id,
            filters,
            properties,
        })
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        check_packet_id(self.packet_id)?;
        check_properties_fit(version, &self.properties)?;
        if self.filters.is_empty() {
            return Err(EncodeError::EmptyList(PacketType::Subscribe));
        }
        if self.properties.subscription_identifiers.len() > 1 {
            return Err(EncodeError::InvalidProperty(0x0B));
        }
        let mut len = 2;
        if version.is_v5() {
            len += self.properties.section_len(Scope::Subscribe)?;
        }
        for f in &self.filters {
            check_topic_filter(f.filter.as_str(), version, TopicBounds::FORMAT)
                .map_err(EncodeError::InvalidTopicFilter)?;
            if !version.is_v5()
                && (f.no_local
                    || f.retain_as_published
                    || f.retain_handling != RetainHandling::SendAtSubscribe)
            {
                return Err(EncodeError::NotInVersion("subscription options"));
            }
            if f.no_local && f.is_shared() && version.is_v5() {
                return Err(EncodeError::NoLocalOnSharedSubscription);
            }
            len += 2 + f.filter.len() + 1;
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
            self.properties.write_section(Scope::Subscribe, dst)?;
        }
        for f in &self.filters {
            put_string(dst, &f.filter);
            dst.put_u8(f.options());
        }
        Ok(())
    }
}

fn decode_options(
    filter: Utf8,
    options: u8,
    version: Version,
) -> Result<SubscribeFilter, DecodeError> {
    let qos =
        QoS::from_bits(options & 0b11).ok_or(DecodeError::InvalidSubscriptionOptions(options))?;
    let mut f = SubscribeFilter::new(filter, qos);
    if version.is_v5() {
        f.no_local = options & NO_LOCAL != 0;
        f.retain_as_published = options & RETAIN_AS_PUBLISHED != 0;
        f.retain_handling = match (options >> RETAIN_HANDLING_SHIFT) & 0b11 {
            0 => RetainHandling::SendAtSubscribe,
            1 => RetainHandling::SendIfNew,
            2 => RetainHandling::DoNotSend,
            _ => return Err(DecodeError::InvalidSubscriptionOptions(options)),
        };
        if options & 0b1100_0000 != 0 {
            return Err(DecodeError::InvalidSubscriptionOptions(options));
        }
    } else if options & !0b11 != 0 {
        return Err(DecodeError::InvalidSubscriptionOptions(options));
    }
    Ok(f)
}

/// A `SUBACK`: one reason per filter of the `SUBSCRIBE`, in order. Under 3.1.1
/// the reasons are the return codes `GrantedQos0`, `GrantedQos1`,
/// `GrantedQos2` (which are `Success`, `GrantedQos1`, `GrantedQos2`) and
/// `UnspecifiedError` for `0x80`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    pub packet_id: u16,
    pub reasons: Vec<ReasonCode>,
    /// Empty under 3.1.1.
    pub properties: Properties,
}

impl SubAck {
    pub(super) fn decode(
        r: &mut Reader,
        version: Version,
        limits: &Limits,
    ) -> Result<SubAck, DecodeError> {
        let packet_id = read_packet_id(r)?;
        let properties = if version.is_v5() {
            Properties::decode(r, Scope::SubAck, limits)?
        } else {
            Properties::default()
        };
        let reasons = decode_reasons(r, PacketType::SubAck, version, limits)?;
        Ok(SubAck {
            packet_id,
            reasons,
            properties,
        })
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        check_packet_id(self.packet_id)?;
        check_properties_fit(version, &self.properties)?;
        check_reasons(&self.reasons, PacketType::SubAck, version)?;
        let mut len = 2 + self.reasons.len();
        if version.is_v5() {
            len += self.properties.section_len(Scope::SubAck)?;
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
            self.properties.write_section(Scope::SubAck, dst)?;
        }
        dst.extend(self.reasons.iter().map(|r| r.code()));
        Ok(())
    }
}

/// Reads reason codes to the end of the packet; there is at least one.
pub(super) fn decode_reasons(
    r: &mut Reader,
    kind: PacketType,
    version: Version,
    limits: &Limits,
) -> Result<Vec<ReasonCode>, DecodeError> {
    if r.is_empty() {
        return Err(DecodeError::EmptyList(kind));
    }
    if r.remaining() > limits.max_subscriptions {
        return Err(DecodeError::LimitExceeded {
            what: "reason codes in one packet",
            max: limits.max_subscriptions,
        });
    }
    let mut reasons = Vec::with_capacity(r.remaining());
    while !r.is_empty() {
        let code = r.u8()?;
        let reason = ReasonCode::from_code(code)
            .filter(|reason| reason_allowed(*reason, kind, version))
            .ok_or(DecodeError::InvalidReasonCode { packet: kind, code })?;
        reasons.push(reason);
    }
    Ok(reasons)
}

pub(super) fn check_reasons(
    reasons: &[ReasonCode],
    kind: PacketType,
    version: Version,
) -> Result<(), EncodeError> {
    if reasons.is_empty() {
        return Err(EncodeError::EmptyList(kind));
    }
    match reasons.iter().find(|r| !reason_allowed(**r, kind, version)) {
        Some(bad) => Err(EncodeError::InvalidReasonCode {
            packet: kind,
            code: bad.code(),
        }),
        None => Ok(()),
    }
}

/// 3.1.1's `SUBACK` allows `0x00`, `0x01`, `0x02` and `0x80` only.
fn reason_allowed(reason: ReasonCode, kind: PacketType, version: Version) -> bool {
    if version.is_v5() {
        return reason.permitted_in(kind);
    }
    matches!(
        reason,
        ReasonCode::Success
            | ReasonCode::GrantedQos1
            | ReasonCode::GrantedQos2
            | ReasonCode::UnspecifiedError
    )
}

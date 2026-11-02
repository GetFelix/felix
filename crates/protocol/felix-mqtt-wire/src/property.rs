//! 5.0 properties: every identifier, where it may appear, and its value rules.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError};
use crate::limits::{Limits, MAX_FIELD_BYTES, MAX_VARINT};
use crate::protocol::QoS;
use crate::reader::Reader;
use crate::text::Utf8;
use crate::topic::{TopicBounds, check_topic_name};
use crate::writer::{put_binary, put_string, put_varint, varint_len};

/// The properties of one packet. A field is `None` (or empty) when the property
/// is absent. Which properties a packet may carry is checked on decode and on
/// encode; naming one that does not belong is a protocol error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Properties {
    /// `true` when the payload is UTF-8 text.
    pub payload_format_indicator: Option<bool>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<Utf8>,
    pub response_topic: Option<Utf8>,
    pub correlation_data: Option<Bytes>,
    /// More than one only in a `PUBLISH`. Each is 1 to 268435455.
    pub subscription_identifiers: Vec<u32>,
    pub session_expiry_interval: Option<u32>,
    pub assigned_client_identifier: Option<Utf8>,
    pub server_keep_alive: Option<u16>,
    pub authentication_method: Option<Utf8>,
    pub authentication_data: Option<Bytes>,
    pub request_problem_information: Option<bool>,
    pub will_delay_interval: Option<u32>,
    pub request_response_information: Option<bool>,
    pub response_information: Option<Utf8>,
    pub server_reference: Option<Utf8>,
    pub reason_string: Option<Utf8>,
    /// Never zero.
    pub receive_maximum: Option<u16>,
    pub topic_alias_maximum: Option<u16>,
    /// Never zero.
    pub topic_alias: Option<u16>,
    /// 0 or 1; a server never advertises 2.
    pub maximum_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub user_properties: Vec<(Utf8, Utf8)>,
    /// Never zero.
    pub maximum_packet_size: Option<u32>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
}

/// The packet a property block belongs to. The specification's table of where
/// each property may appear is keyed on this; the will has its own column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    Connect,
    Will,
    ConnAck,
    Publish,
    /// `PUBACK`, `PUBREC`, `PUBREL` and `PUBCOMP` share one column.
    Ack,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    Disconnect,
    Auth,
}

impl Scope {
    fn name(self) -> &'static str {
        match self {
            Scope::Connect => "CONNECT",
            Scope::Will => "a will",
            Scope::ConnAck => "CONNACK",
            Scope::Publish => "PUBLISH",
            Scope::Ack => "PUBACK, PUBREC, PUBREL or PUBCOMP",
            Scope::Subscribe => "SUBSCRIBE",
            Scope::SubAck => "SUBACK",
            Scope::Unsubscribe => "UNSUBSCRIBE",
            Scope::UnsubAck => "UNSUBACK",
            Scope::Disconnect => "DISCONNECT",
            Scope::Auth => "AUTH",
        }
    }
}

mod id {
    pub(super) const PAYLOAD_FORMAT: u8 = 0x01;
    pub(super) const MESSAGE_EXPIRY: u8 = 0x02;
    pub(super) const CONTENT_TYPE: u8 = 0x03;
    pub(super) const RESPONSE_TOPIC: u8 = 0x08;
    pub(super) const CORRELATION_DATA: u8 = 0x09;
    pub(super) const SUBSCRIPTION_ID: u8 = 0x0B;
    pub(super) const SESSION_EXPIRY: u8 = 0x11;
    pub(super) const ASSIGNED_CLIENT_ID: u8 = 0x12;
    pub(super) const SERVER_KEEP_ALIVE: u8 = 0x13;
    pub(super) const AUTH_METHOD: u8 = 0x15;
    pub(super) const AUTH_DATA: u8 = 0x16;
    pub(super) const REQUEST_PROBLEM_INFO: u8 = 0x17;
    pub(super) const WILL_DELAY: u8 = 0x18;
    pub(super) const REQUEST_RESPONSE_INFO: u8 = 0x19;
    pub(super) const RESPONSE_INFO: u8 = 0x1A;
    pub(super) const SERVER_REFERENCE: u8 = 0x1C;
    pub(super) const REASON_STRING: u8 = 0x1F;
    pub(super) const RECEIVE_MAXIMUM: u8 = 0x21;
    pub(super) const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
    pub(super) const TOPIC_ALIAS: u8 = 0x23;
    pub(super) const MAXIMUM_QOS: u8 = 0x24;
    pub(super) const RETAIN_AVAILABLE: u8 = 0x25;
    pub(super) const USER_PROPERTY: u8 = 0x26;
    pub(super) const MAXIMUM_PACKET_SIZE: u8 = 0x27;
    pub(super) const WILDCARD_AVAILABLE: u8 = 0x28;
    pub(super) const SUBSCRIPTION_ID_AVAILABLE: u8 = 0x29;
    pub(super) const SHARED_AVAILABLE: u8 = 0x2A;
}

fn is_known(id: u8) -> bool {
    matches!(id, 0x01 | 0x02 | 0x03 | 0x08 | 0x09 | 0x0B | 0x11..=0x13 | 0x15..=0x1A | 0x1C | 0x1F | 0x21..=0x2A)
}

/// The specification's table of which packets each property may appear in.
fn allowed(id: u8, scope: Scope) -> bool {
    use Scope::*;
    match id {
        0x01..=0x03 | 0x08 | 0x09 => matches!(scope, Publish | Will),
        0x0B => matches!(scope, Publish | Subscribe),
        0x11 => matches!(scope, Connect | ConnAck | Disconnect),
        0x12 | 0x13 | 0x1A | 0x24 | 0x25 | 0x28..=0x2A => scope == ConnAck,
        0x15 | 0x16 => matches!(scope, Connect | ConnAck | Auth),
        0x17 | 0x19 => scope == Connect,
        0x18 => scope == Will,
        0x1C => matches!(scope, ConnAck | Disconnect),
        0x1F => matches!(scope, ConnAck | Ack | SubAck | UnsubAck | Disconnect | Auth),
        0x21 | 0x22 | 0x27 => matches!(scope, Connect | ConnAck),
        0x23 => scope == Publish,
        0x26 => true,
        _ => false,
    }
}

impl Properties {
    pub fn is_empty(&self) -> bool {
        *self == Properties::default()
    }

    /// Reads the property length and then that many bytes of properties.
    pub(crate) fn decode(
        r: &mut Reader,
        scope: Scope,
        limits: &Limits,
    ) -> Result<Properties, DecodeError> {
        let len = r.varint()? as usize;
        let mut body = r.sub(len)?;
        let mut p = Properties::default();
        while !body.is_empty() {
            let raw = body.varint()?;
            let id = u8::try_from(raw)
                .ok()
                .filter(|id| is_known(*id))
                .ok_or(DecodeError::UnknownProperty(raw))?;
            if !allowed(id, scope) {
                return Err(DecodeError::PropertyNotAllowed {
                    id,
                    packet: scope.name(),
                });
            }
            p.read_one(id, &mut body, limits)?;
        }
        if p.authentication_data.is_some() && p.authentication_method.is_none() {
            return Err(DecodeError::MissingProperty(id::AUTH_METHOD));
        }
        Ok(p)
    }

    fn read_one(&mut self, id: u8, r: &mut Reader, limits: &Limits) -> Result<(), DecodeError> {
        match id {
            id::PAYLOAD_FORMAT => set(&mut self.payload_format_indicator, id, flag(r.u8()?, id)?),
            id::MESSAGE_EXPIRY => set(&mut self.message_expiry_interval, id, r.u32()?),
            id::CONTENT_TYPE => set(&mut self.content_type, id, r.string()?),
            id::RESPONSE_TOPIC => {
                let topic = r.string()?;
                let bounds = TopicBounds {
                    max_bytes: limits.max_topic_bytes,
                    max_levels: limits.max_topic_levels,
                };
                check_topic_name(topic.as_str(), bounds)
                    .map_err(|_| DecodeError::InvalidPropertyValue(id))?;
                set(&mut self.response_topic, id, topic)
            }
            id::CORRELATION_DATA => set(&mut self.correlation_data, id, r.binary()?),
            id::SUBSCRIPTION_ID => {
                let value = r.varint()?;
                if value == 0 {
                    return Err(DecodeError::InvalidPropertyValue(id));
                }
                if self.subscription_identifiers.len() >= limits.max_repeated_properties {
                    return Err(DecodeError::LimitExceeded {
                        what: "subscription identifiers",
                        max: limits.max_repeated_properties,
                    });
                }
                self.subscription_identifiers.push(value);
                Ok(())
            }
            id::SESSION_EXPIRY => set(&mut self.session_expiry_interval, id, r.u32()?),
            id::ASSIGNED_CLIENT_ID => set(&mut self.assigned_client_identifier, id, r.string()?),
            id::SERVER_KEEP_ALIVE => set(&mut self.server_keep_alive, id, r.u16()?),
            id::AUTH_METHOD => set(&mut self.authentication_method, id, r.string()?),
            id::AUTH_DATA => set(&mut self.authentication_data, id, r.binary()?),
            id::REQUEST_PROBLEM_INFO => set(
                &mut self.request_problem_information,
                id,
                flag(r.u8()?, id)?,
            ),
            id::WILL_DELAY => set(&mut self.will_delay_interval, id, r.u32()?),
            id::REQUEST_RESPONSE_INFO => set(
                &mut self.request_response_information,
                id,
                flag(r.u8()?, id)?,
            ),
            id::RESPONSE_INFO => set(&mut self.response_information, id, r.string()?),
            id::SERVER_REFERENCE => set(&mut self.server_reference, id, r.string()?),
            id::REASON_STRING => set(&mut self.reason_string, id, r.string()?),
            id::RECEIVE_MAXIMUM => set(&mut self.receive_maximum, id, nonzero_u16(r.u16()?, id)?),
            id::TOPIC_ALIAS_MAXIMUM => set(&mut self.topic_alias_maximum, id, r.u16()?),
            id::TOPIC_ALIAS => set(&mut self.topic_alias, id, nonzero_u16(r.u16()?, id)?),
            id::MAXIMUM_QOS => {
                let qos = match r.u8()? {
                    0 => QoS::AtMostOnce,
                    1 => QoS::AtLeastOnce,
                    _ => return Err(DecodeError::InvalidPropertyValue(id)),
                };
                set(&mut self.maximum_qos, id, qos)
            }
            id::RETAIN_AVAILABLE => set(&mut self.retain_available, id, flag(r.u8()?, id)?),
            id::USER_PROPERTY => {
                if self.user_properties.len() >= limits.max_repeated_properties {
                    return Err(DecodeError::LimitExceeded {
                        what: "user properties",
                        max: limits.max_repeated_properties,
                    });
                }
                let name = r.string()?;
                let value = r.string()?;
                self.user_properties.push((name, value));
                Ok(())
            }
            id::MAXIMUM_PACKET_SIZE => {
                let size = r.u32()?;
                if size == 0 {
                    return Err(DecodeError::InvalidPropertyValue(id));
                }
                set(&mut self.maximum_packet_size, id, size)
            }
            id::WILDCARD_AVAILABLE => set(
                &mut self.wildcard_subscription_available,
                id,
                flag(r.u8()?, id)?,
            ),
            id::SUBSCRIPTION_ID_AVAILABLE => set(
                &mut self.subscription_identifiers_available,
                id,
                flag(r.u8()?, id)?,
            ),
            id::SHARED_AVAILABLE => set(
                &mut self.shared_subscription_available,
                id,
                flag(r.u8()?, id)?,
            ),
            other => Err(DecodeError::UnknownProperty(u32::from(other))),
        }
    }

    /// Bytes the properties take, not counting the length that precedes them.
    /// Refuses a property the packet may not carry or a value the format
    /// cannot hold, so it runs before anything is written.
    pub(crate) fn encoded_len(&self, scope: Scope) -> Result<usize, EncodeError> {
        let mut len = 0usize;
        self.visit(scope, &mut |item| {
            len += item.len();
            Ok(())
        })?;
        Ok(len)
    }

    /// Bytes for the property length and the properties.
    pub(crate) fn section_len(&self, scope: Scope) -> Result<usize, EncodeError> {
        let len = self.encoded_len(scope)?;
        Ok(varint_len(len) + len)
    }

    /// Writes the property length and the properties. `encoded_len` has
    /// accepted them already.
    pub(crate) fn write_section(
        &self,
        scope: Scope,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        put_varint(dst, self.encoded_len(scope)?);
        self.visit(scope, &mut |item| {
            item.write(dst);
            Ok(())
        })
    }

    /// Calls `f` for each property present, in identifier order, after checking
    /// that it belongs in `scope` and holds a value the format allows.
    fn visit(
        &self,
        scope: Scope,
        f: &mut dyn FnMut(Item<'_>) -> Result<(), EncodeError>,
    ) -> Result<(), EncodeError> {
        if self.authentication_data.is_some() && self.authentication_method.is_none() {
            return Err(EncodeError::InvalidProperty(id::AUTH_DATA));
        }
        let mut emit = |id: u8, item: Item<'_>| -> Result<(), EncodeError> {
            if !allowed(id, scope) {
                return Err(EncodeError::InvalidProperty(id));
            }
            f(item)
        };
        if let Some(v) = self.payload_format_indicator {
            emit(id::PAYLOAD_FORMAT, Item::Byte(id::PAYLOAD_FORMAT, v.into()))?;
        }
        if let Some(v) = self.message_expiry_interval {
            emit(id::MESSAGE_EXPIRY, Item::U32(id::MESSAGE_EXPIRY, v))?;
        }
        if let Some(v) = &self.content_type {
            emit(id::CONTENT_TYPE, Item::Str(id::CONTENT_TYPE, v))?;
        }
        if let Some(v) = &self.response_topic {
            check_topic_name(v.as_str(), TopicBounds::FORMAT)
                .map_err(|_| EncodeError::InvalidProperty(id::RESPONSE_TOPIC))?;
            emit(id::RESPONSE_TOPIC, Item::Str(id::RESPONSE_TOPIC, v))?;
        }
        if let Some(v) = &self.correlation_data {
            emit(
                id::CORRELATION_DATA,
                Item::Bin(id::CORRELATION_DATA, binary(v)?),
            )?;
        }
        for &v in &self.subscription_identifiers {
            if v == 0 || v > MAX_VARINT {
                return Err(EncodeError::InvalidProperty(id::SUBSCRIPTION_ID));
            }
            emit(id::SUBSCRIPTION_ID, Item::Var(id::SUBSCRIPTION_ID, v))?;
        }
        if let Some(v) = self.session_expiry_interval {
            emit(id::SESSION_EXPIRY, Item::U32(id::SESSION_EXPIRY, v))?;
        }
        if let Some(v) = &self.assigned_client_identifier {
            emit(id::ASSIGNED_CLIENT_ID, Item::Str(id::ASSIGNED_CLIENT_ID, v))?;
        }
        if let Some(v) = self.server_keep_alive {
            emit(id::SERVER_KEEP_ALIVE, Item::U16(id::SERVER_KEEP_ALIVE, v))?;
        }
        if let Some(v) = &self.authentication_method {
            emit(id::AUTH_METHOD, Item::Str(id::AUTH_METHOD, v))?;
        }
        if let Some(v) = &self.authentication_data {
            emit(id::AUTH_DATA, Item::Bin(id::AUTH_DATA, binary(v)?))?;
        }
        if let Some(v) = self.request_problem_information {
            emit(
                id::REQUEST_PROBLEM_INFO,
                Item::Byte(id::REQUEST_PROBLEM_INFO, v.into()),
            )?;
        }
        if let Some(v) = self.will_delay_interval {
            emit(id::WILL_DELAY, Item::U32(id::WILL_DELAY, v))?;
        }
        if let Some(v) = self.request_response_information {
            emit(
                id::REQUEST_RESPONSE_INFO,
                Item::Byte(id::REQUEST_RESPONSE_INFO, v.into()),
            )?;
        }
        if let Some(v) = &self.response_information {
            emit(id::RESPONSE_INFO, Item::Str(id::RESPONSE_INFO, v))?;
        }
        if let Some(v) = &self.server_reference {
            emit(id::SERVER_REFERENCE, Item::Str(id::SERVER_REFERENCE, v))?;
        }
        if let Some(v) = &self.reason_string {
            emit(id::REASON_STRING, Item::Str(id::REASON_STRING, v))?;
        }
        if let Some(v) = self.receive_maximum {
            if v == 0 {
                return Err(EncodeError::InvalidProperty(id::RECEIVE_MAXIMUM));
            }
            emit(id::RECEIVE_MAXIMUM, Item::U16(id::RECEIVE_MAXIMUM, v))?;
        }
        if let Some(v) = self.topic_alias_maximum {
            emit(
                id::TOPIC_ALIAS_MAXIMUM,
                Item::U16(id::TOPIC_ALIAS_MAXIMUM, v),
            )?;
        }
        if let Some(v) = self.topic_alias {
            if v == 0 {
                return Err(EncodeError::InvalidProperty(id::TOPIC_ALIAS));
            }
            emit(id::TOPIC_ALIAS, Item::U16(id::TOPIC_ALIAS, v))?;
        }
        if let Some(v) = self.maximum_qos {
            if v == QoS::ExactlyOnce {
                return Err(EncodeError::InvalidMaximumQos);
            }
            emit(id::MAXIMUM_QOS, Item::Byte(id::MAXIMUM_QOS, v as u8))?;
        }
        if let Some(v) = self.retain_available {
            emit(
                id::RETAIN_AVAILABLE,
                Item::Byte(id::RETAIN_AVAILABLE, v.into()),
            )?;
        }
        for (name, value) in &self.user_properties {
            emit(id::USER_PROPERTY, Item::Pair(name, value))?;
        }
        if let Some(v) = self.maximum_packet_size {
            if v == 0 {
                return Err(EncodeError::InvalidProperty(id::MAXIMUM_PACKET_SIZE));
            }
            emit(
                id::MAXIMUM_PACKET_SIZE,
                Item::U32(id::MAXIMUM_PACKET_SIZE, v),
            )?;
        }
        if let Some(v) = self.wildcard_subscription_available {
            emit(
                id::WILDCARD_AVAILABLE,
                Item::Byte(id::WILDCARD_AVAILABLE, v.into()),
            )?;
        }
        if let Some(v) = self.subscription_identifiers_available {
            emit(
                id::SUBSCRIPTION_ID_AVAILABLE,
                Item::Byte(id::SUBSCRIPTION_ID_AVAILABLE, v.into()),
            )?;
        }
        if let Some(v) = self.shared_subscription_available {
            emit(
                id::SHARED_AVAILABLE,
                Item::Byte(id::SHARED_AVAILABLE, v.into()),
            )?;
        }
        Ok(())
    }
}

/// One property ready to write.
enum Item<'a> {
    Byte(u8, u8),
    U16(u8, u16),
    U32(u8, u32),
    Var(u8, u32),
    Str(u8, &'a Utf8),
    Bin(u8, &'a Bytes),
    Pair(&'a Utf8, &'a Utf8),
}

impl Item<'_> {
    fn len(&self) -> usize {
        1 + match self {
            Item::Byte(..) => 1,
            Item::U16(..) => 2,
            Item::U32(..) => 4,
            Item::Var(_, v) => varint_len(*v as usize),
            Item::Str(_, s) => 2 + s.len(),
            Item::Bin(_, b) => 2 + b.len(),
            Item::Pair(k, v) => 4 + k.len() + v.len(),
        }
    }

    fn write(&self, dst: &mut BytesMut) {
        match self {
            Item::Byte(id, v) => {
                dst.put_u8(*id);
                dst.put_u8(*v);
            }
            Item::U16(id, v) => {
                dst.put_u8(*id);
                dst.put_u16(*v);
            }
            Item::U32(id, v) => {
                dst.put_u8(*id);
                dst.put_u32(*v);
            }
            Item::Var(id, v) => {
                dst.put_u8(*id);
                put_varint(dst, *v as usize);
            }
            Item::Str(id, s) => {
                dst.put_u8(*id);
                put_string(dst, s);
            }
            Item::Bin(id, b) => {
                dst.put_u8(*id);
                put_binary(dst, b);
            }
            Item::Pair(k, v) => {
                dst.put_u8(id::USER_PROPERTY);
                put_string(dst, k);
                put_string(dst, v);
            }
        }
    }
}

fn set<T>(slot: &mut Option<T>, id: u8, value: T) -> Result<(), DecodeError> {
    if slot.is_some() {
        return Err(DecodeError::DuplicateProperty(id));
    }
    *slot = Some(value);
    Ok(())
}

fn flag(byte: u8, id: u8) -> Result<bool, DecodeError> {
    match byte {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::InvalidPropertyValue(id)),
    }
}

fn nonzero_u16(value: u16, id: u8) -> Result<u16, DecodeError> {
    if value == 0 {
        return Err(DecodeError::InvalidPropertyValue(id));
    }
    Ok(value)
}

pub(crate) fn binary(b: &Bytes) -> Result<&Bytes, EncodeError> {
    if b.len() > MAX_FIELD_BYTES {
        return Err(EncodeError::FieldTooLong);
    }
    Ok(b)
}

#[cfg(test)]
mod tests;

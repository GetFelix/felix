//! `CONNECT`: the first packet a client sends.

use bytes::{BufMut, Bytes, BytesMut};

use super::check_properties_fit;
use crate::error::{DecodeError, EncodeError};
use crate::limits::{Limits, MAX_FIELD_BYTES};
use crate::property::{Properties, Scope};
use crate::protocol::{QoS, Version};
use crate::reader::Reader;
use crate::text::Utf8;
use crate::topic::{TopicBounds, check_topic_name};
use crate::writer::{put_binary, put_string};

const CLEAN_START: u8 = 0b0000_0010;
const WILL: u8 = 0b0000_0100;
const WILL_QOS_SHIFT: u8 = 3;
const WILL_RETAIN: u8 = 0b0010_0000;
const PASSWORD: u8 = 0b0100_0000;
const USERNAME: u8 = 0b1000_0000;

/// A `CONNECT` packet. Its protocol level is the connection's version from here
/// on.
///
/// Whether an empty client identifier is acceptable with `clean_start` false is
/// the session's decision: 3.1.1 answers it with a `CONNACK` refusal, not a
/// closed connection, so the codec passes it through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub version: Version,
    /// 3.1.1 calls this "clean session".
    pub clean_start: bool,
    pub keep_alive: u16,
    pub client_id: Utf8,
    pub will: Option<Will>,
    pub username: Option<Utf8>,
    pub password: Option<Bytes>,
    /// Empty under 3.1.1.
    pub properties: Properties,
}

/// The message the server publishes if the client goes away uncleanly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    pub topic: Utf8,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    /// Empty under 3.1.1.
    pub properties: Properties,
}

impl Connect {
    pub(super) fn decode(r: &mut Reader, limits: &Limits) -> Result<Connect, DecodeError> {
        let name = r.binary()?;
        let legacy = name.as_ref() == b"MQIsdp";
        if name.as_ref() != b"MQTT" && !legacy {
            return Err(DecodeError::InvalidProtocolName);
        }
        let level = r.u8()?;
        let version = match Version::from_level(level) {
            Some(v) if !legacy => v,
            _ => return Err(DecodeError::UnsupportedProtocolVersion(level)),
        };

        let flags = r.u8()?;
        if flags & 1 != 0 {
            return Err(DecodeError::InvalidConnectFlags(flags));
        }
        let has_will = flags & WILL != 0;
        let will_qos = (flags >> WILL_QOS_SHIFT) & 0b11;
        let will_retain = flags & WILL_RETAIN != 0;
        if !has_will && (will_qos != 0 || will_retain) {
            return Err(DecodeError::InvalidConnectFlags(flags));
        }
        let has_username = flags & USERNAME != 0;
        let has_password = flags & PASSWORD != 0;
        if !version.is_v5() && has_password && !has_username {
            return Err(DecodeError::InvalidConnectFlags(flags));
        }
        let keep_alive = r.u16()?;
        let properties = if version.is_v5() {
            Properties::decode(r, Scope::Connect, limits)?
        } else {
            Properties::default()
        };

        let client_id = r.string()?;
        if client_id.len() > limits.max_client_id_bytes {
            return Err(DecodeError::LimitExceeded {
                what: "client identifier bytes",
                max: limits.max_client_id_bytes,
            });
        }

        let will = if has_will {
            let qos = QoS::from_bits(will_qos).ok_or(DecodeError::InvalidQos)?;
            let will_properties = if version.is_v5() {
                Properties::decode(r, Scope::Will, limits)?
            } else {
                Properties::default()
            };
            let topic = r.string()?;
            let bounds = TopicBounds {
                max_bytes: limits.max_topic_bytes,
                max_levels: limits.max_topic_levels,
            };
            check_topic_name(topic.as_str(), bounds).map_err(DecodeError::InvalidTopicName)?;
            let payload = r.binary()?;
            Some(Will {
                topic,
                payload,
                qos,
                retain: will_retain,
                properties: will_properties,
            })
        } else {
            None
        };

        let username = if has_username {
            Some(r.string()?)
        } else {
            None
        };
        let password = if has_password {
            Some(r.binary()?)
        } else {
            None
        };

        Ok(Connect {
            version,
            clean_start: flags & CLEAN_START != 0,
            keep_alive,
            client_id,
            will,
            username,
            password,
            properties,
        })
    }

    pub(super) fn body_len(&self, version: Version) -> Result<usize, EncodeError> {
        if self.version != version {
            return Err(EncodeError::VersionMismatch);
        }
        check_properties_fit(version, &self.properties)?;
        if !version.is_v5() && self.password.is_some() && self.username.is_none() {
            return Err(EncodeError::PasswordWithoutUsername);
        }
        // Protocol name, level, flags, keep alive, then the client identifier.
        let mut len = 2 + 4 + 1 + 1 + 2 + 2 + self.client_id.len();
        if version.is_v5() {
            len += self.properties.section_len(Scope::Connect)?;
        }
        if let Some(will) = &self.will {
            check_properties_fit(version, &will.properties)?;
            check_topic_name(will.topic.as_str(), TopicBounds::FORMAT)
                .map_err(EncodeError::InvalidTopicName)?;
            if will.payload.len() > MAX_FIELD_BYTES {
                return Err(EncodeError::FieldTooLong);
            }
            if version.is_v5() {
                len += will.properties.section_len(Scope::Will)?;
            }
            len += 2 + will.topic.len() + 2 + will.payload.len();
        }
        if let Some(username) = &self.username {
            len += 2 + username.len();
        }
        if let Some(password) = &self.password {
            if password.len() > MAX_FIELD_BYTES {
                return Err(EncodeError::FieldTooLong);
            }
            len += 2 + password.len();
        }
        Ok(len)
    }

    pub(super) fn write_body(
        &self,
        version: Version,
        dst: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        let mut flags = 0u8;
        if self.clean_start {
            flags |= CLEAN_START;
        }
        if let Some(will) = &self.will {
            flags |= WILL | (will.qos as u8) << WILL_QOS_SHIFT;
            if will.retain {
                flags |= WILL_RETAIN;
            }
        }
        if self.username.is_some() {
            flags |= USERNAME;
        }
        if self.password.is_some() {
            flags |= PASSWORD;
        }

        dst.put_u16(4);
        dst.put_slice(b"MQTT");
        dst.put_u8(version.level());
        dst.put_u8(flags);
        dst.put_u16(self.keep_alive);
        if version.is_v5() {
            self.properties.write_section(Scope::Connect, dst)?;
        }
        put_string(dst, &self.client_id);
        if let Some(will) = &self.will {
            if version.is_v5() {
                will.properties.write_section(Scope::Will, dst)?;
            }
            put_string(dst, &will.topic);
            put_binary(dst, &will.payload);
        }
        if let Some(username) = &self.username {
            put_string(dst, username);
        }
        if let Some(password) = &self.password {
            put_binary(dst, password);
        }
        Ok(())
    }
}

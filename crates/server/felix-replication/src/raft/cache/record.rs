//! A cache group's entry as bytes in the log. The stamp is not here: it is
//! the entry's `timestamp_micros`.
//!
//! Durable, so the layout is explicit and versioned. An entry this build
//! cannot read is an error, never skipped: a member that skipped one would
//! hold a different state from one that applied it.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};

use super::{Condition, Fields, invalid, put_key};

const VERSION: u8 = 1;

const PUT: u8 = 0;
const DELETE: u8 = 1;
const EXPIRE: u8 = 2;
const ADD: u8 = 3;
const RETIRE: u8 = 4;

const COND_NONE: u8 = 0;
const COND_ABSENT: u8 = 1;
const COND_VERSION: u8 = 2;

/// Which request a write is, so a retry is applied once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Request {
    /// The client, across connections and leaders.
    pub(crate) producer: u64,
    pub(crate) sequence: u64,
    /// The producer retries nothing below this sequence, so the answers
    /// kept for it can go.
    pub(crate) floor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CacheCommand {
    Put {
        key: String,
        value: Bytes,
        /// On the stamps' clock, in microseconds. Zero is never.
        expires_at: u64,
        cond: Condition,
        request: Request,
    },
    Delete {
        key: String,
        /// The version the key must hold, if the delete names one.
        version: Option<u64>,
        request: Request,
    },
    /// The leader saw `version` of `key` expire. Removes that version only.
    Expire { key: String, version: u64 },
    Add {
        key: String,
        delta: i64,
        request: Request,
    },
    /// `producer` sends nothing below `floor` again, usually one past its
    /// last sequence. Its answers go; the floor stays, so a copy of an
    /// answered request still in flight is not applied again.
    Retire { producer: u64, floor: u64 },
}

fn put_request(buf: &mut BytesMut, request: &Request) {
    buf.put_u64_le(request.producer);
    buf.put_u64_le(request.sequence);
    buf.put_u64_le(request.floor);
}

fn put_cond(buf: &mut BytesMut, cond: Condition) {
    let (tag, version) = match cond {
        Condition::None => (COND_NONE, 0),
        Condition::Absent => (COND_ABSENT, 0),
        Condition::Version(version) => (COND_VERSION, version),
    };
    buf.put_u8(tag);
    buf.put_u64_le(version);
}

fn request(fields: &mut Fields<'_>) -> io::Result<Request> {
    Ok(Request {
        producer: fields.u64()?,
        sequence: fields.u64()?,
        floor: fields.u64()?,
    })
}

fn cond(fields: &mut Fields<'_>) -> io::Result<Condition> {
    let (tag, version) = (fields.u8()?, fields.u64()?);
    match tag {
        COND_NONE => Ok(Condition::None),
        COND_ABSENT => Ok(Condition::Absent),
        COND_VERSION => Ok(Condition::Version(version)),
        found => Err(invalid(format!("a cache entry has condition {found}"))),
    }
}

impl CacheCommand {
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(VERSION);
        match self {
            Self::Put {
                key,
                value,
                expires_at,
                cond,
                request,
            } => {
                buf.put_u8(PUT);
                put_request(&mut buf, request);
                put_cond(&mut buf, *cond);
                buf.put_u64_le(*expires_at);
                put_key(&mut buf, key);
                // The rest of the entry: the log already delimits it.
                buf.put_slice(value);
            }
            Self::Delete {
                key,
                version,
                request,
            } => {
                buf.put_u8(DELETE);
                put_request(&mut buf, request);
                put_cond(
                    &mut buf,
                    version.map_or(Condition::None, Condition::Version),
                );
                put_key(&mut buf, key);
            }
            Self::Expire { key, version } => {
                buf.put_u8(EXPIRE);
                buf.put_u64_le(*version);
                put_key(&mut buf, key);
            }
            Self::Add {
                key,
                delta,
                request,
            } => {
                buf.put_u8(ADD);
                put_request(&mut buf, request);
                buf.put_i64_le(*delta);
                put_key(&mut buf, key);
            }
            Self::Retire { producer, floor } => {
                buf.put_u8(RETIRE);
                buf.put_u64_le(*producer);
                buf.put_u64_le(*floor);
            }
        }
        buf.freeze()
    }

    pub(crate) fn decode(payload: &Bytes) -> io::Result<Self> {
        let mut rest = payload.clone();
        let mut fields = Fields {
            what: "a cache entry",
            rest: &mut rest,
        };
        let version = fields.u8()?;
        if version != VERSION {
            return Err(invalid(format!("a cache entry has version {version}")));
        }
        let command = match fields.u8()? {
            PUT => {
                let request = request(&mut fields)?;
                let cond = cond(&mut fields)?;
                let expires_at = fields.u64()?;
                let key = fields.key()?;
                return Ok(Self::Put {
                    key,
                    value: rest,
                    expires_at,
                    cond,
                    request,
                });
            }
            DELETE => {
                let request = request(&mut fields)?;
                let version = match cond(&mut fields)? {
                    Condition::None => None,
                    Condition::Version(version) => Some(version),
                    Condition::Absent => {
                        return Err(invalid(
                            "a cache delete requires the key absent".to_string(),
                        ));
                    }
                };
                Self::Delete {
                    key: fields.key()?,
                    version,
                    request,
                }
            }
            EXPIRE => {
                let version = fields.u64()?;
                Self::Expire {
                    key: fields.key()?,
                    version,
                }
            }
            ADD => {
                let request = request(&mut fields)?;
                let delta = fields.i64()?;
                Self::Add {
                    key: fields.key()?,
                    delta,
                    request,
                }
            }
            RETIRE => Self::Retire {
                producer: fields.u64()?,
                floor: fields.u64()?,
            },
            found => return Err(invalid(format!("a cache entry has kind {found}"))),
        };
        fields.end()?;
        Ok(command)
    }
}

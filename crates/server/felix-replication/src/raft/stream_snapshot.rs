//! A stream shard's snapshot data: what a member that holds no record or
//! entry below the snapshot cannot read from its log. That is the cut
//! batches it must refuse by name ([`ForgottenCuts`]) and the consumer-group
//! table folded up to the snapshot's last entry ([`GroupTable`]).
//!
//! ```text
//! (empty)                     nothing carried
//! ForgottenCuts::encode       cut batches only, as before group tables
//! u32  magic "FLST"           both:
//! u32  crc32 of the rest
//! u32  cut batches length, then ForgottenCuts::encode (or nothing)
//! GroupTable::encode, to the end
//! ```
//!
//! A snapshot with no group writes keeps the older layout, so a member that
//! predates group tables still reads every snapshot a fleet without them
//! sends.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use felix_broker::GroupTable;
use felix_storage::disk_log::ForgottenCuts;

use super::codec::invalid;

const MAGIC: u32 = 0x464C_5354;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct StreamSnapshot {
    pub(crate) cuts: ForgottenCuts,
    pub(crate) groups: GroupTable,
}

impl StreamSnapshot {
    pub(crate) fn encode(&self) -> Bytes {
        if self.groups == GroupTable::default() {
            return if self.cuts.is_empty() {
                Bytes::new()
            } else {
                self.cuts.encode().into()
            };
        }
        let cuts = if self.cuts.is_empty() {
            Vec::new()
        } else {
            self.cuts.encode()
        };
        let mut body = BytesMut::new();
        body.put_u32(cuts.len() as u32);
        body.put_slice(&cuts);
        body.put_slice(&self.groups.encode());
        let mut out = BytesMut::with_capacity(8 + body.len());
        out.put_u32(MAGIC);
        out.put_u32(crc32fast::hash(&body));
        out.put_slice(&body);
        out.freeze()
    }

    /// Refused when the data is not one of the layouts above: a member that
    /// loaded part of it would refuse or fold differently from the others.
    pub(crate) fn decode(data: &[u8]) -> io::Result<Self> {
        if data.is_empty() {
            return Ok(Self::default());
        }
        if data.len() < 8 || u32::from_be_bytes(data[..4].try_into().expect("4 bytes")) != MAGIC {
            return Ok(Self {
                cuts: decode_cuts(data)?,
                groups: GroupTable::default(),
            });
        }
        let crc = u32::from_be_bytes(data[4..8].try_into().expect("4 bytes"));
        let body = &data[8..];
        if crc32fast::hash(body) != crc {
            return Err(damaged());
        }
        let len = body
            .get(..4)
            .map(|len| u32::from_be_bytes(len.try_into().expect("4 bytes")) as usize)
            .ok_or_else(damaged)?;
        let cuts = body.get(4..4 + len).ok_or_else(damaged)?;
        let cuts = if cuts.is_empty() {
            ForgottenCuts::default()
        } else {
            decode_cuts(cuts)?
        };
        let groups = GroupTable::decode(&body[4 + len..]).ok_or_else(damaged)?;
        // `encode` writes an empty table in the older layout, so one state
        // has one encoding.
        if groups == GroupTable::default() {
            return Err(damaged());
        }
        Ok(Self { cuts, groups })
    }
}

fn decode_cuts(data: &[u8]) -> io::Result<ForgottenCuts> {
    ForgottenCuts::decode(data)
        .ok_or_else(|| invalid("a stream snapshot's forgotten cut batches do not parse".into()))
}

fn damaged() -> io::Error {
    invalid("a stream snapshot's group table does not parse".into())
}

#[cfg(test)]
mod tests;

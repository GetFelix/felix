//! A cache group's snapshot data: the projection as of one log id.
//!
//! It holds the log's time, every value unexpired at that time, every
//! counter, and each producer's kept answers. It is a function of the
//! applied entries alone, so two members that snapshot at the same index
//! write the same bytes.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};

use super::state::{Answer, CacheState, Entry, Producer};
use super::{Fields, invalid, put_key};

const VERSION: u8 = 1;

const WRITTEN: u8 = 0;
const REMOVED: u8 = 1;
const ABSENT: u8 = 2;
const REFUSED: u8 = 3;
const REFUSED_AT: u8 = 4;
const SUM: u8 = 5;
const FORGOTTEN: u8 = 6;

fn put_answer(buf: &mut BytesMut, answer: Answer) {
    let (tag, value) = match answer {
        Answer::Written { version } => (WRITTEN, version),
        Answer::Removed { version } => (REMOVED, version),
        Answer::Absent => (ABSENT, 0),
        Answer::Refused { version: None } => (REFUSED, 0),
        Answer::Refused {
            version: Some(version),
        } => (REFUSED_AT, version),
        Answer::Sum(sum) => (SUM, sum as u64),
        Answer::Forgotten => (FORGOTTEN, 0),
    };
    buf.put_u8(tag);
    buf.put_u64_le(value);
}

fn answer(fields: &mut Fields<'_>) -> io::Result<Answer> {
    let (tag, value) = (fields.u8()?, fields.u64()?);
    Ok(match tag {
        WRITTEN => Answer::Written { version: value },
        REMOVED => Answer::Removed { version: value },
        ABSENT => Answer::Absent,
        REFUSED => Answer::Refused { version: None },
        REFUSED_AT => Answer::Refused {
            version: Some(value),
        },
        SUM => Answer::Sum(value as i64),
        FORGOTTEN => Answer::Forgotten,
        found => return Err(invalid(format!("a cache snapshot has answer {found}"))),
    })
}

impl CacheState {
    pub(crate) fn snapshot(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(VERSION);
        buf.put_u64_le(self.time);
        // An expired value is left out. No write or read can tell: each
        // treats it as absent.
        let live = || {
            self.entries
                .iter()
                .filter(|(_, entry)| entry.live(self.time))
        };
        buf.put_u64_le(live().count() as u64);
        for (key, entry) in live() {
            put_key(&mut buf, key);
            buf.put_u64_le(entry.version);
            buf.put_u64_le(entry.expires_at);
            buf.put_u32_le(entry.value.len() as u32);
            buf.put_slice(&entry.value);
        }
        buf.put_u64_le(self.counters.len() as u64);
        for (key, sum) in &self.counters {
            put_key(&mut buf, key);
            buf.put_i64_le(*sum);
        }
        buf.put_u64_le(self.producers.len() as u64);
        for (producer, known) in &self.producers {
            buf.put_u64_le(*producer);
            buf.put_u64_le(known.floor);
            buf.put_u64_le(known.answers.len() as u64);
            for (sequence, answer) in &known.answers {
                buf.put_u64_le(*sequence);
                put_answer(&mut buf, *answer);
            }
        }
        buf.freeze()
    }

    /// The state a snapshot holds. `applied` says whether the snapshot is
    /// as of some entry. Only one that is not may have no data: every
    /// snapshot a cache group builds has some.
    pub(crate) fn restore(data: &Bytes, applied: bool) -> io::Result<Self> {
        let mut state = Self::default();
        if data.is_empty() {
            return match applied {
                false => Ok(state),
                true => Err(invalid(
                    "a cache snapshot as of an entry has no data".to_string(),
                )),
            };
        }
        let mut rest = data.clone();
        let mut fields = Fields {
            what: "a cache snapshot",
            rest: &mut rest,
        };
        let version = fields.u8()?;
        if version != VERSION {
            return Err(invalid(format!("a cache snapshot has version {version}")));
        }
        state.time = fields.u64()?;
        for _ in 0..fields.u64()? {
            let key = fields.key()?;
            let entry = Entry {
                version: fields.u64()?,
                expires_at: fields.u64()?,
                // A copy: a slice would pin the whole snapshot buffer for
                // as long as one value from it lives.
                value: Bytes::copy_from_slice(&fields.bytes()?),
            };
            if entry.expires_at != 0 {
                state.expiring.insert((entry.expires_at, key.clone()));
            }
            state.entries.insert(key, entry);
        }
        for _ in 0..fields.u64()? {
            let key = fields.key()?;
            state.counters.insert(key, fields.i64()?);
        }
        for _ in 0..fields.u64()? {
            let producer = fields.u64()?;
            let mut known = Producer {
                floor: fields.u64()?,
                ..Producer::default()
            };
            for _ in 0..fields.u64()? {
                let sequence = fields.u64()?;
                known.answers.insert(sequence, answer(&mut fields)?);
            }
            state.producers.insert(producer, known);
        }
        fields.end()?;
        Ok(state)
    }
}

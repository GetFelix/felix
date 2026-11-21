//! The bodies of `RaftBeats` and `RaftBeatsAnswer`: several heartbeats for
//! one broker, each a whole `Raft` body, and one answer per heartbeat in the
//! same order. See `docs/internal-protocol.md`, "Raft heartbeats in one
//! frame".

use std::io;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{LAYOUT, expect_empty, expect_layout, invalid, len_u32, take_u8, take_u16, take_u32};

/// The second byte of a `RaftBeats` body.
const BEATS: u8 = 9;
/// The second byte of a `RaftBeatsAnswer` body.
const BEATS_ANSWERED: u8 = 7;
/// A heartbeat the member answered: its `RaftAnswer` body follows.
const ANSWERED: u8 = 1;
/// A heartbeat the member did not answer before the frame's answer went.
/// Its sender sends it again on its own.
const LATE: u8 = 2;

/// Most heartbeats one frame carries. A few hundred bytes each, so a full
/// frame stays well under the internal frame limit.
pub(crate) const MAX_BEATS: usize = 512;

/// A `RaftBeats` body. `deadline` goes where a `Raft` body has its own, so
/// the listener bounds the frame the same way.
pub(crate) fn encode(deadline: Duration, beats: &[Bytes]) -> io::Result<Bytes> {
    if beats.len() > MAX_BEATS {
        return Err(invalid(format!("{} heartbeats in one frame", beats.len())));
    }
    let mut buf = BytesMut::new();
    buf.put_u8(LAYOUT);
    buf.put_u8(BEATS);
    buf.put_u32(u32::try_from(deadline.as_millis()).unwrap_or(u32::MAX));
    buf.put_u16(beats.len() as u16);
    for beat in beats {
        buf.put_u32(len_u32(beat.len())?);
        buf.extend_from_slice(beat);
    }
    Ok(buf.freeze())
}

/// The frame's deadline and its heartbeats' `Raft` bodies, in order.
pub(crate) fn decode(mut buf: Bytes) -> io::Result<(Duration, Vec<Bytes>)> {
    expect_layout(&mut buf)?;
    if take_u8(&mut buf)? != BEATS {
        return Err(invalid("not a heartbeat frame".to_owned()));
    }
    let deadline = Duration::from_millis(u64::from(take_u32(&mut buf)?));
    let count = usize::from(take_u16(&mut buf)?);
    if count > MAX_BEATS {
        return Err(invalid(format!("{count} heartbeats in one frame")));
    }
    let mut beats = Vec::with_capacity(count);
    for _ in 0..count {
        beats.push(take_body(&mut buf)?);
    }
    expect_empty(&buf)?;
    Ok((deadline, beats))
}

/// A `RaftBeatsAnswer` body: each heartbeat's `RaftAnswer` body, or `None`
/// for one that was late.
pub(crate) fn encode_answers(answers: &[Option<Bytes>]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(LAYOUT);
    buf.put_u8(BEATS_ANSWERED);
    buf.put_u16(answers.len().min(MAX_BEATS) as u16);
    for answer in answers.iter().take(MAX_BEATS) {
        match answer {
            // A reply is a few hundred bytes at most.
            Some(body) => {
                buf.put_u8(ANSWERED);
                buf.put_u32(body.len() as u32);
                buf.extend_from_slice(body);
            }
            None => buf.put_u8(LATE),
        }
    }
    buf.freeze()
}

/// The answers in a `RaftBeatsAnswer` body, refused unless there are exactly
/// `expected`: one heartbeat's answer must never reach another's caller.
pub(crate) fn decode_answers(mut buf: Bytes, expected: usize) -> io::Result<Vec<Option<Bytes>>> {
    expect_layout(&mut buf)?;
    if take_u8(&mut buf)? != BEATS_ANSWERED {
        return Err(invalid("not a heartbeat answer".to_owned()));
    }
    let count = usize::from(take_u16(&mut buf)?);
    if count != expected {
        return Err(invalid(format!("{count} answers to {expected} heartbeats")));
    }
    let mut answers = Vec::with_capacity(count);
    for _ in 0..count {
        answers.push(match take_u8(&mut buf)? {
            ANSWERED => Some(take_body(&mut buf)?),
            LATE => None,
            other => return Err(invalid(format!("answer tag {other}"))),
        });
    }
    expect_empty(&buf)?;
    Ok(answers)
}

fn take_body(buf: &mut Bytes) -> io::Result<Bytes> {
    let len = take_u32(buf)? as usize;
    if len > buf.remaining() {
        return Err(super::short());
    }
    Ok(buf.split_to(len))
}

#[cfg(test)]
mod tests;

//! Writing fields into a packet.

use bytes::{BufMut, Bytes, BytesMut};

use crate::limits::MAX_VARINT;
use crate::text::Utf8;

/// Bytes a variable byte integer takes. `value` must be at most `MAX_VARINT`.
pub(crate) fn varint_len(value: usize) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

pub(crate) fn put_varint(dst: &mut BytesMut, value: usize) {
    debug_assert!(value <= MAX_VARINT as usize);
    let mut value = value;
    loop {
        let byte = (value % 128) as u8;
        value /= 128;
        if value == 0 {
            dst.put_u8(byte);
            return;
        }
        dst.put_u8(byte | 0x80);
    }
}

/// The caller has checked the field fits: `Utf8` is at most 65535 bytes by
/// construction, and binary data is checked when the packet size is computed.
pub(crate) fn put_string(dst: &mut BytesMut, s: &Utf8) {
    dst.put_u16(s.len() as u16);
    dst.put_slice(s.as_bytes());
}

pub(crate) fn put_binary(dst: &mut BytesMut, b: &Bytes) {
    dst.put_u16(b.len() as u16);
    dst.put_slice(b);
}

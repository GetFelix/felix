use bytes::{Bytes, BytesMut};

use super::*;
use crate::writer::{put_varint, varint_len};

fn reader(bytes: &[u8]) -> Reader {
    Reader::new(Bytes::copy_from_slice(bytes))
}

#[test]
fn varints_round_trip_at_every_width_boundary() {
    for value in [
        0u32,
        1,
        127,
        128,
        129,
        16_383,
        16_384,
        2_097_151,
        2_097_152,
        268_435_455,
    ] {
        let mut buf = BytesMut::new();
        put_varint(&mut buf, value as usize);
        assert_eq!(buf.len(), varint_len(value as usize), "{value}");
        assert_eq!(reader(&buf).varint().unwrap(), value);
    }
}

#[test]
fn varint_widths_match_the_specification_table() {
    let widths = [
        (127, 1),
        (128, 2),
        (16_383, 2),
        (16_384, 3),
        (2_097_151, 3),
        (2_097_152, 4),
    ];
    for (value, width) in widths {
        assert_eq!(varint_len(value), width, "{value}");
    }
}

#[test]
fn varints_must_be_short_and_minimal() {
    assert_eq!(
        reader(&[0x80, 0x00]).varint(),
        Err(DecodeError::MalformedLength)
    );
    assert_eq!(
        reader(&[0xFF, 0xFF, 0x80, 0x00]).varint(),
        Err(DecodeError::MalformedLength)
    );
    assert_eq!(
        reader(&[0x80, 0x80, 0x80, 0x80]).varint(),
        Err(DecodeError::MalformedLength)
    );
    assert_eq!(reader(&[0x80]).varint(), Err(DecodeError::Truncated));
    assert_eq!(reader(&[]).varint(), Err(DecodeError::Truncated));
    assert_eq!(reader(&[0xFF, 0xFF, 0xFF, 0x7F]).varint(), Ok(268_435_455));
}

#[test]
fn reads_are_bounds_checked() {
    assert_eq!(reader(&[1]).u16(), Err(DecodeError::Truncated));
    assert_eq!(reader(&[1, 2, 3]).u32(), Err(DecodeError::Truncated));
    assert_eq!(reader(&[0, 3, 1]).binary(), Err(DecodeError::Truncated));
    assert_eq!(reader(&[1]).take(2), Err(DecodeError::Truncated));
    assert!(reader(&[1]).sub(2).is_err());
}

#[test]
fn binary_and_string_fields_are_slices() {
    let mut r = reader(&[0, 2, b'h', b'i', 0, 1, 9, 7]);
    assert_eq!(r.string().unwrap(), "hi");
    assert_eq!(r.binary().unwrap().as_ref(), &[9]);
    assert_eq!(r.remaining(), 1);
    assert_eq!(r.finish(), Err(DecodeError::TrailingBytes(1)));
    assert_eq!(r.rest().as_ref(), &[7]);
    assert!(r.is_empty());
    assert_eq!(r.finish(), Ok(()));
}

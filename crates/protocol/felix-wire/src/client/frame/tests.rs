use bytes::{Bytes, BytesMut};

use crate::error::Error;
use crate::{Frame, FrameHeader, MAGIC, VERSION};

#[test]
fn round_trip() {
    // Encoding then decoding should preserve header and payload.
    let frame = Frame::new(0x1, Bytes::from_static(b"hello")).expect("frame");
    let encoded = frame.encode();
    let decoded = Frame::decode(encoded).expect("decode");
    assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
    assert_eq!(decoded.header.flags, 0x1);
}

#[test]
fn decode_rejects_invalid_magic() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&0xDEADBEEFu32.to_be_bytes());
    buf.extend_from_slice(&VERSION.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    let err = FrameHeader::decode(buf.freeze()).expect_err("invalid magic");
    assert!(matches!(err, Error::InvalidMagic));
}

#[test]
fn decode_rejects_unsupported_version() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&MAGIC.to_be_bytes());
    buf.extend_from_slice(&0xFFFFu16.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    let err = FrameHeader::decode(buf.freeze()).expect_err("unsupported version");
    assert!(matches!(err, Error::UnsupportedVersion(0xFFFF)));
}

#[test]
fn decode_rejects_incomplete_header() {
    let err = FrameHeader::decode(Bytes::from_static(b"short")).expect_err("incomplete");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn decode_rejects_incomplete_payload() {
    let header = FrameHeader {
        magic: MAGIC,
        version: VERSION,
        flags: 0,
        length: 5,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(b"hi");
    let err = Frame::decode(buf.freeze()).expect_err("incomplete payload");
    assert!(matches!(err, Error::Incomplete));
}

#[test]
fn frame_header_encode_decode() {
    let flags = crate::KNOWN_FLAGS;
    let header = FrameHeader::new(flags, 0xABCD);
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    let decoded = FrameHeader::decode(buf.freeze()).expect("decode");
    assert_eq!(decoded.magic, MAGIC);
    assert_eq!(decoded.version, VERSION);
    assert_eq!(decoded.flags, flags);
    assert_eq!(decoded.length, 0xABCD);
}

#[test]
fn frame_decode_error_cases() {
    // Test frame with invalid header length
    let short_bytes = Bytes::from_static(b"short");
    let result = Frame::decode(short_bytes);
    assert!(result.is_err());

    // Test frame header with incomplete payload
    let mut buf = BytesMut::new();
    let header = FrameHeader::new(0, 100); // Claims 100 bytes
    header.encode(&mut buf);
    buf.extend_from_slice(b"only_10"); // But only has 7 bytes
    let result = Frame::decode(buf.freeze());
    assert!(result.is_err());
}

/// All sixteen flag bits are defined, so no header is refused for its flags
/// alone; a body that cannot be parsed is the decoders' to refuse. The check
/// stays in `FrameHeader::decode` for the day a flag word is widened.
#[test]
fn every_flag_value_is_a_known_flag_or_the_extended_marker() {
    assert_eq!(crate::KNOWN_FLAGS | crate::FLAG_EXTENDED, u16::MAX);
    for flags in [crate::KNOWN_FLAGS, crate::FLAG_EXTENDED, u16::MAX] {
        let mut buf = BytesMut::new();
        FrameHeader::new(flags, 0).encode(&mut buf);
        let bytes = buf.freeze();
        assert_eq!(
            FrameHeader::decode(bytes.clone()).expect("known").flags,
            flags
        );
        assert!(Frame::decode(bytes).is_ok());
    }
}

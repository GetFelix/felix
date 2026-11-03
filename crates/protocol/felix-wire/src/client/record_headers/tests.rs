use bytes::Bytes;

use crate::RecordHeaders;
use crate::error::Error;

/// Key `k1`, then `a: 1` and `bc: xyz`.
const WITH_KEY: &[u8] = &[
    0x01, // flags: a key follows
    0x00, 0x00, 0x00, 0x02, b'k', b'1', // key
    0x00, 0x00, 0x00, 0x02, // count
    0x00, 0x01, b'a', 0x00, 0x00, 0x00, 0x01, b'1', // a: 1
    0x00, 0x02, b'b', b'c', 0x00, 0x00, 0x00, 0x03, b'x', b'y', b'z', // bc: xyz
];

/// No key, one header `a: 1`.
const NO_KEY: &[u8] = &[
    0x00, // flags
    0x00, 0x00, 0x00, 0x01, // count
    0x00, 0x01, b'a', 0x00, 0x00, 0x00, 0x01, b'1', // a: 1
];

fn with_key() -> RecordHeaders {
    RecordHeaders {
        key: Some(Bytes::from_static(b"k1")),
        headers: vec![
            ("a".to_owned(), Bytes::from_static(b"1")),
            ("bc".to_owned(), Bytes::from_static(b"xyz")),
        ],
    }
}

#[test]
fn a_block_with_a_key_and_two_headers_is_these_bytes() {
    assert_eq!(with_key().encode().expect("encode").as_ref(), WITH_KEY);
    assert_eq!(
        RecordHeaders::decode(Bytes::from_static(WITH_KEY)).expect("decode"),
        with_key()
    );
    RecordHeaders::validate(WITH_KEY).expect("validate");
}

#[test]
fn a_block_without_a_key_is_these_bytes() {
    let block = RecordHeaders {
        key: None,
        headers: vec![("a".to_owned(), Bytes::from_static(b"1"))],
    };
    assert_eq!(block.encode().expect("encode").as_ref(), NO_KEY);
    assert_eq!(
        RecordHeaders::decode(Bytes::from_static(NO_KEY)).expect("decode"),
        block
    );
    RecordHeaders::validate(NO_KEY).expect("validate");
}

#[test]
fn a_block_with_nothing_in_it_is_refused_both_ways() {
    let empty = RecordHeaders::default();
    assert!(empty.is_empty());
    assert!(matches!(
        empty.encode(),
        Err(Error::InvalidRecordHeaders(_))
    ));
    let five_zeros = [0u8; 5];
    assert!(matches!(
        RecordHeaders::validate(&five_zeros),
        Err(Error::InvalidRecordHeaders(_))
    ));
    assert!(RecordHeaders::decode(Bytes::copy_from_slice(&five_zeros)).is_err());

    // An empty key is a key, so a block holding only that is not empty.
    let only_key = RecordHeaders {
        key: Some(Bytes::new()),
        headers: Vec::new(),
    };
    let bytes = only_key.encode().expect("encode");
    assert_eq!(RecordHeaders::decode(bytes).expect("decode"), only_key);
}

#[test]
fn names_repeat_and_keep_their_order_and_an_empty_key_is_a_key() {
    let block = RecordHeaders {
        key: Some(Bytes::new()),
        headers: vec![
            ("x".to_owned(), Bytes::from_static(b"2")),
            ("x".to_owned(), Bytes::from_static(b"1")),
            ("y".to_owned(), Bytes::new()),
        ],
    };
    let decoded = RecordHeaders::decode(block.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, block);
    assert!(!decoded.is_empty());
}

#[test]
fn a_decoded_key_and_value_are_slices_of_the_block() {
    let block = Bytes::from_static(WITH_KEY);
    let decoded = RecordHeaders::decode(block.clone()).expect("decode");
    let range = block.as_ptr_range();
    let key = decoded.key.expect("key");
    assert!(range.contains(&key.as_ptr()));
    assert!(range.contains(&decoded.headers[1].1.as_ptr()));
}

#[test]
fn a_reserved_block_flag_is_refused() {
    for bit in 1..8u8 {
        let mut block = NO_KEY.to_vec();
        block[0] |= 1 << bit;
        assert!(
            matches!(
                RecordHeaders::validate(&block),
                Err(Error::InvalidRecordHeaders(_))
            ),
            "bit {bit}"
        );
        assert!(RecordHeaders::decode(Bytes::from(block)).is_err());
    }
}

#[test]
fn every_truncation_is_refused() {
    for block in [WITH_KEY, NO_KEY] {
        for cut in 0..block.len() {
            assert!(RecordHeaders::validate(&block[..cut]).is_err(), "cut {cut}");
            assert!(
                RecordHeaders::decode(Bytes::copy_from_slice(&block[..cut])).is_err(),
                "cut {cut}"
            );
        }
    }
}

#[test]
fn trailing_bytes_are_refused() {
    let mut block = NO_KEY.to_vec();
    block.push(0);
    assert!(matches!(
        RecordHeaders::validate(&block),
        Err(Error::InvalidRecordHeaders(_))
    ));
    assert!(RecordHeaders::decode(Bytes::from(block)).is_err());
}

#[test]
fn a_name_that_is_not_utf8_is_refused() {
    let mut block = NO_KEY.to_vec();
    block[7] = 0xFF;
    assert!(matches!(
        RecordHeaders::validate(&block),
        Err(Error::InvalidUtf8)
    ));
    assert!(matches!(
        RecordHeaders::decode(Bytes::from(block)),
        Err(Error::InvalidUtf8)
    ));
}

#[test]
fn an_empty_name_is_refused_both_ways() {
    let block = [0u8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    assert!(matches!(
        RecordHeaders::validate(&block),
        Err(Error::InvalidRecordHeaders(_))
    ));
    assert!(RecordHeaders::decode(Bytes::copy_from_slice(&block)).is_err());

    let empty_name = RecordHeaders {
        key: None,
        headers: vec![(String::new(), Bytes::new())],
    };
    assert!(matches!(
        empty_name.encode(),
        Err(Error::InvalidRecordHeaders(_))
    ));
}

#[test]
fn a_count_the_block_cannot_hold_is_refused_before_it_is_used() {
    let mut block = vec![0u8];
    block.extend_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        RecordHeaders::validate(&block),
        Err(Error::Incomplete)
    ));
    assert!(matches!(
        RecordHeaders::decode(Bytes::from(block)),
        Err(Error::Incomplete)
    ));
}

#[test]
fn a_key_length_past_the_end_is_refused() {
    let block = [1u8, 0xFF, 0xFF, 0xFF, 0xFF, b'k'];
    assert!(matches!(
        RecordHeaders::validate(&block),
        Err(Error::Incomplete)
    ));
}

#[test]
fn a_name_prefix_is_found_without_decoding() {
    // `a` and `bc`, plus a key that looks like a reserved name.
    assert!(RecordHeaders::any_name_starts_with(WITH_KEY, "b").unwrap());
    assert!(RecordHeaders::any_name_starts_with(WITH_KEY, "bc").unwrap());
    assert!(RecordHeaders::any_name_starts_with(WITH_KEY, "a").unwrap());
    assert!(!RecordHeaders::any_name_starts_with(WITH_KEY, "c").unwrap());
    // Case matters, and the key is not a header name.
    assert!(!RecordHeaders::any_name_starts_with(WITH_KEY, "B").unwrap());
    assert!(!RecordHeaders::any_name_starts_with(WITH_KEY, "k").unwrap());
    assert!(!RecordHeaders::any_name_starts_with(NO_KEY, "felix.").unwrap());
}

#[test]
fn the_reserved_prefix_matches_felix_names_only() {
    let block = RecordHeaders {
        key: None,
        headers: vec![
            ("trace".to_owned(), Bytes::from_static(b"1")),
            ("felix.mqtt.topic".to_owned(), Bytes::new()),
        ],
    }
    .encode()
    .unwrap();
    assert!(RecordHeaders::any_name_starts_with(&block, crate::RESERVED_HEADER_PREFIX).unwrap());
    let plain = RecordHeaders {
        key: None,
        headers: vec![("felixa".to_owned(), Bytes::new())],
    }
    .encode()
    .unwrap();
    assert!(!RecordHeaders::any_name_starts_with(&plain, crate::RESERVED_HEADER_PREFIX).unwrap());
}

#[test]
fn a_prefix_search_refuses_a_block_that_does_not_parse() {
    assert!(RecordHeaders::any_name_starts_with(&[1u8, 0xFF, 0xFF, 0xFF, 0xFF], "a").is_err());
    assert!(RecordHeaders::any_name_starts_with(&[0u8, 0, 0, 0, 0, 7], "a").is_err());
}

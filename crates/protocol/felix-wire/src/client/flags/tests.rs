use crate::{
    EXT_RECORD_HEADERS, FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH,
    FLAG_EVENT_BATCH_OFFSETS, FLAG_EXTENDED, KNOWN_EXT_FLAGS, KNOWN_FLAGS, answer_flags,
    has_unknown_flags, offer_flags, peer_flags_ext,
};

#[test]
fn unknown_flags_are_detected() {
    assert!(!has_unknown_flags(FLAG_BINARY_PUBLISH_BATCH));
    assert!(!has_unknown_flags(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_BINARY_PUBLISH_ACKED
    ));
    assert!(!has_unknown_flags(0));
    assert!(!has_unknown_flags(FLAG_EVENT_BATCH_OFFSETS));

    // The lowest undefined bit must be rejected rather than masked off.
    // Derived from `KNOWN_FLAGS` rather than written as a literal: this
    // assertion was `0x0020` until that bit was defined, at which point it
    // quietly became a claim about a *known* flag and failed. Computing it
    // keeps the test about the property instead of about one bit.
    // `FLAG_EXTENDED` is the last free bit and is accepted, so when there is
    // no other undefined bit the loop finds none.
    let first_undefined = (0..16u16)
        .map(|bit| 1u16 << bit)
        .find(|bit| (KNOWN_FLAGS | FLAG_EXTENDED) & bit == 0);
    if let Some(first_undefined) = first_undefined {
        assert!(has_unknown_flags(first_undefined));
        assert!(has_unknown_flags(
            FLAG_BINARY_PUBLISH_BATCH | first_undefined
        ));
    }
}

#[test]
fn the_extended_flag_is_the_last_bit_and_not_a_known_flag() {
    assert_eq!(FLAG_EXTENDED, 0x8000);
    assert_eq!(KNOWN_FLAGS & FLAG_EXTENDED, 0);
    assert_eq!(crate::ORIGINAL_V1_FLAGS & FLAG_EXTENDED, 0);
    // Every other bit is taken, so this is the last one.
    assert_eq!(KNOWN_FLAGS | FLAG_EXTENDED, u16::MAX);
    // Accepted as a frame flag; its extension word is the decoders' to check.
    assert!(!has_unknown_flags(
        FLAG_EXTENDED | FLAG_BINARY_PUBLISH_BATCH
    ));
}

#[test]
fn record_headers_is_the_first_extension_bit() {
    assert_eq!(EXT_RECORD_HEADERS, 0x0000_0001);
    assert_eq!(KNOWN_EXT_FLAGS, EXT_RECORD_HEADERS);
}

#[test]
fn a_peer_with_no_extension_offers_the_flags_it_always_did() {
    assert_eq!(offer_flags(KNOWN_FLAGS, 0), (KNOWN_FLAGS, None));
    assert_eq!(
        offer_flags(KNOWN_FLAGS, KNOWN_EXT_FLAGS),
        (KNOWN_FLAGS | FLAG_EXTENDED, Some(KNOWN_EXT_FLAGS))
    );
    // A marker with no word behind it is not left standing.
    assert_eq!(offer_flags(FLAG_EXTENDED, 0), (0, None));
}

#[test]
fn an_extension_word_goes_only_to_a_peer_that_offered_the_flag() {
    assert_eq!(
        answer_flags(KNOWN_FLAGS, EXT_RECORD_HEADERS, KNOWN_FLAGS),
        (KNOWN_FLAGS, None)
    );
    assert_eq!(
        answer_flags(KNOWN_FLAGS | FLAG_EXTENDED, EXT_RECORD_HEADERS, KNOWN_FLAGS),
        (KNOWN_FLAGS, None),
        "the marker is cleared for a peer that cannot read the word"
    );
    assert_eq!(
        answer_flags(KNOWN_FLAGS, EXT_RECORD_HEADERS, KNOWN_FLAGS | FLAG_EXTENDED),
        (KNOWN_FLAGS | FLAG_EXTENDED, Some(EXT_RECORD_HEADERS))
    );
    // Nothing to send, nothing to mark.
    assert_eq!(
        answer_flags(KNOWN_FLAGS, 0, KNOWN_FLAGS | FLAG_EXTENDED),
        (KNOWN_FLAGS, None)
    );
}

#[test]
fn a_peer_extension_word_counts_only_with_the_marker() {
    assert_eq!(peer_flags_ext(KNOWN_FLAGS, Some(EXT_RECORD_HEADERS)), 0);
    assert_eq!(
        peer_flags_ext(KNOWN_FLAGS | FLAG_EXTENDED, Some(EXT_RECORD_HEADERS)),
        EXT_RECORD_HEADERS
    );
    assert_eq!(peer_flags_ext(KNOWN_FLAGS | FLAG_EXTENDED, None), 0);
}

#[test]
fn the_bit_is_new_and_not_assumed_of_an_old_peer() {
    assert_eq!(
        crate::FLAG_BINARY_PUBLISH_IDEMPOTENT
            & (crate::KNOWN_FLAGS & !crate::FLAG_BINARY_PUBLISH_IDEMPOTENT),
        0
    );
    assert!(crate::supports(
        crate::KNOWN_FLAGS,
        crate::FLAG_BINARY_PUBLISH_IDEMPOTENT
    ));
    assert_eq!(
        crate::ORIGINAL_V1_FLAGS & crate::FLAG_BINARY_PUBLISH_IDEMPOTENT,
        0
    );
}

#[test]
fn the_ack_code_flag_is_new_and_not_in_the_frozen_set() {
    let others = crate::KNOWN_FLAGS & !crate::FLAG_BINARY_PUBLISH_ACK_CODE;
    assert_eq!(crate::FLAG_BINARY_PUBLISH_ACK_CODE & others, 0);
    assert_eq!(
        crate::ORIGINAL_V1_FLAGS & crate::FLAG_BINARY_PUBLISH_ACK_CODE,
        0
    );
}

#[test]
fn the_ack_detail_flag_is_new_and_not_in_the_frozen_set() {
    let others = crate::KNOWN_FLAGS & !crate::FLAG_BINARY_PUBLISH_ACK_DETAIL;
    assert_eq!(crate::FLAG_BINARY_PUBLISH_ACK_DETAIL & others, 0);
    assert_eq!(
        crate::ORIGINAL_V1_FLAGS & crate::FLAG_BINARY_PUBLISH_ACK_DETAIL,
        0
    );
}

#[test]
fn the_ack_offset_flag_is_new_and_not_in_the_frozen_set() {
    let others = crate::KNOWN_FLAGS & !crate::FLAG_BINARY_PUBLISH_ACK_OFFSET;
    assert_eq!(crate::FLAG_BINARY_PUBLISH_ACK_OFFSET & others, 0);
    assert_eq!(
        crate::ORIGINAL_V1_FLAGS & crate::FLAG_BINARY_PUBLISH_ACK_OFFSET,
        0
    );
}

#[test]
fn the_publisher_flag_is_new_and_not_in_the_frozen_set() {
    let others = crate::KNOWN_FLAGS & !crate::FLAG_EVENT_BATCH_PUBLISHER;
    assert_eq!(crate::FLAG_EVENT_BATCH_PUBLISHER & others, 0);
    assert_eq!(
        crate::ORIGINAL_V1_FLAGS & crate::FLAG_EVENT_BATCH_PUBLISHER,
        0
    );
}

#[test]
fn the_timestamps_flag_is_new_and_not_in_the_frozen_set() {
    let others = crate::KNOWN_FLAGS & !crate::FLAG_EVENT_BATCH_TIMESTAMPS;
    assert_eq!(crate::FLAG_EVENT_BATCH_TIMESTAMPS & others, 0);
    assert_eq!(
        crate::ORIGINAL_V1_FLAGS & crate::FLAG_EVENT_BATCH_TIMESTAMPS,
        0
    );
}

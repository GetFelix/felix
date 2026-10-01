use crate::{
    FLAG_BINARY_PUBLISH_ACKED, FLAG_BINARY_PUBLISH_BATCH, FLAG_EVENT_BATCH_OFFSETS, KNOWN_FLAGS,
    has_unknown_flags,
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
    let first_undefined = (0..16u16)
        .map(|bit| 1u16 << bit)
        .find(|bit| KNOWN_FLAGS & bit == 0)
        .expect("a free flag bit");
    assert!(has_unknown_flags(first_undefined));
    assert!(has_unknown_flags(FLAG_BINARY_PUBLISH_BATCH | 0x8000));
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

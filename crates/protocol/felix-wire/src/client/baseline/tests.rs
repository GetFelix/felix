use bytes::Bytes;

use super::*;
use crate::binary::decode_publish_batch;
use crate::{Error, Frame};

#[test]
fn the_baselines_are_frozen() {
    // A bit added after 0.8 must not change these: a 0.8 peer would then be
    // refused by a newer one.
    assert_eq!(CLIENT_BASELINE_FLAGS, 0x1FFF);
    assert_eq!(CLIENT_BASELINE_FEATURES, 0x7B77_FFFF);
    assert_eq!(CLIENT_BASELINE_FEATURES_HI, 0x01FF);
    assert_eq!(BROKER_BASELINE_FLAGS, 0x7FFF);
    assert_eq!(BROKER_BASELINE_FEATURES, 0x762D_7E24);
    assert_eq!(BROKER_BASELINE_FEATURES_HI, 0x019F);
}

#[test]
fn the_original_v1_flags_are_unchanged() {
    assert_eq!(ORIGINAL_V1_FLAGS, 0x0007);
}

#[test]
fn the_baselines_are_known_bits_and_never_the_markers() {
    assert_eq!(BROKER_BASELINE_FLAGS & !KNOWN_FLAGS, 0);
    assert_eq!(CLIENT_BASELINE_FLAGS & !BROKER_BASELINE_FLAGS, 0);
    assert_eq!(BROKER_BASELINE_FLAGS & FLAG_EXTENDED, 0);
    assert_eq!(CLIENT_BASELINE_FEATURES & !KNOWN_FEATURES, 0);
    assert_eq!(BROKER_BASELINE_FEATURES & !KNOWN_FEATURES, 0);
    assert_eq!(CLIENT_BASELINE_FEATURES & FEATURE_EXTENDED, 0);
    assert_eq!(CLIENT_BASELINE_FEATURES_HI & !KNOWN_FEATURES_HI, 0);
    assert_eq!(BROKER_BASELINE_FEATURES_HI & !KNOWN_FEATURES_HI, 0);
}

#[test]
fn the_opt_in_bits_stay_out_of_the_client_baseline() {
    for flag in [FLAG_EVENT_BATCH_PUBLISHER, FLAG_EVENT_BATCH_TIMESTAMPS] {
        assert_eq!(CLIENT_BASELINE_FLAGS & flag, 0, "{flag:#06x}");
    }
    for feature in [
        FEATURE_ACK_ON_COMMIT,
        FEATURE_GROUP_PUBLISHER,
        FEATURE_RECORD_TIMESTAMPS,
    ] {
        assert_eq!(CLIENT_BASELINE_FEATURES & feature, 0, "{feature:#x}");
    }
}

#[test]
fn a_full_offer_meets_the_baseline() {
    let (features, hi) = offer_features(CLIENT_BASELINE_FEATURES, CLIENT_BASELINE_FEATURES_HI);
    assert_eq!(
        client_missing_baseline(Some(CLIENT_BASELINE_FLAGS), Some(features), hi),
        (0, 0, 0)
    );
}

#[test]
fn a_client_that_predates_negotiation_lacks_everything() {
    assert_eq!(
        client_missing_baseline(None, None, None),
        (
            CLIENT_BASELINE_FLAGS,
            CLIENT_BASELINE_FEATURES,
            CLIENT_BASELINE_FEATURES_HI
        )
    );
}

#[test]
fn one_missing_bit_is_named() {
    let (features, hi) = offer_features(CLIENT_BASELINE_FEATURES, CLIENT_BASELINE_FEATURES_HI);
    assert_eq!(
        client_missing_baseline(
            Some(CLIENT_BASELINE_FLAGS & !FLAG_BINARY_PUBLISH_KEYED),
            Some(features),
            hi
        ),
        (FLAG_BINARY_PUBLISH_KEYED, 0, 0)
    );
    assert_eq!(
        client_missing_baseline(
            Some(CLIENT_BASELINE_FLAGS),
            Some(features & !FEATURE_ERROR_CODES),
            hi
        ),
        (0, FEATURE_ERROR_CODES, 0)
    );
}

#[test]
fn an_extended_word_without_its_marker_does_not_count() {
    assert_eq!(
        client_missing_baseline(
            Some(CLIENT_BASELINE_FLAGS),
            Some(CLIENT_BASELINE_FEATURES),
            Some(CLIENT_BASELINE_FEATURES_HI),
        ),
        (0, 0, CLIENT_BASELINE_FEATURES_HI)
    );
}

#[test]
fn bits_past_the_baseline_are_still_negotiated() {
    // A feature bit added after 0.8 is neither required nor refused.
    let future_hi = 0x8000_0000;
    let (features, hi) = offer_features(
        CLIENT_BASELINE_FEATURES,
        CLIENT_BASELINE_FEATURES_HI | future_hi,
    );
    assert_eq!(
        client_missing_baseline(Some(CLIENT_BASELINE_FLAGS), Some(features), hi),
        (0, 0, 0)
    );
    assert_eq!(KNOWN_FEATURES_HI & future_hi, 0);
}

#[test]
fn an_unknown_layout_bit_is_still_rejected() {
    // The extension word is where the next layout bit goes; one this version
    // does not know fails the frame rather than being read past.
    let mut payload = Vec::new();
    payload.extend_from_slice(&0x0000_0002u32.to_be_bytes());
    let frame = Frame::new(
        FLAG_BINARY_PUBLISH_BATCH | FLAG_EXTENDED,
        Bytes::from(payload),
    )
    .expect("frame");
    assert!(matches!(
        decode_publish_batch(&frame),
        Err(Error::UnknownExtension(0x2))
    ));
}

#[test]
fn a_broker_missing_a_baseline_bit_is_named() {
    let (features, hi) = offer_features(BROKER_BASELINE_FEATURES, BROKER_BASELINE_FEATURES_HI);
    assert_eq!(
        broker_missing_baseline(BROKER_BASELINE_FLAGS, Some(features), hi),
        (0, 0, 0)
    );
    assert_eq!(
        broker_missing_baseline(ORIGINAL_V1_FLAGS, None, None),
        (
            BROKER_BASELINE_FLAGS & !ORIGINAL_V1_FLAGS,
            BROKER_BASELINE_FEATURES,
            BROKER_BASELINE_FEATURES_HI
        )
    );
}

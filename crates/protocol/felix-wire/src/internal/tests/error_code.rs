use super::*;

/// Retryability is a property of the code, so a requester does not have to
/// re-derive it from a message string.
#[test]
fn error_codes_say_whether_to_retry() {
    for code in [
        ErrorCode::StaleRoute,
        ErrorCode::Unavailable,
        ErrorCode::Overload,
    ] {
        assert!(code.is_retryable(), "{code:?}");
    }
    for code in [
        ErrorCode::Unauthorized,
        ErrorCode::ProtocolVersion,
        ErrorCode::Malformed,
        ErrorCode::StorageFailed,
    ] {
        assert!(!code.is_retryable(), "{code:?}");
    }
}

/// The divergence codes say what a leader may do next, and getting these
/// backwards is how a diverged follower gets hammered or a lagging one gets
/// abandoned.
#[test]
fn a_gap_is_retryable_and_a_conflict_is_not() {
    assert!(
        ErrorCode::LogGap.is_retryable(),
        "a follower that named the offset it wants was told not to retry",
    );
    assert!(
        !ErrorCode::LogConflict.is_retryable(),
        "diverged logs do not converge by retrying",
    );
    assert!(
        !ErrorCode::FencedEpoch.is_retryable(),
        "the fence does not lift",
    );
    assert!(
        !ErrorCode::StaleSet.is_retryable(),
        "the newer set does not go away",
    );
}

/// Every code survives the wire as itself. A code that decoded as a neighbour
/// would turn "stop, we have diverged" into "try again".
#[test]
fn every_error_code_round_trips() {
    for code in [
        ErrorCode::StaleRoute,
        ErrorCode::Unavailable,
        ErrorCode::Unauthorized,
        ErrorCode::Overload,
        ErrorCode::ProtocolVersion,
        ErrorCode::Malformed,
        ErrorCode::StorageFailed,
        ErrorCode::LogGap,
        ErrorCode::LogConflict,
        ErrorCode::FencedEpoch,
        ErrorCode::UnsupportedKind,
        ErrorCode::StaleSet,
    ] {
        assert_eq!(ErrorCode::from_u16(code as u16).expect("known"), code);
    }
}

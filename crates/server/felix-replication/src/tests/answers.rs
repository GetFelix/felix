//! Reading an answer, on its own. Getting one of these backwards either
//! hammers a diverged follower or abandons a healthy one.

use super::*;

#[test]
fn each_answer_means_one_thing() {
    assert_eq!(
        read_answer(&stored(9)),
        Progress::Stored { durable_offset: 9 },
    );
    assert_eq!(
        read_answer(&refused(ErrorCode::LogGap, 4)),
        Progress::Resume { offset: 4 },
    );
    assert_eq!(
        read_answer(&refused(ErrorCode::LogConflict, 0)),
        Progress::Halted(Halt::Diverged),
    );
    assert_eq!(
        read_answer(&refused(ErrorCode::FencedEpoch, 0)),
        Progress::Halted(Halt::Fenced),
    );
}

/// Everything else is this moment rather than this pairing.
#[test]
fn the_recoverable_refusals_are_retried() {
    for code in [
        ErrorCode::StaleRoute,
        ErrorCode::Unavailable,
        ErrorCode::Overload,
        ErrorCode::Malformed,
        ErrorCode::StorageFailed,
        ErrorCode::Unauthorized,
    ] {
        assert_eq!(read_answer(&refused(code, 0)), Progress::Retry, "{code:?}");
    }
}

/// An answer that is not part of this exchange is retried rather than read
/// as divergence — stopping replication over a protocol confusion would be
/// the worse mistake.
#[test]
fn an_unexpected_answer_is_retried() {
    assert_eq!(
        read_answer(&InternalMessage::HelloOk(felix_wire::internal::HelloOk {
            correlation_id: 0,
            node_id: "broker-b".to_string(),
        })),
        Progress::Retry,
    );
}

use felix_wire::{ErrorCode, PublishRefusalReason, RetryClass};

use super::{AckLost, BrokerError, PublishRefused, outcome_unknown};

fn broker(code: ErrorCode, retry: RetryClass) -> anyhow::Error {
    BrokerError::new(code, retry, "no").into()
}

/// **Only the errors that may have written something are unknown.** A write
/// that failed as `forbidden`, refused, or invalid wrote nothing, and a writer
/// must be able to tell that from a lost answer.
#[test]
fn outcome_unknown_separates_unwritten_from_possibly_written() {
    let lost: anyhow::Error = AckLost {
        reason: "reset".into(),
    }
    .into();
    assert!(outcome_unknown(&lost));
    assert!(outcome_unknown(&lost.context("publish_if")));
    assert!(outcome_unknown(&broker(
        ErrorCode::QuorumTimeout,
        RetryClass::OutcomeUnknown
    )));

    assert!(!outcome_unknown(&broker(
        ErrorCode::Forbidden,
        RetryClass::Fatal
    )));
    assert!(!outcome_unknown(&broker(
        ErrorCode::NotLeader,
        RetryClass::Redirect
    )));
    let refused: anyhow::Error = PublishRefused {
        reason: PublishRefusalReason::OffsetMismatch { tail: 3 },
        message: "stale".into(),
    }
    .into();
    assert!(!outcome_unknown(&refused));
    assert!(!outcome_unknown(&anyhow::anyhow!(
        "a conditional publish needs at least one record"
    )));
}

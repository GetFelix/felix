use felix_client::{ErrorCode, RetryClass};

use super::*;

fn broker(code: ErrorCode, retry: RetryClass) -> anyhow::Error {
    BrokerError::new(code, retry, "x").into()
}

#[test]
fn an_acknowledged_write_is_ok_or_forwarded() {
    assert_eq!(
        classify(Ok(Some(7)), false),
        Outcome::Ok { offset: Some(7) }
    );
    assert_eq!(
        classify(Ok(None), true),
        Outcome::Forwarded { offset: None }
    );
    assert_eq!(classify(Ok(Some(7)), false).describe(), "ok offset 7");
}

#[test]
fn a_typed_refusal_that_wrote_nothing_is_refused() {
    let err = broker(ErrorCode::ShardUnavailable, RetryClass::Retry);
    assert_eq!(
        classify(Err(err), false),
        Outcome::Refused("shard_unavailable".into())
    );
    let err = broker(ErrorCode::NotLeader, RetryClass::Retry);
    assert!(matches!(classify(Err(err), false), Outcome::Refused(_)));
}

#[test]
fn a_code_that_may_have_written_is_indeterminate() {
    let err = broker(ErrorCode::LeadershipLost, RetryClass::OutcomeUnknown);
    assert_eq!(
        classify(Err(err), false),
        Outcome::Indeterminate("leadership_lost".into())
    );
}

#[test]
fn a_lost_acknowledgement_is_indeterminate_even_with_context() {
    let err = anyhow::Error::new(AckLost {
        reason: "closed".into(),
    })
    .context("publish");
    assert!(matches!(
        classify(Err(err), false),
        Outcome::Indeterminate(_)
    ));
}

#[test]
fn an_untyped_failure_is_no_answer_and_ends_the_connection() {
    let outcome = classify(Err(anyhow::anyhow!("connection closed")), false);
    assert!(matches!(outcome, Outcome::NoAnswer(_)));
    assert!(!outcome.connection_usable());
    assert!(Outcome::Refused("x".into()).connection_usable());
}

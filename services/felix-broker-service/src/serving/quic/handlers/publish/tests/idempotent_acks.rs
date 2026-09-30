//! A refusal of an idempotent publish carries its reason; every other
//! encoding, and every other error, is the prose it always was.

use felix_wire::PublishRefusalReason;

use super::*;

#[test]
fn a_typed_refusal_is_answered_as_publish_refused() {
    let err = anyhow::Error::from(felix_broker::BrokerError::SequenceGap { expected: 4 });
    match AckEncoding::Idempotent.refuse(3, &err) {
        Outgoing::Message(Message::PublishRefused {
            request_id: 3,
            reason: PublishRefusalReason::SequenceGap { expected: 4 },
            ..
        }) => {}
        other => panic!("expected a typed refusal, got {other:?}"),
    }
    let err = anyhow::Error::from(felix_broker::BrokerError::UnknownProducer { producer_id: 1 });
    assert!(matches!(
        AckEncoding::Idempotent.refuse(3, &err),
        Outgoing::Message(Message::PublishRefused {
            reason: PublishRefusalReason::UnknownProducer,
            ..
        })
    ));
    let err = anyhow::Error::from(felix_broker::BrokerError::SequenceExpired { sequence: 0 });
    assert!(matches!(
        AckEncoding::Idempotent.refuse(3, &err),
        Outgoing::Message(Message::PublishRefused {
            reason: PublishRefusalReason::SequenceExpired,
            ..
        })
    ));
}

/// An error with no producer meaning stays a `publish_error`, so a
/// producer treats it as a failure to get an answer and re-sends.
#[test]
fn an_untyped_error_stays_a_publish_error() {
    let err = anyhow::anyhow!("lease lapsed before the record could be committed");
    assert!(matches!(
        AckEncoding::Idempotent.refuse(3, &err),
        Outgoing::Message(Message::PublishError { request_id: 3, .. })
    ));
}

/// The JSON and binary encodings never emit `publish_refused`, whatever
/// the error: a client that did not send `publish_idempotent` did not
/// say it could decode one.
#[test]
fn plain_publishes_are_never_answered_with_a_refusal() {
    let err = anyhow::Error::from(felix_broker::BrokerError::SequenceGap { expected: 4 });
    assert!(matches!(
        AckEncoding::Json.refuse(3, &err),
        Outgoing::Message(Message::PublishError { request_id: 3, .. })
    ));
    assert!(matches!(
        AckEncoding::Binary.refuse(3, &err),
        Outgoing::PublishAck {
            forwarded_to: _,
            request_id: 3,
            error: Some(_),
            ..
        }
    ));
}

/// Success is a plain `publish_ok` in the idempotent encoding, the same
/// frame the producer already reads for an ordinary batch.
#[test]
fn success_is_a_plain_publish_ok() {
    assert!(matches!(
        AckEncoding::Idempotent.ok(9, None),
        Outgoing::Message(Message::PublishOk {
            request_id: 9,
            offset: None
        })
    ));
    assert!(matches!(
        AckEncoding::Idempotent.ok(9, Some(4)),
        Outgoing::Message(Message::PublishOk {
            request_id: 9,
            offset: Some(4)
        })
    ));
}

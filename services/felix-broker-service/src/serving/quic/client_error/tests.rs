use felix_wire::{ErrorCode, Message, RetryClass};

use super::{ClientError, ErrorCodeSupport, MOVING_RETRY_AFTER_MS};
use crate::serving::forward::ForwardError;
use crate::shards::routing::Reason;
use felix_replication::quorum::QuorumError;

fn code_of(err: anyhow::Error) -> (ErrorCode, RetryClass) {
    let classified = ClientError::from_anyhow(&err);
    assert_eq!(
        classified.message(),
        err.to_string(),
        "the text must not change"
    );
    (classified.code().clone(), classified.retry())
}

#[test]
fn a_quorum_timeout_is_outcome_unknown_not_a_refusal() {
    let timed_out = QuorumError::TimedOut {
        what: "batch",
        timeout: std::time::Duration::from_millis(5),
    };
    assert_eq!(
        timed_out.to_string(),
        "the batch is durable here but did not reach a majority within 5ms"
    );
    assert_eq!(
        code_of(timed_out.into()),
        (ErrorCode::QuorumTimeout, RetryClass::OutcomeUnknown)
    );
    let lost = QuorumError::LeadershipLost {
        what: "write",
        detail: "shard leadership moved",
    };
    assert_eq!(
        lost.to_string(),
        "shard leadership moved before the write could reach a quorum"
    );
    assert_eq!(
        code_of(lost.into()),
        (ErrorCode::LeadershipLost, RetryClass::OutcomeUnknown)
    );
}

#[test]
fn broker_errors_map_to_codes() {
    let missing = felix_broker::BrokerError::StreamNotFound {
        tenant_id: "t".into(),
        namespace: "n".into(),
        stream: "s".into(),
    };
    assert_eq!(
        code_of(missing.into()),
        (ErrorCode::NotFound, RetryClass::RetryAfter)
    );
    assert_eq!(
        code_of(felix_broker::BrokerError::Storage("disk".into()).into()),
        (ErrorCode::Storage, RetryClass::OutcomeUnknown)
    );
    assert_eq!(
        code_of(felix_broker::BrokerError::StorageFull("disk".into()).into()),
        (ErrorCode::Overloaded, RetryClass::RetryAfter)
    );
    // Context on top does not hide the cause.
    let wrapped = anyhow::Error::from(felix_broker::BrokerError::TenantNotFound("t".into()))
        .context("publish");
    assert_eq!(
        ClientError::from_anyhow(&wrapped).code(),
        &ErrorCode::NotFound
    );
}

#[test]
fn forward_errors_keep_what_the_owner_said() {
    use felix_wire::internal::ErrorCode as Internal;
    let refused = |code| ForwardError::Refused {
        stream: "s".into(),
        detail: "d".into(),
        code,
    };
    assert_eq!(
        code_of(refused(Some(Internal::Overload)).into()).0,
        ErrorCode::Overloaded
    );
    let fenced = ClientError::from_anyhow(&refused(Some(Internal::FencedEpoch)).into());
    assert_eq!(fenced.code(), &ErrorCode::ShardUnavailable);
    let Message::Error { detail, .. } = fenced.into_message() else {
        unreachable!()
    };
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("fenced"));
    assert_eq!(
        code_of(refused(None).into()),
        (ErrorCode::ShardUnavailable, RetryClass::Retry)
    );
    let lost = ForwardError::Indeterminate {
        node_id: "b".into(),
        detail: "timeout".into(),
    };
    assert_eq!(
        code_of(lost.into()),
        (ErrorCode::Unacknowledged, RetryClass::OutcomeUnknown)
    );
}

#[test]
fn anything_unrecognised_is_internal() {
    assert_eq!(
        code_of(anyhow::anyhow!("boom")),
        (ErrorCode::Internal, RetryClass::OutcomeUnknown)
    );
}

#[test]
fn an_unavailable_shard_names_its_reason() {
    let message = ClientError::unavailable(&Reason::NotReady, "not yet").into_message();
    let Message::Error {
        code,
        retry,
        detail,
        ..
    } = message
    else {
        unreachable!()
    };
    assert_eq!(code, Some(ErrorCode::ShardUnavailable));
    assert_eq!(retry, Some(RetryClass::Retry));
    assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("not_ready"));
}

#[test]
fn codes_reach_only_a_client_that_offered_them() {
    let coded = ClientError::forbidden("forbidden").into_publish_error(3);

    let silent = ErrorCodeSupport::default();
    silent.negotiate(0, felix_wire::KNOWN_FLAGS);
    assert_eq!(
        silent.shape(coded.clone()),
        Message::publish_error(3, "forbidden")
    );
    assert!(silent.binary_ack());

    let asked = ErrorCodeSupport::default();
    asked.negotiate(
        felix_wire::FEATURE_ERROR_CODES,
        felix_wire::ORIGINAL_V1_FLAGS,
    );
    assert_eq!(asked.shape(coded.clone()), coded);
    assert!(!asked.binary_ack());
}

#[test]
fn a_write_refused_by_the_fence_is_fenced_and_retryable() {
    use crate::shards::lifecycle::fence::Fenced;

    let at_claim =
        ClientError::from_anyhow(&anyhow::Error::from(Fenced::NotServing).context("publish"));
    let at_admission = ClientError::not_enqueued(&Fenced::NotServing.into());
    let lease_lapsed = ClientError::from_anyhow(&Fenced::LeaseLapsed.into());
    for refused in [at_claim, at_admission, lease_lapsed] {
        assert_eq!(refused.code(), &ErrorCode::ShardUnavailable);
        assert_eq!(refused.retry(), RetryClass::Retry);
        let Message::Error { detail, .. } = refused.into_message() else {
            unreachable!()
        };
        assert_eq!(detail.and_then(|d| d.reason).as_deref(), Some("fenced"));
    }
    assert_eq!(
        ClientError::not_enqueued(&anyhow::anyhow!("queue full")).code(),
        &ErrorCode::Overloaded
    );
}

/// A publish refused because its shard is still moving says so, keeps the
/// `retry` class, and suggests a short pause.
#[test]
fn a_moving_shard_is_a_retry_with_a_pause() {
    let error = ClientError::unavailable(&Reason::Moving, "shard is moving");
    assert_eq!(error.code(), &ErrorCode::ShardUnavailable);
    assert_eq!(error.retry(), RetryClass::Retry);
    let Message::PublishError { detail, .. } = error.into_publish_error(7) else {
        panic!("a publish error");
    };
    let detail = detail.expect("detail");
    assert_eq!(detail.reason.as_deref(), Some("moving"));
    assert_eq!(detail.retry_after_ms, Some(MOVING_RETRY_AFTER_MS));
}

/// The detail bit is negotiated on its own: a client that took only the code
/// bit must not be sent a layout it cannot parse.
#[test]
fn ack_detail_reaches_only_a_client_that_offered_it() {
    let code_only = ErrorCodeSupport::default();
    code_only.negotiate(0, felix_wire::FLAG_BINARY_PUBLISH_ACK_CODE);
    assert!(code_only.binary_ack());
    assert!(!code_only.binary_ack_detail());

    let both = ErrorCodeSupport::default();
    both.negotiate(0, felix_wire::KNOWN_FLAGS);
    assert!(both.binary_ack_detail());
}

use felix_wire::{ErrorCode, Message, RetryClass};

use super::{CACHE_FULL_REASON, ClientError, MOVING_RETRY_AFTER_MS};
use crate::serving::forward::ForwardError;
use crate::shards::routing::Reason;
use felix_replication::engine::CommitError;

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
    let timed_out = CommitError::TimedOut {
        what: "batch",
        timeout: std::time::Duration::from_millis(5),
    };
    assert_eq!(
        timed_out.to_string(),
        "the batch did not reach a majority within 5ms"
    );
    assert_eq!(
        code_of(timed_out.into()),
        (ErrorCode::QuorumTimeout, RetryClass::OutcomeUnknown)
    );
    let lost = CommitError::LeadershipLost {
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
fn a_write_refused_before_it_was_appended_is_retryable_not_unknown() {
    let refused = CommitError::NotAppended {
        what: "write",
        detail: "no majority has answered this leader lately, so nothing was written",
    };
    assert_eq!(
        code_of(refused.into()),
        (ErrorCode::ShardUnavailable, RetryClass::Retry)
    );
}

/// Only the typed refusal is retryable. A lost leadership whose text says
/// nothing was written is still answered as it always was: the text is not
/// what decides.
#[test]
fn a_lost_leadership_saying_nothing_was_written_stays_outcome_unknown() {
    let shaped = CommitError::LeadershipLost {
        what: "write",
        detail: "the records are not the rest of the producer's batch, so nothing was written",
    };
    assert_eq!(
        code_of(shaped.into()),
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

/// Each way a head trim is refused has a code a client can act on, not a
/// bare `invalid_request`, and a trim that timed out may have landed.
#[test]
fn every_trim_refusal_has_its_own_code() {
    use felix_broker::BrokerError as E;
    use felix_wire::trim_reason;

    let classify = |err: E| {
        let classified = ClientError::from_broker(&err, err.to_string());
        (
            classified.code().clone(),
            classified.retry(),
            classified.detail().and_then(|d| d.reason.clone()),
        )
    };
    let reason = |name: &str| Some(name.to_string());
    assert_eq!(
        classify(E::HeadTrimNotEnabled {
            feature: "head_trim"
        }),
        (
            ErrorCode::FeatureNotEnabled,
            RetryClass::Fatal,
            reason(trim_reason::HEAD_TRIM)
        )
    );
    assert_eq!(
        classify(E::TrimOnCommitStream),
        (ErrorCode::StreamHasCommits, RetryClass::Fatal, None)
    );
    assert_eq!(
        classify(E::TrimTimedOut { offset: 9 }),
        (
            ErrorCode::Unacknowledged,
            RetryClass::OutcomeUnknown,
            reason(trim_reason::TIMED_OUT)
        )
    );
    assert_eq!(
        classify(E::TrimPastTail {
            before: 5,
            offset: 4
        }),
        (
            ErrorCode::InvalidRequest,
            RetryClass::Fatal,
            reason(trim_reason::PAST_TAIL)
        )
    );
    assert_eq!(
        classify(E::TrimNeedsDurableStream),
        (
            ErrorCode::InvalidRequest,
            RetryClass::Fatal,
            reason(trim_reason::NOT_DURABLE)
        )
    );
}

/// On Raft, a write the engine took and could not settle is outcome unknown
/// with the code the stream writes use; one it never took is retryable.
#[test]
fn a_raft_write_is_retryable_only_before_its_proposal() {
    use felix_broker::BrokerError;
    let stalled = BrokerError::ProposalsStalled {
        waited: std::time::Duration::from_secs(5),
    };
    assert_eq!(
        code_of(stalled.into()),
        (ErrorCode::ShardUnavailable, RetryClass::Retry)
    );
    let timed_out = BrokerError::WriteUnsettled {
        timed_out: true,
        detail: "no commit within 5s".into(),
    };
    assert_eq!(
        code_of(timed_out.into()),
        (ErrorCode::QuorumTimeout, RetryClass::OutcomeUnknown)
    );
    let lost = BrokerError::WriteUnsettled {
        timed_out: false,
        detail: "the term ended".into(),
    };
    assert_eq!(
        code_of(lost.into()),
        (ErrorCode::LeadershipLost, RetryClass::OutcomeUnknown)
    );
}

/// A full Raft cache shard is `limit_exceeded` with reason `cache_full`,
/// a refusal a client does not retry, whether the shard is led here or the
/// op was forwarded to its owner.
#[test]
fn a_full_cache_shard_is_limit_exceeded_with_its_reason() {
    use felix_wire::internal::ErrorCode as Internal;
    let forwarded = ForwardError::Refused {
        stream: "s".into(),
        detail: "full".into(),
        code: Some(Internal::CacheFull),
    };
    for err in [
        ClientError::cache_full("full"),
        ClientError::from_anyhow(&forwarded.into()),
    ] {
        assert_eq!(err.code(), &ErrorCode::LimitExceeded);
        assert_eq!(err.retry(), RetryClass::Fatal);
        assert_eq!(
            err.detail().and_then(|d| d.reason.as_deref()),
            Some(CACHE_FULL_REASON)
        );
    }
}

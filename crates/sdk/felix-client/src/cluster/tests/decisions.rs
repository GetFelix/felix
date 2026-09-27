//! The retry decision for every code and retry class.

use std::time::Duration;

use felix_wire::{ErrorCode, ErrorDetail, RetryClass};

use crate::cluster::retry::{
    Attempt, NOT_FOUND_GRACE, Next, Retrying, next_step, route_went_stale, wants_reconnect,
};

fn coded(code: ErrorCode) -> anyhow::Error {
    let retry = code.default_retry();
    coded_as(code, retry, None)
}

fn coded_as(code: ErrorCode, retry: RetryClass, detail: Option<ErrorDetail>) -> anyhow::Error {
    crate::error::refused(
        "publish failed",
        "refused".into(),
        Some(code),
        Some(retry),
        detail,
    )
}

const BACKOFF: Next = Next::Backoff {
    at_least: Duration::ZERO,
};

fn entry() -> Attempt {
    Attempt::default()
}

fn routed() -> Attempt {
    Attempt {
        routed: true,
        ..Attempt::default()
    }
}

fn resending() -> Attempt {
    Attempt {
        resend_ambiguous: true,
        ..Attempt::default()
    }
}

/// The whole table, from the entry broker, for a caller that will not
/// re-send an ambiguous write.
#[test]
fn every_code_from_the_entry_broker() {
    let table = [
        (ErrorCode::Unauthenticated, Next::Fail),
        (ErrorCode::Forbidden, Next::Fail),
        (ErrorCode::InvalidRequest, Next::Fail),
        (ErrorCode::LimitExceeded, Next::Fail),
        (ErrorCode::NotFound, BACKOFF),
        (ErrorCode::Overloaded, BACKOFF),
        (ErrorCode::ShardUnavailable, BACKOFF),
        (ErrorCode::Draining, BACKOFF),
        (ErrorCode::NotLeader, BACKOFF),
        (ErrorCode::QuorumTimeout, Next::Fail),
        (ErrorCode::LeadershipLost, Next::Fail),
        (ErrorCode::Unacknowledged, Next::Fail),
        (ErrorCode::Internal, Next::Fail),
        (ErrorCode::Storage, Next::Fail),
        (ErrorCode::StaleClaim, BACKOFF),
    ];
    assert_eq!(table.len(), ErrorCode::ALL.len(), "a code is missing");
    for (code, expected) in table {
        assert_eq!(next_step(&coded(code.clone()), entry()), expected, "{code}");
    }
}

/// **A cached route that answers "nothing applied" is dropped at once**,
/// rather than asked again after a backoff it cannot outwait.
#[test]
fn a_routed_retry_or_redirect_reroutes() {
    for code in [
        ErrorCode::ShardUnavailable,
        ErrorCode::Draining,
        ErrorCode::NotLeader,
    ] {
        assert_eq!(
            next_step(&coded(code.clone()), routed()),
            Next::Reroute,
            "{code}"
        );
        assert!(route_went_stale(&coded(code)));
    }
    for reason in [
        "not_assigned",
        "owner_unavailable",
        "not_ready",
        "stale",
        "fenced",
    ] {
        let detail = ErrorDetail {
            reason: Some(reason.into()),
            ..ErrorDetail::default()
        };
        let err = coded_as(ErrorCode::ShardUnavailable, RetryClass::Retry, Some(detail));
        assert_eq!(next_step(&err, routed()), Next::Reroute, "{reason}");
    }
    // Overload and missing streams are not about the route.
    assert_eq!(next_step(&coded(ErrorCode::Overloaded), routed()), BACKOFF);
    assert!(!route_went_stale(&coded(ErrorCode::Overloaded)));
    assert!(!route_went_stale(&coded(ErrorCode::Forbidden)));
    // Nor is a stale group claim: the leader answered, and knows the shard.
    assert_eq!(next_step(&coded(ErrorCode::StaleClaim), routed()), BACKOFF);
    assert!(!route_went_stale(&coded(ErrorCode::StaleClaim)));
}

/// **An ambiguous outcome is re-sent only when the caller said a duplicate is
/// acceptable or impossible.**
#[test]
fn outcome_unknown_is_resent_only_on_request() {
    for code in [
        ErrorCode::QuorumTimeout,
        ErrorCode::LeadershipLost,
        ErrorCode::Unacknowledged,
        ErrorCode::Internal,
        ErrorCode::Storage,
    ] {
        assert_eq!(
            next_step(&coded(code.clone()), entry()),
            Next::Fail,
            "{code}"
        );
        assert_eq!(
            next_step(&coded(code.clone()), routed()),
            Next::Fail,
            "{code}"
        );
        assert_eq!(
            next_step(&coded(code.clone()), resending()),
            BACKOFF,
            "{code}"
        );
    }
}

/// The retry class decides, not the code: a code this client does not know
/// is acted on by the class it came with.
#[test]
fn an_unknown_code_follows_its_class() {
    let code = || ErrorCode::Unknown("rebalancing".into());
    assert_eq!(
        next_step(&coded_as(code(), RetryClass::Retry, None), routed()),
        Next::Reroute
    );
    assert_eq!(
        next_step(&coded_as(code(), RetryClass::Fatal, None), entry()),
        Next::Fail
    );
    assert_eq!(
        next_step(&coded_as(code(), RetryClass::OutcomeUnknown, None), entry()),
        Next::Fail
    );
}

#[test]
fn retry_after_waits_at_least_as_long_as_asked() {
    let detail = ErrorDetail {
        retry_after_ms: Some(750),
        ..ErrorDetail::default()
    };
    let err = coded_as(ErrorCode::Overloaded, RetryClass::RetryAfter, Some(detail));
    assert_eq!(
        next_step(&err, entry()),
        Next::Backoff {
            at_least: Duration::from_millis(750)
        }
    );
}

/// **`not_found` is retried for a bounded time, not a bounded count.** The
/// window is what a promoted broker needs to hear about the stream; a caller
/// with a large attempt budget must not spend all of it on a stream that does
/// not exist.
#[test]
fn not_found_stops_being_retried_after_its_grace() {
    let err = coded(ErrorCode::NotFound);
    let after = |waited| Attempt {
        not_found_for: Some(waited),
        ..Attempt::default()
    };
    assert_eq!(next_step(&err, after(Duration::ZERO)), BACKOFF);
    assert_eq!(
        next_step(&err, after(NOT_FOUND_GRACE - Duration::from_millis(1))),
        BACKOFF
    );
    assert_eq!(next_step(&err, after(NOT_FOUND_GRACE)), Next::Fail);
}

/// The clock starts at the first `not_found`, not at the first attempt.
#[test]
fn the_not_found_clock_starts_at_the_first_not_found() {
    let mut retrying = Retrying::default();
    let overloaded = coded(ErrorCode::Overloaded);
    assert_eq!(retrying.next(&overloaded, entry()), BACKOFF);
    let not_found = coded(ErrorCode::NotFound);
    assert_eq!(retrying.next(&not_found, entry()), BACKOFF);
    assert_eq!(retrying.next(&not_found, entry()), BACKOFF);
}

/// **A peer that sent no code is handled as it always was**: prose, retried
/// unless it names a refused credential.
#[test]
fn an_uncoded_error_keeps_the_old_rules() {
    let refused = crate::error::refused("publish failed", "forbidden".into(), None, None, None);
    assert_eq!(next_step(&refused, routed()), Next::Fail);
    for message in [
        "connection lost",
        "stream not found",
        "shard unavailable: fenced",
    ] {
        assert_eq!(
            next_step(&anyhow::anyhow!(message), routed()),
            BACKOFF,
            "{message}"
        );
    }
    assert!(!route_went_stale(&anyhow::anyhow!("shard unavailable")));
}

/// A typed idempotent refusal ends the loop: a sequence gap is not mended by
/// sending the batch again.
#[test]
fn a_publish_refusal_is_final() {
    let err: anyhow::Error = crate::PublishRefused {
        reason: crate::PublishRefusalReason::SequenceGap { expected: 3 },
        message: "gap".into(),
    }
    .into();
    assert_eq!(next_step(&err, resending()), Next::Fail);
}

/// Only a broker that is going away, or one that did not answer at all, is
/// worth replacing.
#[test]
fn only_draining_or_silence_replaces_the_client() {
    assert!(wants_reconnect(&coded(ErrorCode::Draining)));
    assert!(wants_reconnect(&anyhow::anyhow!("connection lost")));
    for code in [
        ErrorCode::Forbidden,
        ErrorCode::NotFound,
        ErrorCode::Overloaded,
        ErrorCode::ShardUnavailable,
        ErrorCode::QuorumTimeout,
    ] {
        assert!(!wants_reconnect(&coded(code.clone())), "{code}");
    }
}

use felix_wire::{ErrorCode, RetryClass};

use super::{Kind, classify, kind_for_code};

#[test]
fn every_code_maps_to_a_class_that_matches_its_retry_class() {
    let expected = [
        ("unauthenticated", Kind::Auth),
        ("forbidden", Kind::Auth),
        ("not_found", Kind::NotFound),
        ("invalid_request", Kind::Generic),
        ("shard_unavailable", Kind::ShardUnavailable),
        ("not_leader", Kind::ShardUnavailable),
        ("quorum_timeout", Kind::OutcomeUnknown),
        ("leadership_lost", Kind::OutcomeUnknown),
        ("unacknowledged", Kind::OutcomeUnknown),
        ("overloaded", Kind::Overloaded),
        ("limit_exceeded", Kind::Generic),
        ("draining", Kind::Connection),
        ("internal", Kind::OutcomeUnknown),
        ("storage", Kind::OutcomeUnknown),
        ("stale_claim", Kind::Generic),
        ("stale_width", Kind::Generic),
        ("revoked", Kind::Auth),
    ];
    assert_eq!(expected.len(), ErrorCode::ALL.len(), "a code is unmapped");
    for code in ErrorCode::ALL {
        let (_, kind) = expected
            .iter()
            .find(|(name, _)| *name == code.as_str())
            .unwrap_or_else(|| panic!("{code} is not in the table"));
        assert_eq!(
            kind_for_code(code.as_str(), code.default_retry()),
            *kind,
            "{code}"
        );
    }
}

#[test]
fn an_outcome_unknown_retry_class_wins_over_the_code() {
    // `overloaded` is sent this way once the batch was already queued.
    assert_eq!(
        kind_for_code("overloaded", RetryClass::OutcomeUnknown),
        Kind::OutcomeUnknown
    );
    // And `internal` before any write is safe to retry, so it is not.
    assert_eq!(kind_for_code("internal", RetryClass::Retry), Kind::Generic);
}

#[test]
fn an_unknown_code_is_the_base_class() {
    assert_eq!(kind_for_code("brand_new", RetryClass::Retry), Kind::Generic);
    assert_eq!(
        kind_for_code("brand_new", RetryClass::OutcomeUnknown),
        Kind::OutcomeUnknown
    );
}

#[test]
fn without_a_code_the_message_decides() {
    let cases = [
        // Words that used to mean auth are only words now.
        ("publish failed: forbidden", Kind::Generic),
        ("tls: could not load the token file", Kind::Generic),
        ("unknown stream t1/default/nope", Kind::NotFound),
        ("offset trimmed", Kind::Cursor),
        ("connection lost", Kind::Connection),
        ("something nobody foresaw", Kind::Generic),
    ];
    for (message, kind) in cases {
        let classified = classify(&anyhow::anyhow!(message));
        assert_eq!(classified.kind, kind, "{message}");
        assert_eq!(classified.code, None, "{message}");
        assert_eq!(classified.retry, None, "{message}");
    }
}

#[test]
fn a_code_is_found_under_added_context() {
    let err = anyhow::Error::new(felix_client::NotLeaderError {
        node_id: "broker-2".to_string(),
        addr: None,
        generation: 3,
    })
    .context("publish failed; reconnected to another broker");
    let classified = classify(&err);
    assert_eq!(classified.kind, Kind::ShardUnavailable);
    assert_eq!(classified.code.as_deref(), Some("not_leader"));
    assert_eq!(classified.retry, Some("redirect"));
}

#[test]
fn a_lost_subscription_is_a_connection_error() {
    let err = anyhow::Error::new(felix_client::SubscriptionLost {
        reason: "reset by peer".to_string(),
    })
    .context("resubscribe to shard 0 of orders");
    assert_eq!(classify(&err).kind, Kind::Connection);
}

#[test]
fn a_commit_refusal_is_its_own_class() {
    let split = anyhow::Error::new(felix_client::CommitError::NotOnOwningShard {
        index: 1,
        stream: "inventory".to_string(),
        owner: "orders".to_string(),
    });
    assert_eq!(classify(&split).kind, Kind::NotOnOwningShard);
    let count = anyhow::Error::new(felix_client::CommitError::EventCount(2));
    assert_eq!(classify(&count).kind, Kind::EventCount);
    let old = anyhow::Error::new(felix_client::CommitError::Unsupported);
    assert_eq!(classify(&old).kind, Kind::Commit);
}

/// An auth error is recognised by its type, so a message that says nothing
/// about tokens or permissions still classes as auth.
#[test]
fn a_typed_auth_error_is_auth_whatever_it_says() {
    for err in [
        felix_client::AuthError {
            kind: felix_client::AuthErrorKind::Refused,
            message: "quota of 3".to_string(),
        },
        felix_client::AuthError {
            kind: felix_client::AuthErrorKind::Token,
            message: "idp returned 503".to_string(),
        },
    ] {
        let classified = classify(&anyhow::Error::new(err).context("open stream"));
        assert_eq!(classified.kind, Kind::Auth);
    }
}

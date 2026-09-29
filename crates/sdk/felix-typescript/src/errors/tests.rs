use felix_wire::{ErrorCode, RetryClass};

use super::*;

#[test]
fn every_code_maps_to_a_kind_that_matches_its_retry_class() {
    let expected = [
        ("unauthenticated", KIND_AUTH),
        ("forbidden", KIND_AUTH),
        ("not_found", KIND_NOT_FOUND),
        ("invalid_request", KIND_GENERIC),
        ("shard_unavailable", KIND_SHARD_UNAVAILABLE),
        ("not_leader", KIND_SHARD_UNAVAILABLE),
        ("quorum_timeout", KIND_OUTCOME_UNKNOWN),
        ("leadership_lost", KIND_OUTCOME_UNKNOWN),
        ("unacknowledged", KIND_OUTCOME_UNKNOWN),
        ("overloaded", KIND_OVERLOADED),
        ("limit_exceeded", KIND_GENERIC),
        ("draining", KIND_CONNECTION),
        ("internal", KIND_OUTCOME_UNKNOWN),
        ("storage", KIND_OUTCOME_UNKNOWN),
        ("stale_claim", KIND_GENERIC),
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
    assert_eq!(
        kind_for_code("overloaded", RetryClass::OutcomeUnknown),
        KIND_OUTCOME_UNKNOWN
    );
    assert_eq!(kind_for_code("internal", RetryClass::Retry), KIND_GENERIC);
    assert_eq!(kind_for_code("brand_new", RetryClass::Retry), KIND_GENERIC);
}

#[test]
fn without_a_code_the_message_decides_and_keeps_the_old_prefix() {
    let cases = [
        ("publish failed: forbidden", KIND_AUTH),
        ("unknown stream t1/default/nope", KIND_NOT_FOUND),
        ("offset trimmed", KIND_CURSOR),
        ("connection lost", KIND_CONNECTION),
        ("something nobody foresaw", KIND_GENERIC),
    ];
    for (message, kind) in cases {
        assert_eq!(
            encode(&anyhow::anyhow!(message)),
            format!("{kind}: {message}")
        );
    }
}

#[test]
fn a_typed_refusal_carries_its_code_on_a_line_of_its_own() {
    let err = anyhow::Error::new(felix_client::NotLeaderError {
        node_id: "broker-2".to_string(),
        addr: None,
        generation: 3,
    })
    .context("publish failed: with a colon: and a\nnewline");
    let encoded = encode(&err);
    let (head, text) = encoded.split_once('\n').expect("a header line");
    assert_eq!(
        head,
        r#"FELIX_SHARD_UNAVAILABLE {"code":"not_leader","retry":"redirect"}"#
    );
    assert!(text.starts_with("publish failed: with a colon: and a\nnewline"));
}

#[test]
fn a_lost_subscription_is_a_connection_error() {
    let err = anyhow::Error::new(felix_client::SubscriptionLost {
        reason: "reset by peer".to_string(),
    })
    .context("resubscribe to shard 0 of orders");
    assert!(encode(&err).starts_with(&format!("{KIND_CONNECTION}{KIND_SEPARATOR}")));
}

#[test]
fn a_commit_refusal_carries_its_fields() {
    let split = encode(&anyhow::Error::new(CommitError::NotOnOwningShard {
        index: 1,
        stream: "inventory".to_string(),
        owner: "orders".to_string(),
    }));
    assert!(
        split.starts_with(
            r#"FELIX_NOT_ON_OWNING_SHARD {"index":1,"owner":"orders","stream":"inventory"}"#
        ),
        "{split}"
    );
    let count = encode(&anyhow::Error::new(CommitError::EventCount(2)));
    assert!(
        count.starts_with(r#"FELIX_EVENT_COUNT {"count":2}"#),
        "{count}"
    );
    let old = encode(&anyhow::Error::new(CommitError::Unsupported));
    assert!(old.starts_with("FELIX_COMMIT: "), "{old}");
}

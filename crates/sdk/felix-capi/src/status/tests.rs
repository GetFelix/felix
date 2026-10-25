use super::*;

#[test]
fn outcome_unknown_wins_over_the_code() {
    assert_eq!(
        status_for_code("forbidden", RetryClass::OutcomeUnknown),
        FELIX_STATUS_OUTCOME_UNKNOWN
    );
}

#[test]
fn broker_codes_map_to_their_class() {
    for (code, status) in [
        ("unauthenticated", FELIX_STATUS_AUTH),
        ("forbidden", FELIX_STATUS_AUTH),
        ("not_found", FELIX_STATUS_NOT_FOUND),
        ("shard_unavailable", FELIX_STATUS_SHARD_UNAVAILABLE),
        ("not_leader", FELIX_STATUS_SHARD_UNAVAILABLE),
        ("overloaded", FELIX_STATUS_OVERLOADED),
        ("draining", FELIX_STATUS_CONNECTION),
        ("something_new", FELIX_STATUS_ERROR),
    ] {
        assert_eq!(status_for_code(code, RetryClass::Fatal), status, "{code}");
    }
}

#[test]
fn an_error_without_a_code_is_read_conservatively() {
    let classify_text = |text: &str| classify(&anyhow::anyhow!(text.to_string()));
    assert_eq!(
        classify_text("unknown stream t1/ns/s"),
        FELIX_STATUS_NOT_FOUND
    );
    assert_eq!(classify_text("connection lost"), FELIX_STATUS_CONNECTION);
    assert_eq!(classify_text("something odd"), FELIX_STATUS_ERROR);
    // Words that used to mean auth are only words now.
    assert_eq!(classify_text("forbidden"), FELIX_STATUS_ERROR);
    assert_eq!(
        classify_text("tls: could not load the token file"),
        FELIX_STATUS_ERROR
    );
}

/// An auth error is recognised by its type, so a message that says nothing
/// about tokens or permissions still maps to the auth status.
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
        assert_eq!(
            classify(&anyhow::Error::new(err).context("open stream")),
            FELIX_STATUS_AUTH
        );
    }
}

#[test]
fn commit_refusals_are_classified() {
    let unsupported = anyhow::Error::from(CommitError::Unsupported).context("commit");
    assert_eq!(classify(&unsupported), FELIX_STATUS_UNSUPPORTED);
    for refusal in [
        CommitError::EventCount(2),
        CommitError::NotOnOwningShard {
            index: 1,
            stream: "a".into(),
            owner: "b".into(),
        },
    ] {
        assert_eq!(
            classify(&anyhow::Error::from(refusal)),
            FELIX_STATUS_INVALID_ARGUMENT
        );
    }
    // Other broker-lacks-it errors keep their old class.
    assert_eq!(
        classify(&anyhow::anyhow!(
            "this broker does not support conditional publishes"
        )),
        FELIX_STATUS_ERROR
    );
}

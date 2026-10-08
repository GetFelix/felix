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
}

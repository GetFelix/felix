use super::*;

#[test]
fn api_error_helpers_build_expected_codes() {
    let not_found = api_not_found("missing");
    assert_eq!(not_found.status, StatusCode::NOT_FOUND);
    assert_eq!(not_found.body.code, "not_found");

    let not_enabled = api_not_enabled("disabled");
    assert_eq!(not_enabled.status, StatusCode::NOT_FOUND);
    assert_eq!(not_enabled.body.code, "not_enabled");

    let conflict = api_conflict("already_exists", "conflict");
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.body.code, "already_exists");

    let internal = api_internal_message("oops");
    assert_eq!(internal.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(internal.body.code, "internal");

    let unauthorized = api_unauthorized("nope");
    assert_eq!(unauthorized.status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized.body.code, "unauthorized");

    let forbidden = api_forbidden("nope");
    assert_eq!(forbidden.status, StatusCode::FORBIDDEN);
    assert_eq!(forbidden.body.code, "forbidden");

    let validation = api_validation_error("bad");
    assert_eq!(validation.status, StatusCode::BAD_REQUEST);
    assert_eq!(validation.body.code, "validation_error");
}

#[test]
fn api_internal_logs_and_wraps_store_error() {
    let err = StoreError::Unexpected(anyhow::anyhow!("boom"));
    let api = api_internal("storage failed", &err);
    assert_eq!(api.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(api.body.code, "internal");
    assert_eq!(api.body.message, "storage failed");
}

#[test]
fn a_raft_write_out_of_budget_is_a_retryable_503() {
    let err = StoreError::Unexpected(anyhow::Error::new(crate::raft::NoQuorum::for_test()));
    let api = api_internal("failed to create tenant", &err);
    assert_eq!(api.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(api.body.code, "unavailable");

    // Wrapped in context on the way up, it is still found.
    let err = StoreError::Unexpected(
        anyhow::Error::new(crate::raft::NoQuorum::for_test()).context("propose"),
    );
    assert_eq!(
        api_internal("x", &err).status,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

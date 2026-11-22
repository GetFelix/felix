use axum::http::StatusCode;

use super::*;

/// Brokers poll every 2 s during a rolling upgrade, so an older leader must
/// read as a refusal, not a storage fault logged on every poll.
#[test]
fn an_older_leader_is_a_conflict() {
    let older = StoreError::Conflict("no plans here".to_string());
    assert_eq!(refused(older).status, StatusCode::CONFLICT);
    let fault = StoreError::Unexpected(anyhow::anyhow!("disk"));
    assert_eq!(refused(fault).status, StatusCode::INTERNAL_SERVER_ERROR);
}

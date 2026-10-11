use super::*;

fn grouping(user: &str, role: &str) -> GroupingRule {
    GroupingRule {
        user: user.to_string(),
        role: role.to_string(),
    }
}

#[test]
fn a_principal_reaches_only_itself() {
    let groupings = vec![grouping("alice", "role:reader")];
    assert_eq!(reached_by(&groupings, "alice"), vec!["alice"]);
}

#[test]
fn a_role_reaches_its_members_through_nested_roles() {
    let groupings = vec![
        grouping("alice", "role:reader"),
        grouping("role:reader", "role:base"),
        grouping("bob", "role:base"),
        grouping("carol", "role:other"),
    ];
    assert_eq!(
        reached_by(&groupings, "role:base"),
        vec!["alice", "bob", "role:base", "role:reader"]
    );
}

#[test]
fn a_cycle_of_roles_terminates() {
    let groupings = vec![grouping("role:a", "role:b"), grouping("role:b", "role:a")];
    assert_eq!(reached_by(&groupings, "role:a"), vec!["role:a", "role:b"]);
}

#[test]
fn an_idp_group_names_the_whole_tenant() {
    let groupings = vec![
        grouping("alice", "role:reader"),
        grouping("group:https://idp#ops", "role:reader"),
    ];
    assert_eq!(reached_by(&groupings, "role:reader"), vec![ALL_PRINCIPALS]);
}

fn state() -> AppState {
    let store = std::sync::Arc::new(crate::store::memory::InMemoryStore::new(
        crate::store::StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: None,
        },
    ));
    crate::test_support::app_state_ready(store)
}

/// A token minted in the second of a revocation would be refused with the
/// ones it replaced, so a mint waits that second out.
#[tokio::test]
async fn a_mint_waits_out_the_second_of_a_revocation() {
    let state = state();
    revoke(&state, "t1", vec!["alice".into()]).await.unwrap();
    let revoked_at = state
        .store
        .principal_revoked_at("t1", "alice")
        .await
        .unwrap()
        .unwrap();
    wait_out_revocation(&state, "t1", "alice").await.unwrap();
    assert!(crate::auth::refresh::now_secs() > revoked_at);

    // Nobody else waits.
    let started = std::time::Instant::now();
    wait_out_revocation(&state, "t1", "bob").await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(100));
}

#[tokio::test]
async fn a_revocation_far_ahead_does_not_hold_a_mint_for_long() {
    let state = state();
    let ahead = crate::auth::refresh::now_secs() + 3_600;
    state
        .store
        .revoke_principals("t1", vec![ALL_PRINCIPALS.into()], ahead)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    wait_out_revocation(&state, "t1", "alice").await.unwrap();
    assert!(started.elapsed() <= MAX_MINT_WAIT + Duration::from_millis(500));
}

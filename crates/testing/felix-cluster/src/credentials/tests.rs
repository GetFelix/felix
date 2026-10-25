use std::time::Duration;

use felix_controlplane_service::auth::felix_token::{
    BROKER_AUDIENCE, CONTROLPLANE_AUDIENCE, verify_token_for,
};

use super::Credentials;

const TENANT: &str = "t1";

fn credentials(ttl: Duration) -> Credentials {
    let keys = felix_controlplane_service::auth::keys::generate_signing_keys().expect("keys");
    Credentials::new(keys, TENANT, ttl)
}

fn valid_now(credentials: &Credentials, token: &str) -> bool {
    // No leeway, so "valid" means valid at this instant.
    verify_token_for(
        &credentials.keys,
        TENANT,
        token,
        0,
        &[CONTROLPLANE_AUDIENCE],
    )
    .is_ok()
}

#[test]
fn tokens_handed_out_after_a_lifetime_are_still_valid() {
    let credentials = credentials(Duration::from_secs(1));
    assert!(valid_now(&credentials, &credentials.admin_token()));

    std::thread::sleep(Duration::from_millis(2_500));

    assert!(valid_now(&credentials, &credentials.admin_token()));
    assert!(valid_now(&credentials, &credentials.operator_token()));
}

#[tokio::test]
async fn node_token_files_are_rewritten_before_they_expire() {
    let credentials = credentials(Duration::from_secs(1));
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("node.token");
    credentials
        .write_node_token("broker-0", &path)
        .expect("write node token");
    let _renewal = credentials.spawn_node_token_renewal();

    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let token = std::fs::read_to_string(&path).expect("read node token");
    assert!(
        valid_now(&credentials, &token),
        "the broker's token file still holds an expired token"
    );
}

fn broker_accepts(credentials: &Credentials, token: &str) -> bool {
    verify_token_for(&credentials.keys, TENANT, token, 0, &[BROKER_AUDIENCE]).is_ok()
}

#[test]
fn a_client_token_lives_as_long_as_asked() {
    let credentials = credentials(Duration::from_secs(3600));
    let short = credentials.client_token_for(Duration::from_secs(1));
    assert!(broker_accepts(&credentials, &short));

    std::thread::sleep(Duration::from_millis(2_500));

    assert!(
        !broker_accepts(&credentials, &short),
        "it should have expired"
    );
    assert!(broker_accepts(&credentials, &credentials.client_token()));
}

#[test]
fn a_forged_client_token_is_not_signed_by_the_tenant_key() {
    let credentials = credentials(Duration::from_secs(3600));
    let forged = credentials
        .forged_client_token(Duration::from_secs(60))
        .expect("forge");
    assert!(!broker_accepts(&credentials, &forged));
}

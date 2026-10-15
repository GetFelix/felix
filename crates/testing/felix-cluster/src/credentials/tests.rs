use std::time::Duration;

use felix_controlplane_service::auth::felix_token::{CONTROLPLANE_AUDIENCE, verify_token_for};

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

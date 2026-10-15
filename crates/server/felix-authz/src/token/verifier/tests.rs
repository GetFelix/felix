use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use jsonwebtoken::Header;

use super::*;
use crate::token::FelixTokenIssuer;
use crate::token::issuer::now_epoch_seconds;
use crate::token::tests::{encoding_key_from_seed, test_key_material, test_key_store};

#[test]
fn verify_fails_without_verification_keys() {
    let key_store = test_key_store();
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(600),
        key_store.clone(),
    );
    let token = issuer
        .mint(&TenantId::new("tenant-a"), "principal", vec![])
        .expect("token mint");

    let empty_store: Arc<dyn TenantKeyStore> = Arc::new(HashMap::new());
    let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, empty_store);
    let err = verifier
        .verify(&TenantId::new("tenant-a"), &token)
        .expect_err("missing verification keys");
    assert!(matches!(err, AuthzError::MissingVerificationKeys(_)));
}

#[test]
fn verify_fails_on_tenant_mismatch() {
    let mut keys = HashMap::new();
    let material = test_key_material();
    keys.insert("tenant-a".to_string(), material.clone());
    keys.insert("tenant-b".to_string(), material);
    let key_store: Arc<dyn TenantKeyStore> = Arc::new(keys);

    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(600),
        key_store.clone(),
    );
    let token = issuer
        .mint(&TenantId::new("tenant-a"), "principal", vec![])
        .expect("token mint");

    let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
    let err = verifier
        .verify(&TenantId::new("tenant-b"), &token)
        .expect_err("tenant mismatch");
    assert!(matches!(err, AuthzError::TenantMismatch { .. }));
}

#[test]
fn verify_uses_first_key_when_no_kid() {
    let key_store = test_key_store();
    let signing = key_store
        .current_signing_key(&TenantId::new("tenant-a"))
        .expect("signing key");

    let claims = FelixClaims {
        iss: "felix-auth".to_string(),
        aud: "felix-broker".to_string(),
        sub: "principal".to_string(),
        tid: "tenant-a".to_string(),
        exp: now_epoch_seconds() + 600,
        iat: now_epoch_seconds(),
        jti: None,
        perms: vec![],
        act: None,
    };
    let header = Header::new(signing.alg);
    let encoding_key = encoding_key_from_seed(&signing.private_key);
    let token = jsonwebtoken::encode(&header, &claims, &encoding_key).expect("encode token");

    let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
    let verified = verifier
        .verify(&TenantId::new("tenant-a"), &token)
        .expect("verify token");
    assert_eq!(verified.sub, "principal");
}

#[test]
fn verify_succeeds_when_kid_unknown() {
    let key_store = test_key_store();
    let signing = key_store
        .current_signing_key(&TenantId::new("tenant-a"))
        .expect("signing key");

    let claims = FelixClaims {
        iss: "felix-auth".to_string(),
        aud: "felix-broker".to_string(),
        sub: "principal".to_string(),
        tid: "tenant-a".to_string(),
        exp: now_epoch_seconds() + 600,
        iat: now_epoch_seconds(),
        jti: None,
        perms: vec![],
        act: None,
    };
    let mut header = Header::new(signing.alg);
    header.kid = Some("unknown".to_string());
    let encoding_key = encoding_key_from_seed(&signing.private_key);
    let token = jsonwebtoken::encode(&header, &claims, &encoding_key).expect("encode token");

    let verifier = FelixTokenVerifier::new("felix-auth", "felix-broker", 5, key_store);
    let verified = verifier
        .verify(&TenantId::new("tenant-a"), &token)
        .expect("verify token");
    assert_eq!(verified.sub, "principal");
}

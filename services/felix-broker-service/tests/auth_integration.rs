use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use felix_authz::{
    Action, FelixClaims, FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache,
    TenantKeyMaterial,
};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use jsonwebtoken::{Algorithm, EncodingKey, Header};

const TEST_PRIVATE_KEY: [u8; 32] = [7u8; 32];

fn jwks_from_public_key(public_key: &[u8], kid: &str) -> Jwks {
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    }
}

fn build_auth(tenant_id: &str) -> (Arc<BrokerAuth>, TenantKeyMaterial) {
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let jwks = jwks_from_public_key(&public_key, "k1");
    let key_material = TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key,
        jwks: jwks.clone(),
    };
    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://localhost".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    (
        Arc::new(BrokerAuth::with_key_store(key_store)),
        key_material,
    )
}

fn mint_valid_token(tenant_id: &str, key_material: &TenantKeyMaterial) -> String {
    let mut map = HashMap::new();
    map.insert(tenant_id.to_string(), key_material.clone());
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(map),
    );
    issuer
        .mint(
            &TenantId::new(tenant_id),
            "p:test",
            vec![
                "stream.publish:stream:t1/*/*".to_string(),
                "cache.read:cache:t1/*/*".to_string(),
            ],
        )
        .expect("mint token")
}

fn encode_claims(claims: &FelixClaims, kid: &str, private_key: &[u8; 32]) -> String {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(kid.to_string());
    let signing_key = Ed25519SigningKey::from_bytes(private_key);
    let der = signing_key.to_pkcs8_der().expect("der");
    jsonwebtoken::encode(&header, claims, &EncodingKey::from_ed_der(der.as_bytes()))
        .expect("encode")
}

fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test]
async fn broker_auth_accepts_valid_token() -> Result<()> {
    let (auth, key_material) = build_auth("t1");
    let token = mint_valid_token("t1", &key_material);
    let ctx = auth.authenticate("t1", &token).await?;
    assert!(
        ctx.matcher
            .allows(Action::StreamPublish, "stream:t1/default/orders")
    );
    Ok(())
}

#[tokio::test]
async fn broker_auth_rejects_invalid_claims_and_signature() -> Result<()> {
    let (auth, key_material) = build_auth("t1");
    let now = now_epoch_seconds();

    let wrong_aud = FelixClaims {
        iss: "felix-auth".to_string(),
        aud: "wrong-aud".to_string(),
        sub: "p:test".to_string(),
        tid: "t1".to_string(),
        exp: now + 300,
        iat: now,
        jti: None,
        perms: vec!["stream.publish:stream:t1/*/*".to_string()],
        act: None,
    };
    let wrong_aud_token = encode_claims(&wrong_aud, "k1", &TEST_PRIVATE_KEY);
    assert!(auth.authenticate("t1", &wrong_aud_token).await.is_err());

    let expired = FelixClaims {
        exp: now - 60,
        ..wrong_aud
    };
    let expired_token = encode_claims(&expired, "k1", &TEST_PRIVATE_KEY);
    assert!(auth.authenticate("t1", &expired_token).await.is_err());

    let wrong_tid = mint_valid_token("t2", &key_material);
    assert!(auth.authenticate("t1", &wrong_tid).await.is_err());

    let mut bad_sig = mint_valid_token("t1", &key_material);
    if let Some(last) = bad_sig.pop() {
        bad_sig.push(if last == 'a' { 'b' } else { 'a' });
    }
    assert!(auth.authenticate("t1", &bad_sig).await.is_err());
    Ok(())
}

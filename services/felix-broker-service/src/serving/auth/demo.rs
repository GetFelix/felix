//! Demo authentication: a fixed Ed25519 key and a token for one tenant, so a
//! demo broker accepts clients without a control plane.
//!
//! The key is a constant in this file, so anything it signs is forgeable. Only
//! built with the `demo` feature, which the `demos/broker` binaries,
//! `demos/slow-consumer`, `demos/state-divergence` and this crate's tests turn
//! on; the broker binary never has it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
};
use jsonwebtoken::Algorithm;

use crate::serving::auth::{BrokerAuth, ControlPlaneKeyStore};

const DEMO_PRIVATE_KEY: [u8; 32] = [42u8; 32];

/// A broker verifier and a token it accepts, both for the same tenant.
pub struct DemoAuth {
    pub auth: Arc<BrokerAuth>,
    pub tenant_id: String,
    pub token: String,
}

/// Mint a token for `tenant_id` with full stream and cache permissions, and a
/// [`BrokerAuth`] that trusts the key it was signed with.
pub fn demo_auth_for_tenant(tenant_id: &str) -> Result<DemoAuth> {
    let jwks = build_jwks("demo-k1")?;
    let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let key_material = TenantKeyMaterial {
        kid: "demo-k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: DEMO_PRIVATE_KEY,
        public_key,
        jwks: jwks.clone(),
    };

    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    key_store.insert(tenant_id.to_string(), key_material);
    let key_store = Arc::new(key_store);

    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        key_store,
    );

    let perms = vec![
        format!("tenant.manage:tenant:{tenant_id}"),
        format!("ns.manage:namespace:{tenant_id}/*"),
        format!("stream.publish:stream:{tenant_id}/*/*"),
        format!("stream.subscribe:stream:{tenant_id}/*/*"),
        format!("cache.read:cache:{tenant_id}/*/*"),
        format!("cache.write:cache:{tenant_id}/*/*"),
    ];

    let token = issuer
        .mint(&TenantId::new(tenant_id), "p:demo", perms)
        .context("mint demo token")?;

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(tenant_id), jwks);
    let auth = Arc::new(BrokerAuth::with_key_store(key_store));

    Ok(DemoAuth {
        auth,
        tenant_id: tenant_id.to_string(),
        token,
    })
}

/// A token for `subject` in `tenant_id`, signed with the demo key and
/// naming `actor` in its `act` claim, as the control plane's delegation does.
#[cfg(test)]
pub(crate) fn demo_token_with_actor(
    tenant_id: &str,
    subject: &str,
    actor: Option<&str>,
) -> Result<String> {
    let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
    let mut key_store: HashMap<String, TenantKeyMaterial> = HashMap::new();
    key_store.insert(
        tenant_id.to_string(),
        TenantKeyMaterial {
            kid: "demo-k1".to_string(),
            alg: Algorithm::EdDSA,
            private_key: DEMO_PRIVATE_KEY,
            public_key: signing_key.verifying_key().to_bytes(),
            jwks: build_jwks("demo-k1")?,
        },
    );
    FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::new(key_store),
    )
    .mint_with_actor(
        &TenantId::new(tenant_id),
        subject,
        vec![format!("stream.subscribe:stream:{tenant_id}/*/*")],
        actor,
    )
    .context("mint demo token")
}

fn build_jwks(kid: &str) -> Result<Jwks> {
    let signing_key = Ed25519SigningKey::from_bytes(&DEMO_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let x = URL_SAFE_NO_PAD.encode(public_key);
    Ok(Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: kid.to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    })
}

#[cfg(test)]
mod tests;

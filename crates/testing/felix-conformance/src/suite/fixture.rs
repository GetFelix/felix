//! What the suite runs with: a signed token, the broker's auth, and TLS.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use felix_authz::{
    FelixTokenIssuer, Jwk, Jwks, KeyUse, TenantId, TenantKeyCache, TenantKeyMaterial,
    TenantKeyStore,
};
use felix_broker_service::serving::auth::{BrokerAuth, ControlPlaneKeyStore};
use felix_client::ClientConfig;
use jsonwebtoken::Algorithm;
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

// Static test keypair used to make JWT/JWKS tests deterministic and self-contained.
const TEST_PRIVATE_KEY: [u8; 32] = [21u8; 32];

/// A tenant, a token for it, and the broker auth that accepts that token.
pub(crate) struct AuthFixture {
    pub(crate) tenant_id: String,
    pub(crate) token: String,
    pub(crate) broker_auth: Arc<BrokerAuth>,
    signing_keys: Arc<dyn TenantKeyStore>,
    perms: Vec<String>,
}

impl AuthFixture {
    /// The same token as `token`, signed with the same key, living `ttl`.
    pub(crate) fn token_living(&self, ttl: Duration) -> Result<String> {
        let issuer = FelixTokenIssuer::new(
            "felix-auth",
            "felix-broker",
            ttl,
            Arc::clone(&self.signing_keys),
        );
        Ok(issuer.mint(
            &TenantId::new(&self.tenant_id),
            "conformance",
            self.perms.clone(),
        )?)
    }
}

pub(crate) fn build_auth_fixture() -> Result<AuthFixture> {
    let tenant_id = "t1".to_string();
    let signing_key = Ed25519SigningKey::from_bytes(&TEST_PRIVATE_KEY);
    let public_key = signing_key.verifying_key().to_bytes();
    let x = URL_SAFE_NO_PAD.encode(public_key);
    let jwks = Jwks {
        keys: vec![Jwk {
            kty: "OKP".to_string(),
            kid: "k1".to_string(),
            alg: "EdDSA".to_string(),
            use_field: KeyUse::Sig,
            crv: Some("Ed25519".to_string()),
            x: Some(x),
        }],
    };
    let key_material = TenantKeyMaterial {
        kid: "k1".to_string(),
        alg: Algorithm::EdDSA,
        private_key: TEST_PRIVATE_KEY,
        public_key,
        jwks: jwks.clone(),
    };
    let mut keys = HashMap::new();
    keys.insert(tenant_id.clone(), key_material);
    let signing_keys: Arc<dyn TenantKeyStore> = Arc::new(keys);
    let issuer = FelixTokenIssuer::new(
        "felix-auth",
        "felix-broker",
        Duration::from_secs(900),
        Arc::clone(&signing_keys),
    );
    let perms = vec![
        format!("stream.publish:stream:{tenant_id}/default/*"),
        format!("stream.subscribe:stream:{tenant_id}/default/*"),
        format!("cache.read:cache:{tenant_id}/default/*"),
        format!("cache.write:cache:{tenant_id}/default/*"),
    ];
    let token = issuer.mint(&TenantId::new(&tenant_id), "conformance", perms.clone())?;

    let key_store = Arc::new(ControlPlaneKeyStore::new(
        "http://127.0.0.1:1".to_string(),
        Arc::new(TenantKeyCache::default()),
    ));
    key_store.insert_jwks(&TenantId::new(&tenant_id), jwks);
    let broker_auth = Arc::new(BrokerAuth::with_key_store(key_store));
    Ok(AuthFixture {
        tenant_id,
        token,
        broker_auth,
        signing_keys,
        perms,
    })
}

pub(crate) fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

pub(crate) fn build_quinn_client_config(
    cert: CertificateDer<'static>,
) -> Result<QuinnClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    Ok(quinn)
}

pub(crate) fn build_client_config(
    cert: CertificateDer<'static>,
    auth: &AuthFixture,
) -> Result<ClientConfig> {
    let quinn = build_quinn_client_config(cert)?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some(auth.tenant_id.clone());
    config.auth_token = Some(auth.token.clone());
    Ok(config)
}

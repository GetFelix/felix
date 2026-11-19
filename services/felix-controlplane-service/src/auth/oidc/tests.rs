use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{Json, Router, routing::get};
use base64::Engine;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::jwk::KeyAlgorithm;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::keys::expected_key_algorithm;
use super::*;
use crate::auth::idp_registry::ClaimMappings;
use crate::auth::idp_registry::IdpIssuerConfig;

/// Test keys are generated per test binary so no private key lives in the repo.
struct TestKeys {
    rsa_pem: String,
    rsa_n: String,
    rsa_e: String,
    ec_pkcs8: Vec<u8>,
    ec_x: String,
    ec_y: String,
}

fn test_keys() -> &'static TestKeys {
    static KEYS: std::sync::OnceLock<TestKeys> = std::sync::OnceLock::new();
    KEYS.get_or_init(|| {
        use aws_lc_rs::encoding::AsDer;
        use aws_lc_rs::rsa::{KeyPair, KeySize, PublicKeyComponents};
        use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair as _};
        use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};

        let rsa = KeyPair::generate(KeySize::Rsa2048).expect("rsa key");
        let der = rsa.as_der().expect("pkcs8 der");
        let body = STANDARD.encode(der.as_ref());
        let lines: Vec<&str> = body
            .as_bytes()
            .chunks(64)
            .map(|c| std::str::from_utf8(c).expect("base64 is ascii"))
            .collect();
        let rsa_pem = format!(
            "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
            lines.join("\n")
        );
        let public: PublicKeyComponents<Vec<u8>> = (&rsa.public_key().clone()).into();

        let rng = aws_lc_rs::rand::SystemRandom::new();
        let pkcs8 =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).expect("ec key");
        let ec = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref())
            .expect("ec pkcs8");
        // Uncompressed point: 0x04 || x || y.
        let point = ec.public_key().as_ref();
        TestKeys {
            rsa_pem,
            rsa_n: URL_SAFE_NO_PAD.encode(&public.n),
            rsa_e: URL_SAFE_NO_PAD.encode(&public.e),
            ec_pkcs8: pkcs8.as_ref().to_vec(),
            ec_x: URL_SAFE_NO_PAD.encode(&point[1..33]),
            ec_y: URL_SAFE_NO_PAD.encode(&point[33..65]),
        }
    })
}

#[tokio::test]
async fn validates_es256_against_mock_jwks() {
    let kid = "kid-es256";
    let jwks = json!({
        "keys": [{
            "kty": "EC",
            "kid": kid,
            "alg": "ES256",
            "use": "sig",
            "crv": "P-256",
            "x": test_keys().ec_x,
            "y": test_keys().ec_y
        }]
    });

    let (addr, _handle) = spawn_jwks_server(jwks).await;
    let issuer = format!("http://{addr}");
    let token = mint_upstream_token(Algorithm::ES256, &issuer, "aud1", kid);

    let validator = UpstreamOidcValidator::default();
    let validated = validator
        .validate(&token, &[issuer_cfg(&issuer, "aud1")])
        .await
        .expect("valid");
    assert_eq!(validated.issuer, issuer);
    assert_eq!(validated.subject, "user-1");
}

#[test]
fn expected_key_algorithm_covers_supported_oidc_algorithms() {
    let cases = [
        (Algorithm::ES256, Some(KeyAlgorithm::ES256)),
        (Algorithm::RS256, Some(KeyAlgorithm::RS256)),
        (Algorithm::RS384, Some(KeyAlgorithm::RS384)),
        (Algorithm::RS512, Some(KeyAlgorithm::RS512)),
        (Algorithm::PS256, Some(KeyAlgorithm::PS256)),
        (Algorithm::PS384, Some(KeyAlgorithm::PS384)),
        (Algorithm::PS512, Some(KeyAlgorithm::PS512)),
        (Algorithm::ES384, None),
        (Algorithm::EdDSA, None),
        (Algorithm::HS256, None),
    ];
    for (alg, expected) in cases {
        assert_eq!(expected_key_algorithm(alg), expected);
    }
}

#[tokio::test]
async fn rejects_es256_with_wrong_curve_jwk() {
    let kid = "kid-es256";
    let jwks = json!({
        "keys": [{
            "kty": "EC",
            "kid": kid,
            "alg": "ES256",
            "use": "sig",
            "crv": "P-384",
            "x": test_keys().ec_x,
            "y": test_keys().ec_y
        }]
    });

    let (addr, _handle) = spawn_jwks_server(jwks).await;
    let issuer = format!("http://{addr}");
    let token = mint_upstream_token(Algorithm::ES256, &issuer, "aud1", kid);

    let validator = UpstreamOidcValidator::default();
    let err = validator
        .validate(&token, &[issuer_cfg(&issuer, "aud1")])
        .await
        .unwrap_err();
    assert!(matches!(err, OidcError::InvalidJwk(_)));
}

#[tokio::test]
async fn rejects_non_allowed_header_algorithms() {
    // HS* are never allowed as upstream IdP algorithms.
    let now = chrono::Utc::now().timestamp();
    let claims = json!({
        "iss": "https://issuer.example",
        "sub": "user-1",
        "aud": "aud1",
        "iat": now,
        "exp": now + 300
    });
    let hs_header = Header::new(Algorithm::HS256);
    let hs_token = jsonwebtoken::encode(&hs_header, &claims, &EncodingKey::from_secret(b"secret"))
        .expect("hs token");
    let validator = UpstreamOidcValidator::default();
    let hs_err = validator.validate(&hs_token, &[]).await.unwrap_err();
    assert!(matches!(hs_err, OidcError::UnsupportedAlgorithm));

    // EdDSA Felix tokens are not accepted as upstream IdP tokens.
    let signing_key = Ed25519SigningKey::from_bytes(&[1u8; 32]);
    let der = signing_key.to_pkcs8_der().expect("pkcs8 der");
    let header = Header::new(Algorithm::EdDSA);
    let token = jsonwebtoken::encode(&header, &claims, &EncodingKey::from_ed_der(der.as_bytes()))
        .expect("token");
    let err = validator.validate(&token, &[]).await.unwrap_err();
    assert!(matches!(err, OidcError::UnsupportedAlgorithm));
}

/// Most providers sign ID tokens with RS256, the algorithm OIDC requires
/// every provider to support, so it is accepted without configuration.
#[tokio::test]
async fn the_default_allowlist_accepts_rs256() {
    let kid = "kid-default-rs256";
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "kid": kid,
            "alg": "RS256",
            "use": "sig",
            "n": test_keys().rsa_n,
            "e": test_keys().rsa_e
        }]
    });
    let (addr, _handle) = spawn_jwks_server(jwks).await;
    let issuer = format!("http://{addr}");
    let token = mint_upstream_token(Algorithm::RS256, &issuer, "aud1", kid);
    let validated = validator_with_algorithms(Vec::new())
        .validate(&token, &[issuer_cfg(&issuer, "aud1")])
        .await
        .expect("an RS256 token under the default allowlist");
    assert_eq!(validated.subject, "user-1");
}

/// The perf harness's local IdP (scripts/perf/cloud/remote/local-idp.py) has
/// to mint tokens this validator accepts, with the default allowlist.
#[tokio::test]
async fn the_perf_harness_local_idp_tokens_validate() {
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/perf/cloud/remote/local-idp.py"
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let dir_arg = dir.path().to_str().expect("utf-8 path");
    let run = |args: &[&str]| {
        let out = std::process::Command::new("python3")
            .arg(script)
            .args(args)
            .output()
            .expect("python3 runs the script");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).expect("utf-8 output")
    };
    run(&["init", dir_arg]);
    let jwks: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("www/jwks.json")).expect("jwks.json"),
    )
    .expect("jwks json");
    let (addr, _handle) = spawn_jwks_server(jwks).await;
    let issuer = format!("http://{addr}");
    let token = run(&[
        "mint",
        dir_arg,
        &issuer,
        "felix-perf",
        "perf-harness",
        "3600",
    ]);
    let validator = validator_with_algorithms(Vec::new());

    let validated = validator
        .validate(token.trim(), &[issuer_cfg(&issuer, "felix-perf")])
        .await
        .expect("a locally minted token");
    assert_eq!(validated.subject, "perf-harness");

    validator
        .validate(token.trim(), &[issuer_cfg(&issuer, "another-audience")])
        .await
        .expect_err("a token for another audience is refused");
    let expired = run(&[
        "mint",
        dir_arg,
        &issuer,
        "felix-perf",
        "perf-harness",
        "-3600",
    ]);
    validator
        .validate(expired.trim(), &[issuer_cfg(&issuer, "felix-perf")])
        .await
        .expect_err("an expired token is refused");
}

/// An explicit allowlist still narrows the default.
#[tokio::test]
async fn rejects_rs256_when_not_allowlisted() {
    let header = json!({ "alg": "RS256", "typ": "JWT", "kid": "kid-1" });
    let claims = json!({
        "iss": "https://issuer.example",
        "sub": "user-1",
        "aud": "aud1",
        "iat": chrono::Utc::now().timestamp(),
        "exp": chrono::Utc::now().timestamp() + 300
    });
    let token = format!(
        "{}.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header.to_string()),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string())
    );

    let validator = validator_with_algorithms(vec![Algorithm::ES256]);
    let err = validator.validate(&token, &[]).await.unwrap_err();
    assert!(matches!(err, OidcError::UnsupportedAlgorithm));
}

#[tokio::test]
async fn validates_rsa_and_ps_algorithms() {
    let cases = [
        Algorithm::RS256,
        Algorithm::RS384,
        Algorithm::RS512,
        Algorithm::PS256,
        Algorithm::PS384,
        Algorithm::PS512,
    ];

    for alg in cases {
        let kid = format!("kid-{}", alg_name(alg).to_ascii_lowercase());
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "kid": kid,
                "alg": alg_name(alg),
                "use": "sig",
                "n": test_keys().rsa_n,
                "e": test_keys().rsa_e
            }]
        });
        let (addr, _handle) = spawn_jwks_server(jwks).await;
        let issuer = format!("http://{addr}");
        let token = mint_upstream_token(alg, &issuer, "aud1", &kid);
        let validator = validator_with_algorithms(vec![
            Algorithm::ES256,
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::PS256,
            Algorithm::PS384,
            Algorithm::PS512,
        ]);
        let validated = validator
            .validate(&token, &[issuer_cfg(&issuer, "aud1")])
            .await
            .unwrap_or_else(|err| panic!("{} should validate, got: {err}", alg_name(alg)));
        assert_eq!(validated.subject, "user-1");
    }
}

#[tokio::test]
async fn validates_rsa_when_jwk_omits_alg() {
    // Microsoft Entra (and others) publish JWKS signing keys without the
    // optional `alg` member (RFC 7517 §4.4). The key type still pins the
    // algorithm, so an RS256 token must validate against an alg-less RSA
    // JWK. Before the fix this failed with InvalidJwk("missing alg").
    let kid = "kid-no-alg";
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "kid": kid,
            "use": "sig",
            "n": test_keys().rsa_n,
            "e": test_keys().rsa_e
        }]
    });
    let (addr, _handle) = spawn_jwks_server(jwks).await;
    let issuer = format!("http://{addr}");
    let token = mint_upstream_token(Algorithm::RS256, &issuer, "aud1", kid);
    let validator = validator_with_algorithms(vec![Algorithm::ES256, Algorithm::RS256]);
    let validated = validator
        .validate(&token, &[issuer_cfg(&issuer, "aud1")])
        .await
        .expect("alg-less RSA JWK should validate an RS256 token");
    assert_eq!(validated.subject, "user-1");
}

#[tokio::test]
async fn rejects_rsa_and_ps_when_not_allowlisted() {
    let cases = [
        Algorithm::RS256,
        Algorithm::RS384,
        Algorithm::RS512,
        Algorithm::PS256,
        Algorithm::PS384,
        Algorithm::PS512,
    ];
    for alg in cases {
        let kid = format!("kid-{}", alg_name(alg).to_ascii_lowercase());
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "kid": kid,
                "alg": alg_name(alg),
                "use": "sig",
                "n": test_keys().rsa_n,
                "e": test_keys().rsa_e
            }]
        });
        let (addr, _handle) = spawn_jwks_server(jwks).await;
        let issuer = format!("http://{addr}");
        let token = mint_upstream_token(alg, &issuer, "aud1", &kid);
        let validator = validator_with_algorithms(vec![Algorithm::ES256]);
        let err = validator
            .validate(&token, &[issuer_cfg(&issuer, "aud1")])
            .await
            .expect_err("non-allowlisted RSA/PS alg must be rejected");
        assert!(matches!(err, OidcError::UnsupportedAlgorithm));
    }
}

#[tokio::test]
async fn rejects_allowed_alg_with_wrong_jwk_type() {
    let kid = "kid-ps256";
    let jwks = json!({
        "keys": [{
            "kty": "EC",
            "kid": kid,
            "alg": "PS256",
            "use": "sig",
            "crv": "P-256",
            "x": test_keys().ec_x,
            "y": test_keys().ec_y
        }]
    });
    let (addr, _handle) = spawn_jwks_server(jwks).await;
    let issuer = format!("http://{addr}");
    let token = mint_upstream_token(Algorithm::PS256, &issuer, "aud1", kid);
    let validator = validator_with_algorithms(vec![Algorithm::ES256, Algorithm::PS256]);
    let err = validator
        .validate(&token, &[issuer_cfg(&issuer, "aud1")])
        .await
        .unwrap_err();
    assert!(matches!(err, OidcError::InvalidJwk(_)));
}

/// A token naming an allowlisted issuer and an unknown `kid` needs no valid
/// signature to reach the key lookup, so a stream of them must not turn into a
/// stream of fetches against the tenant's IdP.
#[tokio::test]
async fn unknown_kids_do_not_each_fetch_the_jwks() {
    let (addr, hits, _jwks, _handle) = spawn_counting_jwks_server(es256_jwks("kid-es256")).await;
    let issuer = format!("http://{addr}");
    let issuers = [issuer_cfg(&issuer, "aud1")];
    let validator = UpstreamOidcValidator::default();

    for i in 0..20 {
        let token = mint_upstream_token(Algorithm::ES256, &issuer, "aud1", &format!("bogus-{i}"));
        let err = validator.validate(&token, &issuers).await.unwrap_err();
        assert!(matches!(err, OidcError::JwksKeyNotFound), "{err}");
    }
    let concurrent = (0..20).map(|i| {
        let validator = validator.clone();
        let issuers = issuers.clone();
        let token = mint_upstream_token(Algorithm::ES256, &issuer, "aud1", &format!("race-{i}"));
        tokio::spawn(async move { validator.validate(&token, &issuers).await })
    });
    for task in concurrent.collect::<Vec<_>>() {
        assert!(task.await.expect("join").is_err());
    }

    // The first fetch fills the cache and the first miss may refresh it once;
    // everything after that is inside the refresh floor.
    let fetches = hits.load(Ordering::SeqCst);
    assert!(fetches <= 2, "40 bad tokens caused {fetches} JWKS fetches");
}

/// The floor must not stop a genuine rotation: a key the IdP publishes after
/// our last fetch is accepted once the floor has passed.
#[tokio::test]
async fn a_rotated_in_key_is_picked_up_after_the_refresh_floor() {
    let floor = Duration::from_millis(300);
    let (addr, hits, jwks, _handle) = spawn_counting_jwks_server(es256_jwks("kid-old")).await;
    let issuer = format!("http://{addr}");
    let issuers = [issuer_cfg(&issuer, "aud1")];
    let validator = UpstreamOidcValidator::default().with_jwks_refresh_floor(floor);

    let old = mint_upstream_token(Algorithm::ES256, &issuer, "aud1", "kid-old");
    validator.validate(&old, &issuers).await.expect("old key");

    *jwks.write().expect("jwks lock") = es256_jwks("kid-new");
    let new = mint_upstream_token(Algorithm::ES256, &issuer, "aud1", "kid-new");
    // Inside the floor the IdP is not asked again, so the new kid is unknown.
    let err = validator.validate(&new, &issuers).await.unwrap_err();
    assert!(matches!(err, OidcError::JwksKeyNotFound), "{err}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    tokio::time::sleep(floor + Duration::from_millis(50)).await;
    let validated = validator
        .validate(&new, &issuers)
        .await
        .expect("the rotated-in key is accepted after the floor");
    assert_eq!(validated.subject, "user-1");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

fn es256_jwks(kid: &str) -> Value {
    json!({
        "keys": [{
            "kty": "EC",
            "kid": kid,
            "alg": "ES256",
            "use": "sig",
            "crv": "P-256",
            "x": test_keys().ec_x,
            "y": test_keys().ec_y
        }]
    })
}

/// A JWKS endpoint that counts its requests and whose key set can be swapped,
/// the way an IdP's does when it rotates.
async fn spawn_counting_jwks_server(
    jwks: Value,
) -> (
    SocketAddr,
    Arc<AtomicUsize>,
    Arc<std::sync::RwLock<Value>>,
    JoinHandle<()>,
) {
    let hits = Arc::new(AtomicUsize::new(0));
    let jwks = Arc::new(std::sync::RwLock::new(jwks));
    let app = Router::new().route(
        "/jwks",
        get({
            let hits = hits.clone();
            let jwks = jwks.clone();
            move || {
                hits.fetch_add(1, Ordering::SeqCst);
                let body = jwks.read().expect("jwks lock").clone();
                async move { Json(body) }
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = axum::serve(listener, app.into_make_service());
    let handle = tokio::spawn(async move {
        let _ = server.await;
    });
    (addr, hits, jwks, handle)
}

async fn spawn_jwks_server(jwks: Value) -> (SocketAddr, JoinHandle<()>) {
    let app = Router::new().route(
        "/jwks",
        get({
            let jwks = jwks.clone();
            move || {
                let jwks = jwks.clone();
                async move { Json(jwks) }
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = axum::serve(listener, app.into_make_service());
    let handle = tokio::spawn(async move {
        let _ = server.await;
    });
    (addr, handle)
}

fn issuer_cfg(issuer: &str, audience: &str) -> IdpIssuerConfig {
    IdpIssuerConfig {
        issuer: issuer.to_string(),
        audiences: vec![audience.to_string()],
        discovery_url: None,
        jwks_url: Some(format!("{issuer}/jwks")),
        claim_mappings: ClaimMappings {
            subject_claim: "sub".to_string(),
            groups_claim: None,
        },
        ..Default::default()
    }
}

fn validator_with_algorithms(algorithms: Vec<Algorithm>) -> UpstreamOidcValidator {
    UpstreamOidcValidator::new_with_allowed_algorithms(
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        60,
        algorithms,
    )
}

fn mint_upstream_token(alg: Algorithm, issuer: &str, audience: &str, kid: &str) -> String {
    let mut header = Header::new(alg);
    header.kid = Some(kid.to_string());
    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "iss": issuer,
        "sub": "user-1",
        "aud": audience,
        "iat": now,
        "exp": now + 300
    });
    match alg {
        Algorithm::ES256 => jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_ec_der(&test_keys().ec_pkcs8),
        )
        .expect("es256 token"),
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(test_keys().rsa_pem.as_bytes()).expect("rsa pem"),
        )
        .expect("rsa/ps token"),
        _ => panic!("unsupported test algorithm: {alg:?}"),
    }
}

fn alg_name(alg: Algorithm) -> &'static str {
    match alg {
        Algorithm::RS256 => "RS256",
        Algorithm::RS384 => "RS384",
        Algorithm::RS512 => "RS512",
        Algorithm::PS256 => "PS256",
        Algorithm::PS384 => "PS384",
        Algorithm::PS512 => "PS512",
        Algorithm::ES256 => "ES256",
        _ => "unknown",
    }
}

/// A discovery server at `127.0.0.1`, answering with `document(own_base)`.
async fn spawn_discovery_server(
    document: impl Fn(&str) -> Value + Send + Sync + 'static,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let body = document(&base);
    let app = Router::new().route(
        "/.well-known/openid-configuration",
        get(move || {
            let body = body.clone();
            async move { Json(body) }
        }),
    );
    let server = axum::serve(listener, app.into_make_service());
    let handle = tokio::spawn(async move {
        let _ = server.await;
    });
    (base, handle)
}

fn discovery_cfg(issuer: &str) -> IdpIssuerConfig {
    IdpIssuerConfig {
        jwks_url: None,
        ..issuer_cfg(issuer, "aud1")
    }
}

/// A discovery document for another issuer is not this issuer's: its keys
/// would let that issuer's tokens pass as this one's.
#[tokio::test]
async fn discovery_for_another_issuer_is_refused() {
    let (base, _handle) = spawn_discovery_server(|base| {
        json!({"issuer": "https://someone-else.example", "jwks_uri": format!("{base}/jwks")})
    })
    .await;
    let err = UpstreamOidcValidator::default()
        .resolve_jwks_url(&base, &discovery_cfg(&base))
        .await
        .expect_err("mismatched issuer");
    assert!(matches!(err, OidcError::DiscoveryIssuerMismatch), "{err:?}");
}

/// Discovery can name any URL, so the JWKS URL it hands back is held to the
/// same rule as a configured one.
#[tokio::test]
async fn discovery_cannot_point_the_jwks_at_plain_http() {
    let (base, _handle) = spawn_discovery_server(
        |base| json!({"issuer": base, "jwks_uri": "http://idp.example.com/jwks"}),
    )
    .await;
    let err = UpstreamOidcValidator::default()
        .resolve_jwks_url(&base, &discovery_cfg(&base))
        .await
        .expect_err("plain http jwks");
    assert!(matches!(err, OidcError::UrlNotAllowed(_)), "{err:?}");

    let (base, _handle) =
        spawn_discovery_server(|base| json!({"issuer": base, "jwks_uri": format!("{base}/jwks")}))
            .await;
    assert_eq!(
        UpstreamOidcValidator::default()
            .resolve_jwks_url(&base, &discovery_cfg(&base))
            .await
            .expect("matching discovery"),
        format!("{base}/jwks")
    );
}

#[tokio::test]
async fn a_configured_jwks_url_over_plain_http_is_not_fetched() {
    let cfg = IdpIssuerConfig {
        jwks_url: Some("http://idp.example.com/jwks".to_string()),
        ..issuer_cfg("https://idp.example.com", "aud1")
    };
    let err = UpstreamOidcValidator::default()
        .resolve_jwks_url("https://idp.example.com", &cfg)
        .await
        .expect_err("plain http");
    assert!(matches!(err, OidcError::UrlNotAllowed(_)), "{err:?}");
}

/// A redirect would take the fetch past the URL checks, so it is not followed.
#[tokio::test]
async fn discovery_redirects_are_not_followed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let document = json!({"issuer": base, "jwks_uri": format!("{base}/jwks")});
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(|| async { axum::response::Redirect::temporary("/elsewhere") }),
        )
        .route(
            "/elsewhere",
            get(move || {
                let document = document.clone();
                async move { Json(document) }
            }),
        );
    let _handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });

    UpstreamOidcValidator::default()
        .resolve_jwks_url(&base, &discovery_cfg(&base))
        .await
        .expect_err("followed a redirect");
}

#[tokio::test]
async fn the_resolver_refuses_private_addresses() {
    use reqwest::dns::Resolve;
    use std::str::FromStr;

    let resolve =
        |host: &str| PublicOnlyResolver.resolve(reqwest::dns::Name::from_str(host).unwrap());
    assert!(resolve("10.1.2.3").await.is_err());
    assert!(resolve("169.254.169.254").await.is_err());
    assert!(resolve("fd00::1").await.is_err());
    assert!(resolve("127.0.0.1").await.is_ok(), "loopback stays allowed");
}

/// An issuer holding its keys inline, at a host that cannot be reached: any
/// fetch would fail the exchange.
fn inline_cfg(kids: &[&str]) -> IdpIssuerConfig {
    let keys: Vec<Value> = kids
        .iter()
        .map(|kid| es256_jwks(kid)["keys"][0].clone())
        .collect();
    IdpIssuerConfig {
        issuer: "https://idp.invalid".to_string(),
        audiences: vec!["aud1".to_string()],
        jwks: Some(serde_json::from_value(json!({ "keys": keys })).expect("jwks")),
        ..Default::default()
    }
}

fn mint_claims(kid: &str, claims: Value) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_string());
    jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_ec_der(&test_keys().ec_pkcs8),
    )
    .expect("token")
}

fn claims_with(extra: Value) -> Value {
    let now = chrono::Utc::now().timestamp();
    let mut claims = json!({
        "iss": "https://idp.invalid",
        "sub": "machine-1",
        "aud": "aud1",
        "iat": now,
        "exp": now + 60,
    });
    claims
        .as_object_mut()
        .expect("object")
        .extend(extra.as_object().expect("object").clone());
    claims
}

#[tokio::test]
async fn an_inline_jwks_issuer_validates_without_any_fetch() {
    let token = mint_claims("k1", claims_with(json!({})));
    let validated = UpstreamOidcValidator::default()
        .validate(&token, &[inline_cfg(&["k1"])])
        .await
        .expect("validates from the inline keys");
    assert_eq!(validated.subject, "machine-1");
}

#[tokio::test]
async fn an_inline_jwks_issuer_refuses_a_key_outside_its_set() {
    let token = mint_claims("rotated-away", claims_with(json!({})));
    let err = UpstreamOidcValidator::default()
        .validate(&token, &[inline_cfg(&["k1"])])
        .await
        .unwrap_err();
    assert!(matches!(err, OidcError::JwksKeyNotFound), "{err:?}");

    // Same kid, different key: the signature does not verify.
    let mut other = inline_cfg(&["k1"]);
    if let Some(jwks) = other.jwks.as_mut()
        && let jsonwebtoken::jwk::AlgorithmParameters::EllipticCurve(params) =
            &mut jwks.keys[0].algorithm
    {
        params.x = test_keys().ec_y.clone();
    }
    let token = mint_claims("k1", claims_with(json!({})));
    assert!(
        UpstreamOidcValidator::default()
            .validate(&token, &[other])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn an_inline_jwks_overlaps_keys_during_a_rotation() {
    let cfg = inline_cfg(&["old", "new"]);
    let validator = UpstreamOidcValidator::default();
    for kid in ["old", "new"] {
        let token = mint_claims(kid, claims_with(json!({})));
        validator
            .validate(&token, std::slice::from_ref(&cfg))
            .await
            .expect(kid);
    }
}

#[tokio::test]
async fn single_use_jti_refuses_a_replayed_assertion() {
    let mut cfg = inline_cfg(&["k1"]);
    cfg.single_use_jti = true;
    cfg.max_token_lifetime_seconds = Some(120);
    let validator = UpstreamOidcValidator::default();
    let token = mint_claims("k1", claims_with(json!({"jti": "a"})));
    validator
        .validate(&token, std::slice::from_ref(&cfg))
        .await
        .expect("first use");
    let err = validator
        .validate(&token, std::slice::from_ref(&cfg))
        .await
        .unwrap_err();
    assert!(matches!(err, OidcError::TokenReplayed), "{err:?}");

    let other = mint_claims("k1", claims_with(json!({"jti": "b"})));
    validator
        .validate(&other, std::slice::from_ref(&cfg))
        .await
        .expect("a different jti");

    let missing = mint_claims("k1", claims_with(json!({})));
    assert!(validator.validate(&missing, &[cfg]).await.is_err());
}

#[tokio::test]
async fn without_the_option_a_token_may_be_exchanged_again() {
    let cfg = inline_cfg(&["k1"]);
    let validator = UpstreamOidcValidator::default();
    let token = mint_claims("k1", claims_with(json!({"jti": "a"})));
    for _ in 0..2 {
        validator
            .validate(&token, std::slice::from_ref(&cfg))
            .await
            .expect("ok");
    }
}

#[tokio::test]
async fn a_token_lifetime_over_the_bound_is_refused() {
    let mut cfg = inline_cfg(&["k1"]);
    cfg.max_token_lifetime_seconds = Some(120);
    let validator = UpstreamOidcValidator::default();
    let ok = mint_claims("k1", claims_with(json!({})));
    validator
        .validate(&ok, std::slice::from_ref(&cfg))
        .await
        .expect("60s");
    let now = chrono::Utc::now().timestamp();
    let long = mint_claims("k1", claims_with(json!({"exp": now + 3600})));
    let err = validator.validate(&long, &[cfg]).await.unwrap_err();
    assert!(matches!(err, OidcError::InvalidClaim(_)), "{err:?}");
}

#[test]
fn the_jti_record_is_bounded_and_forgets_only_expired_entries() {
    let store = jti::JtiStore::with_cap(2);
    let now = chrono::Utc::now().timestamp();
    store.claim("i", "a", now, now + 60).expect("a");
    store
        .claim("i", "b", now, now - 1)
        .expect("b, already expired");
    // Full, but b has expired, so c replaces it and a stays.
    store.claim("i", "c", now, now + 60).expect("c");
    assert!(matches!(
        store.claim("i", "a", now, now + 60),
        Err(OidcError::TokenReplayed)
    ));
    assert!(matches!(
        store.claim("i", "d", now, now + 60),
        Err(OidcError::ReplayStoreFull)
    ));
}

#[test]
fn one_issuer_filling_its_record_does_not_affect_another() {
    let store = jti::JtiStore::with_cap(2);
    let now = chrono::Utc::now().timestamp();
    store.claim("noisy", "a", now, now + 60).expect("a");
    store.claim("noisy", "b", now, now + 60).expect("b");
    assert!(matches!(
        store.claim("noisy", "c", now, now + 60),
        Err(OidcError::ReplayStoreFull)
    ));
    store.claim("quiet", "c", now, now + 60).expect("quiet");
}

#[tokio::test]
async fn single_use_caps_the_lifetime_even_when_the_stored_config_does_not() {
    let mut cfg = inline_cfg(&["k1"]);
    cfg.single_use_jti = true;
    let now = chrono::Utc::now().timestamp();
    let token = mint_claims("k1", claims_with(json!({"jti": "z", "exp": now + 3600})));
    let err = UpstreamOidcValidator::default()
        .validate(&token, &[cfg])
        .await
        .unwrap_err();
    assert!(matches!(err, OidcError::InvalidClaim(_)), "{err:?}");
}

#[test]
fn the_jti_record_refuses_tokens_issued_before_it_started() {
    let store = jti::JtiStore::with_cap(10);
    let now = chrono::Utc::now().timestamp();
    assert!(matches!(
        store.claim("i", "a", now - 10, now + 60),
        Err(OidcError::TokenReplayed)
    ));
}

#[test]
fn a_jti_is_held_through_the_last_second_its_token_is_accepted() {
    let store = jti::JtiStore::with_cap(10);
    let now = 1_000_000;
    store.claim_for_test(now - 5, "i", "a", now).expect("first");
    // exp + skew == now: the token still verifies, so its jti still counts.
    assert!(matches!(
        store.claim_for_test(now, "i", "a", now),
        Err(OidcError::TokenReplayed)
    ));
    // A second later the token no longer verifies and the entry may go.
    store
        .claim_for_test(now + 1, "i", "a", now + 61)
        .expect("after");
}

#[test]
fn a_deleted_or_idle_issuer_gives_its_record_back() {
    let store = jti::JtiStore::with_cap(10);
    let now = 1_000_000;
    store.claim_for_test(now, "gone", "a", now + 30).expect("a");
    store.claim_for_test(now, "idle", "a", now + 30).expect("a");
    assert_eq!(store.issuers_held(), 2);
    store.forget_issuer("gone");
    assert_eq!(store.issuers_held(), 1);
    // Another issuer's claim, a minute on, sweeps the idle one.
    store
        .claim_for_test(now + 120, "busy", "b", now + 150)
        .expect("b");
    assert_eq!(store.issuers_held(), 1);
}

#[tokio::test]
async fn an_overlong_jti_is_refused() {
    let mut cfg = inline_cfg(&["k1"]);
    cfg.single_use_jti = true;
    cfg.max_token_lifetime_seconds = Some(120);
    let token = mint_claims("k1", claims_with(json!({"jti": "x".repeat(129)})));
    assert!(
        UpstreamOidcValidator::default()
            .validate(&token, &[cfg.clone()])
            .await
            .is_err()
    );
    let token = mint_claims("k1", claims_with(json!({"jti": "x".repeat(128)})));
    UpstreamOidcValidator::default()
        .validate(&token, &[cfg])
        .await
        .expect("128 bytes is fine");
}

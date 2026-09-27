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

const TEST_EC_PRIVATE_KEY_DER_B64: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgkcZLhh5bmc6yfv8ZrDxWybm+E+aoz2euIJD3fM73VSyhRANCAAQRkD6ZJEwqBms4JDddpbTjl4Ro49h8WRoNVnEcR/Tp6LhwGGZ8Ku1Gw9spY/BCsiW+5AqIqVlNVgGgJFMRbR1V";
const TEST_EC_JWK_X: &str = "EZA-mSRMKgZrOCQ3XaW045eEaOPYfFkaDVZxHEf06eg";
const TEST_EC_JWK_Y: &str = "uHAYZnwq7UbD2ylj8EKyJb7kCoipWU1WAaAkUxFtHVU";

const TEST_PRIVATE_KEY_PEM: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL
UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V
rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H
oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI
Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/
by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd
WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ
Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j
E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn
LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS
bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE
8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl
xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY
2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1
9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0
7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ
mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7
MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4
t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam
QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA
2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ
4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn
PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH
UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8
oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==
-----END RSA PRIVATE KEY-----"#;

const TEST_JWK_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const TEST_JWK_E: &str = "AQAB";

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
            "x": TEST_EC_JWK_X,
            "y": TEST_EC_JWK_Y
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
            "x": TEST_EC_JWK_X,
            "y": TEST_EC_JWK_Y
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

    let validator = UpstreamOidcValidator::default();
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
                "n": TEST_JWK_N,
                "e": TEST_JWK_E
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
            "n": TEST_JWK_N,
            "e": TEST_JWK_E
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
                "n": TEST_JWK_N,
                "e": TEST_JWK_E
            }]
        });
        let (addr, _handle) = spawn_jwks_server(jwks).await;
        let issuer = format!("http://{addr}");
        let token = mint_upstream_token(alg, &issuer, "aud1", &kid);
        let validator = UpstreamOidcValidator::default();
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
            "x": TEST_EC_JWK_X,
            "y": TEST_EC_JWK_Y
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
            "x": TEST_EC_JWK_X,
            "y": TEST_EC_JWK_Y
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
        Algorithm::ES256 => {
            let der = base64::engine::general_purpose::STANDARD
                .decode(TEST_EC_PRIVATE_KEY_DER_B64)
                .expect("ec der");
            jsonwebtoken::encode(&header, &claims, &EncodingKey::from_ec_der(&der))
                .expect("es256 token")
        }
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes()).expect("rsa pem"),
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

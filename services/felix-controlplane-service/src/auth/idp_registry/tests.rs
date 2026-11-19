use super::*;

fn config(issuer: &str, jwks_url: Option<&str>) -> IdpIssuerConfig {
    IdpIssuerConfig {
        issuer: issuer.to_string(),
        audiences: vec!["felix".to_string()],
        discovery_url: None,
        jwks_url: jwks_url.map(str::to_string),
        claim_mappings: ClaimMappings::default(),
        ..Default::default()
    }
}

#[test]
fn https_urls_are_accepted() {
    assert!(check_fetch_url("https://login.example.com/keys", false, false).is_ok());
    assert!(
        config("https://login.example.com", None)
            .validate(false, false)
            .is_ok()
    );
}

#[test]
fn plain_http_is_refused_off_loopback() {
    assert!(check_fetch_url("http://login.example.com/keys", false, false).is_err());
    assert!(check_fetch_url("http://169.254.169.254/latest/meta-data", false, false).is_err());
    // The derived discovery URL is checked when no JWKS URL is set.
    assert!(
        config("http://login.example.com", None)
            .validate(false, false)
            .is_err()
    );
}

#[test]
fn plain_http_is_allowed_on_loopback_or_by_opt_out() {
    assert!(check_fetch_url("http://127.0.0.1:8080/jwks", false, false).is_ok());
    assert!(check_fetch_url("http://localhost/jwks", false, false).is_ok());
    assert!(check_fetch_url("http://[::1]:9000/jwks", false, false).is_ok());
    assert!(check_fetch_url("http://idp.internal/jwks", true, false).is_ok());
}

#[test]
fn other_schemes_and_credentials_are_refused() {
    for url in [
        "file:///etc/passwd",
        "ftp://idp.example.com/jwks",
        "gopher://idp.example.com",
        "https://user:pass@idp.example.com/jwks",
        "not a url",
    ] {
        assert!(check_fetch_url(url, true, false).is_err(), "{url}");
    }
}

#[test]
fn an_issuer_with_a_fragment_is_refused() {
    assert!(
        config(
            "https://idp.example.com#ops",
            Some("https://idp.example.com/k")
        )
        .validate(false, false)
        .is_err()
    );
    assert!(
        config("  ", Some("https://idp.example.com/k"))
            .validate(false, false)
            .is_err()
    );
}

#[test]
fn private_addresses_are_refused_even_over_https() {
    for url in [
        "https://10.0.0.5/jwks",
        "https://172.16.3.4/jwks",
        "https://192.168.1.1/jwks",
        "https://169.254.169.254/latest/meta-data",
        "https://0.0.0.0/jwks",
        "https://[fd00::1]/jwks",
        "https://[fe80::1]/jwks",
        "https://[::ffff:10.0.0.5]/jwks",
    ] {
        assert!(check_fetch_url(url, false, false).is_err(), "{url}");
        assert!(check_fetch_url(url, false, true).is_ok(), "{url}");
    }
    assert!(check_fetch_url("http://10.0.0.5/jwks", true, false).is_err());
    assert!(
        config("https://idp.example.com", Some("https://192.168.0.9/k"))
            .validate(false, false)
            .is_err()
    );
}

#[test]
fn loopback_and_public_addresses_are_not_private() {
    for ip in ["127.0.0.1", "::1", "8.8.8.8", "2606:4700::1111"] {
        assert!(!is_private_address(ip.parse().unwrap()), "{ip}");
    }
    assert!(check_fetch_url("https://127.0.0.1:8443/jwks", false, false).is_ok());
}

fn jwks_with(keys: serde_json::Value) -> jsonwebtoken::jwk::JwkSet {
    serde_json::from_value(serde_json::json!({ "keys": keys })).expect("jwks")
}

fn ec_key(kid: &str) -> serde_json::Value {
    // The P-256 generator point: a valid public key.
    serde_json::json!({
        "kty": "EC", "kid": kid, "crv": "P-256",
        "x": "axfR8uEsQkf4vOblY6RA8ncDfYEt6zOg9KE5RdiYwpY",
        "y": "T-NC4v4af5uO5-tKfA-eFivOM1drMV7Oy7ZAaDe_UfU",
    })
}

#[test]
fn an_inline_jwks_is_accepted_without_a_url() {
    let mut cfg = config("https://idp.example", None);
    cfg.jwks = Some(jwks_with(serde_json::json!([ec_key("a"), ec_key("b")])));
    assert!(cfg.validate(false, false).is_ok());
}

#[test]
fn an_inline_jwks_excludes_urls() {
    let mut cfg = config("https://idp.example", Some("https://idp.example/jwks"));
    cfg.jwks = Some(jwks_with(serde_json::json!([ec_key("a")])));
    assert!(cfg.validate(false, false).is_err());
    let mut cfg = config("https://idp.example", None);
    cfg.discovery_url = Some("https://idp.example/d".to_string());
    cfg.jwks = Some(jwks_with(serde_json::json!([ec_key("a")])));
    assert!(cfg.validate(false, false).is_err());
}

#[test]
fn a_bad_inline_jwks_is_refused() {
    let bad = [
        serde_json::json!([]),
        serde_json::json!([ec_key("a"), ec_key("a")]),
        serde_json::json!([{"kty": "EC", "crv": "P-256", "x": "AA", "y": "AA"}]),
        serde_json::json!([{"kty": "oct", "kid": "h", "k": "c2VjcmV0"}]),
    ];
    for keys in bad {
        let mut cfg = config("https://idp.example", None);
        cfg.jwks = Some(jwks_with(keys.clone()));
        assert!(cfg.validate(false, false).is_err(), "{keys}");
    }
}

#[test]
fn the_lifetime_bound_must_be_sensible() {
    for bound in [0, 86_401] {
        let mut cfg = config("https://idp.example", Some("https://idp.example/jwks"));
        cfg.max_token_lifetime_seconds = Some(bound);
        assert!(cfg.validate(false, false).is_err(), "{bound}");
    }
}

#[test]
fn the_new_fields_leave_an_existing_config_unchanged() {
    let cfg = config("https://idp.example", Some("https://idp.example/jwks"));
    let value = serde_json::to_value(&cfg).expect("json");
    let object = value.as_object().expect("object");
    for field in ["jwks", "single_use_jti", "max_token_lifetime_seconds"] {
        assert!(!object.contains_key(field), "{field}");
    }
    let back: IdpIssuerConfig = serde_json::from_value(value).expect("round trip");
    assert!(back.jwks.is_none() && !back.single_use_jti);
}

#[test]
fn single_use_needs_a_short_lifetime_bound() {
    let mut cfg = config("https://idp.example", Some("https://idp.example/jwks"));
    cfg.single_use_jti = true;
    assert!(cfg.validate(false, false).is_err(), "no bound");
    cfg.max_token_lifetime_seconds = Some(601);
    assert!(cfg.validate(false, false).is_err(), "bound too long");
    cfg.max_token_lifetime_seconds = Some(600);
    assert!(cfg.validate(false, false).is_ok());
}

#[test]
fn an_encryption_key_is_refused_in_an_inline_jwks() {
    let mut cfg = config("https://idp.example", None);
    let mut key = ec_key("a");
    key["use"] = serde_json::json!("enc");
    cfg.jwks = Some(jwks_with(serde_json::json!([key])));
    assert!(cfg.validate(false, false).is_err());
    let mut key = ec_key("a");
    key["use"] = serde_json::json!("sig");
    cfg.jwks = Some(jwks_with(serde_json::json!([key])));
    assert!(cfg.validate(false, false).is_ok());
}

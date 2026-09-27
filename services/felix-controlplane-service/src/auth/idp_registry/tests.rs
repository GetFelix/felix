use super::*;

fn config(issuer: &str, jwks_url: Option<&str>) -> IdpIssuerConfig {
    IdpIssuerConfig {
        issuer: issuer.to_string(),
        audiences: vec!["felix".to_string()],
        discovery_url: None,
        jwks_url: jwks_url.map(str::to_string),
        claim_mappings: ClaimMappings::default(),
    }
}

#[test]
fn https_urls_are_accepted() {
    assert!(check_fetch_url("https://login.example.com/keys", false).is_ok());
    assert!(
        config("https://login.example.com", None)
            .validate(false)
            .is_ok()
    );
}

#[test]
fn plain_http_is_refused_off_loopback() {
    assert!(check_fetch_url("http://login.example.com/keys", false).is_err());
    assert!(check_fetch_url("http://169.254.169.254/latest/meta-data", false).is_err());
    // The derived discovery URL is checked when no JWKS URL is set.
    assert!(
        config("http://login.example.com", None)
            .validate(false)
            .is_err()
    );
}

#[test]
fn plain_http_is_allowed_on_loopback_or_by_opt_out() {
    assert!(check_fetch_url("http://127.0.0.1:8080/jwks", false).is_ok());
    assert!(check_fetch_url("http://localhost/jwks", false).is_ok());
    assert!(check_fetch_url("http://[::1]:9000/jwks", false).is_ok());
    assert!(check_fetch_url("http://idp.internal/jwks", true).is_ok());
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
        assert!(check_fetch_url(url, true).is_err(), "{url}");
    }
}

#[test]
fn an_issuer_with_a_fragment_is_refused() {
    assert!(
        config(
            "https://idp.example.com#ops",
            Some("https://idp.example.com/k")
        )
        .validate(false)
        .is_err()
    );
    assert!(
        config("  ", Some("https://idp.example.com/k"))
            .validate(false)
            .is_err()
    );
}

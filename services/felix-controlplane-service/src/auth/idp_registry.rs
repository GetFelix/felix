//! Identity provider issuer configuration models.
//!
//! Defines IdP issuer settings and claim mapping configuration used by OIDC
//! validation and bootstrap/admin endpoints, and the rules an issuer's URLs
//! are held to.
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Set to `true` to allow plain-HTTP discovery and JWKS URLs on any host.
/// For development only: a JWKS fetched over plain HTTP can be swapped in
/// transit, and whoever swaps it can mint tokens for the tenant.
pub const ALLOW_INSECURE_HTTP_ENV: &str = "FELIX_CONTROLPLANE_OIDC_ALLOW_INSECURE_HTTP";

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClaimMappings {
    pub subject_claim: String,
    pub groups_claim: Option<String>,
}

impl Default for ClaimMappings {
    fn default() -> Self {
        Self {
            subject_claim: "sub".to_string(),
            groups_claim: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct IdpIssuerConfig {
    pub issuer: String,
    pub audiences: Vec<String>,
    pub discovery_url: Option<String>,
    pub jwks_url: Option<String>,
    pub claim_mappings: ClaimMappings,
}

impl IdpIssuerConfig {
    /// The discovery document's URL: the configured one, or the OIDC default
    /// under the issuer.
    pub fn discovery_url(&self) -> String {
        self.discovery_url.clone().unwrap_or_else(|| {
            format!(
                "{}/.well-known/openid-configuration",
                self.issuer.trim_end_matches('/')
            )
        })
    }

    /// Check the config before it is stored: an issuer that can scope group
    /// names, and every URL the control plane would fetch passing
    /// [`check_fetch_url`].
    ///
    /// # Errors
    /// What is wrong, fit to return to the caller.
    pub fn validate(&self, allow_insecure_http: bool) -> Result<(), String> {
        let issuer = self.issuer.trim();
        if issuer.is_empty() {
            return Err("issuer must not be empty".to_string());
        }
        // Group principals are `group:{issuer}#{name}`, split at the first
        // `#`; an OIDC issuer has no fragment, so this costs nothing real.
        if issuer.contains('#') {
            return Err("issuer must not contain '#'".to_string());
        }
        match &self.jwks_url {
            Some(url) => check_fetch_url(url, allow_insecure_http),
            None => check_fetch_url(&self.discovery_url(), allow_insecure_http),
        }
    }
}

/// Whether plain HTTP is allowed beyond loopback, from
/// [`ALLOW_INSECURE_HTTP_ENV`].
pub fn allow_insecure_http_from_env() -> bool {
    std::env::var(ALLOW_INSECURE_HTTP_ENV)
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
}

/// Check a discovery or JWKS URL before the control plane fetches it.
///
/// HTTPS only, because the JWKS is what decides who may mint tokens for the
/// tenant. Plain HTTP is allowed on a loopback host (a local IdP in tests and
/// demos) or everywhere when `allow_insecure_http` is set. No credentials in
/// the URL: they would be sent to whatever the host is.
///
/// # Errors
/// Why the URL is refused.
pub fn check_fetch_url(raw: &str, allow_insecure_http: bool) -> Result<(), String> {
    let url = reqwest::Url::parse(raw).map_err(|err| format!("invalid IdP URL: {err}"))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err("IdP URLs must not carry credentials".to_string());
    }
    let Some(host) = url.host_str() else {
        return Err("IdP URLs must name a host".to_string());
    };
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_insecure_http || is_loopback(host) => Ok(()),
        "http" => Err(format!(
            "IdP URLs must use https (plain http is allowed only for loopback hosts, \
             or everywhere with {ALLOW_INSECURE_HTTP_ENV}=true)"
        )),
        other => Err(format!("unsupported IdP URL scheme: {other}")),
    }
}

fn is_loopback(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests;

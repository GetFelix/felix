//! The opt-in API reference page: which viewer, and where it is served.
use std::str::FromStr;

use anyhow::{Result, anyhow, bail};

/// Where the page is served unless `FELIX_CONTROLPLANE_API_UI_PATH` says otherwise.
pub const DEFAULT_API_UI_PATH: &str = "/api";

/// Which API reference page the control plane serves. Off unless asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiUi {
    Off,
    Scalar,
}

impl FromStr for ApiUi {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "scalar" => Ok(Self::Scalar),
            other => Err(anyhow!(
                "invalid FELIX_CONTROLPLANE_API_UI / api_ui '{other}'; supported: off, scalar"
            )),
        }
    }
}

/// `FELIX_CONTROLPLANE_API_UI`. An unknown value is an error: someone who
/// typed `scaler` expects a page, and "off" would hide the typo.
pub(super) fn api_ui_from_env() -> Result<ApiUi> {
    match std::env::var("FELIX_CONTROLPLANE_API_UI") {
        Ok(value) => value.parse(),
        Err(_) => Ok(ApiUi::Off),
    }
}

/// `FELIX_CONTROLPLANE_API_UI_PATH`, or the default.
pub(super) fn api_ui_path_from_env() -> String {
    std::env::var("FELIX_CONTROLPLANE_API_UI_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_API_UI_PATH.to_string())
}

/// Refuse a path the router could not serve the page on, or that would shadow
/// the API itself.
pub(super) fn validate_api_ui_path(path: &str) -> Result<()> {
    if !path.starts_with('/') || path.len() < 2 {
        bail!(
            "FELIX_CONTROLPLANE_API_UI_PATH / api_ui_path must start with '/' and name a path: '{path}'"
        );
    }
    if path.ends_with('/') || path.contains(['{', '}', '*', '?', '#', ' ']) || path.contains("//") {
        bail!(
            "FELIX_CONTROLPLANE_API_UI_PATH / api_ui_path must be a plain path without a \
             trailing slash, wildcard, query or fragment: '{path}'"
        );
    }
    if path == "/v1" || path.starts_with("/v1/") || path.starts_with("/internal/") {
        bail!("FELIX_CONTROLPLANE_API_UI_PATH / api_ui_path would shadow the API: '{path}'");
    }
    Ok(())
}

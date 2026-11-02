//! The opt-in API reference page, served from the control plane itself.
//!
//! Scalar's page is a short HTML file that loads one script from a CDN, so the
//! control plane serves that file rather than taking a dependency. The script
//! is pinned to an exact version with a Subresource Integrity hash, and the
//! Content-Security-Policy lets the page load that one script and talk only to
//! this origin.
use axum::Router;
use axum::http::{HeaderValue, header};
use axum::response::{Html, IntoResponse};

use crate::config::ApiUi;

/// To change the version, download this file from the CDN and recompute
/// `SCALAR_SRI`: `openssl dgst -sha384 -binary <file> | openssl base64 -A`.
const SCALAR_SCRIPT: &str =
    "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.75.1/dist/browser/standalone.js";
const SCALAR_SRI: &str = "sha384-G+2Q2i9l3PPfmYkk0b4BAIUrs7xb/DNaTL1wCYYab4c9dIKlCyiqzC0Qx3eq9Wf6";

/// The page. The `script` with an id is data for Scalar's loader, not code, so
/// the CSP needs no inline-script allowance. Fonts and telemetry are turned off so
/// the page contacts no third host, whatever the CSP says.
fn scalar_page() -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Felix control plane API</title>
</head>
<body>
<script id="api-reference" data-url="/v1/openapi.json" data-configuration='{{"withDefaultFonts":false,"telemetry":false}}'></script>
<script src="{SCALAR_SCRIPT}" integrity="{SCALAR_SRI}" crossorigin="anonymous"></script>
</body>
</html>
"#
    )
}

/// Scalar paints with injected styles, hence `style-src 'unsafe-inline'`;
/// scripts stay locked to the pinned URL.
fn content_security_policy() -> String {
    format!(
        "default-src 'none'; script-src {SCALAR_SCRIPT}; style-src 'unsafe-inline'; \
         connect-src 'self'; img-src 'self' data:; font-src data:; worker-src blob:; \
         base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    )
}

/// The route serving the page at `path`, or nothing when the page is off.
///
/// Public like `/v1/openapi.json`: the page holds no data, and its "Try it"
/// requests carry the user's own token.
pub fn api_ui_router(ui: ApiUi, path: &str) -> Option<Router> {
    match ui {
        ApiUi::Off => None,
        ApiUi::Scalar => {
            let page = scalar_page();
            let csp = HeaderValue::from_str(&content_security_policy())
                .expect("the policy is plain ASCII");
            Some(Router::new().route(
                path,
                axum::routing::get(move || {
                    let page = page.clone();
                    let csp = csp.clone();
                    async move {
                        (
                            [
                                (header::CONTENT_SECURITY_POLICY, csp),
                                (
                                    header::X_CONTENT_TYPE_OPTIONS,
                                    HeaderValue::from_static("nosniff"),
                                ),
                                (
                                    header::REFERRER_POLICY,
                                    HeaderValue::from_static("no-referrer"),
                                ),
                            ],
                            Html(page),
                        )
                            .into_response()
                    }
                }),
            ))
        }
    }
}

#[cfg(test)]
mod tests;

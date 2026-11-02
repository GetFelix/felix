use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;

use super::*;

async fn get(router: Router, uri: &str) -> axum::response::Response {
    router
        .oneshot(Request::get(uri).body(Body::empty()).expect("request"))
        .await
        .expect("response")
}

#[test]
fn off_serves_nothing() {
    assert!(api_ui_router(ApiUi::Off, "/api").is_none());
}

#[tokio::test]
async fn scalar_serves_a_pinned_page_with_a_locked_down_policy() {
    let router = api_ui_router(ApiUi::Scalar, "/api").expect("router");

    let response = get(router.clone(), "/api").await;
    assert_eq!(response.status(), StatusCode::OK);
    let csp = response
        .headers()
        .get(header::CONTENT_SECURITY_POLICY)
        .expect("csp")
        .to_str()
        .expect("ascii")
        .to_string();
    assert!(
        csp.contains(&format!("script-src {SCALAR_SCRIPT};")),
        "{csp}"
    );
    assert!(csp.contains("connect-src 'self'"), "{csp}");
    assert!(!csp.contains("unsafe-eval"), "{csp}");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let page = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(page.contains(r#"data-url="/v1/openapi.json""#), "{page}");
    assert!(
        page.contains(&format!(r#"src="{SCALAR_SCRIPT}""#)),
        "{page}"
    );
    assert!(
        page.contains(&format!(r#"integrity="{SCALAR_SRI}""#)),
        "{page}"
    );
    assert!(page.contains(r#"crossorigin="anonymous""#), "{page}");

    let elsewhere = get(router, "/docs").await;
    assert_eq!(elsewhere.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_page_moves_with_the_configured_path() {
    let router = api_ui_router(ApiUi::Scalar, "/reference").expect("router");
    assert_eq!(
        get(router.clone(), "/reference").await.status(),
        StatusCode::OK
    );
    assert_eq!(get(router, "/api").await.status(), StatusCode::NOT_FOUND);
}

/// The pin is only worth something if the version in the URL is the one the
/// hash was taken from.
#[test]
fn the_script_is_pinned_to_an_exact_version() {
    assert!(
        SCALAR_SCRIPT.contains("@scalar/api-reference@1."),
        "{SCALAR_SCRIPT}"
    );
    assert!(!SCALAR_SCRIPT.contains("@latest"));
    assert!(SCALAR_SRI.starts_with("sha384-"));
}

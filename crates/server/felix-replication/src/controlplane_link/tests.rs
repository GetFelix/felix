//! The HTTP link puts each call on the wire the control plane routes.
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use parking_lot::Mutex;

use super::*;

async fn serve(app: Router) -> HttpControlPlaneLink {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    // A trailing slash on the base is tolerated.
    HttpControlPlaneLink::new(reqwest::Client::new(), &format!("http://{addr}/"))
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

#[tokio::test]
async fn calls_reach_their_routes_with_the_bearer() {
    type Seen = Arc<Mutex<Vec<(String, Option<String>)>>>;
    let seen: Seen = Arc::default();
    let record = |name: &'static str| {
        let seen = Arc::clone(&seen);
        move |headers: HeaderMap| {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().push((name.to_string(), bearer(&headers)));
                axum::Json(serde_json::json!({ "ok": true }))
            }
        }
    };
    let app = Router::new()
        .route("/v1/nodes", post(record("register")).get(record("page")))
        .route("/v1/nodes/{id}", get(record("node")))
        .route("/v1/nodes/{id}/heartbeat", post(record("heartbeat")))
        .route("/v1/nodes/{id}/{action}", post(record("lifecycle")))
        .route("/v1/nodes/{id}/replica-status", post(record("status")))
        .route("/v1/shard-assignments/snapshot", get(record("snapshot")))
        .route("/v1/shard-assignments/changes", get(record("changes")));
    let link = serve(app).await;
    let body = serde_json::json!({});
    let wait = Duration::from_secs(5);

    link.register("t", &body).await.expect("register");
    link.node("t", "n1").await.expect("node");
    link.heartbeat("t", "n1", &body, wait).await.expect("hb");
    link.lifecycle("t", "n1", "drain").await.expect("drain");
    link.node_catalog_page(Some("t"), None).await.expect("page");
    link.assignment_snapshot(None, wait).await.expect("snap");
    link.assignment_changes(Some("t"), 7, wait, wait)
        .await
        .expect("changes");
    link.replica_status(Some("t"), "n1", &body)
        .await
        .expect("status");

    let names: Vec<_> = seen.lock().iter().map(|(name, _)| name.clone()).collect();
    assert_eq!(
        names,
        [
            "register",
            "node",
            "heartbeat",
            "lifecycle",
            "page",
            "snapshot",
            "changes",
            "status"
        ]
    );
    let bearers: Vec<_> = seen
        .lock()
        .iter()
        .map(|(_, bearer)| bearer.clone())
        .collect();
    assert_eq!(bearers[0].as_deref(), Some("Bearer t"));
    // No credential means no header, not an empty one.
    assert_eq!(bearers[5], None);
}

#[tokio::test]
async fn the_cursor_and_the_checkpoint_travel_as_query_parameters() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let app = Router::new()
        .route(
            "/v1/nodes",
            get({
                let seen = Arc::clone(&seen);
                move |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    seen.lock()
                        .push(format!("cursor={:?}", query.get("cursor")));
                }
            }),
        )
        .route(
            "/v1/shard-assignments/changes",
            get({
                let seen = Arc::clone(&seen);
                move |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    seen.lock().push(format!(
                        "{:?} {:?}",
                        query.get("since"),
                        query.get("wait_ms")
                    ));
                }
            }),
        );
    let link = serve(app).await;
    let wait = Duration::from_secs(20);

    link.node_catalog_page(None, Some("abc"))
        .await
        .expect("page");
    link.assignment_changes(None, 41, wait, wait)
        .await
        .expect("changes");

    assert_eq!(
        *seen.lock(),
        [r#"cursor=Some("abc")"#, r#"Some("41") Some("20000")"#]
    );
}

#[tokio::test]
async fn a_refusal_is_an_answer_and_an_unreachable_server_is_not() {
    let app = Router::new().route(
        "/v1/nodes/{id}",
        get(|| async { (axum::http::StatusCode::FORBIDDEN, "no") }),
    );
    let link = serve(app).await;
    let refused = link.node("t", "n1").await.expect("an answer");
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(refused.text(), "no");

    let nowhere = HttpControlPlaneLink::new(reqwest::Client::new(), "http://127.0.0.1:1");
    assert!(nowhere.node("t", "n1").await.is_err());
}

#[tokio::test]
async fn a_body_that_is_cut_short_does_not_hide_the_status() {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;
                // Promises 100 bytes, sends 2, then hangs up.
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\nhi")
                    .await;
            });
        }
    });
    let link = HttpControlPlaneLink::new(reqwest::Client::new(), &format!("http://{addr}"));
    let body = serde_json::json!({});

    let answer = link
        .heartbeat("t", "n1", &body, Duration::from_secs(5))
        .await
        .expect("a status-only call still has its answer");
    assert_eq!(answer.status, StatusCode::OK);
    assert!(answer.json::<serde_json::Value>().is_err());
}

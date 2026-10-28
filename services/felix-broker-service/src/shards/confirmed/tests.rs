use axum::http::StatusCode;
use axum::{Json, Router, routing::get};

use super::*;

fn key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        kind: ShardKind::Stream,
    }
}

async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), server)
}

async fn read(router: Router) -> Confirmed {
    let (base_url, server) = serve(router).await;
    let routes = ControlPlaneRoutes::new(reqwest::Client::new(), &base_url, None);
    let confirmed = routes.confirmed(&key()).await;
    server.abort();
    confirmed
}

/// The shard is named in the query, and the answer is the leader and
/// generation the control plane confirmed.
#[tokio::test]
async fn reads_the_named_shard() {
    let router = Router::new().route(
        "/v1/shard-assignments/confirmed",
        get(
            |axum::extract::Query(query): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >| async move {
                assert_eq!(query["tenant_id"], "t1");
                assert_eq!(query["stream"], "orders");
                assert_eq!(query["shard"], "3");
                assert_eq!(query["kind"], "stream");
                Json(serde_json::json!({ "assignment": {
                    "tenant_id": "t1", "namespace": "ns", "stream": "orders",
                    "shard": 3, "leader": "b2", "generation": 6, "state": "active"
                }}))
            },
        ),
    );
    assert_eq!(
        read(router).await,
        Confirmed::Assigned {
            generation: 6,
            leader: "b2".to_string()
        }
    );
}

#[tokio::test]
async fn no_assignment_is_unassigned() {
    let router = Router::new().route(
        "/v1/shard-assignments/confirmed",
        get(|| async { Json(serde_json::json!({ "assignment": null })) }),
    );
    assert_eq!(read(router).await, Confirmed::Unassigned);
}

/// A control plane without the route, or whose Raft leader predates it,
/// leaves the routing view to answer as before.
#[tokio::test]
async fn an_older_control_plane_does_not_serve_it() {
    assert_eq!(read(Router::new()).await, Confirmed::NotServed);
    let router = Router::new().route(
        "/v1/shard-assignments/confirmed",
        get(|| async { StatusCode::NOT_IMPLEMENTED }),
    );
    assert_eq!(read(router).await, Confirmed::NotServed);
}

/// A failed or unreadable answer is never read as no assignment.
#[tokio::test]
async fn a_failed_read_is_unavailable() {
    let router = Router::new().route(
        "/v1/shard-assignments/confirmed",
        get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
    );
    assert_eq!(read(router).await, Confirmed::Unavailable);
    let router = Router::new().route(
        "/v1/shard-assignments/confirmed",
        get(|| async { "not json" }),
    );
    assert_eq!(read(router).await, Confirmed::Unavailable);
    let routes = ControlPlaneRoutes::new(reqwest::Client::new(), "http://127.0.0.1:1", None);
    assert_eq!(routes.confirmed(&key()).await, Confirmed::Unavailable);
}

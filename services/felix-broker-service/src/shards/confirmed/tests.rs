use axum::http::StatusCode;
use axum::{Json, Router, routing::get};

use felix_router::ShardKind;

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
    let routes = ControlPlaneRoutes::new(
        felix_replication::controlplane_link::HttpControlPlaneLink::new(
            reqwest::Client::new(),
            &base_url,
        ),
        None,
    );
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
    let routes = ControlPlaneRoutes::new(
        felix_replication::controlplane_link::HttpControlPlaneLink::new(
            reqwest::Client::new(),
            "http://127.0.0.1:1",
        ),
        None,
    );
    assert_eq!(routes.confirmed(&key()).await, Confirmed::Unavailable);
}

/// **The read goes through the broker's control-plane link**, so an
/// in-process link (the simulator's) answers it as the HTTP one does.
#[tokio::test]
async fn the_read_goes_through_the_link() {
    let link = crate::test_support::fake_link::FakeLink::default().reply(
        "confirmed_assignment",
        serde_json::json!({ "assignment": {
            "tenant_id": "t1", "namespace": "ns", "stream": "orders",
            "shard": 3, "leader": "b2", "generation": 6, "state": "active"
        }}),
    );
    let routes = ControlPlaneRoutes::new(link.clone(), None);
    assert_eq!(
        routes.confirmed(&key()).await,
        Confirmed::Assigned {
            generation: 6,
            leader: "b2".to_string()
        }
    );
    assert_eq!(link.calls(), ["confirmed_assignment"]);
}

/// **Several shards are read in one request**, answered in order; a control
/// plane that serves only `GET` leaves each to be read alone.
#[tokio::test]
async fn several_shards_are_read_in_one_request() {
    let router = Router::new().route(
        "/v1/shard-assignments/confirmed",
        axum::routing::post(|Json(body): Json<serde_json::Value>| async move {
            assert_eq!(body["keys"][0]["shard"], 3);
            assert_eq!(body["keys"][1]["shard"], 4);
            Json(serde_json::json!({ "items": [
                { "assignment": {
                    "tenant_id": "t1", "namespace": "ns", "stream": "orders",
                    "shard": 3, "leader": "b2", "generation": 6, "state": "active"
                }},
                { "assignment": null },
            ]}))
        }),
    );
    let (base_url, server) = serve(router).await;
    let routes = ControlPlaneRoutes::new(
        felix_replication::controlplane_link::HttpControlPlaneLink::new(
            reqwest::Client::new(),
            &base_url,
        ),
        None,
    );
    let keys = [key(), ShardKey { shard: 4, ..key() }];
    assert_eq!(
        routes.confirmed_many(&keys).await,
        Some(vec![
            Confirmed::Assigned {
                generation: 6,
                leader: "b2".to_string()
            },
            Confirmed::Unassigned,
        ])
    );
    server.abort();

    let only_get = Router::new().route(
        "/v1/shard-assignments/confirmed",
        get(|| async { Json(serde_json::json!({ "assignment": null })) }),
    );
    let (base_url, server) = serve(only_get).await;
    let routes = ControlPlaneRoutes::new(
        felix_replication::controlplane_link::HttpControlPlaneLink::new(
            reqwest::Client::new(),
            &base_url,
        ),
        None,
    );
    assert_eq!(routes.confirmed_many(&keys).await, None);
    server.abort();
}

use std::time::Duration;

use axum::http::StatusCode;
use openraft::error::{RPCError, RaftError};
use openraft::raft::VoteResponse;

use super::*;

type Reply = Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

fn network(addr: &str) -> HttpNetwork {
    HttpNetwork {
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client"),
        target: 2,
        base: format!("http://{addr}"),
        heartbeat: Duration::from_millis(150),
    }
}

async fn vote(addr: &str) -> Reply {
    network(addr).send("vote", &serde_json::json!({})).await
}

#[tokio::test]
async fn a_missing_route_is_unreachable_and_names_the_status() {
    let addr = serve(axum::Router::new()).await;
    let err = vote(&addr).await.expect_err("404");
    assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
    assert!(err.to_string().contains("404"), "{err}");
}

#[tokio::test]
async fn a_withheld_vote_is_unreachable_and_keeps_the_reason() {
    let app = axum::Router::new().route(
        "/internal/raft/vote",
        axum::routing::post(|| async {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "vote withheld until caught up",
            )
        }),
    );
    let addr = serve(app).await;
    let err = vote(&addr).await.expect_err("503");
    assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
    let message = err.to_string();
    assert!(message.contains("503"), "{message}");
    assert!(message.contains("vote withheld"), "{message}");
}

#[tokio::test]
async fn a_long_error_body_is_cut_short() {
    let app = axum::Router::new().route(
        "/internal/raft/vote",
        axum::routing::post(|| async { (StatusCode::FORBIDDEN, "é".repeat(1000)) }),
    );
    let addr = serve(app).await;
    let err = vote(&addr).await.expect_err("403");
    assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
    assert!(err.to_string().len() < 600, "{err}");
}

#[tokio::test]
async fn a_refused_connection_is_unreachable() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    drop(listener);
    let err = vote(&addr).await.expect_err("refused");
    assert!(matches!(err, RPCError::Unreachable(_)), "{err:?}");
}

/// A 2xx that is not a raft reply is a fault in the peer, not an outage.
#[tokio::test]
async fn an_unparseable_success_is_a_network_error() {
    let app = axum::Router::new().route("/internal/raft/vote", axum::routing::post(|| async {}));
    let addr = serve(app).await;
    let err = vote(&addr).await.expect_err("empty body");
    assert!(matches!(err, RPCError::Network(_)), "{err:?}");
}

#[test]
fn backoff_starts_at_the_heartbeat_and_is_capped() {
    let delays: Vec<_> = backoff_delays(Duration::from_millis(150)).take(6).collect();
    assert_eq!(
        delays,
        [150, 300, 600, 1000, 1000, 1000].map(Duration::from_millis)
    );
}

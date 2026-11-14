use std::time::Duration;

use axum::http::StatusCode;
use openraft::errors::RPCError;

use super::*;

type Reply = Result<wire::VoteResponse, RPCError<TypeConfig>>;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

fn network(addr: &str) -> HttpNetwork {
    HttpNetwork::for_tests(addr, crate::raft::hold::tests::hold_at(Default::default()))
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
        &crate::raft::peer_route("vote"),
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
        &crate::raft::peer_route("vote"),
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

/// Every Raft RPC says which member it is for, so whoever is at the address
/// can refuse one that is not for it.
#[tokio::test]
async fn every_rpc_names_the_member_it_is_for() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = {
        let seen = std::sync::Arc::clone(&seen);
        move |headers: axum::http::HeaderMap| {
            let target = headers.get(crate::raft::TARGET_HEADER);
            let target = target.map(|value| value.to_str().expect("text").to_string());
            seen.lock().expect("seen").push(target);
            async { StatusCode::SERVICE_UNAVAILABLE }
        }
    };
    let app = axum::Router::new()
        .route(
            &crate::raft::peer_route("vote"),
            axum::routing::post(record.clone()),
        )
        .route(
            &crate::raft::peer_route("append-entries"),
            axum::routing::post(record.clone()),
        )
        .route(
            &crate::raft::peer_route("install-snapshot"),
            axum::routing::post(record),
        );
    let addr = serve(app).await;
    let hold = crate::raft::hold::tests::hold_at(Default::default());
    let network = HttpNetwork::for_tests_as(9, &addr, hold);

    let _: Reply = network.send("vote", &serde_json::json!({})).await;
    let _: Reply = network.send("append-entries", &serde_json::json!({})).await;
    let _: Reply = network
        .send("install-snapshot", &serde_json::json!({}))
        .await;
    let seen = seen.lock().expect("seen");
    assert_eq!(*seen, vec![Some("9".to_string()); 3]);
}

/// The streamed snapshot is built apart from `send`, so it gets its own
/// check. A request without the header is still served (older senders), so
/// nothing else would notice it going missing.
#[tokio::test]
async fn a_streamed_snapshot_names_the_member_it_is_for() {
    use crate::raft::types::{Snapshot, SnapshotData, SnapshotMeta, StoredMembership};

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = {
        let seen = std::sync::Arc::clone(&seen);
        move |headers: axum::http::HeaderMap, _body: axum::body::Bytes| {
            let target = headers.get(crate::raft::TARGET_HEADER);
            let target = target.map(|value| value.to_str().expect("text").to_string());
            seen.lock().expect("seen").push(target);
            async { StatusCode::SERVICE_UNAVAILABLE }
        }
    };
    let app = axum::Router::new().route(
        &crate::raft::peer_route("install-snapshot"),
        axum::routing::post(record),
    );
    let addr = serve(app).await;
    let hold = crate::raft::hold::tests::hold_at(Default::default());
    let mut network = HttpNetwork::for_tests_as(9, &addr, hold);

    let snapshot = Snapshot {
        meta: SnapshotMeta {
            last_log_id: Some(crate::raft::types::test_log_id(1, 7)),
            last_membership: StoredMembership::default(),
        },
        snapshot: SnapshotData::Memory(bytes::Bytes::from_static(b"state at seven")),
    };
    let sent = network
        .full_snapshot(
            Vote::new_committed(1, 1),
            snapshot,
            std::future::pending(),
            openraft::network::RPCOption::new(Duration::from_secs(5)),
        )
        .await;
    assert!(
        matches!(sent, Err(StreamingError::Unreachable(_))),
        "{sent:?}"
    );
    assert_eq!(*seen.lock().expect("seen"), vec![Some("9".to_string())]);
}

/// A 2xx that is not a raft reply is a fault in the peer, not an outage.
#[tokio::test]
async fn an_unparseable_success_is_a_network_error() {
    let app = axum::Router::new().route(
        &crate::raft::peer_route("vote"),
        axum::routing::post(|| async {}),
    );
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

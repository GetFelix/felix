//! The handoff against a stub control plane: when it asks for a drain, and
//! how the wait for its shards to leave ends.
use std::sync::Mutex;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use serde_json::json;

use super::*;
use crate::shards::ShardKey;
use crate::shards::watch::ShardAssignment;
use crate::test_support::{build_test_client, spawn_axum_with_shutdown, wait_for_listen};

const ME: &str = "broker-a";

fn assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            shard,
            kind: Default::default(),
        },
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
    }
}

fn leading(shards: u32) -> Arc<RwLock<ShardOwnership>> {
    let mut ownership = ShardOwnership::default();
    ownership.reset((0..shards).map(|shard| assignment(shard, ME, 1)).collect());
    Arc::new(RwLock::new(ownership))
}

/// Answers the node listing with `eligible` and records every drain.
async fn stub(eligible: &'static [&'static str]) -> (String, Arc<Mutex<Vec<String>>>) {
    let drained: Arc<Mutex<Vec<String>>> = Arc::default();
    let router = axum::Router::new()
        .route(
            "/v1/nodes",
            get(move || async move {
                let items: Vec<_> = [ME, "broker-b"]
                    .iter()
                    .map(|id| {
                        json!({
                            "node": {
                                "node_id": id,
                                "spec": { "advertise_addr": "127.0.0.1:1", "region": "r" },
                            },
                            "placement": { "eligible": eligible.contains(id) },
                        })
                    })
                    .collect();
                axum::Json(json!({ "items": items }))
            }),
        )
        .route(
            "/v1/nodes/{node_id}/drain",
            post(
                |State(drained): State<Arc<Mutex<Vec<String>>>>, Path(id): Path<String>| async move {
                    drained.lock().expect("lock").push(id);
                    axum::Json(json!({}))
                },
            ),
        )
        .with_state(Arc::clone(&drained));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown, _task) = spawn_axum_with_shutdown(listener, router);
    // Kept serving for the rest of the test.
    std::mem::forget(shutdown);
    wait_for_listen(addr).await.expect("listen");
    (format!("http://{addr}"), drained)
}

fn handoff(base_url: String, ownership: Arc<RwLock<ShardOwnership>>, timeout: Duration) -> Handoff {
    Handoff {
        client: build_test_client().expect("client"),
        base_url,
        node_id: ME.to_string(),
        credential: None,
        ownership,
        timeout,
    }
}

fn never() -> std::future::Pending<()> {
    std::future::pending()
}

#[tokio::test]
async fn a_broker_leading_nothing_asks_for_nothing() {
    let (url, drained) = stub(&[ME, "broker-b"]).await;
    let outcome = handoff(url, leading(0), Duration::from_secs(5))
        .run(never())
        .await;
    assert_eq!(outcome, Outcome::Skipped("leads no shards"));
    assert!(drained.lock().expect("lock").is_empty());
}

/// Draining the only broker would hold its shards for a destination that
/// never comes.
#[tokio::test]
async fn a_broker_with_nobody_to_hand_to_does_not_drain() {
    let (url, drained) = stub(&[ME]).await;
    let outcome = handoff(url, leading(1), Duration::from_secs(5))
        .run(never())
        .await;
    assert_eq!(
        outcome,
        Outcome::Skipped("no other broker can take its shards")
    );
    assert!(drained.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn an_unreachable_control_plane_is_skipped_quickly() {
    // Bound and closed again, so nothing answers on it.
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr");
    let started = Instant::now();
    let outcome = handoff(
        format!("http://{addr}"),
        leading(1),
        Duration::from_secs(30),
    )
    .run(never())
    .await;
    assert_eq!(outcome, Outcome::Skipped("control plane unreachable"));
    assert!(started.elapsed() < CALL_TIMEOUT + Duration::from_secs(1));
}

#[tokio::test]
async fn the_handoff_completes_once_every_shard_is_led_elsewhere() {
    let (url, drained) = stub(&[ME, "broker-b"]).await;
    let ownership = leading(2);
    let watch = Arc::clone(&ownership);
    tokio::spawn(async move {
        for shard in 0..2 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let moved = assignment(shard, "broker-b", 2);
            watch.write().await.apply(&moved.key.clone(), Some(moved));
        }
    });
    let outcome = handoff(url, ownership, Duration::from_secs(10))
        .run(never())
        .await;
    assert_eq!(outcome, Outcome::Completed { handed_off: 2 });
    assert_eq!(*drained.lock().expect("lock"), vec![ME.to_string()]);
}

#[tokio::test]
async fn the_handoff_gives_up_at_its_timeout() {
    let (url, _) = stub(&[ME, "broker-b"]).await;
    let started = Instant::now();
    let outcome = handoff(url, leading(1), Duration::from_millis(300))
        .run(never())
        .await;
    assert_eq!(
        outcome,
        Outcome::TimedOut {
            handed_off: 0,
            remaining: 1
        }
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn a_second_signal_ends_the_wait() {
    let (url, _) = stub(&[ME, "broker-b"]).await;
    let outcome = handoff(url, leading(1), Duration::from_secs(30))
        .run(tokio::time::sleep(Duration::from_millis(200)))
        .await;
    assert!(outcome.interrupted(), "{outcome:?}");
}

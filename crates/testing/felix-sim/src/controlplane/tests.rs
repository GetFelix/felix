use std::cell::Cell;
use std::future::Future;

use felix_controlplane_service::api::types::{NodeHeartbeatRequest, NodeRegistrationResponse};
use tokio::time::Instant;

use super::*;

thread_local! {
    static START: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Virtual time since this test's runtime started.
fn elapsed() -> Duration {
    START.with(|start| start.get().map_or(Duration::ZERO, |at| at.elapsed()))
}

async fn control_plane() -> SimControlPlane {
    START.with(|start| start.set(Some(Instant::now())));
    felix_common::clock::sim::install(elapsed);
    SimControlPlane::new(CpConfig::default()).await.unwrap()
}

async fn register(link: &SimLink, node: &str) -> NodeRegistrationResponse {
    let body = registration(node, "10.0.0.1:7100");
    let response = link.register("t", &body).await.unwrap();
    assert!(response.status.is_success(), "{}", response.text());
    response.json().unwrap()
}

fn heartbeat_body(incarnation: u64) -> NodeHeartbeatRequest {
    NodeHeartbeatRequest {
        incarnation,
        suspects: Default::default(),
    }
}

/// Polls `future` once and reports whether it finished.
async fn finished_now<T>(future: impl Future<Output = T>) -> bool {
    tokio::time::timeout(Duration::from_millis(1), future)
        .await
        .is_ok()
}

#[tokio::test(start_paused = true)]
async fn register_and_heartbeat_round_trip() {
    let cp = control_plane().await;
    let link = cp.link();
    let registered = register(&link, "b1").await;
    assert_eq!(registered.node.node_id, "b1");

    let response = link
        .heartbeat(
            "t",
            "b1",
            &heartbeat_body(registered.node.status.incarnation),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(response.status.is_success(), "{}", response.text());
    let node = link.node("t", "b1").await.unwrap();
    assert!(node.status.is_success());
    let catalog = link.node_catalog_page(Some("t"), None).await.unwrap();
    assert!(catalog.text().contains("b1"));
}

#[tokio::test(start_paused = true)]
async fn slow_delays_each_call_by_the_set_amount() {
    let cp = control_plane().await;
    let link = cp.link();
    cp.set_fault(CpFault::Slow(Duration::from_millis(700)));
    let begun = Instant::now();
    let response = link.node("t", "nobody").await.unwrap();
    assert_eq!(response.status, axum::http::StatusCode::NOT_FOUND);
    assert_eq!(begun.elapsed(), Duration::from_millis(700));
}

#[tokio::test(start_paused = true)]
async fn paused_parks_calls_until_cleared_and_a_timeout_ends_one_early() {
    let cp = control_plane().await;
    let link = cp.link();
    register(&link, "b1").await;
    cp.set_fault(CpFault::Paused);

    let parked = tokio::spawn({
        let link = link.clone();
        async move { link.node("t", "b1").await }
    });
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert!(!parked.is_finished());

    let begun = Instant::now();
    let err = link
        .heartbeat("t", "b1", &heartbeat_body(1), Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("timed out"), "{err}");
    assert_eq!(begun.elapsed(), Duration::from_secs(2));

    cp.set_fault(CpFault::Healthy);
    let late = parked.await.unwrap().unwrap();
    assert!(late.status.is_success());
}

#[tokio::test(start_paused = true)]
async fn down_fails_at_once_and_fails_parked_calls() {
    let cp = control_plane().await;
    let link = cp.link();
    cp.set_fault(CpFault::Paused);
    let parked = tokio::spawn({
        let link = link.clone();
        async move { link.node("t", "b1").await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;

    cp.set_fault(CpFault::Down);
    let begun = Instant::now();
    let err = link.node("t", "b1").await.unwrap_err();
    assert!(err.to_string().contains("refused"), "{err}");
    assert_eq!(begun.elapsed(), Duration::ZERO);
    assert!(parked.await.unwrap().is_err());
    assert!(cp.request(Method::GET, "/v1/nodes", None).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn step_does_nothing_while_paused_or_down() {
    let cp = control_plane().await;
    assert!(cp.step().await.is_some());
    cp.set_fault(CpFault::Paused);
    assert!(cp.step().await.is_none());
    cp.set_fault(CpFault::Down);
    assert!(cp.step().await.is_none());
    cp.set_fault(CpFault::Healthy);
    assert!(cp.step().await.is_some());
    assert!(finished_now(cp.step()).await);
}

#[tokio::test(start_paused = true)]
async fn silent_node_is_expired_by_stepping_after_the_window() {
    let cp = control_plane().await;
    let link = cp.link();
    register(&link, "b1").await;
    let sweep = cp.config().sweep_interval();
    let window = cp.config().silence_before_down();
    let mut waited = Duration::ZERO;
    while waited <= window * 3 {
        tokio::time::sleep(sweep).await;
        waited += sweep;
        cp.step().await.unwrap();
    }
    let node = link.node("t", "b1").await.unwrap().text();
    assert!(node.contains("Down") || node.contains("down"), "{node}");
}

#[tokio::test(start_paused = true)]
async fn three_brokers_get_replica_sets_and_the_election_features() {
    let cp = control_plane().await;
    let link = cp.link();
    for (i, node) in ["b1", "b2", "b3"].into_iter().enumerate() {
        let body = registration(node, &format!("10.0.0.{}:7100", i + 1));
        assert!(link.register("t", &body).await.unwrap().status.is_success());
    }
    cp.create_tenant("t1").await.unwrap();
    cp.create_namespace("t1", "ns").await.unwrap();
    let stream = cp
        .create_durable_stream("t1", "ns", "orders", 4, 3)
        .await
        .unwrap();
    assert_eq!(stream.shards, 4);

    cp.step().await.unwrap();
    let snapshot = cp.assignments().await.unwrap();
    assert_eq!(snapshot.items.len(), 4);
    for item in &snapshot.items {
        let shard = &item.assigned.assignment;
        assert_eq!(shard.replicas.len(), 2, "leader plus two followers");
        assert!(!shard.replicas.contains(&shard.leader));
    }

    cp.finalize_election().await.unwrap();
    let features = cp.fleet_features().await.unwrap();
    assert_eq!(features.serving_nodes, 3);
    for feature in [fleet::JOINT_FENCE, fleet::REPLICA_ELECTION] {
        assert!(features.enabled.contains(feature.name()), "{feature}");
    }
}

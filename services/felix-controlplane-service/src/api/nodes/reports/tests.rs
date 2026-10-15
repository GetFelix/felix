//! Which reports wake placement, and that a woken pass acts on them.
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::advances_move;
use crate::model::{ShardAssignment, ShardState};
use crate::store::ControlPlaneStore;
use crate::test_support::{app_state_ready, one_shard_cluster, shard_zero, token};

fn assignment(state: ShardState, successor: Option<&str>) -> ShardAssignment {
    ShardAssignment {
        key: shard_zero(),
        leader: "broker-x".to_string(),
        replicas: vec!["broker-y".to_string()],
        generation: 4,
        state,
        successor: successor.map(str::to_string),
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
    }
}

/// A report from the leader at `generation`: who is level, and, when it
/// says its tail, how far behind broker-y is.
fn status(
    generation: u64,
    level: &[&str],
    drained: bool,
    y_behind: Option<u64>,
) -> felix_common::membership::ShardReplicaStatus {
    felix_common::membership::ShardReplicaStatus {
        replica_counter_offsets: Vec::new(),
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: Default::default(),
        generation,
        caught_up: level.iter().map(|n| n.to_string()).collect(),
        replica_offsets: y_behind
            .map(|_| felix_common::membership::ReplicaOffset {
                node_id: "broker-y".to_string(),
                durable_offset: 100,
            })
            .into_iter()
            .collect(),
        drained,
        leader_offset: y_behind.map(|behind| 100 + behind),
        halted: Default::default(),
        stream_incarnation: 0,
        by_round: false,
        replica_storage_ids: Default::default(),
    }
}

#[test]
fn only_the_report_a_move_waits_for_wakes_placement() {
    let y = ["broker-y"];
    let staged = assignment(ShardState::Active, Some("broker-y"));
    let fenced = assignment(ShardState::Draining, Some("broker-y"));
    let settled = assignment(ShardState::Active, None);
    let mut joining = assignment(ShardState::Active, None);
    joining.joining = Some("broker-y".to_string());
    let lag = 10;

    assert!(
        advances_move(&staged, &status(4, &y, false, None), lag),
        "successor caught up"
    );
    assert!(
        advances_move(&staged, &status(4, &[], false, Some(lag)), lag),
        "successor within the lag bound"
    );
    assert!(
        advances_move(&joining, &status(4, &[], false, Some(lag)), lag),
        "replacement within the lag bound"
    );
    assert!(
        advances_move(&fenced, &status(4, &y, true, None), lag),
        "leader drained"
    );

    let mut destination_halted = status(4, &[], false, None);
    destination_halted
        .halted
        .push(felix_common::membership::HaltedReplicaStatus {
            node_id: "broker-y".to_string(),
            reason: "diverged".to_string(),
        });
    assert!(
        advances_move(&staged, &destination_halted, lag),
        "successor halted"
    );
    assert!(
        advances_move(&fenced, &destination_halted, lag),
        "fenced toward a successor that halted"
    );

    assert!(
        !advances_move(&staged, &status(4, &[], false, None), lag),
        "still catching up"
    );
    assert!(
        !advances_move(&staged, &status(4, &[], false, Some(lag + 1)), lag),
        "outside the lag bound"
    );
    assert!(
        !advances_move(&fenced, &status(4, &y, false, None), lag),
        "fenced, not drained"
    );
    assert!(
        !advances_move(&settled, &status(4, &y, false, None), lag),
        "no move in progress"
    );
    assert!(
        !advances_move(&fenced, &status(3, &y, true, None), lag),
        "drained at an old generation"
    );
}

/// End to end on one instance: the leader reports drained over the API and
/// placement cuts over without waiting out its interval.
#[tokio::test]
async fn a_drained_report_cuts_over_without_waiting_for_the_interval() {
    let (store, keys) = one_shard_cluster().await;
    let fenced = store
        .put_shard_assignment(ShardAssignment {
            key: shard_zero(),
            leader: "broker-x".to_string(),
            replicas: vec!["broker-y".to_string()],
            generation: 0,
            state: ShardState::Draining,
            successor: Some("broker-y".to_string()),
            joining: None,
            move_started_at_millis: None,
            move_reason: None,
            incarnation: 0,
        })
        .await
        .expect("fenced move");

    let state = app_state_ready(Arc::clone(&store) as _);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let reconciler = crate::cluster::placement::spawn_reconciler(
        Arc::clone(&store) as _,
        Default::default(),
        Default::default(),
        Duration::from_secs(3600),
        "test-holder".to_string(),
        crate::raft::LeadershipGate::Always,
        Arc::clone(&state.placement_wakes),
        shutdown.clone(),
    );
    // Past the interval's immediate first tick.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let report = serde_json::json!({
        "incarnation": 0,
        "shards": [{
            "tenant_id": "t1",
            "namespace": "ns",
            "stream": "orders",
            "shard": 0,
            "generation": fenced.generation,
            "caught_up": ["broker-y"],
            "replica_offsets": [],
            "drained": true,
        }],
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/nodes/broker-x/replica-status")
        .header(
            "authorization",
            format!("Bearer {}", token(&keys, &["node.manage:cluster:*"])),
        )
        .header("content-type", "application/json")
        .body(Body::from(report.to_string()))
        .expect("request");
    let response = crate::api::build_router(state)
        .oneshot(request)
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    tokio::time::timeout(Duration::from_secs(5), async {
        while store
            .get_shard_assignment(&shard_zero())
            .await
            .expect("get")
            .leader
            != "broker-y"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the report woke placement, which cut over");

    shutdown.cancel();
    reconciler.await.expect("reconciler");
}

/// A report posted as `broker-x`, and the status and body it got back.
async fn post_report(
    state: crate::api::AppState,
    keys: &crate::auth::felix_token::TenantSigningKeys,
    shards: serde_json::Value,
) -> (StatusCode, felix_common::membership::ReplicaStatusResponse) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/nodes/broker-x/replica-status")
        .header(
            "authorization",
            format!("Bearer {}", token(keys, &["node.manage:cluster:*"])),
        )
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "incarnation": 0, "shards": shards }).to_string(),
        ))
        .expect("request");
    let response = crate::api::build_router(state)
        .oneshot(request)
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&body).expect("a per-shard answer"),
    )
}

fn shard_json(
    shard: u32,
    generation: u64,
    leader_offset: u64,
    level: &[&str],
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": "orders",
        "shard": shard,
        "generation": generation,
        "caught_up": level,
        "replica_offsets": [],
        "leader_offset": leader_offset,
    })
}

async fn led_by_x(store: &crate::store::memory::InMemoryStore, generation_bumps: u32) -> u64 {
    let mut assignment = ShardAssignment {
        key: shard_zero(),
        leader: "broker-x".to_string(),
        replicas: vec!["broker-y".to_string()],
        generation: 0,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
    };
    for _ in 0..=generation_bumps {
        assignment = store
            .put_shard_assignment(assignment)
            .await
            .expect("assign");
    }
    assignment.generation
}

fn outcomes(
    response: &felix_common::membership::ReplicaStatusResponse,
) -> Vec<felix_common::membership::ReportOutcome> {
    response.shards.iter().map(|shard| shard.outcome).collect()
}

/// **A report the control plane did not store is not answered as stored.**
/// The leader moves its quorum mark on this answer, so a discarded report that
/// read as success let a deposed leader release `Quorum` acks.
#[tokio::test]
async fn every_shard_gets_its_own_outcome_and_any_refusal_is_a_conflict() {
    use felix_common::membership::ReportOutcome::{Accepted, NotLeader, Unassigned};

    let (store, keys) = one_shard_cluster().await;
    let generation = led_by_x(&store, 0).await;
    let state = app_state_ready(Arc::clone(&store) as _);

    let (status, all_in) = post_report(
        state.clone(),
        &keys,
        serde_json::json!([shard_json(0, generation, 10, &["broker-y"])]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(outcomes(&all_in), vec![Accepted]);

    // Shard 7 is nobody's; in the same batch shard 0 is still accepted.
    let (status, mixed) = post_report(
        state.clone(),
        &keys,
        serde_json::json!([
            shard_json(0, generation, 11, &["broker-y"]),
            shard_json(7, generation, 11, &["broker-y"]),
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(outcomes(&mixed), vec![Accepted, Unassigned]);
    assert_eq!(mixed.shards[1].shard, 7, "answers are in request order");

    store
        .put_shard_assignment(ShardAssignment {
            leader: "broker-y".to_string(),
            replicas: vec!["broker-x".to_string()],
            ..store
                .get_shard_assignment(&shard_zero())
                .await
                .expect("get")
        })
        .await
        .expect("moved");
    let (status, deposed) = post_report(
        state,
        &keys,
        serde_json::json!([shard_json(0, generation, 12, &["broker-y"])]),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(outcomes(&deposed), vec![NotLeader]);
}

/// A leader still at an older generation than the assignment, and a report
/// behind the one already held, are both stale: neither is stored, and the
/// leader is told so rather than handed a success.
#[tokio::test]
async fn stale_reports_are_refused_and_do_not_overwrite_a_newer_one() {
    use felix_common::membership::ReportOutcome::{Accepted, Stale};

    let (store, keys) = one_shard_cluster().await;
    let generation = led_by_x(&store, 1).await;
    let state = app_state_ready(Arc::clone(&store) as _);

    let (status, behind) = post_report(
        state.clone(),
        &keys,
        serde_json::json!([shard_json(0, generation - 1, 50, &["broker-y"])]),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(outcomes(&behind), vec![Stale]);
    assert!(
        store.list_replica_reports().await.expect("list").is_empty(),
        "an older generation's report was stored"
    );

    let (_, newer) = post_report(
        state.clone(),
        &keys,
        serde_json::json!([shard_json(0, generation, 20, &[])]),
    )
    .await;
    assert_eq!(outcomes(&newer), vec![Accepted]);
    // A slow request from earlier in the same generation, when broker-y was
    // still level: it must not put that view back.
    let (status, late) = post_report(
        state,
        &keys,
        serde_json::json!([shard_json(0, generation, 15, &["broker-y"])]),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(outcomes(&late), vec![Stale]);
    let held = store.list_replica_reports().await.expect("list");
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].leader_offset, Some(20));
    assert!(
        held[0].caught_up.is_empty(),
        "the late report overwrote the newer one"
    );
}

/// A report measured on another incarnation of the stream than the
/// assignment's is refused. After a restore a recreated stream can be at the
/// deleted one's generation, and a broker that has not seen the recreate yet
/// reports the old log, whose longer tail would refuse every real report.
#[tokio::test]
async fn a_report_on_another_incarnation_is_refused() {
    let (store, keys) = one_shard_cluster().await;
    let assigned = store
        .put_shard_assignment(assignment(ShardState::Active, None))
        .await
        .expect("assign");
    let state = app_state_ready(Arc::clone(&store) as _);
    let send = |stream_incarnation: u64, expected: StatusCode| {
        let state = state.clone();
        let token = token(&keys, &["node.manage:cluster:*"]);
        async move {
            let report = serde_json::json!({
                "incarnation": 0,
                "shards": [{
                    "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
                    "generation": assigned.generation, "caught_up": ["broker-y"],
                    "replica_offsets": [], "leader_offset": 100,
                    "stream_incarnation": stream_incarnation,
                }],
            });
            let request = Request::builder()
                .method("POST")
                .uri("/v1/nodes/broker-x/replica-status")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(report.to_string()))
                .expect("request");
            let response = crate::api::build_router(state)
                .oneshot(request)
                .await
                .expect("response");
            assert_eq!(response.status(), expected);
        }
    };
    send(5, StatusCode::CONFLICT).await;
    assert!(
        store
            .list_replica_reports()
            .await
            .expect("reports")
            .is_empty(),
        "a report on another incarnation's log was stored"
    );
    send(assigned.incarnation, StatusCode::OK).await;
    assert_eq!(
        store.list_replica_reports().await.expect("reports").len(),
        1
    );
}

#[tokio::test]
async fn a_report_without_an_incarnation_is_stale_against_an_incarnated_assignment() {
    let (store, keys) = one_shard_cluster().await;
    // The store owns an assignment's incarnation: give the stream one by
    // finalizing the feature and recreating it.
    let feature = felix_common::fleet::STREAM_INCARNATIONS.name();
    for (i, id) in ["broker-x", "broker-y"].into_iter().enumerate() {
        store
            .register_node(crate::store::contract::nodes::with_features(
                crate::store::contract::nodes::node(id, 7500 + i as u16),
                &[feature],
            ))
            .await
            .expect("node");
    }
    store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");
    let stream = store
        .get_stream(&crate::model::StreamKey {
            tenant_id: "t1".into(),
            namespace: "ns".into(),
            stream: "orders".into(),
        })
        .await
        .expect("stream");
    store
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "t1".into(),
            namespace: "ns".into(),
            stream: "orders".into(),
        })
        .await
        .expect("delete");
    store.create_stream(stream).await.expect("recreate");
    let assigned = store
        .put_shard_assignment(assignment(ShardState::Active, None))
        .await
        .expect("assign");
    assert_ne!(assigned.incarnation, 0);
    let state = app_state_ready(Arc::clone(&store) as _);
    // An old broker: no `stream_incarnation` key at all.
    let report = serde_json::json!({
        "incarnation": 0,
        "shards": [{
            "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
            "generation": assigned.generation, "caught_up": ["broker-y"],
            "replica_offsets": [], "leader_offset": 100,
        }],
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/nodes/broker-x/replica-status")
        .header(
            "authorization",
            format!("Bearer {}", token(&keys, &["node.manage:cluster:*"])),
        )
        .header("content-type", "application/json")
        .body(Body::from(report.to_string()))
        .expect("request");
    let response = crate::api::build_router(state)
        .oneshot(request)
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(
        store
            .list_replica_reports()
            .await
            .expect("reports")
            .is_empty()
    );
}

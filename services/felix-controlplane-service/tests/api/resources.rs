//! Batch creates: a namespace's streams and caches, all or none.
use axum::body::Body;
use axum::http::{Request, StatusCode};
use felix_controlplane_service::model::{CacheKey, StreamKey};
use felix_controlplane_service::store::ControlPlaneStore;
use tower::ServiceExt;

use crate::common::{json_request_as, read_json};
use crate::resource_auth::{Harness, harness, stream_body};

const BATCH: &str = "/v1/tenants/t1/namespaces/payments/resources";

fn cache_body(name: &str) -> serde_json::Value {
    serde_json::json!({ "cache": name, "display_name": name })
}

fn batch(streams: &[serde_json::Value], caches: &[serde_json::Value]) -> serde_json::Value {
    serde_json::json!({ "streams": streams, "caches": caches })
}

async fn send(h: &Harness, request: Request<Body>) -> (StatusCode, serde_json::Value) {
    let response = h.app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    (status, read_json(response).await)
}

fn admin(h: &Harness) -> String {
    h.credentials.tenant_admin("t1")
}

async fn has_stream(h: &Harness, name: &str) -> bool {
    h.store
        .get_stream(&StreamKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            stream: name.to_string(),
        })
        .await
        .is_ok()
}

async fn has_cache(h: &Harness, name: &str) -> bool {
    h.store
        .get_cache(&CacheKey {
            tenant_id: "t1".to_string(),
            namespace: "payments".to_string(),
            cache: name.to_string(),
        })
        .await
        .is_ok()
}

fn statuses(body: &serde_json::Value, kind: &str) -> Vec<String> {
    body[kind]
        .as_array()
        .expect("items")
        .iter()
        .map(|item| item["status"].as_str().expect("status").to_string())
        .collect()
}

#[tokio::test]
async fn a_batch_creates_everything_and_a_retry_changes_nothing() {
    let h = harness().await;
    let body = batch(
        &[stream_body("chat"), stream_body("presence")],
        &[cache_body("cursors")],
    );

    let (status, created) =
        send(&h, json_request_as("POST", BATCH, &admin(&h), body.clone())).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(statuses(&created, "streams"), ["created", "created"]);
    assert_eq!(statuses(&created, "caches"), ["created"]);
    assert_eq!(created["streams"][1]["stream"]["stream"], "presence");
    assert!(has_stream(&h, "chat").await && has_stream(&h, "presence").await);
    assert!(has_cache(&h, "cursors").await);

    let (status, again) = send(&h, json_request_as("POST", BATCH, &admin(&h), body)).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(statuses(&again, "streams"), ["unchanged", "unchanged"]);
    assert_eq!(statuses(&again, "caches"), ["unchanged"]);

    // A caller that died part-way: one item already there as asked.
    let (status, resumed) = send(
        &h,
        json_request_as(
            "POST",
            BATCH,
            &admin(&h),
            batch(&[stream_body("chat"), stream_body("typing")], &[]),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{resumed}");
    assert_eq!(statuses(&resumed, "streams"), ["unchanged", "created"]);
}

#[tokio::test]
async fn an_item_that_exists_differently_creates_nothing() {
    let h = harness().await;
    let mut chat = stream_body("chat");
    let (status, _) = send(
        &h,
        json_request_as(
            "POST",
            "/v1/tenants/t1/namespaces/payments/streams",
            &admin(&h),
            chat.clone(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    chat["shards"] = 4.into();
    let (status, body) = send(
        &h,
        json_request_as(
            "POST",
            BATCH,
            &admin(&h),
            batch(&[stream_body("presence"), chat], &[cache_body("cursors")]),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("stream chat"),
        "{body}"
    );
    assert!(!has_stream(&h, "presence").await);
    assert!(!has_cache(&h, "cursors").await);
}

#[tokio::test]
async fn an_invalid_item_creates_nothing() {
    let h = harness().await;
    let mut no_bound = stream_body("logs");
    no_bound["retention"]["max_age_seconds"] = 0.into();
    let cases = [
        batch(
            &[stream_body("chat"), stream_body("Not An Identifier")],
            &[],
        ),
        batch(&[stream_body("chat"), no_bound], &[]),
        batch(
            &[stream_body("chat")],
            &[cache_body("ok"), cache_body("bad name")],
        ),
        batch(&[stream_body("chat"), stream_body("chat")], &[]),
        batch(
            &[stream_body("chat")],
            &[cache_body("dup"), cache_body("dup")],
        ),
        batch(&[], &[]),
    ];
    for case in cases {
        let (status, body) = send(&h, json_request_as("POST", BATCH, &admin(&h), case)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    assert!(!has_stream(&h, "chat").await);
    assert!(!has_cache(&h, "ok").await);
    assert!(!has_cache(&h, "dup").await);

    let too_many: Vec<_> = (0..257).map(|i| stream_body(&format!("s{i}"))).collect();
    let (status, _) = send(
        &h,
        json_request_as("POST", BATCH, &admin(&h), batch(&too_many, &[])),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!has_stream(&h, "s0").await);
}

#[tokio::test]
async fn a_missing_permission_creates_nothing() {
    let h = harness().await;
    let narrow = h.credentials.token(
        "t1",
        &[
            "stream.manage:stream:t1/payments/*",
            "cache.manage:cache:t1/payments/cursors",
        ],
    );
    let (status, body) = send(
        &h,
        json_request_as(
            "POST",
            BATCH,
            &narrow,
            batch(
                &[stream_body("chat")],
                &[cache_body("cursors"), cache_body("board")],
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(!has_stream(&h, "chat").await);
    assert!(!has_cache(&h, "cursors").await);

    let streams_only = h
        .credentials
        .token("t1", &["stream.manage:stream:t1/payments/*"]);
    let (status, _) = send(
        &h,
        json_request_as(
            "POST",
            BATCH,
            &streams_only,
            batch(&[stream_body("chat")], &[cache_body("cursors")]),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(!has_stream(&h, "chat").await);

    // Permission is checked before existence, so a refused caller learns
    // nothing about what is there.
    let other_namespace = h
        .credentials
        .token("t1", &["stream.manage:stream:t1/billing/*"]);
    let (status, _) = send(
        &h,
        json_request_as(
            "POST",
            "/v1/tenants/t1/namespaces/missing/resources",
            &other_namespace,
            batch(&[stream_body("chat")], &[]),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_missing_namespace_is_not_found() {
    let h = harness().await;
    let (status, _) = send(
        &h,
        json_request_as(
            "POST",
            "/v1/tenants/t1/namespaces/missing/resources",
            &admin(&h),
            batch(&[stream_body("chat")], &[cache_body("cursors")]),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// A batch racing a single create of one of its names with a different
/// configuration: one of them wins, and the batch is never left partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_create_never_leaves_a_partial_batch() {
    let h = harness().await;
    let token = admin(&h);
    for round in 0..100 {
        let names = [
            format!("r{round}a"),
            format!("r{round}b"),
            format!("r{round}c"),
        ];
        let body = batch(
            &names.iter().map(|n| stream_body(n)).collect::<Vec<_>>(),
            &[cache_body(&names[0])],
        );
        let mut contested = stream_body(&names[1]);
        contested["shards"] = 3.into();

        let batch_call = h
            .app
            .clone()
            .oneshot(json_request_as("POST", BATCH, &token, body));
        let single_call = h.app.clone().oneshot(json_request_as(
            "POST",
            "/v1/tenants/t1/namespaces/payments/streams",
            &token,
            contested,
        ));
        let (batched, single) = tokio::join!(tokio::spawn(batch_call), tokio::spawn(single_call));
        let batched = batched.unwrap().unwrap().status();
        let single = single.unwrap().unwrap().status();

        let rest = has_stream(&h, &names[0]).await
            && has_stream(&h, &names[2]).await
            && has_cache(&h, &names[0]).await;
        let none = !has_stream(&h, &names[0]).await
            && !has_stream(&h, &names[2]).await
            && !has_cache(&h, &names[0]).await;
        match (batched, single) {
            (StatusCode::CREATED, StatusCode::CONFLICT) => {
                assert!(rest, "round {round}: the batch won but is partial")
            }
            (StatusCode::CONFLICT, StatusCode::CREATED) => {
                assert!(none, "round {round}: the batch lost but left items")
            }
            other => panic!("round {round}: exactly one create must win, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_single_create_repeated_as_asked_is_not_a_conflict() {
    let h = harness().await;
    let streams = "/v1/tenants/t1/namespaces/payments/streams";
    let caches = "/v1/tenants/t1/namespaces/payments/caches";
    for (uri, body) in [
        (streams, stream_body("chat")),
        (caches, cache_body("cursors")),
    ] {
        let (status, _) = send(&h, json_request_as("POST", uri, &admin(&h), body.clone())).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = send(&h, json_request_as("POST", uri, &admin(&h), body)).await;
        assert_eq!(status, StatusCode::OK);
    }

    let mut chat = stream_body("chat");
    chat["durable"] = true.into();
    let (status, _) = send(&h, json_request_as("POST", streams, &admin(&h), chat)).await;
    assert_eq!(status, StatusCode::CONFLICT);

    let mut cursors = cache_body("cursors");
    cursors["shards"] = 2.into();
    let (status, _) = send(&h, json_request_as("POST", caches, &admin(&h), cursors)).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Omitted counts and an explicit zero both mean one, so they match.
    let mut zero = cache_body("cursors");
    zero["shards"] = 0.into();
    let (status, _) = send(&h, json_request_as("POST", caches, &admin(&h), zero)).await;
    assert_eq!(status, StatusCode::OK);
}

/// Once the fleet finalized `cache_incarnations`, a created cache answers
/// with the incarnation the store gave it, single or batched; a retry keeps
/// it; and a delete is named on its change and in the snapshot's tombstones.
/// Before the finalize none of the new fields appear.
#[tokio::test]
async fn a_cache_answers_its_incarnation_and_its_delete_is_tombstoned() {
    use felix_controlplane_service::model::{
        Node, NodeCapacity, NodeLifecycle, NodeSpec, NodeStatus,
    };
    let h = harness().await;
    let caches = "/v1/tenants/t1/namespaces/payments/caches";
    let broker = h.credentials.broker();

    let (status, early) = send(
        &h,
        json_request_as("POST", caches, &admin(&h), cache_body("early")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(early.get("incarnation").is_none(), "{early}");

    let feature = felix_common::fleet::CACHE_INCARNATIONS.name();
    h.store
        .register_node(Node {
            node_id: "broker-a".to_string(),
            spec: NodeSpec {
                advertise_addr: "10.0.0.4:7000".to_string(),
                client_addr: None,
                kafka_addr: None,
                region: "us-west-2".to_string(),
                zone: None,
                labels: Default::default(),
                capacity: NodeCapacity::default(),
            },
            status: NodeStatus {
                lifecycle: NodeLifecycle::Live,
                last_heartbeat_at_millis: 1,
                registered_at_millis: 1,
                incarnation: 0,
                features: [feature.to_string()].into(),
                storage_id: None,
            },
        })
        .await
        .expect("node");
    h.store
        .finalize_fleet_feature(feature)
        .await
        .expect("finalize");

    let (status, created) = send(
        &h,
        json_request_as("POST", caches, &admin(&h), cache_body("cursors")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let incarnation = created["incarnation"].as_u64().expect("an incarnation");
    let (status, again) = send(
        &h,
        json_request_as("POST", caches, &admin(&h), cache_body("cursors")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(again["incarnation"], incarnation);

    // A name deleted at an incarnation above the clock: the batch's
    // proposal is raised, and the answer must carry the raised one.
    let presence = CacheKey {
        tenant_id: "t1".to_string(),
        namespace: "payments".to_string(),
        cache: "presence".to_string(),
    };
    let mut high = again.clone();
    high["cache"] = "presence".into();
    high["incarnation"] = (1u64 << 60).into();
    h.store
        .create_cache(serde_json::from_value(high).expect("cache"))
        .await
        .expect("create");
    h.store.delete_cache(&presence).await.expect("delete");

    let (status, batched) = send(
        &h,
        json_request_as(
            "POST",
            BATCH,
            &admin(&h),
            batch(&[], &[cache_body("cursors"), cache_body("presence")]),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{batched}");
    assert_eq!(statuses(&batched, "caches"), ["unchanged", "created"]);
    assert_eq!(batched["caches"][0]["cache"]["incarnation"], incarnation);
    let stored = h.store.get_cache(&presence).await.expect("stored");
    assert!(stored.incarnation > 1 << 60, "{}", stored.incarnation);
    assert_eq!(
        batched["caches"][1]["cache"]["incarnation"],
        stored.incarnation
    );

    let deleted = h
        .app
        .clone()
        .oneshot(crate::common::request_as(
            "DELETE",
            &format!("{caches}/cursors"),
            &admin(&h),
        ))
        .await
        .expect("response");
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let (status, changes) = send(
        &h,
        crate::common::request_as("GET", "/v1/caches/changes?since=0", &broker),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let deleted = changes["items"]
        .as_array()
        .expect("items")
        .iter()
        .find(|change| change["op"] == "deleted" && change["key"]["cache"] == "cursors")
        .expect("the delete's change");
    assert_eq!(deleted["deleted_through"], incarnation, "{deleted}");
    let (status, snapshot) = send(
        &h,
        crate::common::request_as("GET", "/v1/caches/snapshot", &broker),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        snapshot["tombstones"],
        serde_json::json!([
            { "tenant_id": "t1", "namespace": "payments", "cache": "cursors",
              "incarnation": incarnation },
            { "tenant_id": "t1", "namespace": "payments", "cache": "presence",
              "incarnation": 1u64 << 60 },
        ])
    );
}

//! Membership client behaviour against a stub control plane.
use std::sync::Mutex;

use axum::Json;
use axum::extract::{Path, State};
use axum::routing::post;
use serde_json::json;

use super::*;
use crate::test_support::{build_test_client, spawn_axum_with_shutdown, wait_for_listen};

/// Records what the broker sent, so a test can assert on the calls rather than
/// on the client's own bookkeeping.
#[derive(Default)]
struct Calls {
    registrations: Vec<serde_json::Value>,
    heartbeats: Vec<u64>,
    drained: Vec<String>,
    deregistered: Vec<String>,
    /// Heartbeats to reject before succeeding, so backoff can be exercised.
    fail_heartbeats: usize,
    /// Answer heartbeats from this incarnation or older with `down`, the way
    /// the control plane answers a node its sweep has expired.
    down_through_incarnation: Option<u64>,
    /// The enabled fleet features every answer carries.
    fleet_features: Vec<String>,
    /// The cadence every answer asks for; 20 ms when unset.
    heartbeat_interval_ms: Option<u64>,
    /// Each heartbeat's body, in order.
    heartbeat_bodies: Vec<serde_json::Value>,
}

type Shared = Arc<Mutex<Calls>>;

fn stub_control_plane(state: Shared) -> axum::Router {
    axum::Router::new()
        .route(
            "/v1/nodes",
            post(
                |State(state): State<Shared>, Json(body): Json<serde_json::Value>| async move {
                    let (incarnation, fleet) = {
                        let mut calls = state.lock().expect("lock");
                        calls.registrations.push(body);
                        (
                            calls.registrations.len() as u64 - 1,
                            calls.fleet_features.clone(),
                        )
                    };
                    Json(json!({
                        "node": { "status": { "incarnation": incarnation, "lifecycle": "live" } },
                        "heartbeat_interval_ms": 20,
                        "expiry_timeout_ms": 60,
                        "fleet_features": fleet,
                    }))
                },
            ),
        )
        .route(
            "/v1/nodes/{node_id}/heartbeat",
            post(
                |State(state): State<Shared>,
                 Path(_): Path<String>,
                 Json(body): Json<serde_json::Value>| async move {
                    let mut calls = state.lock().expect("lock");
                    if calls.fail_heartbeats > 0 {
                        calls.fail_heartbeats -= 1;
                        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
                    }
                    let incarnation = body["incarnation"].as_u64().unwrap_or_default();
                    calls.heartbeats.push(incarnation);
                    calls.heartbeat_bodies.push(body);
                    let lifecycle = match calls.down_through_incarnation {
                        Some(down) if incarnation <= down => "down",
                        _ => "live",
                    };
                    Ok(Json(json!({
                        "lifecycle": lifecycle,
                        "heartbeat_interval_ms": calls.heartbeat_interval_ms.unwrap_or(20),
                        "fleet_features": calls.fleet_features,
                    })))
                },
            ),
        )
        .route(
            "/v1/nodes/{node_id}/drain",
            post(
                |State(state): State<Shared>, Path(id): Path<String>| async move {
                    state.lock().expect("lock").drained.push(id);
                    Json(json!({}))
                },
            ),
        )
        .route(
            "/v1/nodes/{node_id}/deregister",
            post(
                |State(state): State<Shared>, Path(id): Path<String>| async move {
                    state.lock().expect("lock").deregistered.push(id);
                    Json(json!({}))
                },
            ),
        )
        .with_state(state)
}

async fn serve(
    state: Shared,
) -> (
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, stub_control_plane(state));
    wait_for_listen(addr).await.expect("listen");
    (format!("http://{addr}"), stop, handle)
}

fn config() -> MembershipConfig {
    MembershipConfig {
        node_id: "broker-a".to_string(),
        advertise_addr: "10.0.0.4:7000".to_string(),
        client_advertise_addr: None,
        kafka_advertise_addr: None,
        region: "us-west-2".to_string(),
        zone: None,
        region_bridges: Vec::new(),
        features: Default::default(),
    }
}

#[tokio::test]
async fn registration_sends_the_identity_and_returns_the_incarnation() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    let registration = register(
        &client,
        &base_url,
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect("register");
    assert_eq!(registration.node_id, "broker-a");
    assert_eq!(registration.incarnation, 0);
    assert_eq!(registration.heartbeat_interval_ms, 20);

    let sent = calls.lock().expect("lock").registrations[0].clone();
    assert_eq!(sent["node_id"], "broker-a");
    assert_eq!(sent["advertise_addr"], "10.0.0.4:7000");
    assert_eq!(sent["region"], "us-west-2");
    // Observed status is the control plane's to set.
    assert!(sent.get("status").is_none());
    // Omitted, not null, so an older control plane sees the body it expects.
    assert!(sent.get("kafka_addr").is_none());
    assert!(sent.get("zone").is_none());

    let _ = stop.send(());
    let _ = handle.await;
}

#[tokio::test]
async fn registration_sends_the_kafka_address_when_the_listener_is_on() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let config = MembershipConfig {
        kafka_advertise_addr: Some("host.docker.internal:9092".to_string()),
        ..config()
    };

    register(
        &client,
        &base_url,
        &config,
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect("register");

    let sent = calls.lock().expect("lock").registrations[0].clone();
    assert_eq!(sent["kafka_addr"], "host.docker.internal:9092");

    let _ = stop.send(());
    let _ = handle.await;
}

#[tokio::test]
async fn registration_sends_the_zone_when_one_is_set() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let config = MembershipConfig {
        zone: Some("us-west-2b".to_string()),
        ..config()
    };

    register(
        &client,
        &base_url,
        &config,
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect("register");

    let sent = calls.lock().expect("lock").registrations[0].clone();
    assert_eq!(sent["zone"], "us-west-2b");

    let _ = stop.send(());
    let _ = handle.await;
}

/// A broker that cannot register is not a cluster member, so this must not be
/// swallowed into "carry on and hope".
#[tokio::test]
async fn a_rejected_registration_is_an_error() {
    let app = axum::Router::new().route(
        "/v1/nodes",
        post(|| async { (axum::http::StatusCode::CONFLICT, "address in use") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");

    let client = build_test_client().expect("client");
    let err = register(
        &client,
        &format!("http://{addr}"),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect_err("should fail");
    assert!(err.to_string().contains("rejected registration"), "{err}");

    let _ = stop.send(());
    let _ = handle.await;
}

/// Every heartbeat carries this process's incarnation, so one delayed past a
/// restart is rejected rather than counted for its successor.
#[tokio::test]
async fn heartbeats_carry_the_registered_incarnation() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    let mut registration = register(
        &client,
        &base_url,
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect("register");
    registration.incarnation = 7;

    let shutdown = CancellationToken::new();
    let failures = Arc::new(AtomicU64::new(0));
    let beating = tokio::spawn(run_heartbeat(
        client,
        base_url,
        registration,
        shutdown.clone(),
        Arc::clone(&failures),
        Arc::new(crate::cluster::lease::LeaseState::new(
            std::time::Duration::from_secs(30),
        )),
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::default(),
    ));

    // Wait for a few beats rather than a fixed sleep.
    for _ in 0..200 {
        if calls.lock().expect("lock").heartbeats.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    beating.await.expect("heartbeat task");

    let sent = calls.lock().expect("lock").heartbeats.clone();
    assert!(
        sent.len() >= 2,
        "expected repeated heartbeats, got {sent:?}"
    );
    assert!(sent.iter().all(|i| *i == 7), "{sent:?}");
    assert_eq!(failures.load(Ordering::Acquire), 0);

    let _ = stop.send(());
    let _ = handle.await;
}

/// **A new suspicion goes out at once, not an interval later.** The control
/// plane promotes on it, so waiting for the next beat would add the whole
/// interval to a failover. With nothing suspected the field is left out.
#[tokio::test]
async fn a_suspicion_is_sent_without_waiting_for_the_next_beat() {
    let calls: Shared = Arc::new(Mutex::new(Calls {
        heartbeat_interval_ms: Some(60_000),
        ..Calls::default()
    }));
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let mut registration = register(
        &client,
        &base_url,
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect("register");
    registration.heartbeat_interval_ms = 60_000;

    let suspects = Arc::new(felix_replication::suspicion::Suspects::default());
    let shutdown = CancellationToken::new();
    let beating = tokio::spawn(run_heartbeat(
        client,
        base_url,
        registration,
        shutdown.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(crate::cluster::lease::LeaseState::new(
            std::time::Duration::from_secs(30),
        )),
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::clone(&suspects),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(calls.lock().expect("lock").heartbeat_bodies.is_empty());

    suspects.publish(["broker-b".to_string()].into_iter().collect());
    for _ in 0..200 {
        if !calls.lock().expect("lock").heartbeat_bodies.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let bodies = calls.lock().expect("lock").heartbeat_bodies.clone();
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    assert_eq!(bodies[0]["suspects"], json!(["broker-b"]));

    suspects.publish(Default::default());
    for _ in 0..200 {
        if calls.lock().expect("lock").heartbeat_bodies.len() > 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let bodies = calls.lock().expect("lock").heartbeat_bodies.clone();
    assert_eq!(bodies.len(), 2, "{bodies:?}");
    assert!(bodies[1].get("suspects").is_none(), "{bodies:?}");

    shutdown.cancel();
    beating.await.expect("heartbeat task");
    let _ = stop.send(());
    let _ = handle.await;
}

/// The control plane being briefly unreachable must not stop a broker that is
/// otherwise serving. The failure has to be visible, not fatal.
#[tokio::test]
async fn heartbeat_failures_are_counted_and_then_recovered_from() {
    let calls: Shared = Arc::new(Mutex::new(Calls {
        fail_heartbeats: 3,
        ..Calls::default()
    }));
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    let registration = register(
        &client,
        &base_url,
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect("register");
    let shutdown = CancellationToken::new();
    let failures = Arc::new(AtomicU64::new(0));
    let beating = tokio::spawn(run_heartbeat(
        client,
        base_url,
        registration,
        shutdown.clone(),
        Arc::clone(&failures),
        Arc::new(crate::cluster::lease::LeaseState::new(
            std::time::Duration::from_secs(30),
        )),
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::default(),
    ));

    for _ in 0..400 {
        if !calls.lock().expect("lock").heartbeats.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    beating.await.expect("heartbeat task");

    assert!(
        !calls.lock().expect("lock").heartbeats.is_empty(),
        "the loop should recover once the control plane does",
    );
    // Reset on success, so the counter reads "currently failing", not "ever failed".
    assert_eq!(failures.load(Ordering::Acquire), 0);

    let _ = stop.send(());
    let _ = handle.await;
}

#[tokio::test]
async fn draining_and_deregistering_hit_the_right_endpoints() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");

    drain(&client, &base_url, "broker-a", "test-token")
        .await
        .expect("drain");
    deregister(&client, &base_url, "broker-a", "test-token")
        .await
        .expect("deregister");

    {
        let calls = calls.lock().expect("lock");
        assert_eq!(calls.drained, vec!["broker-a".to_string()]);
        assert_eq!(calls.deregistered, vec!["broker-a".to_string()]);
    }

    let _ = stop.send(());
    let _ = handle.await;
}

#[test]
fn backoff_grows_and_then_stops_growing() {
    let interval = Duration::from_millis(100);
    assert_eq!(backoff(interval, 1), interval);
    assert_eq!(backoff(interval, 2), interval * 2);
    assert_eq!(backoff(interval, 3), interval * 4);
    // Capped, so a long outage does not turn into an hours-long retry gap.
    assert_eq!(backoff(interval, 60), MAX_RETRY_BACKOFF);
    assert_eq!(backoff(Duration::from_secs(120), 1), MAX_RETRY_BACKOFF);
}

/// Jitter only ever delays, and only within the documented fraction: a
/// heartbeat pulled earlier would tighten the cadence the control plane sized
/// its expiry window against.
#[test]
fn jitter_delays_within_its_bound() {
    let delay = Duration::from_millis(1000);
    for _ in 0..50 {
        let jittered = jittered(delay);
        assert!(
            jittered >= delay,
            "{jittered:?} moved earlier than {delay:?}"
        );
        assert!(
            jittered <= delay.mul_f64(1.0 + JITTER_FRACTION),
            "{jittered:?} exceeded the jitter bound",
        );
    }
}

/// The acceptance criterion: a refusal and an outage must be tellable apart. A
/// misconfigured broker retrying forever looks exactly like a flaky network if
/// both increment one counter.
#[tokio::test]
async fn a_refusal_and_an_outage_are_different_kinds() {
    // 409 from a live control plane: the server answered and said no.
    let app = axum::Router::new().route(
        "/v1/nodes",
        post(|| async { (axum::http::StatusCode::CONFLICT, "address in use") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");

    let client = build_test_client().expect("client");
    let refused = register(
        &client,
        &format!("http://{addr}"),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect_err("should be refused");
    assert_eq!(
        refused.kind(),
        crate::cluster::membership::metrics::KIND_REJECTED
    );

    let _ = stop.send(());
    let _ = handle.await;

    // Nothing listening: no answer at all.
    let outage = register(
        &client,
        &format!("http://{addr}"),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect_err("should be unavailable");
    assert_eq!(
        outage.kind(),
        crate::cluster::membership::metrics::KIND_UNAVAILABLE
    );
}

/// A 5xx is the control plane failing, not refusing, so it is retryable like an
/// outage rather than terminal like a rejection.
#[tokio::test]
async fn a_server_error_counts_as_an_outage_not_a_refusal() {
    let app = axum::Router::new().route(
        "/v1/nodes",
        post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");

    let client = build_test_client().expect("client");
    let err = register(
        &client,
        &format!("http://{addr}"),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect_err("should fail");
    assert_eq!(
        err.kind(),
        crate::cluster::membership::metrics::KIND_UNAVAILABLE
    );
    assert!(
        matches!(err, MembershipError::Unavailable(_)),
        "a 5xx must stay retryable",
    );

    let _ = stop.send(());
    let _ = handle.await;
}

/// The sweep marks a node down once it has been silent a full window, and a
/// heartbeat never revives it. The broker has to register again, or it keeps
/// running with no lease and no shards for the rest of its life.
#[tokio::test]
async fn a_broker_marked_down_registers_again_and_resumes_heartbeating() {
    let calls: Shared = Arc::new(Mutex::new(Calls {
        down_through_incarnation: Some(0),
        ..Calls::default()
    }));
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let serving = CancellationToken::new();
    serving.cancel();
    let shutdown = CancellationToken::new();
    let lease = Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
        30,
    )));

    let task = spawn(
        client,
        base_url,
        config(),
        crate::cluster::credential::NodeCredential::new("a-node-token"),
        serving,
        shutdown.clone(),
        Arc::clone(&lease),
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::default(),
    );

    for _ in 0..300 {
        if calls.lock().expect("lock").heartbeats.contains(&1) && lease.is_valid_now() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    task.handle.await.expect("membership task");

    {
        let calls = calls.lock().expect("lock");
        assert_eq!(
            calls.registrations.len(),
            2,
            "expected exactly one registration after being marked down",
        );
        assert!(
            calls.heartbeats.contains(&1),
            "heartbeats should carry the new incarnation: {:?}",
            calls.heartbeats,
        );
    }
    assert!(
        lease.is_valid_now(),
        "the new registration renews the lease"
    );
    assert!(!task.fatal.is_cancelled());

    let _ = stop.send(());
    let _ = handle.await;
}

/// A control plane that accepts the heartbeat and never answers must not hold
/// the loop: the lease runs out while it waits, and no retry is ever sent.
#[tokio::test]
async fn a_stalled_heartbeat_is_abandoned_within_the_lease() {
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}/heartbeat",
        post(|| async {
            std::future::pending::<()>().await;
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");

    // The broker's own client: its default deadline is seconds, so a bound
    // seen here can only come from the lease.
    let client = crate::cluster::controlplane_client::build().expect("client");
    let registration = Registration {
        node_id: "broker-a".to_string(),
        token: crate::cluster::credential::NodeCredential::new("a-node-token"),
        incarnation: 0,
        heartbeat_interval_ms: 20,
        fleet_features: Default::default(),
    };
    let shutdown = CancellationToken::new();
    let failures = Arc::new(AtomicU64::new(0));
    // Usable for 300 ms, so each attempt gets 75 ms.
    let lease = Arc::new(crate::cluster::lease::LeaseState::new(
        Duration::from_millis(400),
    ));
    let beating = tokio::spawn(run_heartbeat(
        client,
        format!("http://{addr}"),
        registration,
        shutdown.clone(),
        Arc::clone(&failures),
        lease,
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::default(),
    ));

    let started = std::time::Instant::now();
    while failures.load(Ordering::Acquire) < 3 && started.elapsed() < Duration::from_secs(4) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    assert_eq!(
        beating.await.expect("heartbeat task"),
        HeartbeatEnd::Shutdown
    );
    assert!(
        failures.load(Ordering::Acquire) >= 3,
        "a stalled heartbeat should time out and be retried; waited {:?}",
        started.elapsed(),
    );

    // The handler never returns, so graceful shutdown would wait on it.
    drop(stop);
    handle.abort();
}

/// The default cadence is a 5 s interval against an 11.25 s lease. Uncapped,
/// the backoff reaches 30 s, which outlives the whole expiry window: a control
/// plane back after a short outage would expire the broker before its next
/// retry.
#[test]
fn retries_stay_under_a_quarter_of_the_lease() {
    let usable = Duration::from_millis(11_250);
    let interval = Duration::from_secs(5);
    for failures in 1..20 {
        let delay = backoff(interval, failures).min(retry_cap(usable));
        let worst = delay.mul_f64(1.0 + JITTER_FRACTION);
        assert!(
            worst < usable / 4,
            "retry {failures} may wait {worst:?}, lease is {usable:?}",
        );
    }
    // A long lease is still bounded by the absolute ceiling.
    assert_eq!(retry_cap(Duration::from_secs(3_600)), MAX_RETRY_BACKOFF);
    // And a degenerate one does not spin.
    assert!(retry_cap(Duration::ZERO) > Duration::ZERO);
}

/// A build that implements no fleet features registers the body it always
/// did; one that does sends them.
#[tokio::test]
async fn registration_reports_features_only_when_there_are_some() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("a-node-token");

    register(&client, &base_url, &config(), &credential)
        .await
        .expect("register");
    let with_features = MembershipConfig {
        features: ["jump".to_string()].into(),
        ..config()
    };
    register(&client, &base_url, &with_features, &credential)
        .await
        .expect("register");

    let sent = calls.lock().expect("lock").registrations.clone();
    assert!(sent[0].get("features").is_none(), "{}", sent[0]);
    assert_eq!(sent[1]["features"], json!(["jump"]));

    let _ = stop.send(());
    let _ = handle.await;
}

/// The heartbeat is what opens the gate once an operator finalizes, and a
/// later answer without the feature does not close it again.
#[tokio::test]
async fn heartbeats_open_the_fleet_gate_and_never_close_it() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let jump = felix_common::fleet::FleetFeature::new("jump");
    let fleet = Arc::new(felix_common::fleet::FleetGate::new(["jump"]));

    let registration = register(
        &client,
        &base_url,
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
    )
    .await
    .expect("register");
    let shutdown = CancellationToken::new();
    let beating = tokio::spawn(run_heartbeat(
        client,
        base_url,
        registration,
        shutdown.clone(),
        Arc::new(AtomicU64::new(0)),
        Arc::new(crate::cluster::lease::LeaseState::new(
            std::time::Duration::from_secs(30),
        )),
        Arc::clone(&fleet),
        Arc::default(),
    ));

    let beats = || calls.lock().expect("lock").heartbeats.len();
    let wait_for_beats = |n: usize| async move {
        for _ in 0..500 {
            if beats() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no heartbeats");
    };
    wait_for_beats(2).await;
    assert!(!fleet.supports(jump), "enabled before a finalize");

    calls.lock().expect("lock").fleet_features = vec!["jump".to_string()];
    let seen = beats();
    wait_for_beats(seen + 2).await;
    assert!(fleet.supports(jump));

    // A lagging control-plane instance answers with less.
    calls.lock().expect("lock").fleet_features.clear();
    let seen = beats();
    wait_for_beats(seen + 2).await;
    assert!(fleet.supports(jump), "a stale answer withdrew the feature");

    shutdown.cancel();
    beating.await.expect("heartbeat task");
    let _ = stop.send(());
    let _ = handle.await;
}

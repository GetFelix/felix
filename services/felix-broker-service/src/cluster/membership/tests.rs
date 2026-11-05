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

fn link(
    client: &reqwest::Client,
    base_url: &str,
) -> felix_replication::controlplane_link::HttpControlPlaneLink {
    felix_replication::controlplane_link::HttpControlPlaneLink::new(client.clone(), base_url)
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
        &link(&client, &base_url),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
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
        &link(&client, &base_url),
        &config,
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
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
        &link(&client, &base_url),
        &config,
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
    )
    .await
    .expect("register");

    let sent = calls.lock().expect("lock").registrations[0].clone();
    assert_eq!(sent["zone"], "us-west-2b");

    let _ = stop.send(());
    let _ = handle.await;
}

#[tokio::test]
async fn registration_sends_the_storage_id_as_hex_when_one_is_set() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("a-node-token");

    let mut bytes = [0u8; 16];
    bytes[0] = 0xab;
    register(
        &link(&client, &base_url),
        &config(),
        &credential,
        Some(StorageId::from_bytes(bytes)),
    )
    .await
    .expect("register");
    register(&link(&client, &base_url), &config(), &credential, None)
        .await
        .expect("register");

    let sent = calls.lock().expect("lock").registrations.clone();
    assert_eq!(sent[0]["storage_id"], "ab000000000000000000000000000000");
    // Omitted, not null, so an older control plane sees the body it expects.
    assert!(sent[1].get("storage_id").is_none());

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
        &link(&client, &format!("http://{addr}")),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
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
        &link(&client, &base_url),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
    )
    .await
    .expect("register");
    registration.incarnation = 7;

    let shutdown = CancellationToken::new();
    let failures = Arc::new(AtomicU64::new(0));
    let beating = tokio::spawn(run_heartbeat(
        link(&client, &base_url),
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
        &link(&client, &base_url),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
    )
    .await
    .expect("register");
    registration.heartbeat_interval_ms = 60_000;

    let suspects = Arc::new(felix_replication::suspicion::Suspects::default());
    let shutdown = CancellationToken::new();
    let beating = tokio::spawn(run_heartbeat(
        link(&client, &base_url),
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
        &link(&client, &base_url),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
    )
    .await
    .expect("register");
    let shutdown = CancellationToken::new();
    let failures = Arc::new(AtomicU64::new(0));
    let beating = tokio::spawn(run_heartbeat(
        link(&client, &base_url),
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

    drain(&link(&client, &base_url), "broker-a", "test-token")
        .await
        .expect("drain");
    deregister(&link(&client, &base_url), "broker-a", "test-token")
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
        &link(&client, &format!("http://{addr}")),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
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
        &link(&client, &format!("http://{addr}")),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
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
        &link(&client, &format!("http://{addr}")),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
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
    let data = tempfile::tempdir().expect("tempdir");

    let task = spawn(
        link(&client, &base_url),
        config(),
        crate::cluster::credential::NodeCredential::new("a-node-token"),
        serving,
        shutdown.clone(),
        Arc::clone(&lease),
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::default(),
        Arc::default(),
        tokio::sync::watch::channel(None).0,
        None,
        Some(data.path().to_path_buf()),
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
    assert_eq!(
        felix_storage::run_state::read_registration(data.path()).expect("read"),
        felix_storage::run_state::Recorded::At(1),
        "each registration is recorded in the data directory",
    );

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
        link(&client, &format!("http://{addr}")),
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

    register(&link(&client, &base_url), &config(), &credential, None)
        .await
        .expect("register");
    let with_features = MembershipConfig {
        features: ["jump".to_string()].into(),
        ..config()
    };
    register(&link(&client, &base_url), &with_features, &credential, None)
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
        &link(&client, &base_url),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
    )
    .await
    .expect("register");
    let shutdown = CancellationToken::new();
    let beating = tokio::spawn(run_heartbeat(
        link(&client, &base_url),
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

/// A control plane that answers `GET /v1/nodes/{id}` with `status` until
/// `failures` run out, then with the node at `incarnation` (404 when `None`),
/// registered under [`LOOKUP_STORAGE_ID`].
async fn serve_lookup(
    failures: usize,
    status: axum::http::StatusCode,
    incarnation: Option<u64>,
) -> (
    String,
    Arc<Mutex<usize>>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let asked = Arc::new(Mutex::new(0usize));
    let app = axum::Router::new()
        .route(
            "/v1/nodes/{node_id}",
            axum::routing::get(
                move |State(asked): State<Arc<Mutex<usize>>>, Path(id): Path<String>| async move {
                    let mut asked = asked.lock().expect("lock");
                    *asked += 1;
                    if *asked <= failures {
                        return Err(status);
                    }
                    match incarnation {
                        Some(incarnation) if id == "broker-a" => Ok(Json(json!({
                            "node": {
                                "node_id": id,
                                "status": {
                                    "incarnation": incarnation,
                                    "lifecycle": "live",
                                    "storage_id": LOOKUP_STORAGE_ID,
                                },
                            },
                            "placement": { "eligible": true },
                        }))),
                        _ => Err(axum::http::StatusCode::NOT_FOUND),
                    }
                },
            ),
        )
        .with_state(Arc::clone(&asked));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = spawn_axum_with_shutdown(listener, app);
    wait_for_listen(addr).await.expect("listen");
    (format!("http://{addr}"), asked, stop, handle)
}

const LOOKUP_STORAGE_ID: &str = "0123456789abcdef0123456789abcdef";

/// The startup check reads the node's current incarnation and the storage id
/// it registered with, and a node the control plane has never registered is
/// no evidence either way.
#[tokio::test]
async fn the_lookup_reads_the_registration() {
    let ok = axum::http::StatusCode::OK;
    let (base_url, _, stop, handle) = serve_lookup(0, ok, Some(7)).await;
    let client = build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("a-node-token");
    assert_eq!(
        registered(&link(&client, &base_url), "broker-a", &credential)
            .await
            .expect("lookup"),
        Some(Registered {
            incarnation: 7,
            storage_id: Some(LOOKUP_STORAGE_ID.to_string()),
        })
    );
    assert_eq!(
        registered(&link(&client, &base_url), "broker-z", &credential)
            .await
            .expect("lookup"),
        None
    );
    let _ = stop.send(());
    let _ = handle.await;
}

/// **An unreachable control plane holds startup (#1334).** The broker keeps
/// asking rather than starting without an answer, since starting would
/// announce an id the answer might have rotated. A credential not yet
/// accepted is waited out the same way.
#[tokio::test]
async fn the_startup_lookup_waits_for_an_answer() {
    for status in [
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        axum::http::StatusCode::UNAUTHORIZED,
    ] {
        let (base_url, asked, stop, handle) = serve_lookup(2, status, Some(3)).await;
        let client = build_test_client().expect("client");
        let credential = crate::cluster::credential::NodeCredential::new("a-node-token");
        let answer = tokio::time::timeout(
            Duration::from_secs(20),
            await_registered(&link(&client, &base_url), "broker-a", &credential),
        )
        .await
        .expect("answered once the control plane did")
        .expect("lookup");
        assert_eq!(answer.map(|r| r.incarnation), Some(3), "{status}");
        assert_eq!(*asked.lock().expect("lock"), 3, "{status}");
        let _ = stop.send(());
        let _ = handle.await;
    }

    // Nothing listening at all: still waiting when the test gives up.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let client = build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("a-node-token");
    let waiting = tokio::time::timeout(
        Duration::from_millis(1500),
        await_registered(
            &link(&client, &format!("http://{addr}")),
            "broker-a",
            &credential,
        ),
    )
    .await;
    assert!(waiting.is_err(), "returned without an answer: {waiting:?}");
}

/// A refusal does not change by asking again, so the start fails with it.
#[tokio::test]
async fn a_refused_startup_lookup_fails_the_start() {
    let (base_url, _, stop, handle) =
        serve_lookup(usize::MAX, axum::http::StatusCode::FORBIDDEN, Some(3)).await;
    let client = build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("a-node-token");
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        await_registered(&link(&client, &base_url), "broker-a", &credential),
    )
    .await
    .expect("a refusal is final");
    assert!(refused.is_err());
    let _ = stop.send(());
    let _ = handle.await;
}

/// Spawn membership for a data directory and wait until it either heartbeats
/// or gives up on joining.
async fn join_with(calls: &Shared, data: &std::path::Path) -> MembershipTask {
    let (base_url, stop, handle) = serve(Arc::clone(calls)).await;
    let serving = CancellationToken::new();
    serving.cancel();
    let shutdown = CancellationToken::new();
    let task = spawn(
        link(&build_test_client().expect("client"), &base_url),
        config(),
        crate::cluster::credential::NodeCredential::new("a-node-token"),
        serving,
        shutdown.clone(),
        Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
            30,
        ))),
        Arc::new(felix_common::fleet::FleetGate::for_this_build()),
        Arc::default(),
        Arc::default(),
        tokio::sync::watch::channel(None).0,
        None,
        Some(data.to_path_buf()),
    );
    for _ in 0..300 {
        if task.fatal.is_cancelled() || !calls.lock().expect("lock").heartbeats.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.cancel();
    let _ = stop.send(());
    let _ = handle.await;
    task
}

/// **A directory that took a head trim does not join a fleet without
/// `head_trim`.** An older leader promoted over its logs could not read them.
/// The broker leaves and stops instead of serving.
#[tokio::test]
async fn a_trimmed_directory_refuses_a_fleet_without_head_trim() {
    let data = tempfile::tempdir().expect("tempdir");
    felix_storage::run_state::record_head_trimmed(data.path()).expect("mark");
    let calls: Shared = Arc::default();

    let task = join_with(&calls, data.path()).await;
    assert!(task.fatal.is_cancelled(), "the broker stops");
    let calls = calls.lock().expect("lock");
    assert!(calls.heartbeats.is_empty(), "never heartbeats as a member");
    assert_eq!(calls.deregistered, vec!["broker-a".to_string()]);
    assert_eq!(
        felix_storage::run_state::read_registration(data.path()).expect("read"),
        felix_storage::run_state::Recorded::Unrecorded,
    );
}

/// The same directory joins once the fleet has finalized `head_trim`, and a
/// directory that never trimmed joins either way.
#[tokio::test]
async fn a_trimmed_directory_joins_a_fleet_with_head_trim() {
    let data = tempfile::tempdir().expect("tempdir");
    felix_storage::run_state::record_head_trimmed(data.path()).expect("mark");
    let calls: Shared = Arc::new(Mutex::new(Calls {
        fleet_features: vec![felix_common::fleet::HEAD_TRIM.name().to_string()],
        ..Calls::default()
    }));
    let task = join_with(&calls, data.path()).await;
    assert!(!task.fatal.is_cancelled());
    assert!(!calls.lock().expect("lock").heartbeats.is_empty());

    let untrimmed = tempfile::tempdir().expect("tempdir");
    let calls: Shared = Arc::default();
    let task = join_with(&calls, untrimmed.path()).await;
    assert!(!task.fatal.is_cancelled());
    assert!(!calls.lock().expect("lock").heartbeats.is_empty());
}

/// **A directory that stored a record with headers does not join a fleet
/// without `record_headers`.** An older follower would be held at the record
/// and an older promoted leader could not serve it.
#[tokio::test]
async fn a_directory_with_headers_refuses_a_fleet_without_record_headers() {
    let data = tempfile::tempdir().expect("tempdir");
    felix_storage::run_state::record_headers_stored(data.path()).expect("mark");
    let calls: Shared = Arc::default();

    let task = join_with(&calls, data.path()).await;
    assert!(task.fatal.is_cancelled(), "the broker stops");
    let calls = calls.lock().expect("lock");
    assert!(calls.heartbeats.is_empty(), "never heartbeats as a member");
    assert_eq!(calls.deregistered, vec!["broker-a".to_string()]);
}

/// The same directory joins once the fleet has finalized `record_headers`.
#[tokio::test]
async fn a_directory_with_headers_joins_a_fleet_with_record_headers() {
    let data = tempfile::tempdir().expect("tempdir");
    felix_storage::run_state::record_headers_stored(data.path()).expect("mark");
    let calls: Shared = Arc::new(Mutex::new(Calls {
        fleet_features: vec![felix_common::fleet::RECORD_HEADERS.name().to_string()],
        ..Calls::default()
    }));
    let task = join_with(&calls, data.path()).await;
    assert!(!task.fatal.is_cancelled());
    assert!(!calls.lock().expect("lock").heartbeats.is_empty());
}

/// The refusal names the feature it is waiting on.
#[tokio::test]
async fn the_refusal_names_the_missing_feature() {
    let data = tempfile::tempdir().expect("tempdir");
    felix_storage::run_state::record_headers_stored(data.path()).expect("mark");
    let reason = join_refusal(Some(data.path()), &std::collections::BTreeSet::new())
        .await
        .expect("refused");
    assert!(reason.contains("record_headers"), "{reason}");
    assert!(!reason.contains("head_trim"), "{reason}");
}

/// Registration, the lookup and a lifecycle call each go through the link
/// alone; an unset reply reads as an outage, not a refusal.
#[tokio::test]
async fn a_fake_link_drives_membership() {
    use crate::test_support::fake_link::FakeLink;

    let credential = crate::cluster::credential::NodeCredential::new("a-node-token");
    let link = FakeLink::default().reply(
        "register",
        json!({
            "node": { "status": { "incarnation": 4, "lifecycle": "ready" } },
            "heartbeat_interval_ms": 20,
        }),
    );
    let registration = register(&link, &config(), &credential, None)
        .await
        .expect("register");
    assert_eq!(registration.incarnation, 4);

    let outage = registered(&link, "broker-a", &credential).await;
    assert!(matches!(outage, Err(MembershipError::Unavailable(_))));
    drain(&link, "broker-a", "t").await.expect_err("no reply");
    assert_eq!(link.calls(), ["register", "node", "lifecycle"]);
}

/// Serve the stub control plane from a thread and runtime of its own, so a
/// test can block every worker of its own runtime and still be answered.
fn serve_on_own_thread(state: Shared) -> (String, tokio::sync::oneshot::Sender<()>) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            addr_tx
                .send(listener.local_addr().expect("addr"))
                .expect("send addr");
            let _ = axum::serve(listener, stub_control_plane(state))
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await;
        });
    });
    let addr = addr_rx.recv().expect("stub address");
    (format!("http://{addr}"), stop)
}

/// **Heartbeats keep going while every broker runtime worker is blocked.**
/// A filesystem journal stall can park every worker in a blocking write or
/// fsync at once. The heartbeat is the lease renewal, so if it ran on those
/// workers the broker would lose its lease and every shard it leads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeats_continue_while_every_runtime_worker_is_blocked() {
    let calls: Shared = Arc::default();
    let (base_url, stop) = serve_on_own_thread(Arc::clone(&calls));
    let serving = CancellationToken::new();
    serving.cancel();
    let shutdown = CancellationToken::new();
    let task = spawn(
        link(&build_test_client().expect("client"), &base_url),
        config(),
        crate::cluster::credential::NodeCredential::new("a-node-token"),
        serving,
        shutdown.clone(),
        Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
            30,
        ))),
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::default(),
        Arc::default(),
        tokio::sync::watch::channel(None).0,
        None,
        None,
    );
    for _ in 0..300 {
        if !calls.lock().expect("lock").heartbeats.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !calls.lock().expect("lock").heartbeats.is_empty(),
        "membership never heartbeat"
    );

    // Park both workers. The test body runs on neither, so it can wait them out.
    let parked = Arc::new(std::sync::Barrier::new(3));
    let blockers: Vec<_> = (0..2)
        .map(|_| {
            let parked = Arc::clone(&parked);
            tokio::spawn(async move {
                parked.wait();
                std::thread::sleep(Duration::from_millis(1500));
            })
        })
        .collect();
    parked.wait();
    let before = calls.lock().expect("lock").heartbeats.len();
    std::thread::sleep(Duration::from_millis(1000));
    let during = calls.lock().expect("lock").heartbeats.len() - before;
    for blocker in blockers {
        blocker.await.expect("blocker");
    }

    shutdown.cancel();
    task.handle.await.expect("membership task");
    let _ = stop.send(());
    // 20 ms cadence: a second holds about fifty. A handful proves the loop ran.
    assert!(
        during >= 10,
        "only {during} heartbeats while the runtime's workers were blocked"
    );
}

/// **A broker whose runtime stops for good stops renewing its lease.** The
/// heartbeat has a thread of its own, so it would otherwise keep a deadlocked
/// broker's shards on it forever. Here the pulse is never ticked, as if the
/// main runtime had stopped as soon as the heartbeat started.
#[tokio::test]
async fn a_stopped_runtime_stops_the_heartbeat() {
    let calls: Shared = Arc::default();
    let (base_url, stop, handle) = serve(Arc::clone(&calls)).await;
    let client = build_test_client().expect("client");
    let registration = register(
        &link(&client, &base_url),
        &config(),
        &crate::cluster::credential::NodeCredential::new("a-node-token"),
        None,
    )
    .await
    .expect("register");

    let shutdown = CancellationToken::new();
    let pulse = Arc::new(RuntimePulse::new(Duration::from_millis(200)));
    let beating = tokio::spawn(run_heartbeat_checked(
        link(&client, &base_url),
        registration,
        shutdown.clone(),
        Arc::default(),
        Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
            30,
        ))),
        Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new())),
        Arc::default(),
        Some(pulse),
    ));

    tokio::time::sleep(Duration::from_millis(400)).await;
    let before = calls.lock().expect("lock").heartbeats.len();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = calls.lock().expect("lock").heartbeats.len();
    shutdown.cancel();
    beating.await.expect("heartbeat task");
    let _ = stop.send(());
    let _ = handle.await;

    assert!(
        before > 0,
        "the heartbeat never ran while the runtime was live"
    );
    assert_eq!(
        after, before,
        "heartbeats went out after the runtime stopped"
    );
}

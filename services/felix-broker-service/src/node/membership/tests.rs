use std::time::Duration;

use super::*;

/// **A standalone broker adopts a token file something rewrote**, as a
/// cluster member does. Its catalog sync presents this credential, and an
/// exchange token lasts minutes, so reading the file once at startup left a
/// development broker locked out a quarter of an hour in.
#[tokio::test(start_paused = true)]
async fn a_standalone_broker_adopts_a_rewritten_token_file() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("node.token");
    std::fs::write(&path, "first").expect("write");
    let config = BrokerConfig {
        controlplane_url: Some("http://127.0.0.1:1".to_string()),
        node_token_file: Some(path.clone()),
        ..BrokerConfig::default()
    };
    assert!(config.membership.is_none());
    let credential = NodeCredential::new("first".to_string());
    let shutdown = CancellationToken::new();

    keep_credential_current(
        &config,
        &reqwest::Client::new(),
        &Some(credential.clone()),
        &shutdown,
    );
    std::fs::write(&path, "second").expect("rewrite");
    tokio::time::sleep(credential::rotate::POLL_INTERVAL + Duration::from_secs(1)).await;

    assert_eq!(credential.bearer().as_str(), "second");
    shutdown.cancel();
}

/// A cluster member whose control plane is at `base_url`, with the switch as
/// given and a short startup wait.
fn member(base_url: &str, start_without_controlplane: bool) -> BrokerConfig {
    let features: std::collections::BTreeSet<String> = election_fleet().into_iter().collect();
    BrokerConfig {
        controlplane_url: Some(base_url.to_string()),
        start_without_controlplane,
        controlplane_startup_wait_ms: 200,
        membership: Some(crate::config::MembershipConfig {
            node_id: "broker-a".to_string(),
            advertise_addr: "127.0.0.1:7001".to_string(),
            client_advertise_addr: None,
            kafka_advertise_addr: None,
            region: "local".to_string(),
            zone: None,
            region_bridges: Vec::new(),
            features,
        }),
        ..BrokerConfig::default()
    }
}

fn election_fleet() -> Vec<String> {
    let mut fleet: Vec<String> =
        felix_common::fleet::prerequisites(felix_common::fleet::REPLICA_ELECTION.name())
            .iter()
            .map(|feature| feature.name().to_owned())
            .collect();
    fleet.push(felix_common::fleet::REPLICA_ELECTION.name().to_owned());
    fleet
}

/// A data directory that qualifies to start from its view.
fn qualifying_directory() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("dir");
    let (id, _) =
        felix_storage::storage_identity::load_or_create(dir.path(), || [7; 16]).expect("id");
    let view = crate::cluster::controlplane_view::View {
        node_id: "broker-a".to_string(),
        storage_id: id.to_hex(),
        incarnation: 4,
        fleet: election_fleet(),
        routes: Vec::new(),
        nodes: Vec::new(),
        catalog: Default::default(),
    };
    felix_storage::controlplane_view::record(
        dir.path(),
        &serde_json::to_vec(&view).expect("encode"),
    )
    .expect("record");
    dir
}

/// An address nothing listens on.
fn down() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    format!("http://{addr}")
}

#[tokio::test]
async fn with_the_switch_on_a_down_control_plane_starts_from_the_view() {
    let dir = qualifying_directory();
    let config = member(&down(), true);
    let started = tokio::time::timeout(
        Duration::from_secs(10),
        start(
            &config,
            Some(dir.path()),
            &reqwest::Client::new(),
            &Some(NodeCredential::new("a-node-token".to_string())),
        ),
    )
    .await
    .expect("the wait is bounded")
    .expect("start");
    assert!(matches!(started, Start::FromView(_)), "{started:?}");
}

/// **The switch is off by default, and off is today's start**: the broker
/// waits for the control plane however good its view is.
#[tokio::test]
async fn with_the_switch_off_a_down_control_plane_is_waited_for() {
    assert!(!BrokerConfig::default().start_without_controlplane);
    let dir = qualifying_directory();
    let config = member(&down(), false);
    let waiting = tokio::time::timeout(
        Duration::from_millis(1500),
        start(
            &config,
            Some(dir.path()),
            &reqwest::Client::new(),
            &Some(NodeCredential::new("a-node-token".to_string())),
        ),
    )
    .await;
    assert!(
        waiting.is_err(),
        "started without the control plane: {waiting:?}"
    );
}

/// A control plane that is up and does not accept the credential is not
/// down, so it is waited for.
#[tokio::test]
async fn a_control_plane_refusing_the_credential_is_waited_for() {
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}",
        axum::routing::get(|| async { axum::http::StatusCode::UNAUTHORIZED }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = crate::test_support::spawn_axum_with_shutdown(listener, app);
    crate::test_support::wait_for_listen(addr)
        .await
        .expect("listen");
    let dir = qualifying_directory();
    let config = member(&format!("http://{addr}"), true);
    let waiting = tokio::time::timeout(
        Duration::from_millis(1500),
        start(
            &config,
            Some(dir.path()),
            &reqwest::Client::new(),
            &Some(NodeCredential::new("a-node-token".to_string())),
        ),
    )
    .await;
    assert!(waiting.is_err(), "started over a refusal: {waiting:?}");
    let _ = stop.send(());
    let _ = handle.await;
}

/// Without a view, clients wait for `seeded` as they always did and nothing
/// waits on a restore check. After a start from the view, `seeded` (which
/// registration waits on) does not open clients, and the check is pending.
#[test]
fn the_gates_follow_the_start() {
    let seeded = CancellationToken::new();
    let usual = Gates::new(false, &seeded);
    let from_view = Gates::new(true, &seeded);
    assert!(usual.restore_checked.is_cancelled());
    assert!(!from_view.restore_checked.is_cancelled());
    seeded.cancel();
    assert!(usual.clients.is_cancelled(), "clients follow seeded");
    assert!(
        !from_view.clients.is_cancelled(),
        "clients of a start from the view wait for registration"
    );
}

/// A start from the view whose run rotated the storage id waits for the
/// control plane and records a replaced directory from its answer.
#[tokio::test]
async fn a_start_whose_run_rotated_the_id_waits_and_notes_a_replacement() {
    let dir = qualifying_directory();
    let view = crate::cluster::controlplane_view::read(dir.path(), "broker-a").expect("view");
    let rotated = felix_storage::StorageId::from_bytes([9; 16]);
    let app = axum::Router::new().route(
        "/v1/nodes/{node_id}",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({
                "node": {
                    "node_id": "broker-a",
                    "status": {
                        "incarnation": 4,
                        "lifecycle": "down",
                        "storage_id": "07070707070707070707070707070707",
                    },
                },
                "placement": { "eligible": false },
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = crate::test_support::spawn_axum_with_shutdown(listener, app);
    crate::test_support::wait_for_listen(addr)
        .await
        .expect("listen");
    let config = member(&format!("http://{addr}"), true);
    let credential = Some(NodeCredential::new("a-node-token".to_string()));
    let client = reqwest::Client::new();

    // Still under the registered id: the view is kept and nothing is asked.
    let (kept, replaced) = keep_view_if_opened_under(
        view.clone(),
        dir.path(),
        felix_storage::StorageId::from_bytes([7; 16]),
        &config,
        &client,
        &credential,
    )
    .await
    .expect("kept");
    assert!(kept.is_some() && !replaced);

    let (kept, replaced) =
        keep_view_if_opened_under(view, dir.path(), rotated, &config, &client, &credential)
            .await
            .expect("waited");
    assert!(kept.is_none(), "a rotated id does not go on from the view");
    assert!(replaced, "the control plane names the old id");
    assert!(felix_storage::run_state::replaced(dir.path()).expect("marker"));
    let _ = stop.send(());
    let _ = handle.await;
}

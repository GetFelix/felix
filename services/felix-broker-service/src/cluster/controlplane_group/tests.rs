use super::*;

fn identity(group_id: Option<&str>, engine: &str) -> Identity {
    Identity {
        group_id: group_id.map(str::to_owned),
        shard_engine: engine.to_owned(),
    }
}

/// A directory a broker has run on.
fn used() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("dir");
    felix_storage::storage_identity::load_or_create(dir.path(), || [1; 16]).expect("id");
    dir
}

#[test]
fn the_record_round_trips() {
    let dir = tempfile::tempdir().expect("dir");
    assert_eq!(read(dir.path()).expect("read"), None);
    write(dir.path(), "g-1").expect("write");
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-1"));
    write(dir.path(), "g-2").expect("overwrite");
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-2"));
    assert!(!dir.path().join("controlplane_group.tmp").exists());
}

#[test]
fn an_empty_record_is_an_error_not_no_record() {
    let dir = tempfile::tempdir().expect("dir");
    std::fs::write(dir.path().join(FILE_NAME), "").expect("write");
    assert!(read(dir.path()).is_err());
}

#[test]
fn a_new_directory_holds_no_data_and_a_used_one_does() {
    let dir = tempfile::tempdir().expect("dir");
    assert!(!holds_data(dir.path()));
    assert!(!holds_data(&dir.path().join("not-yet-created")));
    assert!(holds_data(used().path()));
}

#[test]
fn the_decision_table() {
    assert_eq!(decide(Some("a"), Some("a"), true), Verdict::Matches);
    assert_eq!(
        decide(Some("a"), Some("b"), true),
        Verdict::Other {
            stored: "a".to_owned()
        }
    );
    assert_eq!(decide(None, Some("b"), false), Verdict::Adopt);
    assert_eq!(decide(None, Some("b"), true), Verdict::Unrecorded);
    assert_eq!(decide(Some("a"), None, true), Verdict::Skip);
    assert_eq!(decide(None, None, false), Verdict::Skip);
}

#[test]
fn a_new_directory_adopts_the_group() {
    let dir = tempfile::tempdir().expect("dir");
    let group = admit(dir.path(), &identity(Some("g-1"), "hybrid")).expect("admit");
    assert_eq!(group.as_deref(), Some("g-1"));
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-1"));
}

#[test]
fn a_matching_directory_goes_on() {
    let dir = used();
    write(dir.path(), "g-1").expect("write");
    let group = admit(dir.path(), &identity(Some("g-1"), "hybrid")).expect("admit");
    assert_eq!(group.as_deref(), Some("g-1"));
}

#[test]
fn a_directory_from_another_group_is_refused_and_left_alone() {
    let dir = used();
    write(dir.path(), "g-1").expect("write");
    let err = admit(dir.path(), &identity(Some("g-2"), "hybrid")).expect_err("refused");
    let text = err.to_string();
    assert!(text.contains("g-1") && text.contains("g-2"), "{text}");
    assert!(text.contains("wipe its whole data directory"), "{text}");
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-1"));
}

#[test]
fn a_directory_with_data_and_no_record_is_refused_not_adopted() {
    let dir = used();
    let err = admit(dir.path(), &identity(Some("g-1"), "hybrid")).expect_err("refused");
    assert!(err.to_string().contains("wipe its whole data directory"));
    assert_eq!(read(dir.path()).expect("read"), None);
}

#[test]
fn a_control_plane_without_a_group_stores_nothing() {
    let dir = used();
    assert_eq!(
        admit(dir.path(), &identity(None, "hybrid")).expect("admit"),
        None
    );
    assert_eq!(read(dir.path()).expect("read"), None);
}

#[test]
fn another_shard_engine_is_refused_whatever_the_group() {
    let dir = tempfile::tempdir().expect("dir");
    let err = admit(dir.path(), &identity(Some("g-1"), "raft")).expect_err("refused");
    let text = err.to_string();
    assert!(text.contains("runs shard engine hybrid"), "{text}");
    assert!(text.contains("FELIX_SHARD_ENGINE=raft"), "{text}");
    assert_eq!(read(dir.path()).expect("read"), None, "nothing is recorded");
    assert!(admit(dir.path(), &identity(None, "raft")).is_err());
}

#[test]
fn the_guard_refuses_only_another_group() {
    let guard = Guard {
        stored: "g-1".to_owned(),
        refused: CancellationToken::new(),
    };
    assert!(guard.check(Some("g-1")).is_ok());
    assert!(guard.check(None).is_ok());
    assert!(guard.check(Some("g-2")).is_err());
}

/// A control plane that answers 503 once, then `body`, to its identity route.
async fn serve_identity(
    body: serde_json::Value,
) -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let router = axum::Router::new().route(
        "/v1/cluster/identity",
        axum::routing::get(move || {
            let asked = std::sync::Arc::clone(&asked);
            let body = body.clone();
            async move {
                if asked.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    axum::Json(body).into_response()
                } else {
                    StatusCode::SERVICE_UNAVAILABLE.into_response()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = crate::test_support::spawn_axum_with_shutdown(listener, router);
    drop(handle);
    (addr, stop)
}

#[tokio::test]
async fn the_identity_is_asked_again_until_the_control_plane_answers() {
    let (addr, stop) =
        serve_identity(serde_json::json!({ "group_id": "g-1", "shard_engine": "hybrid" })).await;
    let client = crate::test_support::build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("token");
    let identity = tokio::time::timeout(
        Duration::from_secs(10),
        await_identity(&client, &format!("http://{addr}"), &credential),
    )
    .await
    .expect("answers after the 503");
    assert_eq!(
        identity,
        super::Identity {
            group_id: Some("g-1".to_owned()),
            shard_engine: "hybrid".to_owned(),
        }
    );
    let _ = stop.send(());
}

#[tokio::test]
async fn an_identity_without_a_group_reads_as_none() {
    let (addr, stop) = serve_identity(serde_json::json!({ "shard_engine": "hybrid" })).await;
    let client = crate::test_support::build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("token");
    let identity = tokio::time::timeout(
        Duration::from_secs(10),
        await_identity(&client, &format!("http://{addr}"), &credential),
    )
    .await
    .expect("answers");
    assert_eq!(identity.group_id, None);
    let _ = stop.send(());
}

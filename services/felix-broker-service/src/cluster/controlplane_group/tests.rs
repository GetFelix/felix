use super::*;

fn identity(group_id: Option<&str>, engine: &str) -> Identity {
    Identity {
        group_id: group_id.map(str::to_owned),
        shard_engine: engine.to_owned(),
        adopts_existing: false,
    }
}

/// A group that recorded its id over a catalog it already held.
fn upgraded(group_id: &str) -> Identity {
    Identity {
        adopts_existing: true,
        ..identity(Some(group_id), "hybrid")
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

/// Before 0.6.0-preview.8 there was no storage id: logs alone are data.
#[test]
fn a_directory_without_a_storage_id_still_holds_data() {
    let dir = tempfile::tempdir().expect("dir");
    std::fs::create_dir(dir.path().join("t1")).expect("mkdir");
    assert!(holds_data(dir.path()));
    let file = tempfile::tempdir().expect("dir");
    std::fs::write(file.path().join("anything"), b"x").expect("write");
    assert!(holds_data(file.path()));
}

/// The record, its temporary file and a fresh filesystem's lost+found are
/// not data.
#[test]
fn the_record_and_lost_and_found_are_not_data() {
    let dir = tempfile::tempdir().expect("dir");
    std::fs::create_dir(dir.path().join("lost+found")).expect("mkdir");
    std::fs::write(dir.path().join("controlplane_group.tmp"), b"").expect("tmp");
    write(dir.path(), "g-1").expect("write");
    assert!(!holds_data(dir.path()));
}

/// A root that is a file cannot be listed: data, not empty.
#[test]
fn a_root_that_cannot_be_listed_holds_data() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").expect("write");
    assert!(holds_data(&file));
}

#[test]
fn the_decision_table() {
    assert_eq!(decide(Some("a"), Some("a"), false, true), Verdict::Matches);
    for adopts_existing in [false, true] {
        assert_eq!(
            decide(Some("a"), Some("b"), adopts_existing, true),
            Verdict::Other {
                stored: "a".to_owned()
            }
        );
    }
    assert_eq!(decide(None, Some("b"), false, false), Verdict::Adopt);
    assert_eq!(decide(None, Some("b"), false, true), Verdict::Unrecorded);
    assert_eq!(decide(None, Some("b"), true, true), Verdict::Adopt);
    assert_eq!(decide(Some("a"), None, false, true), Verdict::Skip);
    assert_eq!(decide(None, None, false, false), Verdict::Skip);
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
    restore_comes_before_wipe(&text);
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-1"));
}

#[test]
fn a_directory_with_data_and_no_record_is_refused_not_adopted() {
    let dir = used();
    let err = admit(dir.path(), &identity(Some("g-1"), "hybrid")).expect_err("refused");
    restore_comes_before_wipe(&err.to_string());
    assert_eq!(read(dir.path()).expect("read"), None);
}

/// A refusal tells the operator to restore the control plane's export before
/// wiping anything.
fn restore_comes_before_wipe(text: &str) {
    let restore = text.find("`import`").expect("names the import");
    let wipe = text
        .find("wipe its whole data directory")
        .expect("names the wipe");
    assert!(restore < wipe, "{text}");
}

/// An in-place upgrade: the group recorded its id over its existing catalog,
/// so a directory with data from before the record is this group's.
#[test]
fn an_upgraded_group_adopts_a_directory_with_data_and_no_record() {
    let dir = used();
    let group = admit(dir.path(), &upgraded("g-1")).expect("adopted");
    assert_eq!(group.as_deref(), Some("g-1"));
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-1"));
}

/// The control plane lost its volumes, re-formed on an empty catalog, and
/// then restored an export taken before identities existed. It keeps its new
/// id but now adopts, so a broker refused before the restore starts after it.
#[test]
fn a_restore_of_an_export_without_identity_lets_a_refused_directory_adopt() {
    let dir = used();
    admit(dir.path(), &identity(Some("g-new"), "hybrid")).expect_err("refused before");
    let group = admit(dir.path(), &upgraded("g-new")).expect("adopted after the restore");
    assert_eq!(group.as_deref(), Some("g-new"));
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-new"));
}

/// The flag never overrides a record of another group.
#[test]
fn an_upgraded_group_still_refuses_another_groups_record() {
    let dir = used();
    write(dir.path(), "g-1").expect("write");
    admit(dir.path(), &upgraded("g-2")).expect_err("refused");
    assert_eq!(read(dir.path()).expect("read").as_deref(), Some("g-1"));
}

#[test]
fn a_control_plane_without_a_group_stores_no_group_but_records_the_check() {
    let dir = used();
    assert!(!checked_without_group(dir.path()));
    assert_eq!(
        admit(dir.path(), &identity(None, "hybrid")).expect("admit"),
        None
    );
    assert_eq!(read(dir.path()).expect("read"), None);
    assert!(checked_without_group(dir.path()));
}

#[test]
fn a_directory_with_a_record_is_not_marked_groupless() {
    let dir = used();
    write(dir.path(), "g-1").expect("write");
    admit(dir.path(), &identity(None, "hybrid")).expect("admit");
    assert!(!checked_without_group(dir.path()));
}

/// The mark is not data: a group formed later on an empty catalog still
/// adopts a directory that only ever held the mark.
#[test]
fn the_no_group_mark_is_not_data() {
    let dir = tempfile::tempdir().expect("dir");
    admit(dir.path(), &identity(None, "hybrid")).expect("admit");
    assert!(!holds_data(dir.path()));
    assert_eq!(
        admit(dir.path(), &identity(Some("g-1"), "hybrid"))
            .expect("admit")
            .as_deref(),
        Some("g-1")
    );
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
fn the_guard_takes_only_its_own_group() {
    let guard = Guard {
        stored: "g-1".to_owned(),
        refused: CancellationToken::new(),
    };
    assert!(guard.check(Some("g-1"), "page").is_ok());
    assert!(matches!(
        guard.check(None, "page"),
        Err(Refusal::Unclaimed(_))
    ));
    assert!(matches!(
        guard.check(Some("g-2"), "page"),
        Err(Refusal::Mismatch(_))
    ));
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
            adopts_existing: false,
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

#[tokio::test]
async fn an_identity_records_whether_it_adopts_existing_data() {
    let (addr, stop) = serve_identity(serde_json::json!({
        "group_id": "g-1", "shard_engine": "hybrid", "adopts_existing": true,
    }))
    .await;
    let client = crate::test_support::build_test_client().expect("client");
    let credential = crate::cluster::credential::NodeCredential::new("token");
    let identity = tokio::time::timeout(
        Duration::from_secs(10),
        await_identity(&client, &format!("http://{addr}"), &credential),
    )
    .await
    .expect("answers");
    assert!(identity.adopts_existing);
    let _ = stop.send(());
}

/// A control plane that answers `status` to its identity route.
async fn failing_with(status: u16) -> anyhow::Error {
    let status = axum::http::StatusCode::from_u16(status).expect("status");
    let router = axum::Router::new().route(
        "/v1/cluster/identity",
        axum::routing::get(move || async move { status }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (stop, handle) = crate::test_support::spawn_axum_with_shutdown(listener, router);
    drop(handle);
    let client = crate::test_support::build_test_client().expect("client");
    let err = fetch(&client, &format!("http://{addr}"), "token")
        .await
        .expect_err("fails");
    let _ = stop.send(());
    err
}

#[tokio::test]
async fn a_failed_ask_says_what_to_fix() {
    assert_eq!(Failure::of(&failing_with(403).await), Failure::Forbidden);
    assert_eq!(Failure::of(&failing_with(404).await), Failure::Older);
    assert_eq!(Failure::of(&failing_with(503).await), Failure::Other);
}

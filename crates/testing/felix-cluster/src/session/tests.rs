//! The file a second terminal uses to find a running cluster.
//!
//! `remove_if_ours` carries the invariant worth testing: two clusters share one
//! path, so the second to start takes the file over, and the second to *stop*
//! must not delete a session describing a cluster that is still running. Getting
//! that wrong leaves three brokers alive holding their ports with no way to
//! address them — which is exactly the bug this method was added for.
use super::*;

fn session(control_plane: &str) -> Session {
    Session {
        control_plane: control_plane.to_string(),
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        client_token: "client".to_string(),
        admin_token: "admin".to_string(),
        nodes: vec![
            SessionNode {
                node_id: "broker-0".to_string(),
                client_addr: "127.0.0.1:7000".parse().expect("addr"),
                metrics_addr: "127.0.0.1:8000".parse().expect("addr"),
                cert_file: None,
            },
            SessionNode {
                node_id: "broker-1".to_string(),
                client_addr: "127.0.0.1:7001".parse().expect("addr"),
                metrics_addr: "127.0.0.1:8001".parse().expect("addr"),
                cert_file: None,
            },
        ],
    }
}

fn temp() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("felix-cluster.json");
    (dir, path)
}

#[test]
fn a_session_round_trips_through_the_file() {
    let (_dir, path) = temp();
    let written = session("http://127.0.0.1:9000");
    written.write(&path).expect("write");

    let read = Session::read(&path).expect("read");
    assert_eq!(read.control_plane, written.control_plane);
    assert_eq!(read.client_token, written.client_token);
    assert_eq!(read.admin_token, written.admin_token);
    assert_eq!(read.nodes.len(), 2);
    assert_eq!(read.nodes[1].client_addr, written.nodes[1].client_addr);
}

/// The file holds a bearer token, so it must not be world-readable.
#[cfg(unix)]
#[test]
fn the_file_is_written_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let (_dir, path) = temp();
    session("http://127.0.0.1:9000")
        .write(&path)
        .expect("write");

    let mode = std::fs::metadata(&path).expect("stat").permissions().mode();
    assert_eq!(
        mode & 0o077,
        0,
        "a file carrying a bearer token must not be readable by anyone else",
    );
}

/// Not finding one is the common case — no cluster is running — so the error
/// says what to do rather than naming a missing file.
#[test]
fn a_missing_session_explains_how_to_start_one() {
    let (_dir, path) = temp();
    let err = Session::read(&path).expect_err("nothing written yet");
    assert!(
        err.to_string().contains("task cluster:up"),
        "the error should point at the fix: {err}",
    );
}

#[test]
fn a_corrupt_session_is_an_error_rather_than_a_default() {
    let (_dir, path) = temp();
    std::fs::write(&path, b"{ not json").expect("write");
    assert!(Session::read(&path).is_err());
}

#[test]
fn nodes_are_addressable_by_id() {
    let s = session("http://127.0.0.1:9000");
    assert_eq!(
        s.node("broker-1").expect("broker-1").client_addr.port(),
        7001
    );
    assert!(s.node("broker-9").is_none());
}

/// The straightforward case: a cluster removes the session it wrote.
#[test]
fn a_cluster_removes_its_own_session() {
    let (_dir, path) = temp();
    let mine = session("http://127.0.0.1:9000");
    mine.write(&path).expect("write");

    mine.remove_if_ours(&path);
    assert!(!path.exists(), "a cluster should clean up after itself");
}

/// **The invariant.** A second cluster took the file over; stopping the first
/// must leave it alone, or the second is left running and unreachable.
#[test]
fn a_cluster_does_not_remove_a_session_that_another_took_over() {
    let (_dir, path) = temp();
    let first = session("http://127.0.0.1:9000");
    first.write(&path).expect("write");

    let second = session("http://127.0.0.1:9999");
    second.write(&path).expect("overwrite");

    first.remove_if_ours(&path);

    assert!(path.exists(), "the second cluster's session was deleted");
    assert_eq!(
        Session::read(&path).expect("read").control_plane,
        "http://127.0.0.1:9999",
        "and it must still describe the cluster that is actually running",
    );
}

/// Removing twice, or removing when nothing is there, is not an error: teardown
/// runs on paths that may already have been cleaned.
#[test]
fn removing_an_absent_session_is_harmless() {
    let (_dir, path) = temp();
    let mine = session("http://127.0.0.1:9000");
    mine.remove_if_ours(&path);
    mine.write(&path).expect("write");
    mine.remove_if_ours(&path);
    mine.remove_if_ours(&path);
    assert!(!path.exists());
}

/// A file that cannot be parsed is not ours to delete either — it may belong to
/// a newer harness whose format this one does not know.
#[test]
fn a_corrupt_session_is_left_alone() {
    let (_dir, path) = temp();
    std::fs::write(&path, b"{ not json").expect("write");
    session("http://127.0.0.1:9000").remove_if_ours(&path);
    assert!(path.exists());
}

#[test]
fn the_default_path_is_in_the_temp_directory() {
    let path = default_path();
    assert!(path.starts_with(std::env::temp_dir()));
    assert_eq!(
        path.file_name().and_then(|n| n.to_str()),
        Some("felix-cluster.json"),
    );
}

/// `live_session` decides whether another cluster is still answering. A file
/// pointing at a control plane that is gone must read as "no live cluster",
/// otherwise the panes attach to something that no longer exists.
#[tokio::test]
async fn a_session_pointing_at_a_dead_control_plane_is_not_live() {
    let (_dir, path) = temp();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    session(&format!("http://{addr}"))
        .write(&path)
        .expect("write");
    assert!(live_session(&path).await.is_none());
}

#[tokio::test]
async fn no_session_file_means_no_live_cluster() {
    let (_dir, path) = temp();
    assert!(live_session(&path).await.is_none());
}

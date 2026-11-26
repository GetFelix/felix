//! The operator's storage id rotation.

use super::*;

#[test]
fn the_command_gives_the_directory_a_new_id_and_starts_the_raft_marks_over() {
    let dir = tempfile::tempdir().expect("dir");
    let root = dir.path();
    let (first, _) = felix_storage::storage_identity::load_or_create(root, || [3; 16]).expect("id");
    std::fs::write(root.join("raft-marks"), b"marks").expect("marks");

    run(vec!["--dir".to_string(), root.display().to_string()]).expect("rotate");

    let now = felix_storage::storage_identity::load(root)
        .expect("read")
        .expect("an id");
    assert_ne!(now, first);
    assert!(!root.join("raft-marks").exists());
}

#[test]
fn a_directory_without_a_storage_id_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let err = rotate(dir.path()).expect_err("no id");
    assert!(err.to_string().contains("no storage id"), "{err}");
    assert!(
        felix_storage::storage_identity::load(dir.path())
            .expect("read")
            .is_none()
    );
}

/// **A directory a broker holds is refused**, its id unchanged: the broker
/// would keep serving and voting under the id the command rotated away.
#[test]
fn a_directory_a_broker_holds_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let root = dir.path();
    let (first, _) = felix_storage::storage_identity::load_or_create(root, || [3; 16]).expect("id");
    let broker = crate::node::storage::lock_root(root).expect("the broker's lock");

    let err = rotate(root).expect_err("rotated under a running broker");
    let text = format!("{err:#}");
    assert!(
        text.contains(&root.display().to_string()) && text.contains("another broker process"),
        "{text}"
    );
    let now = felix_storage::storage_identity::load(root)
        .expect("read")
        .expect("an id");
    assert_eq!(now, first, "the id changed under the broker");
    drop(broker);
    rotate(root).expect("rotate once the broker stopped");
}

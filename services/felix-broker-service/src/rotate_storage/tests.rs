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

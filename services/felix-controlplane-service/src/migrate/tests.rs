use super::*;

/// The export holds signing-key seeds; nobody but its owner reads it.
#[cfg(unix)]
#[test]
fn an_export_is_readable_only_by_its_owner() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.json");
    write_private(&path, b"{}").expect("write");
    let mode = std::fs::metadata(&path)
        .expect("metadata")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "mode {mode:o}");
    assert_eq!(std::fs::read(&path).expect("read"), b"{}");
}

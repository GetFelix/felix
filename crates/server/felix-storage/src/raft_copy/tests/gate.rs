//! The gate: a root put back below a copy of its sequence gets a new
//! storage id before any member opens.

use super::*;

/// Three mark writes, so the root's sequence is 3.
async fn three_writes(root: &Root) {
    let copies = root.open();
    let marks = copies.mark_log();
    marks.fresh(Arc::from(SHARD), 5).await.expect("fresh");
    let copy = marks.for_copy(Arc::from(SHARD), 5);
    copy.vote(vote(1, "a#5", false)).await.expect("vote");
    copy.vote(vote(2, "a#5", false)).await.expect("vote");
    assert_eq!(marks.sequence(), 3);
}

fn marks_file(root: &Root) -> PathBuf {
    root.path().join(super::super::marks::FILE_NAME)
}

/// A copy of the root taken during the run, put back after more votes:
/// its incarnation and run token are current, and only the sequence shows
/// it went back.
#[tokio::test]
async fn a_root_behind_the_witness_fails_the_gate_and_rotates() {
    let root = Root::new();
    three_writes(&root).await;
    let backup = std::fs::read(marks_file(&root)).expect("backup");
    {
        let copies = root.open();
        copies
            .mark_log()
            .for_copy(Arc::from(SHARD), 5)
            .vote(vote(3, "a#5", false))
            .await
            .expect("vote");
    }
    std::fs::write(marks_file(&root), backup).expect("restore");

    let before = storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    let behind = match root.gate(None) {
        Gate::Behind(behind) => behind,
        Gate::Passed(_) => panic!("a root behind its witness passed"),
    };
    assert_eq!((behind.root, behind.witness), (3, Some(4)));
    let passed = behind.rotate(root.path(), [9; 16]).expect("rotate");
    let after = storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    assert_ne!(after, before);
    assert_eq!(after, StorageId::from_bytes([9; 16]));
    let copies = CopyRoot::open(root.path(), passed, minter());
    assert_eq!(copies.storage_id(), after);
    drop(copies);
    // The witness names the old id now, so the rotated root passes.
    assert!(matches!(root.gate(None), Gate::Passed(_)));
}

#[tokio::test]
async fn a_root_behind_the_control_plane_fails_the_gate() {
    let root = Root::new();
    three_writes(&root).await;
    let gate = check(root.path(), None, Some(4)).expect("gate");
    match gate {
        Gate::Behind(behind) => {
            assert_eq!(
                (behind.root, behind.witness, behind.control_plane),
                (3, None, Some(4))
            );
        }
        Gate::Passed(_) => panic!("a root behind the control plane passed"),
    }
}

/// A crash between the root's write and a copy's leaves the root ahead,
/// which is no restore.
#[tokio::test]
async fn a_root_ahead_of_its_copies_passes() {
    let root = Root::new();
    three_writes(&root).await;
    let id = storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    crate::run_state::witness::record_sequence(root.path(), Some(root.shm.path()), id, 2);
    assert!(matches!(root.gate(Some(1)), Gate::Passed(_)));
    assert!(matches!(root.gate(Some(3)), Gate::Passed(_)));
    assert!(matches!(root.gate(Some(4)), Gate::Behind(_)));
}

/// A record under another storage id is from before a rotation.
#[tokio::test]
async fn a_witness_of_another_storage_id_is_not_compared() {
    let root = Root::new();
    three_writes(&root).await;
    crate::run_state::witness::record_sequence(
        root.path(),
        Some(root.shm.path()),
        StorageId::from_bytes([7; 16]),
        100,
    );
    assert!(matches!(root.gate(None), Gate::Passed(_)));
}

#[test]
fn a_root_with_no_storage_id_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let err = check(dir.path(), None, None).err().expect("no id");
    assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
}

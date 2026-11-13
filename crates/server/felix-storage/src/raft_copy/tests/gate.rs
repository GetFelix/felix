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
        other => panic!("a root behind its witness was not behind: {other:?}"),
    };
    assert_eq!((behind.root, behind.witness), (3, Some(4)));
    let passed = behind.rotate([9; 16]).expect("rotate");
    let after = storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    assert_ne!(after, before);
    assert_eq!(after, StorageId::from_bytes([9; 16]));
    let copies = CopyRoot::open(root.path(), passed, minter());
    assert_eq!(copies.storage_id(), after);
    // The rotation started the mark log over.
    assert_eq!(copies.mark_log().sequence(), 0);
    assert_eq!(copies.mark_log().marks(SHARD), None);
    drop(copies);
    assert!(matches!(root.gate(None), Gate::Passed(_)));
    // And it passes after a reboot too, with no witness and no control-plane
    // copy: a rotated root has nothing a copy could vote from.
    root.forget_witness();
    assert!(matches!(
        check(root.path(), NO_EVIDENCE).expect("gate"),
        Gate::Passed(_)
    ));
}

/// The rotations outside the gate (a restore, a crash under relaxed fsync,
/// the operator's command) start the mark log over too. Kept, its sequence
/// would have no copy under the new id, and a reboot would hold the root for
/// good.
#[tokio::test]
async fn a_rotation_outside_the_gate_starts_the_mark_log_over() {
    let root = Root::new();
    three_writes(&root).await;
    crate::run_state::copy_trust::rotate_storage_id(root.path(), [8; 16]).expect("rotate");
    assert!(!marks_file(&root).exists());
    root.forget_witness();
    match check(root.path(), NO_EVIDENCE).expect("gate") {
        Gate::Passed(passed) => assert_eq!(format!("{passed:?}"), "GatePassed { sequence: 0 }"),
        other => panic!("a rotated root was not passed: {other:?}"),
    }
}

/// The control plane holds its copy per storage id. One under another id is
/// from before a rotation and is not compared, so it neither fails a root
/// nor vouches for one.
#[tokio::test]
async fn a_control_plane_copy_of_another_storage_id_is_not_compared() {
    let root = Root::new();
    three_writes(&root).await;
    root.forget_witness();
    let other = |sequence| ControlPlaneCopy {
        storage_id: StorageId::from_bytes([7; 16]),
        sequence,
    };
    for copy in [other(100), other(3)] {
        let gate = check(
            root.path(),
            Evidence {
                witness_dir: Some(root.shm.path()),
                control_plane: Some(copy),
                restored: false,
            },
        )
        .expect("gate");
        assert!(matches!(gate, Gate::Unchecked(_)), "{gate:?}");
    }
}

#[tokio::test]
async fn a_root_behind_the_control_plane_fails_the_gate() {
    let root = Root::new();
    three_writes(&root).await;
    let gate = check(
        root.path(),
        Evidence {
            witness_dir: None,
            control_plane: Some(root.copy(4)),
            restored: false,
        },
    )
    .expect("gate");
    match gate {
        Gate::Behind(behind) => {
            assert_eq!(
                (behind.root, behind.witness, behind.control_plane),
                (3, None, Some(4))
            );
        }
        other => panic!("a root behind the control plane was not behind: {other:?}"),
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

/// A record under another storage id is from before a rotation, so it is
/// no copy of this root's sequence.
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
    assert!(matches!(root.gate(None), Gate::Unchecked(_)));
    assert!(matches!(root.gate(Some(3)), Gate::Passed(_)));
}

/// A VM snapshot put back and the machine rebooted: the witness is empty
/// and the control plane has no sequence. The marks went back with the
/// directories, so nothing at the root can show it; the root waits for
/// evidence instead of opening its copies with rolled-back logs.
#[tokio::test]
async fn a_rebooted_root_with_marks_and_no_copy_of_its_sequence_is_unchecked() {
    let root = Root::new();
    three_writes(&root).await;
    root.forget_witness();
    match root.gate(None) {
        Gate::Unchecked(unchecked) => assert_eq!(unchecked.root, 3),
        other => panic!("a root nothing vouches for was not held: {other:?}"),
    }
    // The same on a host with no witness at all.
    assert!(matches!(
        check(root.path(), NO_EVIDENCE).expect("gate"),
        Gate::Unchecked(_)
    ));
    // A control-plane copy it is not behind lets it pass.
    assert!(matches!(root.gate(Some(3)), Gate::Passed(_)));
}

/// A root with no marks has no copy that could vote with a rolled-back log.
#[test]
fn a_root_with_no_marks_passes_without_a_copy() {
    let root = Root::new();
    assert!(matches!(
        check(root.path(), NO_EVIDENCE).expect("gate"),
        Gate::Passed(_)
    ));
}

/// The incarnation verdict alone is enough: a root from before the node's
/// last registration is behind even when its sequence matches.
#[tokio::test]
async fn a_root_behind_the_incarnation_is_behind() {
    let root = Root::new();
    three_writes(&root).await;
    let gate = check(
        root.path(),
        Evidence {
            witness_dir: Some(root.shm.path()),
            control_plane: Some(root.copy(3)),
            restored: true,
        },
    )
    .expect("gate");
    match gate {
        Gate::Behind(behind) => assert!(behind.restored),
        other => panic!("a root behind the incarnation was not behind: {other:?}"),
    }
}

#[test]
fn a_root_with_no_storage_id_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let err = check(dir.path(), NO_EVIDENCE).expect_err("no id");
    assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
}

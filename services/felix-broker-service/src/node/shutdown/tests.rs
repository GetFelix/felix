//! A stop is recorded as clean only when nothing it acknowledged can still be
//! unsynced.
use super::*;

#[test]
fn only_a_full_flush_with_no_writer_cut_short_is_clean() {
    assert!(stopped_cleanly(true, &[], false));
    assert!(!stopped_cleanly(false, &[], false), "a failed flush");
    assert!(
        !stopped_cleanly(true, &["quic_connections"], false),
        "a step still running may write after the flush"
    );
    assert!(
        !stopped_cleanly(true, &[], true),
        "a poisoned log may have dropped pages it acknowledged"
    );
    assert!(
        stopped_cleanly(true, &["membership_deregister"], false),
        "a control plane that is down does not make the stop unclean"
    );
}

/// **A run whose Raft restore gate rotated the id stops clean under the new
/// id.** The restart keeps it: recording the stop under the id the run began
/// with would read as a relaxed crash and rotate a second time.
#[test]
fn a_clean_stop_after_a_rotation_in_the_run_keeps_the_new_id() {
    use felix_storage::run_state::{self, BootId};
    let dir = tempfile::tempdir().expect("dir");
    let boot = Some(BootId::from_bytes([3; 16]));
    let begun =
        run_state::begin(dir.path(), true, None, boot, || false, || [1; 16]).expect("begin");
    // The gate's rotation, as `Behind::rotate` makes it.
    let rotated = run_state::copy_trust::rotate_storage_id(dir.path(), [2; 16]).expect("rotate");
    assert_ne!(rotated, begun.id);

    record_clean_stop(dir.path(), begun.id).expect("clean stop");

    let restart =
        run_state::begin(dir.path(), true, None, boot, || false, || [4; 16]).expect("restart");
    assert_eq!(restart.last_run, run_state::LastRun::Clean);
    assert!(!restart.rotated, "the restart rotated again");
    assert_eq!(restart.id, rotated);
}

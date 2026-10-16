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

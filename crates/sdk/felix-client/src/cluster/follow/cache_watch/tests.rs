use super::WatchProgress;

#[test]
fn a_live_watch_resumes_after_the_last_change_handed_out() {
    let mut watch = WatchProgress::new(10, None, None, false);
    assert_eq!(watch.resume_from, 10);
    assert!(!watch.observe_change(12));
    assert_eq!(watch.resume_from, 13);
}

/// Replay arrives in log order, so each replayed change moves the position,
/// and nothing before the requested offset is skipped.
#[test]
fn a_replay_resumes_after_what_was_replayed_not_where_live_began() {
    let mut watch = WatchProgress::new(50, Some(20), None, false);
    assert_eq!(watch.resume_from, 20);
    watch.observe_change(25);
    assert_eq!(watch.resume_from, 26);
}

/// Retained values arrive in key order with any offset below where live
/// began, so the position stays at 0 until the last one is handed out.
#[test]
fn retained_values_hold_the_position_until_the_state_phase_ends() {
    let mut watch = WatchProgress::new(100, None, Some(2), false);
    assert!(!watch.observe_change(90));
    assert_eq!(watch.resume_from, 0);
    assert!(
        watch.observe_change(40),
        "the last retained value ends the phase"
    );
    assert_eq!(watch.resume_from, 100);
    assert!(!watch.observe_change(105));
    assert_eq!(watch.resume_from, 106);
}

#[test]
fn a_resnapshot_only_moves_the_position_once_live() {
    let mut watch = WatchProgress::new(100, Some(5), None, true);
    watch.observe_change(90);
    watch.observe_change(40);
    assert_eq!(watch.resume_from, 5);
    watch.observe_change(101);
    assert_eq!(watch.resume_from, 102);
}

#[test]
fn a_lag_resumes_where_the_broker_says() {
    let mut watch = WatchProgress::new(10, None, None, false);
    watch.observe_change(11);
    watch.observe_lag(30);
    assert_eq!(watch.resume_from, 30);
}

#[test]
fn a_lag_mid_state_resumes_from_the_start() {
    let mut watch = WatchProgress::new(100, None, Some(3), false);
    watch.observe_change(70);
    watch.observe_lag(100);
    assert_eq!(watch.resume_from, 0);
}

/// The old owner's position is where every change below it was queued, so the
/// watch can skip to it; one handed out past it still counts.
#[test]
fn a_move_resumes_past_whichever_is_further() {
    let mut watch = WatchProgress::new(10, None, None, false);
    watch.observe_change(14);
    watch.observe_move(Some(20));
    assert_eq!(watch.resume_from, 20);

    let mut watch = WatchProgress::new(10, None, None, false);
    watch.observe_change(24);
    watch.observe_move(Some(20));
    assert_eq!(watch.resume_from, 25);
}

#[test]
fn a_move_without_a_position_resumes_after_the_last_change() {
    let mut watch = WatchProgress::new(10, None, None, false);
    watch.observe_change(14);
    watch.observe_move(None);
    assert_eq!(watch.resume_from, 15);
}

/// After the reopen, a resnapshot on the new owner arrives in key order, so
/// only live changes there move the position.
#[test]
fn a_resnapshot_after_a_move_only_moves_the_position_once_live() {
    let mut watch = WatchProgress::new(10, None, None, false);
    watch.observe_move(Some(20));
    watch.rebase(40, true);
    watch.observe_change(35);
    assert_eq!(watch.resume_from, 20);
    watch.observe_change(41);
    assert_eq!(watch.resume_from, 42);
}

/// A reopen inside the state phase asks for retained values again, so the
/// phase starts over with the new owner's count instead of ending.
#[test]
fn a_retained_reopen_mid_state_restarts_the_state_phase() {
    let mut watch = WatchProgress::new(100, None, Some(3), false);
    watch.observe_change(70);
    watch.reopened(120, false, Some(2));
    assert!(watch.in_state_phase());
    assert_eq!(watch.resume_from, 0);
    assert!(!watch.observe_change(90));
    assert!(
        watch.observe_change(110),
        "the new owner's last value ends it"
    );
    assert_eq!(watch.resume_from, 120);
}

#[test]
fn a_retained_reopen_onto_an_empty_state_ends_the_phase() {
    let mut watch = WatchProgress::new(100, None, Some(3), false);
    watch.reopened(120, false, Some(0));
    assert!(!watch.in_state_phase());
    assert_eq!(watch.resume_from, 120);
}

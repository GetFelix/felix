use super::{ReplayGate, shown_term};

/// A converted store's terms start at 2^62, which neither a float gauge nor
/// a signed status field holds exactly.
#[test]
fn a_term_past_the_conversion_base_is_shown_without_it() {
    assert_eq!(shown_term(0), 0);
    assert_eq!(shown_term(7), 7);
    assert_eq!(shown_term((1 << 62) - 1), (1 << 62) - 1);
    assert_eq!(shown_term(1 << 62), 0);
    assert_eq!(shown_term((1 << 62) + 7), 7);
}

/// Before openraft reports its first metrics both indexes read `None`. The
/// gate must stay shut then, or a probe in that window opens it at 0.
#[test]
fn the_replay_gate_stays_shut_before_the_first_metrics() {
    let gate = ReplayGate::new(Some(0), Some(4));
    assert!(gate.check(None, None).is_err());
    assert!(gate.check(Some(4), Some(1)).is_err());
    gate.check(Some(4), Some(4)).expect("applied the tail");
}

/// A leader that cut the log back below the tail leaves less to apply.
#[test]
fn the_replay_gate_opens_at_the_end_of_a_cut_log() {
    let gate = ReplayGate::new(Some(1), Some(6));
    gate.check(Some(3), Some(3)).expect("applied the cut log");
    let empty = ReplayGate::new(Some(4), Some(4));
    empty
        .check(None, None)
        .expect("nothing past the saved index");
}

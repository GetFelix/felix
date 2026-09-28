use super::*;

const SECOND: Duration = Duration::from_secs(1);

#[test]
fn an_empty_or_unrelated_file_is_the_true_clock() {
    assert_eq!(Setting::parse(""), None);
    assert_eq!(Setting::parse("# nothing\nunknown=3\n"), None);
    assert_eq!(Setting::parse("rate=fast\noffset_ms=soon"), None);
}

#[test]
fn a_file_parses_offset_and_rate() {
    assert_eq!(
        Setting::parse("offset_ms=-2500\nrate=0.5\n"),
        Some(Setting {
            offset_ms: -2500,
            rate: 0.5,
        }),
    );
    // A negative or non-finite rate is ignored, not obeyed.
    assert_eq!(
        Setting::parse("rate=-1\nrate=NaN\noffset_ms=7"),
        Some(Setting {
            offset_ms: 7,
            rate: 1.0,
        }),
    );
}

#[test]
fn an_offset_is_a_step_that_applies_at_once() {
    let mut skew = Skew::default();
    skew.set(Setting::parse("offset_ms=5000"), 10 * SECOND);
    assert_eq!(skew.offset_at(10 * SECOND), 5_000_000_000);
    assert_eq!(skew.offset_at(20 * SECOND), 5_000_000_000);

    skew.set(Setting::parse("offset_ms=-1000"), 20 * SECOND);
    assert_eq!(skew.offset_at(20 * SECOND), -1_000_000_000);
}

#[test]
fn a_rate_drifts_from_when_it_was_seen() {
    let mut skew = Skew::default();
    skew.set(Setting::parse("rate=3"), 100 * SECOND);
    assert_eq!(
        skew.offset_at(100 * SECOND),
        0,
        "a rate change is not a step"
    );
    // Ten real seconds at 3x is thirty on this clock: twenty ahead.
    assert_eq!(skew.offset_at(110 * SECOND), 20_000_000_000);
}

/// Changing the rate again keeps what has drifted so far, so the clock does
/// not jump back when it slows down.
#[test]
fn a_second_rate_change_keeps_the_drift_so_far() {
    let mut skew = Skew::default();
    skew.set(Setting::parse("rate=2"), Duration::ZERO);
    skew.set(Setting::parse("rate=1"), 5 * SECOND);
    assert_eq!(skew.offset_at(5 * SECOND), 5_000_000_000);
    assert_eq!(skew.offset_at(50 * SECOND), 5_000_000_000);

    skew.set(Setting::parse("rate=0.5"), 50 * SECOND);
    assert_eq!(skew.offset_at(60 * SECOND), 0);
}

#[test]
fn removing_the_file_goes_back_to_the_true_clock() {
    let mut skew = Skew::default();
    skew.set(Setting::parse("offset_ms=100\nrate=10"), Duration::ZERO);
    assert_ne!(skew.offset_at(SECOND), 0);
    skew.set(None, SECOND);
    assert_eq!(skew.offset_at(SECOND), 0);
    assert_eq!(skew.offset_at(100 * SECOND), 0);
}

/// End to end through the process-wide follower: a step shows up in both
/// readings, and stopping puts the true clock back. The only test in this
/// crate that touches the global, so nothing else can see the skew.
#[test]
fn following_a_file_skews_both_readings() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("clock");
    std::fs::write(&path, "offset_ms=3600000\n").expect("write");

    let true_wall = super::super::wall_millis();
    follow(&path);
    let skewed_wall = super::super::wall_millis();
    let skewed_boot = super::super::boottime();
    stop_following(&path);
    let true_boot = super::super::boottime();

    assert!(
        skewed_wall >= true_wall + 3_590_000,
        "the wall clock was not stepped"
    );
    assert!(
        skewed_boot >= true_boot + Duration::from_secs(3590),
        "the lease clock was not stepped",
    );
    assert!(super::super::wall_millis() < true_wall + 60_000);
}

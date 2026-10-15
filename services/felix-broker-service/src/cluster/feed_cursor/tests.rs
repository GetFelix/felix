use super::*;

#[test]
fn an_answer_at_or_past_the_cursor_is_fresh() {
    let mut behind = BehindCursor::default();
    assert_eq!(behind.judge(false, Instant::now()), Verdict::Fresh);
}

#[test]
fn an_answer_behind_the_cursor_is_stale_until_it_has_lasted() {
    let start = Instant::now();
    let mut behind = BehindCursor::default();
    assert_eq!(behind.judge(true, start), Verdict::Stale);
    assert_eq!(
        behind.judge(true, start + RESET_AFTER - Duration::from_millis(1)),
        Verdict::Stale
    );
    assert_eq!(behind.judge(true, start + RESET_AFTER), Verdict::Reset);
    // The reset is taken once; the next stale answer starts a new wait.
    assert_eq!(behind.judge(true, start + RESET_AFTER), Verdict::Stale);
}

#[test]
fn a_fresh_answer_restarts_the_wait() {
    let start = Instant::now();
    let mut behind = BehindCursor::default();
    assert_eq!(behind.judge(true, start), Verdict::Stale);
    assert_eq!(behind.judge(false, start + RESET_AFTER), Verdict::Fresh);
    assert_eq!(behind.judge(true, start + RESET_AFTER), Verdict::Stale);
}

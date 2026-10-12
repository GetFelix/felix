use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::{Pending, wait_for};

/// A question asked after the reader stopped fails at once instead of
/// waiting for an answer nothing will give.
#[test]
fn a_question_on_a_dead_stream_fails_at_once() {
    let pending: Pending = Arc::default();
    let dead = AtomicBool::new(true);
    assert!(wait_for(&pending, &dead, 7).is_err());
    assert!(
        pending.lock().unwrap().is_empty(),
        "the question's slot was left behind"
    );
}

#[test]
fn a_question_on_a_live_stream_waits() {
    let pending: Pending = Arc::default();
    let dead = AtomicBool::new(false);
    let (_asked, _answer) = wait_for(&pending, &dead, 7).expect("registered");
    assert!(pending.lock().unwrap().contains_key(&7));
}

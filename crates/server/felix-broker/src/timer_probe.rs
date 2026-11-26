//! Counts the per-publish timers the publish path arms on this thread, for
//! tests that assert a publish arms none. Test builds only.

use std::cell::Cell;

thread_local! {
    static ARMED: Cell<usize> = const { Cell::new(0) };
}

/// How many publish timers were armed on this thread so far.
pub fn armed() -> usize {
    ARMED.with(Cell::get)
}

/// Count one publish timer armed on this thread.
pub fn note_armed() {
    ARMED.with(|armed| armed.set(armed.get() + 1));
}

//! The clock `felix_common::clock` reads in a run: tokio's paused virtual
//! time, counted from when the run installed it.

use std::cell::Cell;
use std::time::Duration;

use tokio::time::Instant;

thread_local! {
    static START: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Virtual time since [`install`] was called on this thread.
fn reading() -> Duration {
    START.with(|start| start.get().map_or(Duration::ZERO, |at| at.elapsed()))
}

/// Point `felix_common::clock` at this thread's tokio clock, counting from
/// now. Call on the runtime a run will use, with its clock paused.
///
/// The reader is a plain function over a thread local, so runs on different
/// threads (parallel tests) each see their own clock.
pub(crate) fn install() {
    START.with(|start| start.set(Some(Instant::now())));
    felix_common::clock::sim::install(reading);
}

#[cfg(test)]
mod tests;

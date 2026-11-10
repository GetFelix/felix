//! Steps of the small state-file writes where a test can stop the writer, so
//! a crash test can cut the power at each one in turn. A no-op outside tests.

use std::path::Path;

#[cfg(test)]
static WATCHED: parking_lot::Mutex<std::collections::BTreeMap<std::path::PathBuf, Watch>> =
    parking_lot::Mutex::new(std::collections::BTreeMap::new());

#[cfg(test)]
struct Watch {
    seen: usize,
    stop_at: Option<usize>,
}

/// Count the steps of writes under `dir`. With `stop_at`, that step and
/// every one after it fails, as a dead process would never run them.
#[cfg(test)]
pub(super) fn watch(dir: &Path, stop_at: Option<usize>) {
    WATCHED
        .lock()
        .insert(dir.to_path_buf(), Watch { seen: 0, stop_at });
}

/// Stop watching `dir`; returns how many steps ran or were refused.
#[cfg(test)]
pub(super) fn unwatch(dir: &Path) -> usize {
    WATCHED.lock().remove(dir).map_or(0, |watch| watch.seen)
}

/// A step of a write under `dir`. Fails from a watching test's stop on.
#[cfg(test)]
pub(super) fn reached(dir: &Path) -> std::io::Result<()> {
    let mut watched = WATCHED.lock();
    let Some(watch) = watched.get_mut(dir) else {
        return Ok(());
    };
    let step = watch.seen;
    watch.seen += 1;
    if watch.stop_at.is_some_and(|stop| step >= stop) {
        return Err(std::io::Error::other("stopped at a crash point"));
    }
    Ok(())
}

#[cfg(not(test))]
#[inline(always)]
pub(super) fn reached(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

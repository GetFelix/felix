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
    under: bool,
}

/// Count the steps of writes under `dir`. With `stop_at`, that step and
/// every one after it fails, as a dead process would never run them.
#[cfg(test)]
pub(crate) fn watch(dir: &Path, stop_at: Option<usize>) {
    insert(dir, stop_at, false);
}

/// Like [`watch`], but also counts the steps of writes in any directory
/// below `dir`, in the order they run.
#[cfg(test)]
pub(crate) fn watch_under(dir: &Path, stop_at: Option<usize>) {
    insert(dir, stop_at, true);
}

#[cfg(test)]
fn insert(dir: &Path, stop_at: Option<usize>, under: bool) {
    WATCHED.lock().insert(
        dir.to_path_buf(),
        Watch {
            seen: 0,
            stop_at,
            under,
        },
    );
}

/// Stop watching `dir`; returns how many steps ran or were refused.
#[cfg(test)]
pub(crate) fn unwatch(dir: &Path) -> usize {
    WATCHED.lock().remove(dir).map_or(0, |watch| watch.seen)
}

/// A step of a write under `dir`. Fails from a watching test's stop on.
#[cfg(test)]
pub(crate) fn reached(dir: &Path) -> std::io::Result<()> {
    stop_or_count(dir)?;
    run_probe(dir);
    Ok(())
}

#[cfg(test)]
fn stop_or_count(dir: &Path) -> std::io::Result<()> {
    let mut watched = WATCHED.lock();
    let Some(at) = dir.ancestors().find(|at| {
        watched
            .get(*at)
            .is_some_and(|watch| *at == dir || watch.under)
    }) else {
        return Ok(());
    };
    let Some(watch) = watched.get_mut(at) else {
        return Ok(());
    };
    let step = watch.seen;
    watch.seen += 1;
    if watch.stop_at.is_some_and(|stop| step >= stop) {
        return Err(std::io::Error::other("stopped at a crash point"));
    }
    Ok(())
}

#[cfg(test)]
type Probe = Box<dyn FnMut() + Send>;

#[cfg(test)]
static PROBE: parking_lot::Mutex<Option<(std::path::PathBuf, Probe)>> =
    parking_lot::Mutex::new(None);

/// Run `probe` at every step of a write in or below `dir`, until
/// [`unprobe`], so a test can look at the files between two steps.
#[cfg(test)]
pub(crate) fn probe_under(dir: &Path, probe: impl FnMut() + Send + 'static) {
    *PROBE.lock() = Some((dir.to_path_buf(), Box::new(probe)));
}

#[cfg(test)]
pub(crate) fn unprobe() {
    PROBE.lock().take();
}

#[cfg(test)]
fn run_probe(dir: &Path) {
    // Taken out while it runs, so a step it reaches itself does not run it
    // again.
    let mut slot = PROBE.lock();
    let Some((under, mut probe)) = slot.take_if(|(under, _)| dir.starts_with(under)) else {
        return;
    };
    drop(slot);
    probe();
    let mut slot = PROBE.lock();
    if slot.is_none() {
        *slot = Some((under, probe));
    }
}

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn reached(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

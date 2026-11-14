//! The gauges for shard directories a Raft rotation kept aside.
//!
//! Measuring walks every kept directory, so it runs on a thread of its own and
//! coalesces: a rotation of every shard on a root measures that root a few
//! times, not once per shard.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use crate::disk_log::layout::KEPT_DIR;
use crate::metrics_names;

/// Each kept directory's copies and bytes as last measured, and the ones a
/// thread is measuring, each with whether it must look again before it exits.
///
/// std's maps, not seeded ones: a static is built once per process, so a
/// seeded map would draw its keys from whichever simulation ran first.
#[derive(Default)]
struct Gauges {
    measured: std::collections::HashMap<PathBuf, (u64, u64)>,
    running: std::collections::HashMap<PathBuf, bool>,
}

static GAUGES: LazyLock<parking_lot::Mutex<Gauges>> = LazyLock::new(Default::default);

/// Measure `root`'s kept directory again, in the background, after it
/// changed.
pub(crate) fn refresh(root: &Path) {
    start(root.join(KEPT_DIR));
}

/// Measure `root`'s kept directory once, if nothing has yet in this
/// process and no walk is running, so copies kept before a restart are
/// counted.
#[cfg(feature = "raft")]
pub(crate) fn note(root: &Path) {
    let kept = root.join(KEPT_DIR);
    let seen = {
        let gauges = GAUGES.lock();
        gauges.measured.contains_key(&kept) || gauges.running.contains_key(&kept)
    };
    if !seen {
        start(kept);
    }
}

fn start(kept: PathBuf) {
    {
        let mut gauges = GAUGES.lock();
        if let Some(again) = gauges.running.get_mut(&kept) {
            *again = true;
            return;
        }
        gauges.running.insert(kept.clone(), false);
    }
    if felix_common::blocking::on_runtime() {
        // A simulator keeps every thread's work on its own runtime.
        measure_until_idle(&kept);
        return;
    }
    let dir = kept.clone();
    let spawned = std::thread::Builder::new()
        .name("felix-kept-gauges".into())
        .spawn(move || measure_until_idle(&dir));
    if let Err(err) = spawned {
        tracing::warn!(error = %err, "could not start a thread to measure kept shard copies; measuring inline");
        measure_until_idle(&kept);
    }
}

fn measure_until_idle(kept: &Path) {
    let _slot = Slot(kept);
    loop {
        let measured = measure(kept);
        let mut gauges = GAUGES.lock();
        gauges.measured.insert(kept.to_path_buf(), measured);
        let (copies, bytes) = gauges
            .measured
            .values()
            .fold((0u64, 0u64), |(copies, bytes), (more, size)| {
                (copies + more, bytes + size)
            });
        metrics::gauge!(metrics_names::RAFT_KEPT_COPIES).set(copies as f64);
        metrics::gauge!(metrics_names::RAFT_KEPT_COPY_BYTES).set(bytes as f64);
        match gauges.running.get_mut(kept) {
            Some(again) if *again => *again = false,
            _ => {
                gauges.running.remove(kept);
                return;
            }
        }
    }
}

/// Frees a directory's slot if its thread panics, so the next change
/// measures it again.
struct Slot<'a>(&'a Path);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            GAUGES.lock().running.remove(self.0);
        }
    }
}

fn measure(kept: &Path) -> (u64, u64) {
    let (mut copies, mut bytes) = (0u64, 0u64);
    if let Ok(entries) = crate::fs::read_dir(kept) {
        for entry in entries.flatten() {
            copies += 1;
            bytes += size_of(&entry.path());
        }
    }
    (copies, bytes)
}

fn size_of(path: &Path) -> u64 {
    let Ok(meta) = crate::fs::metadata(path) else {
        return 0;
    };
    if !meta.is_dir() {
        return meta.len();
    }
    let Ok(entries) = crate::fs::read_dir(path) else {
        return 0;
    };
    entries.flatten().map(|entry| size_of(&entry.path())).sum()
}

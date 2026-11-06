//! A power cut that lands in the middle of an append.
//!
//! A cut at an arbitrary instant rarely finds an append in flight under
//! `FsyncMode::OnCommit`, because a simulated flush takes no time. So the
//! cut is armed and lands on the next append to a log segment. Every file,
//! that one included, keeps what the crash model allows
//! (`io::writeback::crash_contents`), and the append in flight is then
//! forced to come back short or torn: its size reached the disk in full or
//! in part (at least one byte of it), and each 512 B sector of its data
//! arrived or did not, with at least one missing when the size is whole. A missing sector followed by one that arrived is
//! where recovery has to tell a torn tail from interior corruption.

use std::path::Path;
use std::sync::Arc;

use super::DirNode;
use crate::io::writeback::{SECTOR, SplitMix64, Writeback, crash_contents};

pub(super) enum Power {
    On,
    /// The next append to a segment cuts the power. `on_cut` is told the
    /// segment, below the root, and where the append started.
    Armed {
        seed: u64,
        on_cut: Box<dyn FnOnce(&Path, u64) + Send>,
    },
    /// The power is out. This is what the device held at the cut, which the
    /// reboot puts in place of the tree the dying process still writes to.
    Out(Arc<DirNode>),
}

/// What a file reads back as after a cut during the append of
/// `current[from..end]`, the newest bytes of the file. The bytes before it
/// follow the crash model, with the size reaching at least `from`. The
/// append's own size and sectors are drawn so it never comes back whole.
pub(super) fn torn_append(
    durable: &[u8],
    current: &[u8],
    from: usize,
    end: usize,
    rng: &mut SplitMix64,
) -> Vec<u8> {
    let mut out = crash_contents(durable, &current[..from], rng, Writeback::AnySubset);
    // A size that covers the append covers everything before it; bytes the
    // model lost there read as zeros.
    out.resize(from, 0);
    // At least one byte past `from`, so there is always a tail to repair.
    let len = from + 1 + rng.below((end - from) as u64) as usize;
    let old = |at: usize| durable.get(at).copied().unwrap_or(0);
    let mut sectors = Vec::new();
    let mut at = from;
    while at < len {
        let next = (at + 1).next_multiple_of(SECTOR).min(len);
        sectors.push((at, next, rng.coin()));
        at = next;
    }
    if len == end && sectors.iter().all(|(_, _, arrived)| *arrived) {
        let lost = rng.below(sectors.len() as u64) as usize;
        sectors[lost].2 = false;
    }
    let gap_then_data = sectors
        .iter()
        .skip_while(|(_, _, arrived)| *arrived)
        .any(|(_, _, arrived)| *arrived);
    tracing::info!(
        target: "felix_storage::sim_fs",
        from,
        appended = end - from,
        size_kept = len - from,
        sectors = sectors.len(),
        lost = sectors.iter().filter(|(_, _, arrived)| !*arrived).count(),
        gap_then_data,
        "power cut tore an append"
    );
    for (start, stop, arrived) in sectors {
        if arrived {
            out.extend_from_slice(&current[start..stop]);
        } else {
            out.extend((start..stop).map(old));
        }
    }
    out
}

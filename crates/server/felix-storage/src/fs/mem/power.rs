//! A power cut that lands in the middle of an append.
//!
//! A cut at an arbitrary instant rarely finds an append in flight under
//! `FsyncMode::OnCommit`, because a simulated flush takes no time. So the
//! cut is armed and lands on the next append to a log segment whose earlier
//! bytes were all synced. That append comes back torn in a way the model
//! allows: its length reached the disk, its data only up to some sector
//! boundary, and the rest reads as zeros. Recovery must repair that tail.
//! Every other file follows the usual crash rules.

use std::sync::Arc;

use super::DirNode;
use crate::io::writeback::{SECTOR, SplitMix64};

pub(super) enum Power {
    On,
    /// The next clean append to a segment cuts the power.
    Armed {
        seed: u64,
        on_cut: Box<dyn FnOnce() + Send>,
    },
    /// The power is out. This is what the device held at the cut, which the
    /// reboot puts in place of the tree the dying process still writes to.
    Out(Arc<DirNode>),
}

/// What a file reads back as after a cut during the append of
/// `current[from..end]`, where everything before `from` was synced: the bytes
/// before the append, then the append up to a sector boundary inside it (or
/// none of it), then zeros to `end`.
pub(super) fn torn_append(
    current: &[u8],
    from: usize,
    end: usize,
    rng: &mut SplitMix64,
) -> Vec<u8> {
    let first = (from + 1).next_multiple_of(SECTOR);
    let inside = if first < end {
        1 + (end - 1 - first) / SECTOR
    } else {
        0
    };
    let pick = rng.below(inside as u64 + 1) as usize;
    let zeros_from = if pick == 0 {
        from
    } else {
        first + (pick - 1) * SECTOR
    };
    let mut out = current[..zeros_from].to_vec();
    out.resize(end, 0);
    out
}

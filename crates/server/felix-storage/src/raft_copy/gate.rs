//! The gate in front of Raft: a data root that went back opens no member
//! under its storage id, and a root nothing can vouch for opens none at all.
//!
//! The root sequence rises with every synced mark write. The run witness
//! holds a copy for as long as the machine stays up, and the control plane
//! holds the last one a heartbeat carried. A root whose sequence is below
//! either, or whose incarnation is behind the control plane's, was put back
//! from a copy, and its copies may vote again in a term they already voted
//! in, so it gets a new storage id and every directory in it rotates when it
//! opens. A root ahead of a copy crashed between its write and the copy's,
//! and passes. A root that holds marks with neither copy to compare against
//! (a reboot cleared the witness and the control plane has none) is
//! unchecked: it waits for evidence rather than trusting silence.

use std::io;
use std::path::Path;

use super::marks::MarkLog;
use crate::run_state::{copy_trust, witness};
use crate::storage_identity;

/// What a start knows about its data root from outside it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Evidence<'a> {
    /// Where the run witness is kept ([`witness::default_dir`]). `None` on
    /// a host that has none.
    pub witness_dir: Option<&'a Path>,
    /// The last root sequence the control plane heard for this storage id.
    /// The control plane does not keep it yet (issue #2441), so today this
    /// is always `None`.
    pub control_plane: Option<u64>,
    /// The start's incarnation verdict: the control plane registered this
    /// node after the root last recorded a registration
    /// ([`crate::run_state::Recorded::behind`]), so the root is from before
    /// then.
    pub restored: bool,
}

/// What the gate found.
#[derive(Debug)]
pub enum Gate {
    Passed(GatePassed),
    Behind(Behind),
    Unchecked(Unchecked),
}

/// A root that passed the gate: a copy of its sequence was there to compare
/// against and the root was not behind it or the incarnation, or it holds
/// no marks yet. Only [`check`] and [`Behind::rotate`] make one, and a
/// [`super::CopyRoot`] opens only from one, so no member opens on a root
/// the gate did not judge.
///
/// ```compile_fail,E0451
/// fn forge(marks: felix_storage::raft_copy::MarkLog) -> felix_storage::raft_copy::GatePassed {
///     felix_storage::raft_copy::GatePassed { marks }
/// }
/// ```
pub struct GatePassed {
    pub(super) marks: MarkLog,
}

impl std::fmt::Debug for GatePassed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatePassed")
            .field("sequence", &self.marks.sequence())
            .finish()
    }
}

/// A root that is behind: below a copy of its sequence, or from before the
/// node's last registration.
pub struct Behind {
    pub root: u64,
    pub witness: Option<u64>,
    pub control_plane: Option<u64>,
    pub restored: bool,
    marks: MarkLog,
}

/// A root that holds marks with no copy of its sequence to compare them
/// against. No member opens on it; check again once the witness or the
/// control plane has a copy.
#[derive(Debug)]
pub struct Unchecked {
    pub root: u64,
}

impl Behind {
    /// Give the checked root the storage id `fresh`, through
    /// [`copy_trust::rotate_storage_id`], so every copy in it rotates when
    /// it opens. The witness records the root's sequence under the new id,
    /// and the root passes.
    pub fn rotate(self, fresh: [u8; 16]) -> io::Result<GatePassed> {
        let root = self.marks.root();
        let id = copy_trust::rotate_storage_id(root, fresh).map_err(io::Error::other)?;
        self.marks.set_storage_id(id);
        self.marks.record_witness();
        Ok(GatePassed { marks: self.marks })
    }
}

impl std::fmt::Debug for Behind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Behind")
            .field("root", &self.root)
            .field("witness", &self.witness)
            .field("control_plane", &self.control_plane)
            .field("restored", &self.restored)
            .finish()
    }
}

/// Judge the root at `root`: open its mark log and compare its sequence
/// with the one the run witness recorded for the root's storage id and with
/// the control plane's, and take the incarnation verdict. Behind if any of
/// them says so. Unchecked if the root holds marks and there is no copy of
/// its sequence to compare against. Fails if the root has no storage id or
/// its mark log is damaged.
pub fn check(root: &Path, evidence: Evidence<'_>) -> io::Result<Gate> {
    let id = storage_identity::load(root)
        .map_err(io::Error::other)?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} has no storage id", root.display()),
            )
        })?;
    let marks = MarkLog::open(root, id, evidence.witness_dir.map(Path::to_path_buf))?;
    let sequence = marks.sequence();
    // A record under another storage id is from before a rotation, which
    // starts the comparison over.
    let witnessed = witness::recorded_sequence(root, evidence.witness_dir)
        .filter(|(recorded, _)| *recorded == id)
        .map(|(_, sequence)| sequence);
    let control_plane = evidence.control_plane;
    let behind = evidence.restored
        || witnessed.is_some_and(|copy| sequence < copy)
        || control_plane.is_some_and(|copy| sequence < copy);
    Ok(if behind {
        Gate::Behind(Behind {
            root: sequence,
            witness: witnessed,
            control_plane,
            restored: evidence.restored,
            marks,
        })
    } else if sequence > 0 && witnessed.is_none() && control_plane.is_none() {
        // A restore from a copy taken during the last run carries the
        // current incarnation, so only a sequence can show it went back.
        Gate::Unchecked(Unchecked { root: sequence })
    } else {
        Gate::Passed(GatePassed { marks })
    })
}

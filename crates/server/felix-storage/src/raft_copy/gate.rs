//! The gate in front of Raft: a data root that went back opens no member
//! under its storage id.
//!
//! The root sequence rises with every synced mark write. The run witness
//! holds a copy for as long as the machine stays up, and the control plane
//! holds the last one a heartbeat carried. A root whose sequence is below
//! either was put back from a copy, and its copies may vote again in a term
//! they already voted in, so it gets a new storage id and every directory
//! in it rotates when it opens. A root ahead of a copy crashed between its
//! write and the copy's, and passes.

use std::io;
use std::path::Path;

use super::marks::MarkLog;
use crate::run_state::{copy_trust, witness};
use crate::storage_identity;

/// What the gate found.
pub enum Gate {
    Passed(GatePassed),
    Behind(Behind),
}

/// A root that passed the gate. Only [`check`] and [`Behind::rotate`] make
/// one, and a [`super::CopyRoot`] opens only from one, so no member opens on
/// a root the gate did not judge.
///
/// ```compile_fail,E0451
/// fn forge(marks: felix_storage::raft_copy::MarkLog) -> felix_storage::raft_copy::GatePassed {
///     felix_storage::raft_copy::GatePassed { marks }
/// }
/// ```
pub struct GatePassed {
    pub(super) marks: MarkLog,
}

/// A root whose sequence is below a copy of it.
pub struct Behind {
    pub root: u64,
    pub witness: Option<u64>,
    pub control_plane: Option<u64>,
    marks: MarkLog,
}

impl Behind {
    /// Give the root the storage id `fresh`, through
    /// [`copy_trust::rotate_storage_id`], so every copy in it rotates when
    /// it opens. The root then passes.
    pub fn rotate(self, root: &Path, fresh: [u8; 16]) -> io::Result<GatePassed> {
        let id = copy_trust::rotate_storage_id(root, fresh).map_err(io::Error::other)?;
        self.marks.set_storage_id(id);
        Ok(GatePassed { marks: self.marks })
    }
}

impl std::fmt::Debug for Behind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Behind")
            .field("root", &self.root)
            .field("witness", &self.witness)
            .field("control_plane", &self.control_plane)
            .finish()
    }
}

/// Judge the root at `root`: open its mark log and compare its sequence
/// with the one the run witness under `witness_dir` recorded for the root's
/// storage id, and with `control_plane`, the last sequence the control
/// plane heard for that storage id. Fails if the root has no storage id or
/// its mark log is damaged.
pub fn check(
    root: &Path,
    witness_dir: Option<&Path>,
    control_plane: Option<u64>,
) -> io::Result<Gate> {
    let id = storage_identity::load(root)
        .map_err(io::Error::other)?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} has no storage id", root.display()),
            )
        })?;
    let marks = MarkLog::open(root, id, witness_dir.map(Path::to_path_buf))?;
    let sequence = marks.sequence();
    // A record under another storage id is from before a rotation, which
    // starts the comparison over.
    let witnessed = witness::recorded_sequence(root, witness_dir)
        .filter(|(recorded, _)| *recorded == id)
        .map(|(_, sequence)| sequence);
    let behind = witnessed.is_some_and(|copy| sequence < copy)
        || control_plane.is_some_and(|copy| sequence < copy);
    Ok(if behind {
        Gate::Behind(Behind {
            root: sequence,
            witness: witnessed,
            control_plane,
            marks,
        })
    } else {
        Gate::Passed(GatePassed { marks })
    })
}

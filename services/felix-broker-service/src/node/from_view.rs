//! Starting from the control-plane view while the control plane is down.
//!
//! A broker with `FELIX_START_WITHOUT_CONTROLPLANE` on, whose startup lookup
//! found the control plane down, may open its storage and rejoin its shards'
//! elections from what the control plane last told it, kept in the data
//! directory (`crate::cluster::controlplane_view`). Until it registers it
//! votes, stands, leads and replicates, but serves no client. See
//! docs/control-plane.md, "Starting while the control plane is down".

use felix_common::random::HashMap;
use std::collections::BTreeSet;
use std::path::Path;

use felix_router::NodeRef;

use crate::cluster::controlplane_view::{self, View};

/// Why a data directory may not start from its view. Each one waits for the
/// control plane instead, as a start without the switch does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Refusal {
    /// No view, or one that does not decode or is another node's: a new,
    /// wiped or upgraded directory, or one whose node deregistered.
    NoView,
    /// The directory records no control-plane metadata group: it has not yet
    /// been checked against the control plane, and only that check can tell a
    /// recreated control plane (`crate::cluster::controlplane_group`).
    NoGroup,
    /// The storage id on disk is not the one the view was registered under:
    /// a new or rotated id the control plane never registered, which must
    /// register before it can act.
    OtherDirectory {
        on_disk: Option<String>,
        registered: String,
    },
    /// The directory carries a replaced-storage marker newer than the view:
    /// the control plane last registered the node under another storage id,
    /// so its copies here may be ones the node no longer holds, and only a
    /// registration under the current id says which.
    Replaced,
    /// No run witness outside the directory names it as the last run on
    /// this machine left it: it may be a restored copy, which nothing inside
    /// it can rule out, or the machine restarted since.
    Unwitnessed,
    /// The fleet had not finalized `replica_election`, so nothing could
    /// elect without the control plane anyway.
    NoElection,
    /// The view's fleet has features this build does not report, which the
    /// control plane would refuse it for.
    Unreported(Vec<String>),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoView => f.write_str("there is no control-plane view for this node"),
            Self::NoGroup => f.write_str(
                "the data directory records no control-plane metadata group; the control plane \
                 has to be asked once before it can start without it",
            ),
            Self::OtherDirectory {
                on_disk,
                registered,
            } => write!(
                f,
                "the storage id on disk ({}) is not the one the view was registered under \
                 ({registered})",
                on_disk.as_deref().unwrap_or("none"),
            ),
            Self::Replaced => {
                f.write_str("the control plane last registered this node under another storage id")
            }
            Self::Unwitnessed => f.write_str(
                "no run witness shows this is the data directory the last run on this machine \
                 left; it may be restored from a copy, or the machine restarted",
            ),
            Self::NoElection => f.write_str("the view's fleet has not finalized replica_election"),
            Self::Unreported(missing) => write!(
                f,
                "this build does not report {} that the view's fleet finalized",
                missing.join(", "),
            ),
        }
    }
}

/// The view under `root` for `node_id`, if this directory may start from it:
/// it is present and this node's, the directory records a control-plane
/// metadata group, the storage id on disk is the one it was
/// registered under, it was written after any replaced-storage marker, the
/// run witness in `witness` names it in `boot`, its fleet finalized
/// `replica_election`, and `reported` has every feature its fleet finalized.
/// Reads only; call before the run begins, since beginning one may rotate the
/// id and writes a new witness.
pub(super) fn qualify(
    root: &Path,
    node_id: &str,
    reported: &BTreeSet<String>,
    witness: Option<&Path>,
    boot: Option<felix_storage::run_state::BootId>,
) -> Result<View, Refusal> {
    let view = controlplane_view::read(root, node_id).ok_or(Refusal::NoView)?;
    // Unreadable reads as none: a start that cannot tell waits.
    if crate::cluster::controlplane_group::read(root)
        .ok()
        .flatten()
        .is_none()
    {
        return Err(Refusal::NoGroup);
    }
    // Unreadable reads as no id: a start that cannot tell waits.
    let on_disk = felix_storage::storage_identity::load(root)
        .ok()
        .flatten()
        .map(|id| id.to_hex());
    if on_disk.as_deref() != Some(view.storage_id.as_str()) {
        return Err(Refusal::OtherDirectory {
            on_disk,
            registered: view.storage_id,
        });
    }
    if !registered_since_replaced(root, &view) {
        return Err(Refusal::Replaced);
    }
    // A restored copy keeps its storage id and its view, and without the
    // control plane nothing else shows it is behind (#2071).
    if !felix_storage::run_state::witness::witnessed(root, witness, boot) {
        return Err(Refusal::Unwitnessed);
    }
    let election = felix_common::fleet::REPLICA_ELECTION.name();
    if !view.fleet.iter().any(|feature| feature == election) {
        return Err(Refusal::NoElection);
    }
    let missing: Vec<String> = view
        .fleet
        .iter()
        .filter(|feature| !reported.contains(*feature))
        .cloned()
        .collect();
    if !missing.is_empty() {
        return Err(Refusal::Unreported(missing));
    }
    Ok(view)
}

/// Whether `view` came from a registration made after the directory was last
/// marked replaced, or there is no marker. Marking removes the view, so one
/// beside a marker was written since; the id and incarnation are a second
/// check. Such a registration was of the id the marker names, so the control
/// plane's routes since already account for the copies this directory lost.
/// Unreadable, or a marker that does not say when it was written, reads as
/// replaced: a start that cannot tell waits.
fn registered_since_replaced(root: &Path, view: &View) -> bool {
    use felix_storage::run_state::ReplacedMark;
    match felix_storage::run_state::read_replaced(root) {
        Ok(None) => true,
        Ok(Some(ReplacedMark::Under { storage_id, after })) => {
            storage_id == view.storage_id && view.incarnation > after
        }
        Ok(Some(ReplacedMark::Unknown)) | Err(_) => false,
    }
}

/// Whether the run that began on the directory is still under the storage id
/// `view` was registered under. Beginning a run rotates the id after a crash
/// that may have lost acknowledged records, and a rotated id must register
/// before it acts, so such a start waits for the control plane after all.
pub(super) fn opened_under(view: &View, id: felix_storage::StorageId) -> bool {
    view.storage_id == id.to_hex()
}

/// The view's node addresses, for routing until the node catalog answers.
/// Each counts as live: the elections need to reach it, and one that is
/// down simply does not answer.
pub(super) fn nodes(view: &View) -> HashMap<String, NodeRef> {
    view.nodes
        .iter()
        .map(|node| {
            (
                node.node_id.clone(),
                NodeRef {
                    node_id: node.node_id.clone(),
                    advertise_addr: node.advertise_addr,
                    region: node.region.clone(),
                    live: true,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests;

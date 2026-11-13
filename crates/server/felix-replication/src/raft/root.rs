//! Opening a broker's data root for its Raft members: through the restore
//! gate, and not at all while nothing outside the root vouches for it.
//!
//! The gate (`felix_storage::raft_copy::check`) compares the root's
//! sequence with the run witness's copy and the control plane's. A root
//! behind either gets a new storage id here, so every copy in it rotates
//! as it opens. A root that holds marks with no copy to compare against is
//! held: [`open_root`] asks the control plane again every `retry` and opens
//! nothing until a copy arrives or the broker stops. See
//! `docs/per-shard-raft.md`, "The root sequence".

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use felix_storage::StorageId;
use felix_storage::raft_copy::{
    ControlPlaneCopy, CopyRoot, Evidence, Gate, GatePassed, MarkLog, check, random_copy_id,
};
use tokio_util::sync::CancellationToken;

/// What a heartbeat carries of an open root: its storage id and its last
/// synced root sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootReport {
    pub storage_id: StorageId,
    pub sequence: u64,
}

impl RootReport {
    /// The report of `marks` now. The sequence is the synced one, never a
    /// write still in flight: a power cut after an unsynced value would
    /// leave the root behind the control plane's copy and rotate every copy
    /// for nothing.
    pub fn of(marks: &MarkLog) -> Self {
        Self {
            storage_id: marks.storage_id(),
            sequence: marks.sequence(),
        }
    }
}

/// Run the gate on `root` until it opens, and open the root for members.
///
/// `restored` is the start's incarnation verdict. `control_plane` asks the
/// control plane for its copy of this node's root sequence; `None` is no
/// copy, whether it holds none or did not answer. A root behind a copy is
/// given a new storage id first. A root nothing vouches for is checked
/// again every `retry`; `Ok(None)` if `stop` fires first.
///
/// Call it only once the start's restore check has answered: `restored`
/// must be its verdict, and a rotation that check makes removes the mark
/// log, which must not be open by then.
pub async fn open_root<F, Fut>(
    root: &Path,
    witness_dir: Option<&Path>,
    restored: bool,
    mut control_plane: F,
    retry: Duration,
    stop: &CancellationToken,
) -> io::Result<Option<CopyRoot>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<ControlPlaneCopy>>,
{
    let mut held = false;
    loop {
        let copy = tokio::select! {
            () = stop.cancelled() => return Ok(None),
            copy = control_plane() => copy,
        };
        match judge(
            root.to_path_buf(),
            witness_dir.map(Path::to_path_buf),
            restored,
            copy,
        )
        .await?
        {
            Judged::Open(passed) => {
                if held {
                    tracing::info!(root = %root.display(), "the Raft restore gate passed the data root");
                }
                return Ok(Some(CopyRoot::open(root, passed, random_copy_id)));
            }
            Judged::Held(sequence) => {
                if !held {
                    tracing::error!(
                        root = %root.display(),
                        sequence,
                        "this data root holds Raft votes and logs, and neither the run witness \
                         nor the control plane has a copy of its root sequence, as after a \
                         reboot with the control plane down or a restored snapshot; no Raft \
                         member opens until the control plane answers with one. If it has none, \
                         stop the broker and run `felix-broker rotate-storage-id`",
                    );
                    held = true;
                }
            }
        }
        tokio::select! {
            () = stop.cancelled() => return Ok(None),
            () = tokio::time::sleep(retry) => {}
        }
    }
}

enum Judged {
    Open(GatePassed),
    /// Unchecked at this root sequence.
    Held(u64),
}

/// One pass of the gate, off the runtime: it reads and may rewrite files.
async fn judge(
    root: PathBuf,
    witness_dir: Option<PathBuf>,
    restored: bool,
    control_plane: Option<ControlPlaneCopy>,
) -> io::Result<Judged> {
    tokio::task::spawn_blocking(move || {
        let gate = check(
            &root,
            Evidence {
                witness_dir: witness_dir.as_deref(),
                control_plane,
                restored,
            },
        )?;
        match gate {
            Gate::Passed(passed) => Ok(Judged::Open(passed)),
            Gate::Behind(behind) => {
                tracing::warn!(
                    root = %root.display(),
                    behind = ?behind,
                    "this data root is behind a copy of its root sequence, as a restored \
                     snapshot is; giving it a new storage id so every Raft copy in it starts \
                     over as a new member",
                );
                behind
                    .rotate(felix_common::random::uuid_v4().into_bytes())
                    .map(Judged::Open)
            }
            Gate::Unchecked(unchecked) => Ok(Judged::Held(unchecked.root)),
        }
    })
    .await
    .map_err(io::Error::other)?
}

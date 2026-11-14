//! Opening a broker's data root for its Raft members: through the restore
//! gate, and not at all while nothing outside the root vouches for it.
//!
//! The gate (`felix_storage::raft_copy::check`) compares the root's
//! sequence with the run witness's copy and the control plane's. A root
//! behind either gets a new storage id here, so every copy in it rotates
//! when it next opens; the caller stops the broker so the restart registers
//! under the new id. A root that holds marks with no copy to compare against is
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

/// 1 while the data root is held because nothing vouches for its root
/// sequence, so no Raft member on the broker opens.
const ROOT_HELD: &str = "felix_broker_raft_root_held";

/// What [`open_root`] did with the data root.
pub enum RootOpened {
    /// The gate passed it and its Raft members may open.
    Open(CopyRoot),
    /// It was behind a copy of its root sequence and now has the storage id
    /// `storage_id`. Nothing in it is opened: the caller stops the broker, so
    /// the restart registers under the new id.
    Rotated {
        storage_id: StorageId,
        /// The root's sequence and the copies it was behind.
        sequence: u64,
        witness: Option<u64>,
        control_plane: Option<u64>,
    },
    /// `stop` fired first.
    Stopped,
}

impl std::fmt::Debug for RootOpened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(copies) => f.debug_tuple("Open").field(&copies.storage_id()).finish(),
            Self::Rotated {
                storage_id,
                sequence,
                witness,
                control_plane,
            } => f
                .debug_struct("Rotated")
                .field("storage_id", storage_id)
                .field("sequence", sequence)
                .field("witness", witness)
                .field("control_plane", control_plane)
                .finish(),
            Self::Stopped => f.write_str("Stopped"),
        }
    }
}

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
/// given a new storage id and not opened ([`RootOpened::Rotated`]). A root
/// nothing vouches for is checked again every `retry`, with the
/// `felix_broker_raft_root_held` gauge at 1 meanwhile.
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
) -> io::Result<RootOpened>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<ControlPlaneCopy>>,
{
    let mut held = false;
    let opened = loop {
        let copy = tokio::select! {
            () = stop.cancelled() => break RootOpened::Stopped,
            copy = control_plane() => copy,
        };
        match judge(
            root.to_path_buf(),
            witness_dir.map(Path::to_path_buf),
            restored,
            copy,
        )
        .await
        {
            Ok(Judged::Open(passed)) => {
                if held {
                    tracing::info!(root = %root.display(), "the Raft restore gate passed the data root");
                }
                break RootOpened::Open(CopyRoot::open(root, passed, random_copy_id));
            }
            Ok(Judged::Rotated(rotated)) => break rotated,
            Err(err) => {
                if held {
                    metrics::gauge!(ROOT_HELD).set(0.0);
                }
                return Err(err);
            }
            Ok(Judged::Held(sequence)) => {
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
                    metrics::gauge!(ROOT_HELD).set(1.0);
                }
            }
        }
        tokio::select! {
            () = stop.cancelled() => break RootOpened::Stopped,
            () = tokio::time::sleep(retry) => {}
        }
    };
    if held {
        metrics::gauge!(ROOT_HELD).set(0.0);
    }
    Ok(opened)
}

enum Judged {
    Open(GatePassed),
    Rotated(RootOpened),
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
                let (sequence, witness, control_plane) =
                    (behind.root, behind.witness, behind.control_plane);
                let passed = behind.rotate(felix_common::random::uuid_v4().into_bytes())?;
                Ok(Judged::Rotated(RootOpened::Rotated {
                    storage_id: passed.storage_id(),
                    sequence,
                    witness,
                    control_plane,
                }))
            }
            Gate::Unchecked(unchecked) => Ok(Judged::Held(unchecked.root)),
        }
    })
    .await
    .map_err(io::Error::other)?
}

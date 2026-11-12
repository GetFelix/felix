//! The `form-without` waiver: an operator's word that a member's machine is
//! gone for good, so a first start may form the group without its answer.
//!
//! First boot forms only when every configured member answers and is empty
//! ([`super::join`]), which a permanently lost machine blocks forever. The
//! waiver names the absent slots in a file in the data directory. It is used
//! once: the member removes it before forming, and also when it finds it has
//! no first start to make. The rules and their limits are in
//! `docs/metadata-raft-design.md` ("Forming without a lost machine").
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::{NodeId, store};

const FILE: &str = "form-without";

#[derive(Debug, Serialize, Deserialize)]
struct Waiver {
    cluster_id: String,
    slots: BTreeSet<NodeId>,
}

/// Record that the group named `cluster_id` may form without the members
/// in `slots` answering. Run it on a stopped member; returns a line saying
/// what was written.
///
/// Refuses a member that holds Raft state, since that member has no first
/// start left to make. A store that only went through a blocked first start
/// holds none, so the waiver still applies to it.
pub async fn form_without(
    data_dir: &Path,
    cluster_id: &str,
    slots: &BTreeSet<NodeId>,
) -> Result<String> {
    anyhow::ensure!(!cluster_id.is_empty(), "--cluster-id must not be empty");
    anyhow::ensure!(!slots.is_empty(), "name at least one --slot");
    anyhow::ensure!(
        !store::stopped_member_holds_state(data_dir, cluster_id).await?,
        "this member holds raft state, so it is not making a first start; \
         form-without applies only to a member that has never formed or joined a group"
    );
    std::fs::create_dir_all(data_dir).context("create raft data dir")?;
    let waiver = Waiver {
        cluster_id: cluster_id.to_string(),
        slots: slots.clone(),
    };
    let path = data_dir.join(FILE);
    // Written beside and renamed, so a crash never leaves half a waiver.
    let tmp = data_dir.join(format!("{FILE}.tmp"));
    let mut file = File::create(&tmp).context("create the form-without file")?;
    file.write_all(&serde_json::to_vec(&waiver).context("encode the waiver")?)
        .context("write the form-without file")?;
    file.sync_all().context("sync the form-without file")?;
    std::fs::rename(&tmp, &path).context("place the form-without file")?;
    sync_dir(data_dir)?;
    let slots: Vec<String> = slots.iter().map(NodeId::to_string).collect();
    Ok(format!(
        "wrote {}: cluster {cluster_id} may form once without slot(s) {}",
        path.display(),
        slots.join(", ")
    ))
}

/// The slots this member may form without, if a waiver for `cluster_id` is
/// in `data_dir`. A waiver for another cluster or one that does not decode
/// is an error: guessing what the operator meant is not safe either way.
pub(super) fn load(data_dir: &Path, cluster_id: &str) -> Result<Option<BTreeSet<NodeId>>> {
    let path = data_dir.join(FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).context(format!("read {}", path.display())),
    };
    let waiver: Waiver =
        serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))?;
    anyhow::ensure!(
        waiver.cluster_id == cluster_id,
        "{} is for raft cluster {:?}, but this member is configured for {cluster_id:?}; \
         remove the file or fix FELIX_RAFT_CLUSTER_ID",
        path.display(),
        waiver.cluster_id
    );
    Ok(Some(waiver.slots))
}

/// Remove the waiver, durably. Returns whether there was one.
pub(super) fn remove(data_dir: &Path) -> Result<bool> {
    let path = data_dir.join(FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err).context(format!("remove {}", path.display())),
    }
    sync_dir(data_dir)?;
    Ok(true)
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("sync {}", dir.display()))
}

#[cfg(test)]
mod tests;

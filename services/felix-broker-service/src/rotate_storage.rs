//! `felix-broker rotate-storage-id`: give a stopped broker's data directory
//! a new storage id.
//!
//! Run it after putting back a snapshot of a data directory taken while the
//! broker ran. Every copy on the directory then starts over: peers resync
//! the hybrid's shards, and each Raft shard directory opens as a new member
//! that never voted. Without it, a snapshot newer than what the control
//! plane last heard from the broker can vote a second time in a term it
//! already voted in. See `docs/per-shard-raft.md`, "The root sequence", and
//! the runbook in `docs-site/src/content/docs/deployment/backup-and-restore.md`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use felix_storage::StorageId;

const USAGE: &str = "\
usage: felix-broker rotate-storage-id [--dir DIR]

Gives the broker's data directory a new storage id, so every copy on it
starts over as a new one. Run it with the broker stopped, after restoring the
directory from a snapshot. DIR defaults to FELIX_DURABLE_STORAGE_DIR.";

/// Run `rotate-storage-id`; `args` starts after the subcommand.
pub fn run(args: Vec<String>) -> Result<()> {
    let mut dir = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dir" => dir = Some(PathBuf::from(args.next().context("--dir needs a path")?)),
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => bail!("unknown argument {other}\n\n{USAGE}"),
        }
    }
    let root = match dir {
        Some(dir) => dir,
        None => {
            crate::config::DurableStorageConfig::from_env()?
                .context("FELIX_DURABLE_STORAGE_DIR is not set; pass --dir")?
                .root
        }
    };
    let (previous, id) = rotate(&root)?;
    println!("{}: storage id {previous} -> {id}", root.display());
    Ok(())
}

/// Rotate the storage id of the data directory at `root`, which must have
/// one. Returns the old id and the new.
fn rotate(root: &Path) -> Result<(StorageId, StorageId)> {
    let previous = felix_storage::storage_identity::load(root)
        .with_context(|| format!("read the storage id under {}", root.display()))?
        .with_context(|| {
            format!(
                "{} has no storage id; is it a broker's data directory?",
                root.display()
            )
        })?;
    let id = crate::node::storage::rotate_storage_id(root)?;
    Ok((previous, id))
}

#[cfg(test)]
mod tests;

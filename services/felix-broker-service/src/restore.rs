//! `felix-broker restore-point`: cut a stopped broker's shard logs back to a
//! backup point.
//!
//! The point comes from `felix-controlplane admin backup-point`, which read
//! each shard's committed offsets from its leader. A copy of the leader's
//! shard directories taken while it ran holds at least those records and
//! usually more; this cuts each log back to the point, below its commit
//! offset where it has to (see `DiskLog::restore_to`). Run with the broker
//! stopped, over the data directory its normal configuration names.
//!
//! The runbook is `docs-site/src/content/docs/deployment/backup-and-restore.md`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use felix_broker::LogKind;
use felix_storage::disk_log::layout::shard_dir;
use serde::Deserialize;

use crate::config::{BrokerConfig, DurableStorageConfig};

/// The only manifest layout this broker reads.
const FORMAT_VERSION: u32 = 1;

const USAGE: &str = "\
usage: felix-broker restore-point --point FILE [--node NODE]

Cuts every shard log in the broker's durable storage back to the backup
point in FILE. Run it with the broker stopped. With --node, only the shards
the point names NODE as leader of; without it, every shard in the point
whose data is present here.";

/// Run `restore-point`; `args` starts after the subcommand.
pub async fn run(args: Vec<String>) -> Result<()> {
    let mut point = None;
    let mut node = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--point" => point = Some(PathBuf::from(args.next().context("--point needs a file")?)),
            "--node" => node = Some(args.next().context("--node needs a node id")?),
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => bail!("unknown argument {other}\n\n{USAGE}"),
        }
    }
    let Some(point) = point else {
        bail!("{USAGE}");
    };
    let manifest = Manifest::read(&point)?;
    let durable = DurableStorageConfig::from_env()?
        .context("FELIX_DURABLE_STORAGE_DIR is not set, so this broker keeps no logs to restore")?;
    let config = BrokerConfig::from_env_or_yaml().context("load broker configuration")?;
    let (broker, storage) = crate::node::storage::open(&config)?;

    let plan = manifest.plan(node.as_deref(), &durable.root)?;
    if plan.is_empty() {
        println!("nothing in {} to restore here", point.display());
    }
    for cut in &plan {
        let shard = &cut.shard;
        let log = broker
            .shard_log(
                cut.kind,
                &shard.tenant_id,
                &shard.namespace,
                &shard.name,
                shard.shard,
            )
            .await
            .with_context(|| format!("open the {:?} log of {}", cut.kind, shard.label()))?;
        let tail = log.tail_offset().await?;
        log.restore_to(cut.offset)
            .await
            .with_context(|| format!("restore the {:?} log of {}", cut.kind, shard.label()))?;
        println!(
            "{} {:?}: {} -> {}",
            shard.label(),
            cut.kind,
            tail,
            cut.offset
        );
    }
    if let Some(storage) = storage {
        storage.shutdown().await?;
    }
    println!(
        "restored {} logs to point {:?}; start this broker, and start its followers without these shards' directories",
        plan.len(),
        manifest.name
    );
    Ok(())
}

/// A backup point as `felix-controlplane admin backup-point` writes it. Only
/// the fields a restore needs.
#[derive(Debug, Deserialize)]
pub(crate) struct Manifest {
    format_version: u32,
    name: String,
    shards: Vec<PointShard>,
}

impl Manifest {
    fn read(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read the backup point {}", path.display()))?;
        let manifest: Self = serde_json::from_str(&text)
            .with_context(|| format!("parse the backup point {}", path.display()))?;
        if manifest.format_version != FORMAT_VERSION {
            bail!(
                "{} is format version {}; this broker reads version {FORMAT_VERSION}",
                path.display(),
                manifest.format_version
            );
        }
        Ok(manifest)
    }

    /// Which logs to cut, and where to.
    ///
    /// With a node, every shard it led at the point, and a missing log whose
    /// point is past zero is an error: the copy is incomplete, and starting
    /// the shard empty would drop records the point says were committed.
    /// Without one, only the shards whose records are here at all.
    pub(crate) fn plan(&self, node: Option<&str>, root: &Path) -> Result<Vec<Cut>> {
        let mut plan = Vec::new();
        for shard in &self.shards {
            if node.is_some_and(|node| node != shard.leader) {
                continue;
            }
            let logs = shard.logs();
            let present = |kind: LogKind| log_dir(root, kind, shard).exists();
            if node.is_none() && !present(logs[0].0) {
                continue;
            }
            for (kind, offset) in logs {
                if present(kind) {
                    plan.push(Cut {
                        shard: shard.clone(),
                        kind,
                        offset,
                    });
                } else if offset > 0 {
                    bail!(
                        "the point puts the {kind:?} log of {} at {offset}, and there is no \
                         copy of it under {}",
                        shard.label(),
                        root.display()
                    );
                }
            }
        }
        Ok(plan)
    }
}

/// One shard in a backup point.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PointShard {
    tenant_id: String,
    namespace: String,
    name: String,
    shard: u32,
    kind: PointKind,
    leader: String,
    logs: PointLogs,
}

impl PointShard {
    /// Each log the point names, the records first.
    fn logs(&self) -> Vec<(LogKind, u64)> {
        let records = match self.kind {
            PointKind::Stream => LogKind::Stream,
            PointKind::Cache => LogKind::Cache,
        };
        let mut logs = vec![(records, self.logs.records)];
        let sidecars = [
            (LogKind::GroupCursors, self.logs.group_cursors),
            (LogKind::GroupDeadLetters, self.logs.group_dead_letters),
            (LogKind::Counters, self.logs.counters),
        ];
        logs.extend(
            sidecars
                .into_iter()
                .filter_map(|(kind, offset)| Some((kind, offset?))),
        );
        logs
    }

    fn label(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.tenant_id, self.namespace, self.name, self.shard
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PointKind {
    Stream,
    Cache,
}

#[derive(Debug, Clone, Deserialize)]
struct PointLogs {
    records: u64,
    #[serde(default)]
    group_cursors: Option<u64>,
    #[serde(default)]
    group_dead_letters: Option<u64>,
    #[serde(default)]
    counters: Option<u64>,
}

/// One log to cut back.
#[derive(Debug)]
pub(crate) struct Cut {
    shard: PointShard,
    kind: LogKind,
    offset: u64,
}

/// Where a shard's log of `kind` lives under the durable root; the same
/// layout `node::storage::open` gives each store.
fn log_dir(root: &Path, kind: LogKind, shard: &PointShard) -> PathBuf {
    let store = match kind {
        LogKind::Stream => root.to_path_buf(),
        LogKind::Cache => root.join("caches"),
        LogKind::GroupCursors => root.join("groups"),
        LogKind::GroupDeadLetters => root.join("dead-letters"),
        LogKind::Counters => root.join("counters"),
    };
    shard_dir(
        &store,
        &felix_storage::log::ShardKey {
            tenant: shard.tenant_id.clone(),
            namespace: shard.namespace.clone(),
            stream: shard.name.clone(),
            shard: shard.shard,
        },
    )
}

#[cfg(test)]
mod tests;

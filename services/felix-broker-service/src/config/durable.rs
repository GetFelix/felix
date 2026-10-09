//! Environment configuration for the broker's durable storage.
//!
//! A module of its own because the rest of `config` owns transport, batching and
//! queue tuning, and durability is a separate concern with its own failure modes.
//! The one thing they share is the `FELIX_*` naming convention.
//!
//! Durability is opt-in. With `FELIX_DURABLE_STORAGE_DIR` unset the broker runs
//! in memory only, and any stream the control plane marks `durable: true` is
//! rejected at registration rather than silently downgraded to a guarantee the
//! broker cannot keep.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_storage::log::{FsyncMode, LogConfig, OffloadTarget};

/// Durable storage settings resolved from the environment.
#[derive(Debug, Clone)]
pub struct DurableStorageConfig {
    /// Root directory holding one subdirectory per stream shard.
    pub root: PathBuf,
    /// Segment, index and fsync policy passed to every log.
    pub log: LogConfig,
    /// Where stream logs copy their sealed segments
    /// (`FELIX_DURABLE_OFFLOAD_DIR`). `None` copies nothing.
    pub offload_dir: Option<PathBuf>,
}

impl DurableStorageConfig {
    /// Read the configuration, or `None` when durable storage is not enabled.
    pub fn from_env() -> Result<Option<Self>> {
        let Some(root) = std::env::var("FELIX_DURABLE_STORAGE_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };

        let log = LogConfig {
            segment_size_bytes: parse_env("FELIX_DURABLE_SEGMENT_BYTES")?
                .unwrap_or(LogConfig::default().segment_size_bytes),
            index_spacing_bytes: parse_env("FELIX_DURABLE_INDEX_SPACING_BYTES")?
                .unwrap_or(LogConfig::default().index_spacing_bytes),
            fsync_mode: fsync_mode_from_env()?,
            max_records_per_read: parse_env("FELIX_DURABLE_MAX_RECORDS_PER_READ")?
                .unwrap_or(LogConfig::default().max_records_per_read),
            preallocate_segments: parse_bool_env("FELIX_DURABLE_PREALLOCATE")?
                .unwrap_or(LogConfig::default().preallocate_segments),
            verify_all_on_open: parse_bool_env("FELIX_DURABLE_VERIFY_ALL_ON_OPEN")?
                .unwrap_or(LogConfig::default().verify_all_on_open),
            repair_checksum_tail: parse_bool_env("FELIX_DURABLE_REPAIR_CHECKSUM_TAIL")?
                .unwrap_or(LogConfig::default().repair_checksum_tail),
            // Off by default: rolling segments in the background measured worse
            // than rolling them inline, because a device-level flush does not
            // overlap with concurrent writes. Exposed anyway, since that is a
            // property of the platform's fsync rather than of the design.
            rollover_threshold_percent: parse_env("FELIX_DURABLE_ROLLOVER_THRESHOLD_PERCENT")?
                .unwrap_or(LogConfig::default().rollover_threshold_percent),
            max_overshoot_percent: parse_env("FELIX_DURABLE_MAX_OVERSHOOT_PERCENT")?
                .unwrap_or(LogConfig::default().max_overshoot_percent),
            // Unset means unbounded growth
            retention_bytes: parse_env("FELIX_DURABLE_RETENTION_BYTES")?,
            retention_age: parse_env::<u64>("FELIX_DURABLE_RETENTION_SECONDS")?
                .map(Duration::from_secs),
            retention_check_interval: parse_env::<u64>("FELIX_DURABLE_RETENTION_INTERVAL_SECONDS")?
                .map(Duration::from_secs)
                .unwrap_or(LogConfig::default().retention_check_interval),
            max_open_sealed_segments: LogConfig::default().max_open_sealed_segments,
            // Set per store: only stream logs offload. See `stream_log`.
            offload: None,
            // Set from the broker's cluster configuration when the stores open.
            retention_hold: LogConfig::default().retention_hold,
        };
        let offload_dir = std::env::var("FELIX_DURABLE_OFFLOAD_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from);
        // Fail at startup rather than at the first durable publish.
        log.validate()
            .map_err(|err| anyhow::anyhow!("invalid durable storage configuration: {err}"))?;

        Ok(Some(Self {
            root: PathBuf::from(root),
            log,
            offload_dir,
        }))
    }

    /// The configuration for stream logs: `log`, plus offload when it is on.
    /// Caches, counters and consumer groups compact rather than age out, so
    /// they keep `log` as it is.
    ///
    /// Offloaded keys start with `cluster_node` (`FELIX_NODE_ID`) when the
    /// broker has one, and otherwise with an id generated once for this data
    /// directory and kept in it, so brokers sharing an archive never write
    /// to the same key.
    pub fn stream_log(&self, cluster_node: Option<&str>) -> Result<LogConfig> {
        let offload = match &self.offload_dir {
            Some(dir) => Some(OffloadTarget::LocalDir {
                dir: dir.clone(),
                node: match cluster_node {
                    Some(node) => node.to_string(),
                    None => storage_node_id(&self.root)?,
                },
            }),
            None => None,
        };
        Ok(LogConfig {
            offload,
            ..self.log.clone()
        })
    }

    /// One-line summary for the startup log.
    pub fn summary(&self) -> String {
        let durability = match self.log.fsync_mode {
            FsyncMode::None => "no fsync (data at risk until the OS flushes)".to_string(),
            FsyncMode::Periodic { interval } => {
                format!("fsync every {}ms", interval.as_millis())
            }
            FsyncMode::OnCommit => "fsync before every acknowledgement".to_string(),
        };
        let retention = match (self.log.retention_bytes, self.log.retention_age) {
            (None, None) => " retention=off (log grows unbounded)".to_string(),
            (bytes, age) => {
                let mut parts = Vec::new();
                if let Some(bytes) = bytes {
                    parts.push(format!("{bytes}B"));
                }
                if let Some(age) = age {
                    parts.push(format!("{}s", age.as_secs()));
                }
                format!(" retention={}", parts.join("/"))
            }
        };
        let offload = match &self.offload_dir {
            Some(dir) => format!(" offload={}", dir.display()),
            None => String::new(),
        };
        format!(
            "root={} segment={}B index_spacing={}B {durability}{retention}{offload}",
            self.root.display(),
            self.log.segment_size_bytes,
            self.log.index_spacing_bytes,
        )
    }
}

/// Where a broker without `FELIX_NODE_ID` keeps the id its offloaded keys
/// start with.
const NODE_ID_FILE: &str = "node-id";

/// This data directory's node id, generated and written on first use.
///
/// Losing the file only means later copies go under a new id: the manifest
/// records each copy's full key, so nothing already recorded moves.
fn storage_node_id(root: &Path) -> Result<String> {
    let path = root.join(NODE_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(id) if !id.trim().is_empty() => return Ok(id.trim().to_string()),
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    std::fs::create_dir_all(root).with_context(|| format!("create {}", root.display()))?;
    // Written whole and renamed into place, so a crash never leaves a
    // half-written id to be read back.
    let temporary = root.join(format!("{NODE_ID_FILE}.tmp"));
    {
        let file = std::fs::File::create(&temporary)
            .with_context(|| format!("create {}", temporary.display()))?;
        std::io::Write::write_all(&mut &file, id.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&temporary, &path).with_context(|| format!("write {}", path.display()))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(id)
}

/// `none` | `periodic` | `on_commit`, defaulting to the `LogConfig` default.
///
/// `periodic` reads its interval from `FELIX_DURABLE_FSYNC_INTERVAL_MS`.
fn fsync_mode_from_env() -> Result<FsyncMode> {
    let interval = parse_env::<u64>("FELIX_DURABLE_FSYNC_INTERVAL_MS")?.map(Duration::from_millis);
    let Some(raw) = std::env::var("FELIX_DURABLE_FSYNC_MODE").ok() else {
        // No mode given: keep the default policy but honour an explicit
        // interval, since setting only the interval clearly means "periodic".
        return Ok(match (LogConfig::default().fsync_mode, interval) {
            (FsyncMode::Periodic { .. }, Some(interval)) => FsyncMode::Periodic { interval },
            (mode, _) => mode,
        });
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "none" | "off" => Ok(FsyncMode::None),
        "on_commit" | "on-commit" | "commit" => Ok(FsyncMode::OnCommit),
        "periodic" => Ok(FsyncMode::Periodic {
            interval: interval.unwrap_or(Duration::from_millis(250)),
        }),
        other => bail!(
            "FELIX_DURABLE_FSYNC_MODE must be one of none, periodic, on_commit (got {other:?})"
        ),
    }
}

fn parse_env<T: std::str::FromStr>(name: &str) -> Result<Option<T>>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .trim()
            .parse::<T>()
            .map(Some)
            .map_err(|err| anyhow::anyhow!("{err}"))
            .with_context(|| format!("parse {name}")),
    }
}

fn parse_bool_env(name: &str) -> Result<Option<bool>> {
    match std::env::var(name) {
        Err(_) => Ok(None),
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(None),
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            other => bail!("{name} must be a boolean (got {other:?})"),
        },
    }
}

#[cfg(test)]
mod tests;

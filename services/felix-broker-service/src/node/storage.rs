//! Opening what the broker keeps on disk, and building the `Broker` over it.
//!
//! Durable storage is opened before the listener binds: recovering segments
//! can take time and can fail, and both are better surfaced as a startup
//! error than as a failed publish once traffic is arriving.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_broker::{Broker, DurableStorage};
use felix_storage::log::FsyncMode;
use felix_storage::{EphemeralCache, StorageId};

use crate::cluster::membership::Registered;
use crate::config::{BrokerConfig, DurableStorageConfig};

/// Open durable storage, the cache, consumer groups and counters, and build
/// the broker over them. Without durable storage everything but the cache is
/// left out and the cache lives in memory.
///
/// Also returns the data directory's storage id, read before the broker
/// registers so the control plane and peers only ever see the id of the data
/// it serves; whether this start rotated it: after a crash that may have
/// lost records the broker acknowledged, or because `registered`, the
/// control plane's record of this node, names a later incarnation than the
/// one the directory recorded; whether `registered` names another
/// storage id; and what the directory kept of its ballots ([`Kept`]).
pub(crate) fn open(
    config: &BrokerConfig,
    registered: Option<&Registered>,
) -> Result<(Broker, Option<DurableStorage>, StorageId, bool, bool, Kept)> {
    open_with(config, DurableStorageConfig::from_env()?, registered)
}

/// [`open`] with the durable settings given rather than read from the
/// environment.
pub(crate) fn open_with(
    config: &BrokerConfig,
    durable_config: Option<DurableStorageConfig>,
    registered: Option<&Registered>,
) -> Result<(Broker, Option<DurableStorage>, StorageId, bool, bool, Kept)> {
    // Test-only: the cluster harness's power loss. Armed before any store
    // opens, since the model takes what is already on disk as durable.
    #[cfg(debug_assertions)]
    felix_storage::fault::arm_power_loss_from_env().context("arm the simulated power loss")?;
    // Before any store opens: a store's recovery is the first write of this
    // run, and the record that a run is under way must already be on disk.
    let (storage_id, unclean, replaced, kept) = match &durable_config {
        Some(durable) => start_run(
            &durable.root,
            &durable.log,
            registered,
            witness_dir().as_deref(),
        )?,
        // Nothing on disk outlives the process, so a restart is a wipe.
        None => (storage_id(None)?, false, false, Kept::default()),
    };
    let hold = retention_hold(config);
    let durable_storage = match &durable_config {
        Some(durable) => {
            tracing::info!(config = %durable.summary(), "opening durable stream storage");
            let cluster_node = config.membership.as_ref().map(|m| m.node_id.as_str());
            let stream_log = felix_storage::log::LogConfig {
                retention_hold: hold,
                ..durable
                    .stream_log(cluster_node)
                    .context("resolve the node id for offloaded segments")?
            };
            let storage = DurableStorage::open(&durable.root, stream_log)
                .with_context(|| format!("open durable storage at {}", durable.root.display()))?;
            Some(storage)
        }
        None => {
            tracing::info!(
                "durable stream storage disabled (set FELIX_DURABLE_STORAGE_DIR to enable)"
            );
            None
        }
    };

    // The cache is a log when there is a disk to put one on.
    //
    // Under `caches/`, not the stream root: a shard directory is named from a
    // hash of its tenant, namespace and stream, so a cache and a stream sharing
    // a name would otherwise interleave their records. See
    // `docs/cache-on-log.md`.
    //
    // Without durable storage the cache stays in memory, which is the only
    // thing it can be: there is nowhere to write a log.
    let cache: Box<dyn felix_storage::StorageApi + Send> = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("caches");
            tracing::info!(root = %root.display(), "opening the cache on durable storage");
            Box::new(
                felix_storage::LogCache::open(
                    &root,
                    felix_storage::log::LogConfig {
                        retention_hold: hold,
                        ..durable.log.clone()
                    },
                )
                .with_context(|| format!("open the cache log at {}", root.display()))?,
            )
        }
        None => {
            tracing::info!("cache is in memory and is lost on restart");
            Box::new(EphemeralCache::new())
        }
    };

    // Consumer-group positions live on their own root, for the same reason the
    // cache does: a stream named `orders` and a cache named `orders` must not
    // share a directory, and neither must the group state for either.
    //
    // Only with durable storage. A group whose position is lost on restart
    // redelivers everything it had already processed, so an in-memory version
    // would be worse than not offering queues at all.
    let consumer_groups = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("groups");
            tracing::info!(root = %root.display(), "opening consumer-group state");
            let dead_root = durable.root.join("dead-letters");
            Some((
                std::sync::Arc::new(
                    felix_broker::ConsumerGroups::open(&root, durable.log.clone()).with_context(
                        || format!("open the consumer-group log at {}", root.display()),
                    )?,
                ),
                std::sync::Arc::new(
                    felix_broker::DeadLetters::open(&dead_root, durable.log.clone()).with_context(
                        || format!("open the dead-letter log at {}", dead_root.display()),
                    )?,
                ),
            ))
        }
        None => None,
    };

    let broker = Broker::new(cache)
        .with_topic_capacity(config.subscriber_queue_capacity.max(1))
        .context("configure subscriber queue depth")?
        .with_subscriber_queue_policy(config.subscriber_queue_policy)
        .with_subscriber_queue_max_bytes(config.subscriber_queue_max_bytes)
        .with_subscriber_queue_max_bytes_total(config.subscriber_queue_max_bytes_total)
        .with_replay_ring_max_bytes(
            config.replay_ring_max_bytes,
            config.replay_ring_max_bytes_total,
        )
        .with_cache_watch_max_bytes(
            config.cache_watch_max_bytes,
            config.cache_watch_max_bytes_total,
        );
    let broker = match consumer_groups {
        Some((groups, dead_letters)) => broker.with_consumer_groups(
            groups,
            dead_letters,
            Duration::from_millis(config.group_visibility_timeout_ms),
            config.group_max_attempts,
        ),
        None => broker,
    };
    if let Some(reader) = broker.group_reader() {
        reader.set_max_in_flight(config.group_max_in_flight);
    }
    let broker = match durable_storage.clone() {
        Some(storage) => broker.with_durable_storage(storage),
        None => broker,
    };
    // Counters on a root of their own, beside the cache's rather than inside
    // it: a counter record is a new durable shape, and a store of its own
    // keeps its blast radius to the counters. Only with durable storage — a
    // sum any restart resets is worse than refusing to count.
    let broker = match &durable_config {
        Some(durable) => {
            let root = durable.root.join("counters");
            tracing::info!(root = %root.display(), "opening counters");
            broker.with_counters(std::sync::Arc::new(
                felix_storage::CounterStore::open(&root, durable.log.clone())
                    .with_context(|| format!("open the counter log at {}", root.display()))?,
            ))
        }
        None => broker,
    };
    Ok((broker, durable_storage, storage_id, unclean, replaced, kept))
}

/// Start a run on `root` and say what it started under: the storage id,
/// whether it was rotated, and whether the control plane last registered
/// this node under another id, now or at any start on this directory since.
/// That is the evidence of a wipe a copy cannot find in a set written since
/// (#1331): what the node held under the old id is gone or set aside. It is
/// kept on disk before the new id registers, so a restart after that still
/// knows. A node the control plane never registered, or one registered before
/// storage ids, held nothing under another id, so a new broker's copies vote
/// however new its directory is.
///
/// The last flag says the rotation was this directory's own crash, and that
/// the directory never replaced a lost copy ([`note_lost_copy`]). Ballots
/// are always synced, so such a crash kept every one. A restore rolls them
/// back, and a wiped directory's ballots may have been taken after the node
/// lost others, so neither keeps every promise it made. Without the control
/// plane's record of this node, or one without a storage id, nothing shows
/// the rotation was not a lost copy, so the flag is false (#2047).
///
/// Every run also writes a new run witness under `witness`, whether or not
/// the control plane answered, so a later start from the view never takes a
/// copy from before this run for the directory.
fn start_run(
    root: &Path,
    log: &felix_storage::log::LogConfig,
    registered: Option<&Registered>,
    witness: Option<&Path>,
) -> Result<(StorageId, bool, bool, Kept)> {
    // The id before this run rotates it: the control plane naming that one
    // is what tells a crash of this directory from a lost copy.
    let before = felix_storage::storage_identity::load(root)
        .with_context(|| format!("read the storage id under {}", root.display()))?;
    let begun = begin_run(root, log, registered.map(|r| r.incarnation))?;
    felix_storage::run_state::witness::witness(root, witness, this_boot(), fresh_storage_id())
        .with_context(|| format!("write the run witness for {}", root.display()))?;
    let restored = begun.origin == felix_storage::storage_identity::Origin::Loaded
        && begun.recorded.behind(registered.map(|r| r.incarnation));
    let before = before
        .filter(|_| begun.origin == felix_storage::storage_identity::Origin::Loaded)
        .map(|id| id.to_hex());
    // Before `note_replaced`, which may write its marker for this crash.
    let lost = note_lost_copy(root, before.as_deref(), restored, registered)?;
    let own_id = registered
        .and_then(|r| r.storage_id.as_deref())
        .is_some_and(|prior| before.as_deref() == Some(prior));
    let kept = Kept {
        promises: begun.rotated && !restored && own_id && !lost,
        // A record from before storage ids cannot tell this directory from a
        // wiped one, as for `promises` (#2047).
        ballots: !restored
            && !lost
            && (!begun.rotated || own_id)
            && registered.is_none_or(|r| r.storage_id.is_some()),
        // A set names a copy by the id the control plane registered, so
        // without one no set can readmit it.
        readmits: registered.is_some_and(|r| r.storage_id.is_some()),
    };
    let replaced = note_replaced(root, begun.id, registered)?;
    Ok((begun.id, begun.rotated, replaced, kept))
}

/// What a start found its directory kept of the ballots it took.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Kept {
    /// An unclean start whose crash kept every ballot: the flag
    /// [`start_run`]'s documentation describes.
    pub(crate) promises: bool,
    /// The directory kept every ballot it ever took, at a clean start or an
    /// unclean one: never restored, never a replacement for a lost copy,
    /// not registered under a record without a storage id, and after a
    /// rotation the control plane's record names the id it had before. What
    /// a fence answer attests as kept.
    pub(crate) ballots: bool,
    /// When `ballots` is false, a copy here counts again once a set
    /// introduced after the loss readmits it: the start was checked against
    /// a control plane record with a storage id.
    pub(crate) readmits: bool,
}

impl Kept {
    /// What a fence answer may attest as kept, or readmitted. A start from the view has not
    /// checked the directory against the control plane, and a restored one
    /// reads as kept until it has (`CountsForgotten`, `TrustsRegistered` in
    /// `docs/formal/FelixShardUnified.tla`), so such a run attests nothing.
    pub(crate) fn attested(self, from_view: bool) -> Self {
        Self {
            promises: self.promises,
            ballots: self.ballots && !from_view,
            readmits: self.readmits && !from_view,
        }
    }
}

/// Whether the directory at `root` replaced a copy the node lost, now or at
/// any start since, recorded on disk before it returns. Now when `restored`,
/// or when `registered` names a storage id other than `before`, the id the
/// directory had before this start rotated it (`None` for a directory with
/// none). A directory without the marker that has the replaced-storage one
/// counts as lost: an older build wrote that one after a crash too, so it
/// cannot say which it was.
pub(crate) fn note_lost_copy(
    root: &Path,
    before: Option<&str>,
    restored: bool,
    registered: Option<&Registered>,
) -> Result<bool> {
    let context = || format!("read the lost-copy marker under {}", root.display());
    let marked = felix_storage::run_state::lost_copy(root).with_context(context)?;
    let earlier = match marked {
        Some(lost) => lost,
        None => felix_storage::run_state::replaced(root).with_context(context)?,
    };
    let now = restored
        || registered
            .and_then(|r| r.storage_id.as_deref())
            .is_some_and(|prior| before != Some(prior));
    let lost = earlier || now;
    if marked != Some(lost) {
        felix_storage::run_state::record_lost_copy(root, lost)
            .with_context(|| format!("record the lost-copy marker under {}", root.display()))?;
    }
    Ok(lost)
}

/// Whether the node held copies under a storage id other than `id`: the
/// control plane's record `registered` names another one, now or at any
/// earlier start on `root`. Recorded on disk before it returns.
pub(crate) fn note_replaced(
    root: &Path,
    id: StorageId,
    registered: Option<&Registered>,
) -> Result<bool> {
    let replaced_after = registered
        .filter(|r| {
            r.storage_id
                .as_deref()
                .is_some_and(|prior| prior != id.to_hex())
        })
        .map(|r| r.incarnation);
    let replaced_now = replaced_after.is_some();
    if let Some(after) = replaced_after {
        felix_storage::run_state::record_replaced(root, id, after)
            .with_context(|| format!("record a replaced storage id under {}", root.display()))?;
    }
    Ok(replaced_now
        || felix_storage::run_state::replaced(root).with_context(|| {
            format!("read the replaced-storage marker under {}", root.display())
        })?)
}

/// The id kept under the durable root, or, without durable storage, a random
/// one for this process that is never written down: a restart of such a
/// broker has lost everything, and a new id says so.
pub(crate) fn storage_id(root: Option<&Path>) -> Result<StorageId> {
    match root {
        Some(root) => felix_storage::storage_identity::load_or_create(root, fresh_storage_id)
            .map(|(id, _)| id)
            .with_context(|| format!("read the storage id under {}", root.display())),
        None => Ok(StorageId::from_bytes(fresh_storage_id())),
    }
}

/// Start a run on the data directory at `root` and return the id to announce.
/// A machine crash under `Periodic` or `None` may have lost records peers
/// counted, and a directory older than the node's last registration may hold
/// fewer than they counted, so either comes back with a new id. On Linux a
/// process crash in the same boot keeps it. See `felix_storage::run_state`.
fn begin_run(
    root: &Path,
    log: &felix_storage::log::LogConfig,
    registered: Option<u64>,
) -> Result<felix_storage::run_state::Begun> {
    let relaxed = !matches!(log.fsync_mode, FsyncMode::OnCommit);
    let begun = felix_storage::run_state::begin(
        root,
        relaxed,
        registered,
        this_boot(),
        || felix_storage::inspect::any_repair_pending(root, log),
        fresh_storage_id,
    )
    .with_context(|| format!("start a run on {}", root.display()))?;
    // A failed fsync may drop acknowledged pages with the machine still up.
    felix_storage::run_state::watch_poison(root);
    if begun.rotated {
        tracing::warn!(
            root = %root.display(),
            "starting under a new storage id, and leading no shard from the copy it led before"
        );
    }
    Ok(begun)
}

/// Where run witnesses are kept. See `felix_storage::run_state::witness`.
pub(crate) fn witness_dir() -> Option<std::path::PathBuf> {
    felix_storage::run_state::witness::default_dir()
}

/// The machine's boot id. Debug builds take it from the file
/// `FELIX_STORAGE_BOOT_ID_FILE` names when it exists, which is how the
/// cluster harness restarts a broker as if its machine had rebooted.
pub(crate) fn this_boot() -> Option<felix_storage::run_state::BootId> {
    #[cfg(debug_assertions)]
    if let Some(path) = std::env::var_os("FELIX_STORAGE_BOOT_ID_FILE")
        && let Ok(text) = std::fs::read_to_string(path)
    {
        return felix_storage::run_state::BootId::parse(&text);
    }
    felix_storage::run_state::this_boot()
}

/// Record that this run on `root` may have lost records it acknowledged with
/// the machine still up, so a crash before the next start rotates the id.
pub(crate) fn forget_boot(root: &Path) -> Result<()> {
    felix_storage::run_state::forget_boot(root)
        .with_context(|| format!("record an unsafe stop under {}", root.display()))
}

/// Record that this run on `root` stopped with everything it acknowledged
/// synced, so the next start keeps `id`.
pub(crate) fn mark_clean_stop(root: &Path, id: StorageId) -> Result<()> {
    felix_storage::run_state::mark_clean(root, id)
        .with_context(|| format!("record a clean stop under {}", root.display()))
}

/// Give the data directory under `root` a new storage id, for when its
/// contents have become an older copy of themselves.
pub(crate) fn rotate_storage_id(root: &Path) -> Result<StorageId> {
    rotate_storage_id_after(root, |_| Ok(()))
}

/// [`rotate_storage_id`], running `before` with the new id first.
pub(crate) fn rotate_storage_id_after(
    root: &Path,
    before: impl FnOnce(StorageId) -> Result<()>,
) -> Result<StorageId> {
    let fresh = fresh_storage_id();
    before(StorageId::from_bytes(fresh))?;
    // Through copy-trust, so every copy on the directory is distrusted first.
    felix_storage::run_state::copy_trust::rotate_storage_id(root, fresh)
        .with_context(|| format!("write a new storage id under {}", root.display()))
}

fn fresh_storage_id() -> [u8; 16] {
    felix_common::random::uuid_v4().into_bytes()
}

/// How stream and cache logs treat a retention hold they saved.
///
/// A broker with neither membership nor a peer transport never replicates:
/// nothing would ever advance a log's commit offset, and it has no follower
/// to protect, so a hold saved while it was clustered is lifted. Read from
/// configuration, so a clustered broker whose replication has not started
/// yet keeps its holds.
fn retention_hold(config: &BrokerConfig) -> felix_storage::log::RetentionHold {
    if config.membership.is_none() && config.peer_transport.is_none() {
        felix_storage::log::RetentionHold::Lifted
    } else {
        felix_storage::log::RetentionHold::AsSaved
    }
}

#[cfg(test)]
mod tests;

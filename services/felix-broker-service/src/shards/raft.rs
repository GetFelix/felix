//! The stream shards a Raft group runs on this broker.
//!
//! [`RaftShards`] takes every durable stream shard out of the feed's
//! assignments before the hybrid sees them and keeps this broker's Raft
//! members open: a member per shard this broker is a replica of, on the
//! shard's own directory. Placement decides where a group forms. Once it
//! has, its members are the group's own, and placement changes none of them.
//! See `docs/per-shard-raft.md`, "Forming a group".
//!
//! No member opens before the data root has passed the restore gate
//! ([`RaftShards::set_root`]), and each opens as a copy through it, so a
//! directory that may not keep its copy id comes back as a new member.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Context;
use felix_broker::{Broker, StreamLog};
use felix_common::fleet::{self, FleetGate};
use felix_common::random::{HashMap, HashSet};
use felix_replication::engine::WriteFence;
use felix_replication::raft::{
    GroupId, GroupRuntime, MemberId, NotBlank, OpenError, RaftEngine, Target, TargetVersion,
};
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_storage::raft_copy::{CopyRoot, EpochBase, holds_log_without_copy};
use futures::StreamExt;

use super::routing::{Dispatch, EngineShards, Reason, Taken};
use super::watch::ShardAssignment;
use super::{ShardKey, ShardKind};
use crate::cluster::lease::{Head, LeaseState};

pub(crate) mod alone_cluster;
pub(crate) mod copy_id;
mod fleet_wait;
pub(crate) mod floor;
mod groups;
pub(crate) mod reform_survey;
mod restart;
pub(crate) use restart::StoppedCopy;
pub(crate) mod standalone;
pub(crate) use groups::Groups as RaftGroups;

/// How long a leader that could not move its stream to the committed tail
/// waits before it tries again.
const LEAD_RETRY: Duration = Duration::from_millis(50);

/// How long the broker that forms a group waits for each other member to
/// say its log is empty. One that does not is asked again on the next pass.
const ASK_WITHIN: Duration = Duration::from_secs(5);

/// How many engine markers a pass reads at once.
const MARKER_READS: usize = 16;

/// Shards no group forms for because placement names fewer brokers than the
/// stream's replication factor.
pub(crate) const SHORT_PLACEMENT: &str = "felix_broker_raft_shards_short_placement";

/// Raft shards open here whose group's voters are not the brokers placement
/// names for the shard.
pub(crate) const OFF_PLACEMENT: &str = "felix_broker_raft_shards_off_placement";

/// Raft shards whose term this broker leads and serves.
pub(crate) const LED: &str = "felix_broker_raft_shards_led";

/// Raft shards this broker leads whose placement names another leader.
pub(crate) const LED_OFF_PLACEMENT: &str = "felix_broker_raft_shards_led_off_placement";

/// Raft shards whose copy here a storage error stopped and that have not
/// rejoined a working group since.
pub(crate) const STOPPED_ON_STORAGE_ERROR: &str =
    "felix_broker_raft_shards_stopped_on_storage_error";

/// This broker's Raft members of stream shards.
pub(crate) struct RaftShards {
    me: Weak<Self>,
    broker: Arc<Broker>,
    runtime: GroupRuntime,
    engine: RaftEngine,
    local: String,
    /// Whether the fleet finalized `raft_written_commit`, read at each
    /// open: a member opened strict stays strict until it opens again.
    fleet: Arc<FleetGate>,
    /// Holds member opens until the fleet's features are known.
    fleet_wait: parking_lot::Mutex<fleet_wait::FleetWait>,
    /// Set by [`Self::stand_after`]: these members open before the control
    /// plane answers, so they do not wait for the fleet.
    votes_unchecked: std::sync::atomic::AtomicBool,
    leading: Arc<Leading>,
    /// Set when a member's log failed to close: its last sync may not have
    /// run, so the broker must not record a clean stop.
    close_failed: std::sync::atomic::AtomicBool,
    /// Every Raft shard in the feed: the broker its placement names as
    /// leader and the voters its leader last reported. Where a request goes
    /// while no member here knows the leader.
    placed: parking_lot::Mutex<HashMap<ShardKey, Placed>>,
    /// Shards whose other members are being asked whether a group reached
    /// them.
    forming: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards whose forming was put off, so the reason is logged once, not
    /// on every pass.
    put_off: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards whose marker could not be read, logged once while it stays so.
    unreadable: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards not placed here whose directory was found unmarked, by
    /// incarnation: only this module marks one, so it is read once.
    unclaimed: parking_lot::Mutex<HashMap<ShardKey, u64>>,
    /// Counts the attempts to claim a directory, failed ones too, so a pass
    /// knows whether a marker it read before taking `open` may have been
    /// written since.
    claims: AtomicU64,
    /// Shards already reported as off their placement, so each is logged
    /// once per time it happens.
    off_placement: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards with a member open here whose placement names only other
    /// brokers. See [`EngineShards::placed_elsewhere`].
    unplaced: parking_lot::Mutex<HashSet<ShardKey>>,
    /// The placement generation each held-back short shard was last warned
    /// for, so a placement version warns once.
    short_warned: parking_lot::Mutex<HashMap<ShardKey, u64>>,
    /// Shards whose directory holds a log with no copy id, logged once.
    carried_over: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Limits the warning for a shard whose open waits on the open-file limit.
    held_warn: parking_lot::Mutex<super::lifecycle::WarnThrottle>,
    /// Members closed after their core stopped on a storage error.
    restarts: parking_lot::Mutex<restart::Restarts>,
    /// Why each copy a storage error stopped here stopped, for the gauge and
    /// `shard_inspect`. A client is told only that it stopped.
    stopped_copies: parking_lot::Mutex<HashMap<ShardKey, StoppedCopy>>,
    /// This broker's part of each applied re-form plan, from the latest
    /// `start` survey, keyed by shard with the incarnation and the epoch it
    /// starts. The feed pass starts a stopped copy from it.
    reform_starts: Arc<parking_lot::Mutex<HashMap<ShardKey, reform_survey::PlannedStart>>>,
    /// The data root, once the restore gate has passed it. No member opens
    /// before.
    root: std::sync::OnceLock<Arc<CopyRoot>>,
    /// What the last pass's feed view carried for each member open at that
    /// view's stream incarnation: its target, or `None` for no target.
    targets: parking_lot::Mutex<HashMap<ShardKey, Option<Target>>>,
    /// The full placement set and stored target of each Raft shard in the
    /// last pass's feed, written under `open`. A form re-checks it before
    /// it initializes.
    formable: parking_lot::Mutex<HashMap<ShardKey, Formable>>,
    /// While true, a form that has its answers does not initialize.
    #[cfg(test)]
    stall_forms: tokio::sync::watch::Sender<bool>,
    /// Every target a member kept, with the term it was told for. `None`
    /// is a kept "no target".
    #[cfg(test)]
    told: parking_lot::Mutex<Vec<(ShardKey, Option<Target>, u64)>>,
    /// While true, a marker read does not return.
    #[cfg(test)]
    stall_reads: tokio::sync::watch::Sender<bool>,
    #[cfg(test)]
    reads_under_lock: AtomicU64,
    #[cfg(test)]
    marker_reads: AtomicU64,
    /// Holds a retention pass between its plan and its proposal.
    #[cfg(test)]
    retention_gate: retention::Gate,
    /// Held across a whole pass, after its marker reads, and a whole
    /// close, so a stream's delete never runs between a member's claim of
    /// its directory and its open.
    open: tokio::sync::Mutex<HashMap<ShardKey, Member>>,
    /// What each open member's `raft-epoch` named at open, `None` for a copy
    /// without one. Only a re-form changes it, which needs the member closed.
    /// Read through [`proven_epoch`].
    epochs: parking_lot::Mutex<HashMap<ShardKey, Option<u64>>>,
    /// The group epoch the newest view's record is at, per shard.
    recorded_epochs: parking_lot::Mutex<HashMap<ShardKey, u64>>,
    /// The group the newest view's record marks formed, per shard.
    formed_marks: parking_lot::Mutex<HashMap<ShardKey, super::watch::RaftFormed>>,
    /// The voters the newest feed shows for each shard, so a leader re-sends
    /// a formed group's floor until the control plane has taken its voters.
    fed_voters: parking_lot::Mutex<HashMap<ShardKey, super::watch::RaftVoters>>,
    /// The copy id of each member open at the last pass's view, for the
    /// copy id report.
    held_copies: parking_lot::Mutex<Vec<copy_id::Held>>,
    /// Set by [`standalone::run`]: every group here has this broker as its
    /// only voter, so a relaxed crash is re-formed locally.
    alone: std::sync::atomic::AtomicBool,
    /// Where a cluster's RF 1 shard asks the control plane to start its
    /// re-formed epoch. Unset on a standalone broker and in a build with
    /// no control plane link.
    alone_writer: std::sync::OnceLock<Arc<dyn alone_cluster::AloneWriter>>,
    /// Held RF 1 shards whose re-form could not be decided when they
    /// opened, by the incarnation opened. The feed pass opens each again
    /// once it can be.
    alone_later: parking_lot::Mutex<HashMap<ShardKey, u64>>,
}

/// Whether a copy of `copy_epoch` is left behind by a record at `epoch`.
/// Every re-form stops the copies it reads and starts them in the new
/// epoch, so a copy that is neither stopped nor mid-start and names an
/// older epoch was never read: it holds its old membership and would elect
/// itself in it. A copy ahead of the record is a stale view, never closed.
fn left_behind(epoch: u64, copy_epoch: u64) -> bool {
    copy_epoch < epoch
}

/// A shard directory an earlier build wrote as a member with no copy id.
/// Opening it as a copy would rotate it, and with it every member of its
/// group, so the brokers would form the group again on empty logs.
#[derive(Debug, thiserror::Error)]
#[error(
    "{dir} holds a Raft log with no copy id, written by a build from before Raft members had \
     one; it is not opened. Delete the stream and create it again, or stop every broker that \
     holds the shard and remove this directory on each"
)]
pub(crate) struct CarriedOver {
    dir: String,
}

/// A copy of a group epoch the shard's record has left, which no re-form
/// read. Opened, it would elect itself in its old membership and serve a
/// log the group moved on from, so it stays closed. Its data is left for
/// the operator, and a later re-form that reads it starts it as a learner.
#[derive(Debug, thiserror::Error)]
#[error(
    "{dir} is a copy of group epoch {copy_epoch}, and the shard's record is at epoch {epoch}; \
     it is not opened"
)]
pub(crate) struct LeftBehind {
    dir: String,
    copy_epoch: u64,
    epoch: u64,
}

/// The fsync mode a member's Raft log opens with. A relaxed broker's own
/// mode lets Raft count an entry once it is written, which is safe only on a
/// root whose run rotates after a machine crash, and only once every broker
/// carries the copy's reservation (`raft_written_commit`). Otherwise
/// `OnCommit`. See `docs/per-shard-raft.md`, "Proposal: commit on a written majority".
///
/// A `none` broker's Raft logs still flush every [`NONE_RAFT_FLUSH`]: under
/// written commit nothing else moves a log's flushed position, and reclaiming
/// a kept copy waits on it.
fn raft_fsync_mode(broker: FsyncMode, written_commit: bool, dir: &Path) -> FsyncMode {
    if written_commit
        && felix_storage::run_state::relaxed(&broker)
        && felix_storage::run_state::rotates_on_crash(dir)
    {
        match broker {
            FsyncMode::None => FsyncMode::Periodic {
                interval: NONE_RAFT_FLUSH,
            },
            relaxed => relaxed,
        }
    } else {
        FsyncMode::OnCommit
    }
}

/// A shard's set and stored target as the feed last carried them, for a
/// form to check it still forms what placement wants.
#[derive(Debug, PartialEq, Eq)]
struct Formable {
    brokers: BTreeSet<String>,
    target: Option<BTreeSet<MemberId>>,
}

/// How often a `none` broker's written-commit Raft logs flush.
const NONE_RAFT_FLUSH: Duration = Duration::from_secs(1);

/// This broker's part in an abandoned epoch, from the shard's record.
struct AbandonedVoter {
    bases: Vec<EpochBase>,
    copy: u64,
    voters: BTreeSet<MemberId>,
}

/// One open member: the log is this module's to close, because the storage
/// provider never opened it.
struct Member {
    incarnation: u64,
    /// The copy id the member votes under, synced before the member opened.
    copy: u64,
    log: DiskLog,
    follow: Option<tokio::task::JoinHandle<()>>,
    /// The leader's retention timer (`retention`), run on every member so
    /// a member that wins a term enforces it at once.
    retention: Option<tokio::task::JoinHandle<()>>,
}

impl RaftShards {
    /// `runtime` must have no group open yet: it reports each shard's
    /// leadership here from this call on.
    pub(crate) fn new(
        broker: Arc<Broker>,
        runtime: GroupRuntime,
        local: String,
        fleet: Arc<FleetGate>,
    ) -> Arc<Self> {
        let terms = Arc::default();
        // A broker without a group reader serves no groups, so its values
        // are never read.
        let (visibility, max_attempts) = broker
            .group_reader()
            .map_or((Duration::from_secs(30), 1), |reader| {
                (reader.visibility(), reader.max_attempts())
            });
        let leading = Arc::new(Leading {
            broker: Arc::clone(&broker),
            groups: Arc::new(groups::Groups::new(
                groups::OnRaft::new(RaftEngine::new(runtime.clone()), Arc::clone(&terms)),
                visibility,
                max_attempts,
            )),
            terms,
            ending: Arc::default(),
            wake: std::sync::OnceLock::new(),
            lease: std::sync::OnceLock::new(),
            #[cfg(test)]
            stall: tokio::sync::watch::Sender::new(false),
            #[cfg(test)]
            stall_ends: tokio::sync::watch::Sender::new(false),
        });
        runtime.report_to(Arc::clone(&leading));
        if let Some(hub) = broker.cache_watches() {
            runtime.report_cache_changes(Arc::clone(hub) as Arc<dyn felix_storage::CacheObserver>);
        }
        let shards = Arc::new_cyclic(|me| Self {
            me: me.clone(),
            engine: RaftEngine::new(runtime.clone()),
            broker,
            runtime,
            local,
            fleet,
            fleet_wait: parking_lot::Mutex::default(),
            votes_unchecked: std::sync::atomic::AtomicBool::new(false),
            leading,
            close_failed: std::sync::atomic::AtomicBool::new(false),
            placed: parking_lot::Mutex::default(),
            forming: parking_lot::Mutex::default(),
            put_off: parking_lot::Mutex::default(),
            unreadable: parking_lot::Mutex::default(),
            unclaimed: parking_lot::Mutex::default(),
            claims: AtomicU64::new(0),
            off_placement: parking_lot::Mutex::default(),
            unplaced: parking_lot::Mutex::default(),
            #[cfg(test)]
            stall_reads: tokio::sync::watch::Sender::new(false),
            #[cfg(test)]
            reads_under_lock: AtomicU64::new(0),
            #[cfg(test)]
            marker_reads: AtomicU64::new(0),
            #[cfg(test)]
            retention_gate: retention::Gate::default(),
            carried_over: parking_lot::Mutex::default(),
            short_warned: parking_lot::Mutex::default(),
            held_warn: parking_lot::Mutex::default(),
            restarts: parking_lot::Mutex::default(),
            stopped_copies: parking_lot::Mutex::default(),
            reform_starts: Arc::default(),
            root: std::sync::OnceLock::new(),
            targets: parking_lot::Mutex::default(),
            formable: parking_lot::Mutex::default(),
            #[cfg(test)]
            stall_forms: tokio::sync::watch::Sender::new(false),
            #[cfg(test)]
            told: parking_lot::Mutex::default(),
            open: tokio::sync::Mutex::default(),
            epochs: parking_lot::Mutex::default(),
            recorded_epochs: parking_lot::Mutex::default(),
            formed_marks: parking_lot::Mutex::default(),
            fed_voters: parking_lot::Mutex::default(),
            held_copies: parking_lot::Mutex::default(),
            alone: std::sync::atomic::AtomicBool::new(false),
            alone_writer: std::sync::OnceLock::new(),
            alone_later: parking_lot::Mutex::default(),
        });
        shards
            .broker
            .set_engine_logs(Arc::clone(&shards) as Arc<dyn felix_broker::EngineLogs>);
        shards
            .broker
            .set_follower_reads(Arc::clone(&shards) as Arc<dyn felix_broker::FollowerReads>);
        shards
    }

    /// Let members open on `root`, which passed the restore gate. Only the
    /// first call counts.
    pub(crate) fn set_root(&self, root: Arc<CopyRoot>) {
        if self.root.set(root).is_ok()
            && let Some(wake) = self.leading.wake.get()
        {
            wake.notify_one();
        }
    }

    /// Members that open before `registered` is cancelled vote but do not
    /// lead until it is ([`replication::raft::GroupRuntime::stand_after`]).
    pub(crate) fn stand_after(&self, registered: tokio_util::sync::CancellationToken) {
        self.votes_unchecked.store(true, Ordering::Release);
        self.runtime.stand_after(registered);
    }

    /// The Raft engine, when this broker holds a member of `shard`.
    pub(crate) fn engine_for(&self, shard: &ShardKey) -> Option<&RaftEngine> {
        self.runtime.holds(shard).then_some(&self.engine)
    }

    /// This broker's member of `shard`, looked up once for a publish.
    pub(crate) fn shard_for(&self, shard: &ShardKey) -> Option<felix_replication::raft::RaftShard> {
        self.engine.shard(shard)
    }

    /// The committed log id of every group this broker leads, with the
    /// stream incarnation the group runs and the group epoch of its copy.
    /// A group whose epoch its copy cannot prove ([`proven_epoch`]) is left
    /// out until it can.
    pub(crate) fn led_floors(&self) -> Vec<floor::Led> {
        let epochs = self.epochs.lock();
        let recorded = self.recorded_epochs.lock();
        let marks = self.formed_marks.lock();
        let fed_voters = self.fed_voters.lock();
        self.runtime
            .led_reports()
            .into_iter()
            .filter_map(|(group, at, voters)| {
                let epoch = proven_epoch(
                    *epochs.get(&*group.shard)?,
                    recorded.get(&*group.shard).copied(),
                )?;
                let voters: Option<std::collections::BTreeMap<String, u64>> =
                    voters.map(|voters| {
                        voters
                            .into_iter()
                            .map(|voter| (voter.broker.to_string(), voter.copy))
                            .collect()
                    });
                let formed = floor::settled(
                    marks.get(&*group.shard),
                    fed_voters.get(&*group.shard),
                    group.epoch,
                    epoch,
                    voters.as_ref(),
                );
                Some(floor::Led {
                    key: (*group.shard).clone(),
                    incarnation: group.epoch,
                    epoch,
                    at,
                    voters,
                    formed,
                })
            })
            .collect()
    }

    /// The copy id of every member open here at the last pass, with what
    /// that pass's view said of the shard.
    pub(crate) fn held_copies(&self) -> Vec<copy_id::Held> {
        self.held_copies.lock().clone()
    }

    /// The consumer groups of this broker's Raft stream shards.
    pub(crate) fn groups(&self) -> &Arc<groups::Groups> {
        &self.leading.groups
    }

    /// Why this broker's copy of `shard` stopped, if a storage error stopped
    /// it and it has not rejoined a working group since. A core that stopped
    /// after the last feed pass is reported before the pass closes it.
    pub(crate) fn stopped_copy(&self, shard: &ShardKey) -> Option<StoppedCopy> {
        if let Some(stopped) = self.stopped_copies.lock().get(shard) {
            return Some(stopped.clone());
        }
        let error = self.runtime.storage_failure(shard)?;
        let term = self.runtime.status(shard).map_or(0, |status| status.term);
        Some(StoppedCopy { term, error })
    }

    /// The term this broker leads `shard` in with its stream brought up to
    /// the committed tail, or `None` while it may not take writes.
    pub(crate) fn leading(&self, shard: &ShardKey) -> Option<u64> {
        self.leading.generation(shard)
    }

    /// Open this broker's member of `key` as a copy on `root`. It forms
    /// nothing: a member on an empty log waits for [`Self::form`] here or
    /// for a leader's first append.
    ///
    /// The group's epoch is the stream's incarnation, so a member of an
    /// earlier incarnation refuses this one's messages. Its pre-vote answer
    /// would otherwise say blank for a directory the new group never touched.
    /// Start this broker's stopped copy in `dir` in the epoch the shard's
    /// `placed` record names, with this broker's part of the plan from the
    /// epoch's seed, or from the `start` survey when the seed has none.
    /// The bases and voters come from the record, never the survey, so a
    /// survey cannot start a copy into an epoch the store did not record.
    /// A copy that is not stopped in the epoch before, or has no start
    /// yet, is left alone: its open then refuses it as stopped and the
    /// next pass tries again. Run again after a crash, the start resumes.
    async fn start_reformed(
        &self,
        key: &ShardKey,
        assignment: &ShardAssignment,
        root: &CopyRoot,
        dir: &std::path::Path,
        config: &LogConfig,
    ) -> anyhow::Result<()> {
        let Some(bases) = assignment.epoch_bases() else {
            return Ok(());
        };
        let epoch = bases.len() as u64;
        let incarnation = assignment.incarnation;
        if self
            .start_abandoned(key, assignment, root, dir, config)
            .await?
        {
            return Ok(());
        }
        // The record's start first: the survey's lives only on the metadata
        // leader, so a leader change before this broker polled loses it.
        let recorded = assignment
            .raft_seed
            .as_ref()
            .filter(|seed| seed.incarnation == incarnation && seed.epoch == epoch)
            .and_then(|seed| seed.starts.get(self.local.as_str()).cloned());
        let Some(wire) = recorded.or_else(|| {
            self.reform_starts
                .lock()
                .get(key)
                .filter(|planned| planned.incarnation == incarnation && planned.epoch == epoch)
                .map(|planned| planned.start.clone())
        }) else {
            return Ok(());
        };
        let due = {
            let dir = dir.to_path_buf();
            tokio::task::spawn_blocking(move || felix_storage::raft_copy::start_due(&dir, epoch))
                .await??
        };
        if !due {
            return Ok(());
        }
        let voters = assignment
            .raft_seed
            .as_ref()
            .filter(|seed| seed.incarnation == incarnation && seed.epoch == epoch)
            .map(|seed| {
                seed.members
                    .iter()
                    .map(|(broker, copy)| MemberId::new(broker.as_str(), *copy))
                    .collect::<BTreeSet<_>>()
            });
        let Some(voters) = voters else {
            // A voter's snapshot names the epoch's voters; without them a
            // start would fold one that names nobody.
            return Ok(());
        };
        let fold = felix_replication::raft::GapSnapshot::new(key.kind, voters);
        root.start_stopped(
            &felix_replication::raft::mark_name(key),
            dir,
            incarnation,
            bases,
            &wire,
            &fold,
            &MemberId::new(self.local.as_str(), wire.voter.unwrap_or_default()).to_string(),
            config,
        )
        .await?;
        tracing::info!(shard = ?key, epoch, voter = wire.voter.is_some(), "started a stopped Raft copy in its re-formed epoch");
        Ok(())
    }

    /// This broker's copy id in the seed of the abandoned epoch `assignment`
    /// is at: its last base is empty and the seed, which the copy's own
    /// first report made, names this broker. `None` for any other epoch.
    fn abandoned_voter(&self, assignment: &ShardAssignment) -> Option<AbandonedVoter> {
        let bases = assignment.epoch_bases()?;
        if bases.last()?.base_last.is_some() {
            return None;
        }
        let epoch = bases.len() as u64;
        let seed = assignment
            .raft_seed
            .as_ref()
            .filter(|seed| seed.incarnation == assignment.incarnation && seed.epoch == epoch)?;
        let copy = *seed.members.get(self.local.as_str())?;
        let voters = seed
            .members
            .iter()
            .map(|(broker, copy)| MemberId::new(broker.as_str(), *copy))
            .collect();
        Some(AbandonedVoter {
            bases,
            copy,
            voters,
        })
    }

    /// Start this broker's blank copy in `dir` as the one voter of the
    /// abandoned epoch `assignment` records, at the epoch's first index.
    /// Returns whether it did. The bases and the voter come from the record
    /// alone, so a crash anywhere resumes from it on the next open.
    async fn start_abandoned(
        &self,
        key: &ShardKey,
        assignment: &ShardAssignment,
        root: &CopyRoot,
        dir: &std::path::Path,
        config: &LogConfig,
    ) -> anyhow::Result<bool> {
        let Some(AbandonedVoter {
            bases,
            copy,
            voters,
        }) = self.abandoned_voter(assignment)
        else {
            return Ok(false);
        };
        let first_index = bases.last().map_or(0, |base| base.first_index);
        let member = MemberId::new(self.local.as_str(), copy);
        // The seed's voters, as a re-form's start takes them: never a set
        // of this broker's own making.
        let fold = felix_replication::raft::GapSnapshot::new(key.kind, voters);
        let started = root
            .start_abandoned(
                &felix_replication::raft::mark_name(key),
                dir,
                assignment.incarnation,
                bases,
                copy,
                &fold,
                &member.to_string(),
                config,
            )
            .await?;
        if started {
            tracing::warn!(shard = ?key, first_index, "started an abandoned Raft shard empty at its epoch's first index");
        }
        Ok(started)
    }

    async fn open_member(
        &self,
        key: &ShardKey,
        assignment: &ShardAssignment,
        root: &CopyRoot,
    ) -> anyhow::Result<Member> {
        self.unclaimed.lock().remove(key);
        let broker = Arc::clone(&self.broker);
        let (shard, incarnation) = (key.clone(), assignment.incarnation);
        let written_commit = self.fleet.supports(fleet::RAFT_WRITTEN_COMMIT);
        let epoch_bases = assignment.epoch_bases();
        // Sums the hybrid kept for this shard would be lost under an empty
        // group, so a shard with any is refused like a log with segments.
        if key.kind == ShardKind::Cache
            && let Some(counters) = self.broker.counters()
        {
            let held = counters
                .dirs_on_disk(&key.tenant_id, &key.namespace, &key.stream)
                .await?;
            if held.contains(&(key.shard, incarnation)) {
                return Err(CarriedOver {
                    dir: format!(
                        "counters of {}/{}/{}/{}",
                        key.tenant_id, key.namespace, key.stream, key.shard
                    ),
                }
                .into());
            }
        }
        // The marker write fsyncs.
        let claim = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let storage = broker
                .durable_storage()
                .context("a Raft shard needs durable storage")?;
            // A cache shard's member lives under the cache store's root, so
            // it never shares a directory with a stream of the same name.
            let (dir, _) = match shard.kind {
                ShardKind::Cache => broker
                    .cache()
                    .as_log_cache()
                    .context("a Raft cache shard needs durable storage")?
                    .claim_for_raft(
                        &shard.tenant_id,
                        &shard.namespace,
                        &shard.stream,
                        shard.shard,
                        incarnation,
                    )?,
                _ => storage.claim_for_raft(
                    &shard.tenant_id,
                    &shard.namespace,
                    &shard.stream,
                    shard.shard,
                    incarnation,
                )?,
            };
            if holds_log_without_copy(&dir)? {
                return Err(CarriedOver {
                    dir: dir.display().to_string(),
                }
                .into());
            }
            // Before the member opens, so nothing appends while the check
            // runs. A copy placed fresh past epoch 0 has no `raft-epoch`
            // and would never count in the group (#2882).
            if let Some(bases) = &epoch_bases {
                let fresh = felix_storage::raft_copy::write_fresh_epoch(
                    &dir,
                    incarnation,
                    bases,
                    storage.config().index_spacing_bytes,
                )?;
                if let felix_storage::raft_copy::FreshEpoch::Refused(why) = fresh {
                    tracing::warn!(shard = ?shard, epoch = bases.len(), why, "a Raft copy without raft-epoch in a group past epoch 0 does not take the epoch; it never votes");
                }
            }
            let config = LogConfig {
                raft_log: true,
                fsync_mode: raft_fsync_mode(storage.config().fsync_mode, written_commit, &dir),
                // A Raft log refuses retention bounds, so the broker-wide
                // ones are left off it: its leader enforces the stream's
                // with head trims (`retention`).
                retention_bytes: None,
                retention_age: None,
                // Only a purge may drop a Raft log's entries, and tiered
                // reads assume a stream log's layout.
                offload: None,
                ..storage.config().clone()
            };
            Ok((dir, config))
        })
        .await;
        // Counted once the marker may be on disk, so a read that started
        // before this saw the old count. A pass dropped mid-claim would skip
        // it; the only caller awaits `take` to the end.
        self.claims.fetch_add(1, Ordering::AcqRel);
        let (dir, config) = claim.context("claim task")??;
        let retention_interval = config.retention_check_interval;
        let mut sent = None;
        let mut later = None;
        if self.alone.load(Ordering::Acquire) {
            self.reform_alone(key, incarnation, root, &dir, &config)
                .await
                .context("re-form the group of one after a relaxed crash")?;
        } else {
            match self
                .reform_alone_in_cluster(key, assignment, root, &dir, &config)
                .await
                .context("re-form the group of one through the control plane")?
            {
                alone_cluster::AloneReform::Recorded(answer) => sent = Some(*answer),
                alone_cluster::AloneReform::Done => {}
                alone_cluster::AloneReform::Later(why) => later = Some(why),
            }
        }
        let only_here = assignment.replicas.iter().all(|b| *b == self.local);
        let assignment = sent.as_ref().unwrap_or(assignment);
        self.start_reformed(key, assignment, root, &dir, &config)
            .await
            .context("start the re-formed copy")?;
        // A stopped or half-started copy is refused by the open itself.
        let epoch = assignment.group_epoch();
        let behind = {
            let dir = dir.clone();
            tokio::task::spawn_blocking(move || -> std::io::Result<Option<u64>> {
                if felix_storage::raft_copy::stopped(&dir)?.is_some()
                    || felix_storage::raft_copy::start_due(&dir, epoch)?
                {
                    return Ok(None);
                }
                let copy_epoch = felix_storage::raft_copy::group_epoch(&dir)?.unwrap_or(0);
                Ok(left_behind(epoch, copy_epoch).then_some(copy_epoch))
            })
            .await
            .context("epoch check task")??
        };
        if let Some(copy_epoch) = behind {
            return Err(LeftBehind {
                dir: dir.display().to_string(),
                copy_epoch,
                epoch,
            }
            .into());
        }
        admit(&self.runtime, key, assignment);
        let opened = self
            .runtime
            .open_copy(GroupId::new(key.clone(), incarnation), &dir, config, root)
            .await
            .context("open the Raft member")?;
        // A kept copy holds records this one lacks, so the shard serves
        // nothing until it is re-formed.
        match later {
            Some(why) if only_here && !opened.kept.held.is_empty() => {
                tracing::info!(shard = ?key, why, "a held one-replica shard is not re-formed yet; it opens again once it can be");
                self.alone_later.lock().insert(key.clone(), incarnation);
            }
            _ => {
                self.alone_later.lock().remove(key);
            }
        }
        let epoch = {
            let dir = dir.clone();
            tokio::task::spawn_blocking(move || felix_storage::raft_copy::group_epoch(&dir)).await
        };
        match epoch {
            Ok(Ok(epoch)) => {
                self.epochs.lock().insert(key.clone(), epoch);
            }
            // The member still serves; its floor and copy id reports wait.
            Ok(Err(err)) => {
                tracing::warn!(shard = ?key, error = %err, "could not read a Raft member's group epoch; its floor and copy id are not reported until it reopens")
            }
            Err(err) => tracing::warn!(shard = ?key, error = %err, "group epoch read task"),
        }
        let mut member = Member {
            incarnation,
            copy: opened.member.copy,
            log: opened.log,
            follow: None,
            retention: None,
        };
        // A cache group keeps its entries and counters in its own state, and
        // its leader expires them: there is no stream to open on the log.
        if key.kind == ShardKind::Cache {
            return Ok(member);
        }
        let handle = self
            .broker
            .open_stream_on_engine_log(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                StreamLog::from_log(member.log.clone()),
            )
            .await
            .context("open the stream on the member's log");
        match handle {
            Ok(handle) => {
                // A zero interval is a valid setting when no log takes
                // retention, and a timer cannot run on it.
                member.retention = (!retention_interval.is_zero()).then(|| {
                    Self::spawn_retention(
                        self.me.clone(),
                        key.clone(),
                        member.log.clone(),
                        handle.clone(),
                        retention_interval,
                    )
                });
                member.follow = self.engine.follow_commits(key, handle);
                Ok(member)
            }
            Err(err) => {
                self.close_member(key, member).await;
                Err(err)
            }
        }
    }

    /// Read the markers of the shards in `assignments` this pass may have
    /// to ask [`Self::claimed`] about, before the pass takes `open`: a slow
    /// or failing read must not hold up the closes and forms that wait on it.
    async fn read_markers(&self, assignments: &HashMap<ShardKey, ShardAssignment>) -> Reads {
        // Taken before any read starts, so a claim racing a read is seen.
        let claims = self.claims.load(Ordering::Acquire);
        // Taken only to see which members are open: no read runs under it.
        let held: HashMap<ShardKey, u64> = self
            .open
            .lock()
            .await
            .iter()
            .map(|(key, member)| (key.clone(), member.incarnation))
            .collect();
        let candidates: Vec<(ShardKey, u64)> = {
            let unclaimed = self.unclaimed.lock();
            assignments
                .iter()
                .filter(|(key, assignment)| {
                    matches!(key.kind, ShardKind::Stream | ShardKind::Cache)
                        && !brokers_of(assignment).contains(self.local.as_str())
                        && unclaimed.get(*key) != Some(&assignment.incarnation)
                        && held.get(*key) != Some(&assignment.incarnation)
                })
                .map(|(key, assignment)| (key.clone(), assignment.incarnation))
                .collect()
        };
        // Only a durable registered stream reaches `claimed`: any other read
        // would be thrown away, and made again on every pass.
        let mut wanted = Vec::with_capacity(candidates.len());
        for (key, incarnation) in candidates {
            if matches!(self.durable_rf(&key).await, Some(Durable::Yes(_))) {
                wanted.push((key, incarnation));
            }
        }
        let read = futures::stream::iter(wanted)
            .map(|(key, incarnation)| async move {
                #[cfg(test)]
                self.marker_reads.fetch_add(1, Ordering::Relaxed);
                let read = self.read_marker(&key, incarnation).await;
                (key, read)
            })
            .buffer_unordered(MARKER_READS)
            .collect()
            .await;
        Reads { claims, read }
    }

    /// Whether `key`'s stream or cache keeps a log, and how many brokers it
    /// asks for. `None` when it is not registered here yet.
    async fn durable_rf(&self, key: &ShardKey) -> Option<Durable> {
        let (tenant, namespace, name) = (&key.tenant_id, &key.namespace, &key.stream);
        if key.kind == ShardKind::Cache {
            // A broker with no durable storage is a cluster member placement
            // gives no cache shard; it only routes to the cache's group. One
            // with storage always has cache logs (`refuse_logless_cache`).
            let metadata = self.broker.cache_metadata(tenant, namespace, name).await?;
            return Some(Durable::Yes(metadata.replication_factor));
        }
        let metadata = self.broker.stream_metadata(tenant, namespace, name).await?;
        Some(if metadata.durable {
            Durable::Yes(metadata.replication_factor)
        } else {
            Durable::No
        })
    }

    async fn read_marker(&self, key: &ShardKey, incarnation: u64) -> anyhow::Result<Option<bool>> {
        let (broker, shard) = (Arc::clone(&self.broker), key.clone());
        let read = tokio::task::spawn_blocking(move || -> anyhow::Result<Option<bool>> {
            let Some(storage) = broker.durable_storage() else {
                return Ok(Some(false));
            };
            if shard.kind == ShardKind::Cache {
                let Some(cache) = broker.cache().as_log_cache() else {
                    return Ok(Some(false));
                };
                return Ok(cache.claimed_for_raft(
                    &shard.tenant_id,
                    &shard.namespace,
                    &shard.stream,
                    shard.shard,
                    incarnation,
                )?);
            }
            Ok(storage.claimed_for_raft(
                &shard.tenant_id,
                &shard.namespace,
                &shard.stream,
                shard.shard,
                incarnation,
            )?)
        })
        .await
        .context("read task")
        .and_then(|read| read);
        #[cfg(test)]
        let _ = self
            .stall_reads
            .subscribe()
            .wait_for(|stalled| !stalled)
            .await;
        read
    }

    /// Whether an earlier run of this broker opened a member of `key`. Such
    /// a member is opened again whatever placement says now: it may be a
    /// voter its group counts on. Called under `open`, with the reads made
    /// before it was taken.
    ///
    /// Only an unmarked directory is remembered. A failed read, or a stream
    /// not yet at `incarnation` here, is read again on the next pass: taken
    /// for "never opened" for good, it would leave a voter closed.
    async fn claimed(&self, key: &ShardKey, incarnation: u64, reads: &mut Reads) -> bool {
        if self.unclaimed.lock().get(key) == Some(&incarnation) {
            return false;
        }
        let read = match reads.take(key) {
            Some(read) => read,
            // Not read before the lock, or another pass claimed a directory
            // while the reads ran: an "unmarked" from before that may be
            // stale.
            None => {
                #[cfg(test)]
                self.reads_under_lock.fetch_add(1, Ordering::Relaxed);
                self.read_marker(key, incarnation).await
            }
        };
        if read.is_ok() {
            self.unreadable.lock().remove(key);
        }
        match read {
            Ok(Some(true)) => true,
            Ok(Some(false)) => {
                self.unclaimed.lock().insert(key.clone(), incarnation);
                false
            }
            Ok(None) => false,
            Err(err) if !self.unreadable.lock().insert(key.clone()) => {
                tracing::debug!(shard = ?key, error = %format!("{err:#}"), "could not read whether this broker held a Raft member of the shard");
                false
            }
            Err(err) => {
                tracing::warn!(shard = ?key, error = %format!("{err:#}"), "could not read whether this broker held a Raft member of the shard");
                false
            }
        }
    }

    /// Form `key`'s group on `brokers`, off the pass: a member that is
    /// down must not slow the feed. Only the broker with the lowest id in
    /// the set gets here, so a set is formed by one broker or by none, and
    /// only once every other broker has named its copy and said no group
    /// reached it. The rest join when the leader's first append arrives.
    ///
    /// The set formed is exactly `target`, the one the control plane stored:
    /// it lists every target of the epoch before a broker can read it, so a
    /// re-form counts the first voters as a voter set. A set formed from
    /// copies no record names could lose a majority unseen (#2688).
    ///
    /// The answers say nothing once placement moves on: the brokers of a
    /// newer set may be asking the same blank members. So it initializes
    /// only if the last pass still fed this set, with this broker lowest,
    /// and this stored target, and puts the form off otherwise.
    /// That is this broker's own view, which may be stale. The control
    /// plane keeps it safe: it holds a set with a stored target until the
    /// group has formed, so no second set gets a target (#2794).
    fn form(
        &self,
        key: ShardKey,
        brokers: BTreeSet<String>,
        incarnation: u64,
        target: Option<BTreeSet<MemberId>>,
    ) {
        let Some(me) = self.me.upgrade() else {
            return;
        };
        // Nothing to ask the others for yet: the control plane stores a
        // target once every broker in the set has named its copy.
        let Some(target) = target else {
            if self.put_off.lock().insert(key.clone()) {
                tracing::info!(shard = ?key, "not forming the shard's Raft group yet: the control plane has stored no target for it");
            }
            return;
        };
        if !self.forming.lock().insert(key.clone()) {
            return;
        }
        tokio::spawn(async move {
            let asked = me
                .runtime
                .first_members(&key, brokers.iter().map(String::as_str), ASK_WITHIN)
                .await
                .map_err(Unformed::Asked)
                .and_then(|members| {
                    if members == target {
                        Ok(members)
                    } else {
                        Err(Unformed::NotTarget {
                            members,
                            target: target.clone(),
                        })
                    }
                });
            #[cfg(test)]
            let _ = me
                .stall_forms
                .subscribe()
                .wait_for(|stalled| !stalled)
                .await;
            match asked {
                Ok(members) => {
                    // Under the pass's lock, so the member formed is the one
                    // the others were asked about. Placement may have moved
                    // while they were asked: the brokers of a newer set
                    // could be asking the same blank members, and both sets
                    // would win a term. Form only what the feed still names.
                    let open = me.open.lock().await;
                    let current = Formable {
                        brokers,
                        target: Some(target),
                    };
                    if me.formable.lock().get(&key) != Some(&current) {
                        if me.put_off.lock().insert(key.clone()) {
                            tracing::info!(shard = ?key, "not forming the shard's Raft group yet: its placement or stored target changed while its members were asked");
                        }
                    } else {
                        me.put_off.lock().remove(&key);
                        if open
                            .get(&key)
                            .is_some_and(|member| member.incarnation == incarnation)
                            && let Err(err) = me.runtime.initialize(&key, members).await
                        {
                            tracing::warn!(shard = ?key, error = %err, "could not form a Raft group");
                        }
                    }
                }
                Err(err) => {
                    let first = me.put_off.lock().insert(key.clone());
                    let held = matches!(&err, Unformed::Asked(err) if err.held_records());
                    match (first, held) {
                        (true, true) => {
                            tracing::warn!(shard = ?key, reason = %err, "not forming the shard's Raft group: it is held until an operator re-forms it (docs/per-shard-raft.md, \"Re-forming a group\")")
                        }
                        (true, false) => {
                            tracing::info!(shard = ?key, reason = %err, "not forming the shard's Raft group yet")
                        }
                        (false, _) => {
                            tracing::debug!(shard = ?key, reason = %err, "not forming the shard's Raft group yet")
                        }
                    }
                }
            }
            me.forming.lock().remove(&key);
        });
    }

    /// Whether `key`'s group here has voters other than the brokers
    /// placement names. Its leader moves it to the target the control plane
    /// derives from placement, so this lasts until the group converges.
    fn off_placement(&self, key: &ShardKey, placed: &BTreeSet<&str>) -> bool {
        let voters = self.runtime.voters(key).unwrap_or_default();
        let differs = !voters.is_empty()
            && voters
                .iter()
                .map(|voter| &*voter.broker)
                .ne(placed.iter().copied());
        if !differs {
            self.off_placement.lock().remove(key);
        } else if self.off_placement.lock().insert(key.clone()) {
            let voters: Vec<&str> = voters.iter().map(|voter| &*voter.broker).collect();
            tracing::warn!(
                shard = ?key,
                ?voters,
                ?placed,
                "a Raft shard's placement differs from its group's members; the group is \
                 converging to its target"
            );
        }
        differs
    }

    /// Stop the member, then close its log: the member writes to the log
    /// until it has stopped.
    async fn close_member(&self, key: &ShardKey, member: Member) {
        self.runtime.close(key).await;
        self.unplaced.lock().remove(key);
        // The hub keys watches by shard, not incarnation: a watch left here
        // would go quiet, or, once the cache is made again, drop the new
        // group's low indexes as duplicates. Ended after the member stopped,
        // so no change is applied behind it.
        if key.kind == ShardKind::Cache
            && let Some(hub) = self.broker.cache_watches()
        {
            hub.end_shard(&key.tenant_id, &key.namespace, &key.stream, key.shard, None);
        }
        self.epochs.lock().remove(key);
        self.recorded_epochs.lock().remove(key);
        self.formed_marks.lock().remove(key);
        self.leading.forget(key);
        if let Some(follow) = member.follow {
            follow.abort();
        }
        if let Some(retention) = member.retention {
            retention.abort();
        }
        if let Err(err) = member.log.close().await {
            self.close_failed
                .store(true, std::sync::atomic::Ordering::Release);
            tracing::warn!(shard = ?key, error = %err, "could not close a Raft member's log");
        }
    }

    /// [`Self::close_member`], and for a stream shard its stream too, which
    /// ends its readers; a later open starts from the log again.
    async fn close_with_stream(&self, key: &ShardKey, member: Member) {
        self.close_member(key, member).await;
        if key.kind == ShardKind::Cache {
            return;
        }
        if let Err(err) = self
            .broker
            .close_stream_shard(&key.tenant_id, &key.namespace, &key.stream, key.shard, None)
            .await
        {
            tracing::warn!(shard = ?key, error = %err, "could not close a Raft shard's stream");
        }
    }

    /// What the member of `key` here knows of its group's leader. Both
    /// [`EngineShards::dispatch`] and [`EngineShards::elected_leader`] read
    /// it, so a forwarded request and a Kafka client are sent to the same
    /// broker.
    fn followed(&self, key: &ShardKey) -> Followed {
        if !self.runtime.holds(key) {
            return Followed::ByPlacement;
        }
        // Led is not enough: the term is served once `Leading` opened it.
        if let Some(term) = self.leading(key) {
            return Followed::Leads(term);
        }
        if self.placed_elsewhere(key) {
            return Followed::ByPlacement;
        }
        // Only a forward needs the status, so a local publish never builds it.
        let Some(status) = self.runtime.status(key) else {
            return Followed::ByPlacement;
        };
        // The hint, not the heard leader: a restarted follower forwards at
        // once, and a forward to a leader that is gone fails and is retried.
        let heard = status.leader_hint.is_some();
        let leader = status
            .leader_hint
            .filter(|leader| *leader.broker != *self.local)
            .map(|leader| leader.broker.to_string());
        match leader {
            // A copy the group has not made a voter yet learns the leader
            // only from its first append: a reported voter knows it. A member
            // that heard itself elected is not that copy: forwarding would
            // bounce back until its term opens.
            None if !heard
                && self.placed.lock().get(key).is_some_and(|placed| {
                    !placed.voters.is_empty() && !placed.voters.contains(&*self.local)
                }) =>
            {
                Followed::ByPlacement
            }
            // Electing, or elected here and still moving to the tail.
            None => Followed::Electing,
            Some(broker) => Followed::Leader {
                broker,
                term: status.term,
            },
        }
    }

    /// The route for a Raft shard with no member open here, or with one
    /// that knows no leader and is not a reported voter (a learner joining).
    /// `None` for a shard the feed has not placed on Raft.
    fn dispatch_without_member(
        &self,
        key: &ShardKey,
        nodes: &dyn Fn(&str) -> Option<SocketAddr>,
    ) -> Option<Dispatch> {
        let placed = self.placed.lock().get(key).cloned()?;
        // Placed here and a voter: the member is still opening, or its open
        // failed and the next feed pass tries again.
        let Some(to) = placed.route(&self.local) else {
            return Some(Dispatch::Unavailable(Reason::NotReady));
        };
        Some(match nodes(to) {
            Some(advertise_addr) => Dispatch::Forward {
                node_id: to.to_string(),
                advertise_addr,
                generation: 0,
                incarnation: 0,
            },
            None => Dispatch::Unavailable(Reason::OwnerUnavailable(to.to_string())),
        })
    }

    #[cfg(test)]
    pub(crate) fn stall_leading(&self, stalled: bool) {
        self.leading.stall.send_replace(stalled);
    }

    #[cfg(test)]
    pub(crate) fn stall_ends(&self, stalled: bool) {
        self.leading.stall_ends.send_replace(stalled);
    }

    #[cfg(test)]
    pub(crate) fn stall_forms(&self, stalled: bool) {
        self.stall_forms.send_replace(stalled);
    }

    #[cfg(test)]
    pub(crate) fn stall_reads(&self, stalled: bool) {
        self.stall_reads.send_replace(stalled);
    }

    #[cfg(test)]
    pub(crate) async fn log_of(&self, shard: &ShardKey) -> Option<DiskLog> {
        self.open
            .lock()
            .await
            .get(shard)
            .map(|member| member.log.clone())
    }
}

/// Why a broker did not form a group it would form.
#[derive(Debug, thiserror::Error)]
enum Unformed {
    #[error(transparent)]
    Asked(NotBlank),
    #[error(
        "the members asked ({members:?}) are not the target the control plane stored ({target:?})"
    )]
    NotTarget {
        members: BTreeSet<MemberId>,
        target: BTreeSet<MemberId>,
    },
}

/// The engine markers a pass read before it took `open`.
struct Reads {
    /// [`RaftShards::claims`] before the first read started.
    claims: u64,
    read: HashMap<ShardKey, anyhow::Result<Option<bool>>>,
}

impl Reads {
    /// Drops every read if a directory was claimed after the reads started
    /// (`claims` is the count now). Called once the pass holds `open`: its
    /// own claims run under it, after `claimed` for the same key, so they
    /// cannot make another key's read stale.
    fn check(&mut self, claims: u64) {
        if claims != self.claims {
            self.read.clear();
        }
    }

    /// `key`'s read, made at the incarnation the same feed names.
    fn take(&mut self, key: &ShardKey) -> Option<anyhow::Result<Option<bool>>> {
        self.read.remove(key)
    }
}

/// A shard held back because placement names fewer brokers than its
/// replication factor.
struct ShortShard {
    key: ShardKey,
    generation: u64,
    replication_factor: u32,
    brokers: Vec<String>,
}

impl RaftShards {
    /// Sets the held-back gauge and warns once per shard and placement
    /// generation. The hold is deliberate, so without this it is silent.
    /// Returns how many warnings it logged.
    fn report_short(&self, short: &[ShortShard]) -> usize {
        let mut logged = 0;
        let mut warned = self.short_warned.lock();
        warned.retain(|key, _| short.iter().any(|held| &held.key == key));
        for held in short {
            if warned.insert(held.key.clone(), held.generation) != Some(held.generation) {
                tracing::warn!(
                    shard = ?held.key,
                    replication_factor = held.replication_factor,
                    placed = ?held.brokers,
                    "no Raft group forms: placement names fewer brokers than the stream's replication factor"
                );
                logged += 1;
            }
        }
        metrics::gauge!(SHORT_PLACEMENT).set(short.len() as f64);
        logged
    }
}

/// The target the control plane stored for a shard, or `None` when it
/// stored none or the view withholds it.
fn stored_target(assignment: &ShardAssignment) -> Option<BTreeSet<MemberId>> {
    let target = assignment
        .target
        .as_ref()
        .filter(|_| !assignment.targets_withheld)?;
    Some(
        target
            .members
            .iter()
            .map(|(node, copy)| MemberId::new(node.as_str(), *copy))
            .collect(),
    )
}

/// Whether a shard's stream or cache keeps a log, with the replica count it
/// asks for, if it names one.
enum Durable {
    No,
    Yes(Option<u32>),
}

/// How long after the feed's last answer started its admissions stand: two
/// held polls, so a healthy feed never lets them lapse.
const ADMITTED_FOR: Duration = Duration::from_secs(
    2 * (super::watch::LONG_POLL_WAIT.as_secs() + super::watch::LONG_POLL_SLACK.as_secs()),
);

/// Tells the runtime who the shard's member hears from beyond its
/// membership ([`GroupRuntime::admit`]).
fn admit(runtime: &GroupRuntime, key: &ShardKey, assignment: &ShardAssignment) {
    let (blank, current) = admitted(assignment);
    runtime.admit(
        &GroupId::new(key.clone(), assignment.incarnation),
        blank,
        current,
    );
}

/// The (in the group now, placed now) brokers for [`admit`]. A copy with no
/// membership also hears from the voters the group's leader reported at
/// this epoch: a leader the placement has moved off still leads while it
/// moves a new member in. A broker removed from the placement and the
/// group is in neither set.
fn admitted(assignment: &ShardAssignment) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut current: BTreeSet<String> = brokers_of(assignment)
        .into_iter()
        .map(str::to_owned)
        .collect();
    if let Some(target) = &assignment.target {
        current.extend(target.members.keys().cloned());
    }
    // As the control plane's own placed nodes: a move's successor and a
    // pending set are placed before either is sent anything.
    current.extend(assignment.successor.iter().cloned());
    if let Some(pending) = &assignment.pending_set {
        current.extend(pending.replicas.iter().cloned());
    }
    let mut blank = current.clone();
    if let Some(voters) = &assignment.raft_voters
        && voters.incarnation == assignment.incarnation
        && voters.epoch == assignment.group_epoch()
    {
        blank.extend(voters.members.keys().cloned());
    }
    (blank, current)
}

/// What a member here knows of its group's leader; see `RaftShards::followed`.
enum Followed {
    /// This broker leads and serves the term.
    Leads(u64),
    /// Another broker leads, as far as the member here was told.
    Leader { broker: String, term: u64 },
    /// No leader yet, or elected here and not serving its term yet.
    Electing,
    /// No member here can say: route by placement.
    ByPlacement,
}

/// Where requests for a Raft shard go while no member here knows its
/// leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Placed {
    /// The broker placement names leader.
    pub(crate) leader: String,
    /// The brokers holding the voters the group's leader last reported for
    /// the shard's incarnation and epoch. Empty before any report.
    pub(crate) voters: BTreeSet<String>,
}

impl Placed {
    fn of(assignment: &ShardAssignment) -> Self {
        let voters = assignment
            .raft_voters
            .as_ref()
            .filter(|voters| {
                voters.incarnation == assignment.incarnation
                    && voters.epoch == assignment.group_epoch()
            })
            .map(|voters| voters.members.keys().cloned().collect())
            .unwrap_or_default();
        Self {
            leader: assignment.leader.clone(),
            voters,
        }
    }

    /// The broker to forward to from `local`, or `None` to answer not ready.
    ///
    /// Placement's leader while it holds a voter. During a move outside the
    /// set placement names the destination before the group has added it,
    /// and the destination answers not ready until it hears the leader
    /// (#3011), so the request goes to a reported voter, which knows the
    /// leader. A voter placed here whose member is not open yet answers not
    /// ready, as it is the one that will serve.
    pub(crate) fn route(&self, local: &str) -> Option<&str> {
        let leader = Some(self.leader.as_str()).filter(|leader| !leader.is_empty());
        let others = || self.voters.iter().map(String::as_str).find(|v| *v != local);
        match leader {
            _ if self.voters.is_empty() => leader.filter(|leader| *leader != local),
            Some(leader) if leader != local && self.voters.contains(leader) => Some(leader),
            _ if self.voters.contains(local) && leader.is_none_or(|leader| leader == local) => None,
            _ => others().or(leader.filter(|leader| *leader != local)),
        }
    }
}

/// The brokers placement names for a shard, lowest id first.
fn brokers_of(assignment: &ShardAssignment) -> BTreeSet<&str> {
    std::iter::once(&assignment.leader)
        .chain(&assignment.replicas)
        .map(String::as_str)
        .filter(|broker| !broker.is_empty())
        .collect()
}

impl std::fmt::Debug for RaftShards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftShards")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl EngineShards for RaftShards {
    fn take<'a>(
        &'a self,
        assignments: &'a mut HashMap<ShardKey, ShardAssignment>,
    ) -> Pin<Box<dyn Future<Output = Taken> + Send + 'a>> {
        Box::pin(async move {
            let mut reads = self.read_markers(assignments).await;
            let mut open = self.open.lock().await;
            reads.check(self.claims.load(Ordering::Acquire));
            let mut taken = Taken::new();
            // A member that knows no leader may be cut off, and its commit
            // index has stopped: subscribers served from its log would wait,
            // told nothing. They are ended, and refused until it follows a
            // leader again.
            for key in open.keys() {
                let leaderless = self
                    .runtime
                    .status(key)
                    .is_some_and(|status| status.leader.is_none());
                if leaderless && key.kind == ShardKind::Stream {
                    self.broker
                        .end_deposed_subscriptions(
                            &key.tenant_id,
                            &key.namespace,
                            &key.stream,
                            key.shard,
                        )
                        .await;
                }
                // A cache watch here would go quiet, which reads as a quiet
                // key. It ends as moved, or as lagged for a client that does
                // not follow moves, so the watch carries on from where it
                // stopped once some member follows a leader.
                if leaderless
                    && key.kind == ShardKind::Cache
                    && let Some(hub) = self.broker.cache_watches()
                {
                    self.engine.end_cache_watches(key, hub.as_ref());
                }
            }
            let streams: Vec<ShardKey> = assignments
                .keys()
                .filter(|key| matches!(key.kind, ShardKind::Stream | ShardKind::Cache))
                .cloned()
                .collect();
            // Every Raft shard in the feed, and whether its placement names
            // as many brokers as the stream asks for.
            let mut fed: HashMap<ShardKey, (ShardAssignment, bool)> = HashMap::default();
            let mut wanted_rf: HashMap<ShardKey, u32> = HashMap::default();
            let mut placed = HashMap::default();
            for key in streams {
                let wanted = match self.durable_rf(&key).await {
                    // A stream in memory has no log for a group to run on.
                    Some(Durable::No) => continue,
                    Some(Durable::Yes(wanted)) => wanted,
                    // Not registered here yet, so not known to be durable.
                    // Neither engine opens it this pass: a log the hybrid
                    // opened now would sit beside the member's later.
                    None => {
                        tracing::debug!(shard = ?key, "assigned stream is not registered yet");
                        assignments.remove(&key);
                        continue;
                    }
                };
                let Some(assignment) = assignments.remove(&key) else {
                    continue;
                };
                taken.push((key.clone(), assignment.routing));
                placed.insert(key.clone(), Placed::of(&assignment));
                // Over the count is an RF 1 shard mid-move: its group already
                // formed, and the destination must report its copy to join.
                let full = wanted
                    .is_some_and(|wanted| brokers_of(&assignment).len() >= wanted.max(1) as usize);
                if let Some(wanted) = wanted {
                    wanted_rf.insert(key.clone(), wanted);
                }
                fed.insert(key, (assignment, full));
            }
            *self.placed.lock() = placed;
            *self.formable.lock() = fed
                .iter()
                .filter(|(_, (_, full))| *full)
                .map(|(key, (assignment, _))| {
                    let formable = Formable {
                        brokers: brokers_of(assignment)
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                        target: stored_target(assignment),
                    };
                    (key.clone(), formable)
                })
                .collect();
            self.unclaimed.lock().retain(|key, _| fed.contains_key(key));
            self.put_off.lock().retain(|key| fed.contains_key(key));
            self.unreadable.lock().retain(|key| fed.contains_key(key));
            self.off_placement
                .lock()
                .retain(|key| fed.contains_key(key));
            self.carried_over.lock().retain(|key| fed.contains_key(key));
            // A member whose placement dropped this broker stays open: it
            // is still a voter of its group. Only a stream that is gone, or
            // made again, closes one.
            let gone: Vec<ShardKey> = open
                .iter()
                .filter(|(key, member)| {
                    fed.get(*key)
                        .is_none_or(|(assignment, _)| assignment.incarnation != member.incarnation)
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in gone {
                let Some(member) = open.remove(&key) else {
                    continue;
                };
                self.close_with_stream(&key, member).await;
            }
            // openraft never restarts a core that stopped on a storage
            // error, and the member answers nothing until it is opened
            // again. It is closed here and opened below, from disk as after
            // a crash, once the disk takes writes.
            let failed: Vec<(ShardKey, String)> = open
                .keys()
                .filter_map(|key| Some((key.clone(), self.runtime.storage_failure(key)?)))
                .collect();
            for (key, why) in failed {
                let Some(member) = open.remove(&key) else {
                    continue;
                };
                tracing::warn!(shard = ?key, error = %why, "a Raft member stopped on a storage error; it opens again from disk once the disk takes writes");
                self.restarts.lock().stopped(&key);
                // Read before the close, which forgets the member's status.
                let term = self.runtime.status(&key).map_or(0, |status| status.term);
                self.stopped_copies
                    .lock()
                    .insert(key.clone(), StoppedCopy { term, error: why });
                self.close_with_stream(&key, member).await;
            }
            // A blank copy of an abandoned epoch opens before its report
            // seeds the epoch, and a start needs it closed. It is opened
            // again below, through the start.
            let candidates: Vec<_> = open
                .iter()
                .filter_map(|(key, member)| {
                    let (assignment, _) = fed.get(key)?;
                    let blank = self
                        .runtime
                        .status(key)
                        .is_some_and(|status| status.term == 0 && status.last_index.is_none());
                    let voter = self.abandoned_voter(assignment).filter(|voter| {
                        blank
                            && voter.copy == member.copy
                            && member.incarnation == assignment.incarnation
                    })?;
                    Some((
                        key.clone(),
                        voter,
                        member.log.dir().to_path_buf(),
                        member.incarnation,
                        member.log.config().index_spacing_bytes,
                    ))
                })
                .collect();
            let mut abandoned = Vec::new();
            for (key, voter, dir, incarnation, spacing) in candidates {
                let due = tokio::task::spawn_blocking(move || {
                    felix_storage::raft_copy::abandon_start_due(
                        &dir,
                        incarnation,
                        &voter.bases,
                        voter.copy,
                        spacing,
                    )
                })
                .await;
                match due {
                    Ok(Ok(true)) => abandoned.push(key),
                    Ok(Ok(false)) => {}
                    Ok(Err(err)) => {
                        tracing::warn!(shard = ?key, error = %err, "could not check an abandoned Raft copy's start")
                    }
                    Err(err) => tracing::warn!(shard = ?key, error = %err, "abandon check task"),
                }
            }
            for key in abandoned {
                if let Some(member) = open.remove(&key) {
                    self.close_with_stream(&key, member).await;
                }
            }
            // A member open in an epoch the record has left, as on a broker
            // cut off while the group re-formed without it, keeps its old
            // membership and would go on leading it. The open below refuses
            // it.
            let left: Vec<ShardKey> = {
                open.iter()
                    .filter(|(key, member)| {
                        fed.get(*key).is_some_and(|(assignment, _)| {
                            // The same test as the open, so a member the open
                            // would refuse is not kept.
                            member.incarnation == assignment.incarnation
                                // The epoch the open group holds, not the cached
                                // read, which can be missing.
                                && self.runtime.reform_epoch(key).is_some_and(|epoch| {
                                    left_behind(assignment.group_epoch(), epoch)
                                })
                        })
                    })
                    .map(|(key, _)| key.clone())
                    .collect()
            };
            for key in left {
                if let Some(member) = open.remove(&key) {
                    tracing::warn!(shard = ?key, "closing a Raft member of a group epoch the shard's record has left");
                    self.close_with_stream(&key, member).await;
                }
            }
            // A held group of one whose re-form could not be decided at its
            // open is closed once it can be, and the open below re-forms it.
            // It never formed, so it served nothing.
            let reform: Vec<ShardKey> = {
                let mut later = self.alone_later.lock();
                later.retain(|key, incarnation| {
                    open.get(key)
                        .is_some_and(|member| member.incarnation == *incarnation)
                });
                later
                    .keys()
                    .filter(|key| {
                        fed.get(*key)
                            .is_some_and(|(assignment, _)| self.alone_blocked(assignment).is_none())
                            && self.runtime.status(key).is_some_and(|status| {
                                status.term == 0 && status.last_index.is_none()
                            })
                            // A learner's staged base shows as no entries
                            // too; the loop below reopens it.
                            && !self.runtime.base_staged(key)
                    })
                    .cloned()
                    .collect()
            };
            for key in reform {
                self.alone_later.lock().remove(&key);
                let Some(member) = open.remove(&key) else {
                    continue;
                };
                tracing::info!(shard = ?key, "re-forming a held one-replica shard: what kept it from re-forming at its open has cleared");
                self.close_with_stream(&key, member).await;
            }
            // A learner adopts a staged re-formed base, or drops a held one
            // it cannot use, only as it opens, so it is closed here and
            // opened below.
            let staged: Vec<ShardKey> = open
                .keys()
                .filter(|key| self.runtime.base_staged(key))
                .cloned()
                .collect();
            for key in staged {
                // A learner is no held group of one, so the retry above must
                // not close what this reopens.
                self.alone_later.lock().remove(&key);
                let Some(member) = open.remove(&key) else {
                    continue;
                };
                tracing::info!(shard = ?key, "a Raft learner's re-formed base changed; it opens again to adopt or drop it");
                self.close_with_stream(&key, member).await;
            }
            self.restarts.lock().retain(|key| fed.contains_key(key));
            self.runtime.retain_admitted(|key| fed.contains_key(key));
            *self.unplaced.lock() = fed
                .iter()
                .filter(|(key, (assignment, _))| {
                    open.contains_key(*key) && !brokers_of(assignment).contains(self.local.as_str())
                })
                .map(|(key, _)| key.clone())
                .collect();
            let mut off_placement = 0_u32;
            let mut short: Vec<ShortShard> = Vec::new();
            // One probe per pass: every shard shares the root, and each heal
            // costs a write, an fsync and a directory sync.
            let mut healed: Option<Result<(), String>> = None;
            // Asked once per pass, and only if a member would open.
            let mut fleet_known: Option<bool> = None;
            for (key, (assignment, full)) in &fed {
                let brokers = brokers_of(assignment);
                let held = || ShortShard {
                    key: key.clone(),
                    generation: assignment.generation,
                    replication_factor: wanted_rf.get(key).copied().unwrap_or(1),
                    brokers: brokers.iter().map(|broker| broker.to_string()).collect(),
                };
                if !open.contains_key(key) {
                    // A shard already counted short stays counted while it
                    // cannot be opened: a pass that could not look says
                    // nothing new, and the gauge and warning must not flap.
                    let keep = !*full && self.short_warned.lock().contains_key(key);
                    // Held by the restore gate: the pass after it opens
                    // the root opens the members.
                    let Some(root) = self.root.get() else {
                        if keep {
                            short.push(held());
                        }
                        continue;
                    };
                    // Held for the fleet like the restore gate holds
                    // them: the pass after it is known opens the members.
                    if !*fleet_known.get_or_insert_with(|| self.fleet_known()) {
                        if keep {
                            short.push(held());
                        }
                        continue;
                    }
                    if !brokers.contains(self.local.as_str())
                        && !self.claimed(key, assignment.incarnation, &mut reads).await
                    {
                        if keep {
                            short.push(held());
                        }
                        continue;
                    }
                    if !self.restarts.lock().due(key) {
                        if keep {
                            short.push(held());
                        }
                        continue;
                    }
                    // A failed mark log stops every member on the root, and
                    // a fresh one cannot open on it either.
                    if (self.restarts.lock().restarting(key) || root.mark_log().failure().is_some())
                        && let Err(err) = match &healed {
                            Some(done) => done.clone(),
                            None => {
                                let done = root.heal().await.map_err(|err| err.to_string());
                                healed = Some(done.clone());
                                done
                            }
                        }
                    {
                        if self.held_warn.lock().due(key) {
                            tracing::warn!(shard = ?key, error = %err, "a Raft member waits for its disk to take writes again before it opens");
                        }
                        if keep {
                            short.push(held());
                        }
                        continue;
                    }
                    match self.open_member(key, assignment, root).await {
                        Ok(member) => {
                            self.carried_over.lock().remove(key);
                            self.held_warn.lock().clear(key);
                            self.restarts.lock().reopened(key);
                            open.insert(key.clone(), member);
                        }
                        // Tried again on the next pass.
                        Err(err) if err.is::<CarriedOver>() || err.is::<LeftBehind>() => {
                            if self.carried_over.lock().insert(key.clone()) {
                                tracing::error!(shard = ?key, error = %err, "not opening a Raft shard");
                            }
                            if keep {
                                short.push(held());
                            }
                            continue;
                        }
                        // Tried again on every pass: the open that follows
                        // a leader's append rejoins it.
                        Err(err)
                            if matches!(
                                err.downcast_ref::<OpenError>(),
                                Some(OpenError::Stopped(_))
                            ) =>
                        {
                            if self.held_warn.lock().due(key) {
                                tracing::warn!(shard = ?key, error = %format!("{err:#}"), "a Raft shard's copy here was stopped by a re-form that did not finish; it rejoins as a learner once a leader commits past its last entry");
                            }
                            if keep {
                                short.push(held());
                            }
                            continue;
                        }
                        // Also tried again on the next pass; counted with
                        // the stream shards that hit the same limit.
                        Err(err) if super::lifecycle::out_of_files(&err) => {
                            super::lifecycle::metrics::record_open_failure();
                            super::lifecycle::metrics::record_open_file_limit();
                            if self.held_warn.lock().due(key) {
                                tracing::warn!(shard = ?key, error = %format!("{err:#}"), "could not open a Raft shard: the open-file limit is exhausted; opening on a later pass");
                            }
                            if keep {
                                short.push(held());
                            }
                            continue;
                        }
                        Err(err) => {
                            tracing::warn!(shard = ?key, error = %format!("{err:#}"), "could not open a Raft shard");
                            if keep {
                                short.push(held());
                            }
                            continue;
                        }
                    }
                }
                if self.off_placement(key, &brokers) {
                    off_placement += 1;
                }
                admit(&self.runtime, key, assignment);
                // The group's leader hands it the leadership. Even the first
                // one: the broker that forms a group wins its first term.
                self.runtime
                    .prefer_as(key, &assignment.leader, assignment.leader_by_operator());
                // A set short of the stream's replica count forms nothing: a
                // group of the brokers placed so far would be formed again,
                // on empty logs, by the ones placed later.
                let blank = self
                    .runtime
                    .status(key)
                    .is_some_and(|status| status.term == 0 && status.last_index.is_none());
                if !*full && blank {
                    short.push(held());
                }
                // Forming writes the group's first entries from index 0. Past
                // epoch 0 an index below the epoch's first one may have been
                // given out, so only a start at that index may begin it.
                let past_zero = assignment.group_epoch() > 0;
                if *full && blank && past_zero && self.held_warn.lock().due(key) {
                    tracing::warn!(shard = ?key, epoch = assignment.group_epoch(), "not forming the shard's Raft group from index 0: it is past epoch 0");
                }
                if *full && blank && !past_zero && brokers.first() == Some(&self.local.as_str()) {
                    let brokers = brokers.iter().map(|broker| broker.to_string()).collect();
                    let target = stored_target(assignment);
                    self.form(key.clone(), brokers, assignment.incarnation, target);
                }
            }
            metrics::gauge!(OFF_PLACEMENT).set(f64::from(off_placement));
            self.report_short(&short);
            // A reopened copy counts as stopped until it hears a leader: a
            // rotated copy with no other copy to catch up from serves
            // nothing until an operator re-forms the group.
            let stopped = {
                let mut stopped = self.stopped_copies.lock();
                stopped.retain(|key, _| {
                    fed.contains_key(key)
                        && !(open.contains_key(key) && self.runtime.hears_leader(key) == Some(true))
                });
                stopped.len()
            };
            metrics::gauge!(STOPPED_ON_STORAGE_ERROR).set(stopped as f64);
            // Kept only for the incarnation the member runs: a version counts
            // within one stream incarnation.
            let targets = fed
                .iter()
                .filter(|(key, (assignment, _))| {
                    // A withheld view tells nothing: the shard keeps the
                    // target it holds instead of being told there is none.
                    !assignment.targets_withheld
                        && open
                            .get(*key)
                            .is_some_and(|member| member.incarnation == assignment.incarnation)
                })
                .map(|(key, (assignment, _))| {
                    // The control plane's members verbatim, copy ids too: a
                    // set it never stored is in no re-form source.
                    let target = assignment.target.as_ref().map(|target| Target {
                        voters: target
                            .members
                            .iter()
                            .map(|(node, copy)| MemberId::new(node.as_str(), *copy))
                            .collect(),
                        version: TargetVersion {
                            incarnation: target.version.incarnation,
                            counter: target.version.counter,
                        },
                    });
                    (key.clone(), target)
                })
                .collect();
            *self.targets.lock() = targets;
            // A short set forms nothing, so it gets no target either: one
            // naming only the brokers placed so far would form that group.
            // A member placement moved off stays open a while, but the
            // control plane refuses a broker outside the set.
            let held = {
                let mut recorded = self.recorded_epochs.lock();
                for (key, (assignment, _)) in &fed {
                    recorded.insert(key.clone(), assignment.group_epoch());
                }
                recorded.retain(|key, _| open.contains_key(key));
                drop(recorded);
                let mut marks = self.formed_marks.lock();
                marks.clear();
                for (key, (assignment, _)) in &fed {
                    if let Some(mark) = assignment.raft_formed {
                        marks.insert(key.clone(), mark);
                    }
                }
                marks.retain(|key, _| open.contains_key(key));
                drop(marks);
                let mut voters = self.fed_voters.lock();
                voters.clear();
                for (key, (assignment, _)) in &fed {
                    if let Some(fed) = &assignment.raft_voters
                        && open.contains_key(key)
                    {
                        voters.insert(key.clone(), fed.clone());
                    }
                }
                drop(voters);
                let epochs = self.epochs.lock();
                fed.iter()
                    .filter(|(_, (_, full))| *full)
                    .filter(|(_, (assignment, _))| {
                        brokers_of(assignment).contains(self.local.as_str())
                    })
                    // A copy id puts its copy in the target, so it votes. One
                    // that cannot prove the record's epoch is a fresh learner or
                    // an old restore: it waits for a `raft-epoch`.
                    .filter(|(key, (assignment, _))| {
                        epochs.get(*key).is_some_and(|stored| {
                            proven_epoch(*stored, Some(assignment.group_epoch()))
                                == Some(assignment.group_epoch())
                        })
                    })
                    .filter_map(|(key, (assignment, _))| {
                        let member = open
                            .get(key)
                            .filter(|member| member.incarnation == assignment.incarnation)?;
                        let in_target = assignment
                            .target
                            .as_ref()
                            .filter(|_| !assignment.targets_withheld)
                            .and_then(|target| target.members.get(&self.local).copied());
                        Some(copy_id::Held {
                            key: key.clone(),
                            incarnation: assignment.incarnation,
                            copy: member.copy,
                            epoch: assignment.group_epoch(),
                            set: std::iter::once(&assignment.leader)
                                .chain(&assignment.replicas)
                                .cloned()
                                .collect(),
                            in_target,
                            withheld: assignment.targets_withheld,
                        })
                    })
                    .collect()
            };
            *self.held_copies.lock() = held;
            let led: Vec<&ShardKey> = open
                .keys()
                .filter(|key| self.leading(key).is_some())
                .collect();
            let led_off_placement = led.iter().filter(|key| {
                fed.get(**key)
                    .is_some_and(|(assignment, _)| assignment.leader != self.local)
            });
            metrics::gauge!(LED_OFF_PLACEMENT).set(led_off_placement.count() as f64);
            metrics::gauge!(LED).set(led.len() as f64);
            taken
        })
    }

    fn dispatch(
        &self,
        key: &ShardKey,
        nodes: &dyn Fn(&str) -> Option<SocketAddr>,
    ) -> Option<Dispatch> {
        let dispatch = match self.followed(key) {
            Followed::ByPlacement => self.dispatch_without_member(key, nodes)?,
            Followed::Leads(term) => Dispatch::Local { generation: term },
            Followed::Electing => Dispatch::Unavailable(Reason::NotReady),
            Followed::Leader { broker, term } => match nodes(&broker) {
                Some(advertise_addr) => Dispatch::Forward {
                    node_id: broker,
                    advertise_addr,
                    generation: term,
                    incarnation: 0,
                },
                None => Dispatch::Unavailable(Reason::OwnerUnavailable(broker)),
            },
        };
        // "Still opening" would hide a copy that a failed fsync stopped.
        Some(match dispatch {
            Dispatch::Unavailable(Reason::NotReady) if self.stopped_copy(key).is_some() => {
                Dispatch::Unavailable(Reason::CopyStopped)
            }
            dispatch => dispatch,
        })
    }

    fn elected_leader(&self, key: &ShardKey) -> Option<String> {
        match self.followed(key) {
            Followed::Leader { broker, .. } => Some(broker),
            _ => None,
        }
    }

    /// A member that follows a leader serves readers from its log. One that
    /// knows no leader may be cut off, with a commit index that has stopped
    /// moving, so it serves none.
    fn serves_reads(&self, key: &ShardKey) -> bool {
        self.runtime.hears_leader(key) == Some(true)
    }

    fn holds(&self, key: &ShardKey) -> bool {
        self.runtime.holds(key)
    }

    /// A member removed from its group is not told, and stays open here
    /// hearing no leader. Answering "not ready" for it would keep a client
    /// that enters here retrying here for good.
    fn placed_elsewhere(&self, key: &ShardKey) -> bool {
        self.unplaced.lock().contains(key) && self.runtime.hears_leader(key) == Some(false)
    }

    /// One past the furthest entry the leader acknowledged, read only once
    /// its term is served: a new leader learns the commit index by
    /// committing in its own term, so before that it may sit below a write
    /// the previous leader acknowledged. Not the commit index alone: a
    /// write answered on a written majority or the lease is acknowledged
    /// before its commit. The marks are read after the served check and
    /// kept only if they are of the served term, so a leader re-elected in
    /// between never reports an older term's marks under the new one.
    ///
    /// Every member open here is listed, not only the ones the copy id
    /// report holds: that set leaves out a member off placement, which
    /// still votes and may lead. A shard no broker listed would be read as
    /// a hybrid one on placement's leader, which does not lead it (#2995),
    /// and a leader left out would hide its term from the collector's
    /// newest-term check.
    fn backup_members(&self) -> Vec<super::routing::BackupMember> {
        self.runtime
            .held_groups()
            .into_iter()
            .filter_map(|group| {
                let key = (*group.shard).clone();
                let status = self.runtime.status(&key)?;
                let (voter_sets, membership_at) = self.runtime.voter_sets(&key)?;
                let acked = (self.leading(&key) == Some(status.term))
                    .then(|| self.runtime.led_acked(&key))
                    .flatten()
                    .filter(|(term, _)| *term == status.term)
                    .map(|(_, last)| last);
                Some(super::routing::BackupMember {
                    key,
                    incarnation: group.epoch,
                    member: status.member,
                    term: status.term,
                    voter_sets,
                    membership_at,
                    committed_end: acked.map(|last| last.index + 1),
                    end_term: acked.map(|last| last.term),
                })
            })
            .collect()
    }

    fn leads_unheard(&self, key: &ShardKey) -> bool {
        self.leading(key).is_some() && self.runtime.hears_leader(key) == Some(false)
    }

    fn wake_with(&self, wake: Arc<tokio::sync::Notify>) {
        if let Some(lease) = self.leading.lease.get() {
            lease.wake_on_head(Arc::clone(&wake));
        }
        let _ = self.leading.wake.set(wake);
    }

    fn follow_lease(&self, lease: Arc<LeaseState>) {
        if let Some(wake) = self.leading.wake.get() {
            lease.wake_on_head(Arc::clone(wake));
        }
        let _ = self.leading.lease.set(lease);
    }

    /// Tells each member that leads the target the view carries, or that it
    /// carries none, once the view is fresh enough for its term (see
    /// [`Lead::met_by`]). Told on every pass: the member keeps only a newer
    /// version, or the same one for a newer term.
    fn answered(&self, as_of: Option<tokio::time::Instant>, checkpoint: Option<u64>) {
        self.runtime
            .admit_until(as_of.map(|as_of| as_of + ADMITTED_FOR));
        let Some(lease) = self.leading.lease.get() else {
            return;
        };
        let head = lease.head();
        // Read after the head: the heartbeat records it first, so a bar
        // from a restored control plane comes with its incarnation.
        let placement = lease.placement();
        self.runtime.ack_incarnation(placement);
        let mut tell = Vec::new();
        {
            let targets = self.targets.lock();
            let mut terms = self.leading.terms.lock();
            let mut reset = false;
            for (key, lead) in terms.iter_mut() {
                reset |= lead.take_bar(head);
                if !lead.met_by(as_of, checkpoint) {
                    continue;
                }
                match targets.get(key) {
                    // From a control-plane member that has not applied the
                    // restore its leader reported: not a fresh view at all.
                    Some(Some(target)) if target.version.incarnation < placement => {}
                    Some(target) => tell.push((key.clone(), target.clone(), lead.term)),
                    None => {}
                }
            }
            // Under the lock `opened` sets it under, so a new term's want
            // is not lost.
            lease.want_head(terms.values().any(|lead| lead.bar.is_none()));
            if reset {
                lease.want_view();
            }
        }
        for (key, target, term) in tell {
            let kept = match &target {
                Some(target) => self.runtime.retarget(&key, target.clone(), term),
                None => self.runtime.untarget(&key, term),
            };
            if kept {
                let version = target.as_ref().map(|target| target.version);
                tracing::info!(shard = ?key, ?version, term, "told the shard's Raft leader its target");
                #[cfg(test)]
                self.told.lock().push((key, target, term));
            }
        }
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move {
            let mut open = self.open.lock().await;
            self.placed.lock().clear();
            for (key, member) in open.drain() {
                self.close_member(&key, member).await;
            }
            self.runtime.shutdown().await;
            !self.close_failed.load(std::sync::atomic::Ordering::Acquire)
        })
    }
}

impl felix_broker::EngineLogs for RaftShards {
    fn close_through<'a>(
        &'a self,
        tenant_id: &'a str,
        namespace: &'a str,
        stream: &'a str,
        through: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut open = self.open.lock().await;
            let closing: Vec<ShardKey> = open
                .iter()
                .filter(|(key, member)| {
                    key.tenant_id == tenant_id
                        && key.namespace == namespace
                        && key.stream == stream
                        && member.incarnation <= through
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in closing {
                if let Some(member) = open.remove(&key) {
                    self.close_member(&key, member).await;
                }
            }
        })
    }
}

/// A follower's log may lack a head trim the leader already answered, and
/// so may a leader cut off from a successor that trimmed. A read that could
/// start below a floor therefore waits for a read index: a follower asks
/// the leader for one, a leader runs its own round unless its lease holds.
/// A leader's tail read is checked again once its subscriber registers.
/// Before the fleet can send the follower's message, the leader serves the
/// read. See
/// `docs/per-shard-raft.md`, "Head trims".
impl felix_broker::FollowerReads for RaftShards {
    fn before_read<'a>(
        &'a self,
        tenant_id: &'a str,
        namespace: &'a str,
        stream: &'a str,
        shard: u32,
        tail: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, felix_broker::NotReadable>> + Send + 'a>>
    {
        Box::pin(async move {
            let key = stream_key(tenant_id, namespace, stream, shard);
            if !self.runtime.holds(&key) {
                return Ok(None);
            }
            if self.runtime.follows(&key) {
                if !self.broker.raft_follower_reads_allowed() {
                    return Err(felix_broker::NotReadable::OnLeader);
                }
            } else if tail && let Some(term) = self.led_term(&key) {
                // A deposed leader's tail subscribers end with its term,
                // so one registered while it still leads needs no round.
                return Ok(Some(term));
            }
            self.read_barrier(&key, stream, shard).await.map(|()| None)
        })
    }

    fn after_tail_join<'a>(
        &'a self,
        tenant_id: &'a str,
        namespace: &'a str,
        stream: &'a str,
        shard: u32,
        term: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), felix_broker::NotReadable>> + Send + 'a>> {
        Box::pin(async move {
            let key = stream_key(tenant_id, namespace, stream, shard);
            // Still leading `term` after the registration: the deposal, if
            // it comes, ends this subscriber with the term's others.
            if self.led_term(&key) == Some(term) {
                return Ok(());
            }
            // Deposed meanwhile, and its end of the term's subscribers may
            // have run already. A follower's barrier now would come after
            // the registration, so whatever the log pass delivered in
            // between is not covered: the subscriber is dropped instead.
            tracing::debug!(stream, shard, term, "a tail subscriber outlived its term");
            Err(felix_broker::NotReadable::Settling)
        })
    }
}

impl RaftShards {
    /// The term this broker leads `key` in and serves. Read from the terms
    /// the deposal removes before it spawns the end of the term's
    /// subscribers, not from the member's lead watch, which can still show
    /// the term after that end ran.
    fn led_term(&self, key: &ShardKey) -> Option<u64> {
        self.leading
            .generation(key)
            .filter(|term| self.runtime.led_term(key) == Some(*term))
    }

    async fn read_barrier(
        &self,
        key: &ShardKey,
        stream: &str,
        shard: u32,
    ) -> Result<(), felix_broker::NotReadable> {
        self.runtime.read_barrier(key).await.map_err(|why| {
            tracing::debug!(stream, shard, why, "a read was refused");
            felix_broker::NotReadable::Settling
        })
    }
}

fn stream_key(tenant_id: &str, namespace: &str, stream: &str, shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

/// The shards this broker leads, as its Raft members report them.
struct Leading {
    broker: Arc<Broker>,
    /// Consumer groups, whose trackers last only as long as the term.
    groups: Arc<groups::Groups>,
    terms: Arc<parking_lot::Mutex<HashMap<ShardKey, Lead>>>,
    /// Deposed terms per shard whose subscribers are still to be ended. No
    /// later term is served while one is left: its subscribers would be
    /// ended with the deposed term's.
    ending: Arc<parking_lot::Mutex<HashMap<ShardKey, usize>>>,
    /// The feed's wake, so the pass after the root opens, or after a term
    /// begins, comes at once.
    wake: std::sync::OnceLock<Arc<tokio::sync::Notify>>,
    /// Brings the feed's head a new term's target waits for.
    lease: std::sync::OnceLock<Arc<LeaseState>>,
    /// While true, a new term's move to the committed tail does not start.
    #[cfg(test)]
    stall: tokio::sync::watch::Sender<bool>,
    /// While true, a deposed term's subscribers are not ended.
    #[cfg(test)]
    stall_ends: tokio::sync::watch::Sender<bool>,
}

struct Lead {
    term: u64,
    /// The stream is at the committed tail, so a write of this term may be
    /// handed over.
    open: bool,
    /// When the term began here. A feed view fetched earlier may hold a
    /// target older than one an earlier leader acted on.
    since: tokio::time::Instant,
    /// The feed's head from a heartbeat sent at or after `since`. A view
    /// fetched after `since` from a control-plane member that lags can
    /// still miss a target written before it, but cannot reach this.
    bar: Option<u64>,
}

impl Lead {
    fn new(term: u64) -> Self {
        Self {
            term,
            open: false,
            since: tokio::time::Instant::now(),
            bar: None,
        }
    }

    /// Take the bar from `head`, the newest a heartbeat brought, once one
    /// sent at or after `since` has. Heads only grow on one feed, so a lower
    /// one means it was reset: it becomes the bar, and `since` moves to now
    /// so a view of the old feed does not meet it. Returns whether it moved.
    fn take_bar(&mut self, head: Option<Head>) -> bool {
        let Some(head) = head.filter(|head| head.sent >= self.since) else {
            return false;
        };
        match self.bar {
            None => self.bar = Some(head.seq),
            Some(bar) if head.seq < bar => {
                self.bar = Some(head.seq);
                self.since = tokio::time::Instant::now();
                return true;
            }
            Some(_) => {}
        }
        false
    }

    /// Whether a feed view fetched by a request that started at `as_of`,
    /// and that reached `checkpoint`, may tell this term its target. Never
    /// before there is a bar: a control plane that sends no head gives none.
    fn met_by(&self, as_of: Option<tokio::time::Instant>, checkpoint: Option<u64>) -> bool {
        self.bar.is_some_and(|bar| {
            as_of.is_some_and(|as_of| as_of >= self.since)
                && checkpoint.is_some_and(|checkpoint| checkpoint >= bar)
        })
    }
}

impl Leading {
    fn forget(&self, key: &ShardKey) {
        self.terms.lock().remove(key);
    }

    /// The term this broker leads `key` in and serves, if it does.
    fn generation(&self, key: &ShardKey) -> Option<u64> {
        self.terms
            .lock()
            .get(key)
            .filter(|lead| lead.open)
            .map(|lead| lead.term)
    }
}

impl WriteFence for Leading {
    fn quiesced(&self, key: &ShardKey) -> bool {
        !self.terms.lock().contains_key(key)
    }

    fn opened(&self, key: &ShardKey, term: u64) {
        {
            let mut terms = self.terms.lock();
            if terms.get(key).is_some_and(|lead| lead.term >= term) {
                return;
            }
            terms.insert(key.clone(), Lead::new(term));
            if let Some(lease) = self.lease.get() {
                lease.want_head(true);
                // After `since`: a long poll held from before it would leave
                // the target waiting until the poll times out.
                lease.want_view();
            }
        }
        if let Some(wake) = self.wake.get() {
            wake.notify_one();
        }
        // A cache group has no stream to bring to the tail: its reads wait
        // for the commit index themselves.
        if key.kind == ShardKind::Cache {
            if let Some(lead) = self
                .terms
                .lock()
                .get_mut(key)
                .filter(|lead| lead.term == term)
            {
                lead.open = true;
            }
            return;
        }
        let (broker, terms, ending, key) = (
            Arc::clone(&self.broker),
            Arc::clone(&self.terms),
            Arc::clone(&self.ending),
            key.clone(),
        );
        // The term is served only once the stream is at the committed tail:
        // a batch committed below it and delivered after it would reach a
        // resuming reader twice.
        #[cfg(test)]
        let mut stall = self.stall.subscribe();
        tokio::spawn(async move {
            #[cfg(test)]
            let _ = stall.wait_for(|stalled| !stalled).await;
            loop {
                if !terms.lock().get(&key).is_some_and(|lead| lead.term == term) {
                    return;
                }
                // The term opens in the same step that marks the stream as
                // led. Marked after a deposal, the stream would stay marked
                // on a follower and its subscribers would get nothing.
                let open = || {
                    let mut terms = terms.lock();
                    match terms.get_mut(&key).filter(|lead| lead.term == term) {
                        Some(lead) if !ending.lock().contains_key(&key) => {
                            lead.open = true;
                            true
                        }
                        _ => false,
                    }
                };
                // `Ok(false)` moved nothing: the stream is not open on the
                // member's log yet, the group cannot say what is committed,
                // or a deposed term's subscribers are still to be ended.
                let opened = broker
                    .lead_from_tail_if(&key.tenant_id, &key.namespace, &key.stream, key.shard, open)
                    .await;
                let gone = !terms.lock().get(&key).is_some_and(|lead| lead.term == term);
                if gone || opened.is_ok_and(|opened| opened) {
                    return;
                }
                tokio::time::sleep(LEAD_RETRY).await;
            }
        });
    }

    fn deposed(&self, key: &ShardKey, term: u64) {
        {
            let mut terms = self.terms.lock();
            if !terms.get(key).is_some_and(|lead| lead.term <= term) {
                return;
            }
            terms.remove(key);
            if key.kind == ShardKind::Cache {
                return;
            }
            // Counted before a later term can be reported, so that term
            // waits for this one's subscribers to be ended.
            *self.ending.lock().entry(key.clone()).or_default() += 1;
        }
        // Claims are this leader's memory. A successor starts without them
        // and hands the records out again once they lapse.
        self.groups.step_down(&groups::any_group_of(key));
        // The deposed term's subscribers are ended: a batch it was handed
        // may settle as a gap, which they would see as a drop. Ended after
        // the term is gone, on a task, so a reader that registers before
        // that task runs is ended with them and resubscribes. One the
        // leader admitted before the term change but that registers after
        // it is dropped with Settling by `after_tail_join`.
        let (broker, ending, key) = (
            Arc::clone(&self.broker),
            Arc::clone(&self.ending),
            key.clone(),
        );
        #[cfg(test)]
        let mut stall = self.stall_ends.subscribe();
        tokio::spawn(async move {
            #[cfg(test)]
            let _ = stall.wait_for(|stalled| !stalled).await;
            broker
                .follow_engine_log(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await;
            // Told the shard moved, so a client follows it to the next
            // leader instead of reading a clean end.
            broker
                .end_deposed_subscriptions(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await;
            let mut ending = ending.lock();
            if let Some(left) = ending.get_mut(&key) {
                *left -= 1;
                if *left == 0 {
                    ending.remove(&key);
                }
            }
        });
    }
}

mod retention;

/// The group epoch a copy can prove: the one its `raft-epoch` names, or 0
/// for a copy without one while the control plane's record (`recorded`) is
/// at epoch 0. Past epoch 0 a copy without the file is a learner placed
/// fresh or a restore older than the epoch, so its epoch is unknown
/// (`None`): it reports no floor and no copy id. See
/// `docs/per-shard-raft.md`, "A copy's group epoch".
fn proven_epoch(stored: Option<u64>, recorded: Option<u64>) -> Option<u64> {
    stored.or_else(|| (recorded == Some(0)).then_some(0))
}

#[cfg(test)]
pub(crate) mod tests;

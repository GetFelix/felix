//! A group of one that re-forms itself past an offset gap.
//!
//! A standalone broker has no other copy and no control plane, so after a
//! relaxed crash rotated its copies there is nobody to ask. The one voter
//! plans its own next epoch from its kept copies with the rule every
//! re-form uses, records the plan, stops, and starts in the new epoch. It
//! acts only on durable evidence that a relaxed crash or a restore rotated
//! the copy ([`crate::run_state::reform_evidence`]), and accepts the loss
//! that allows: the records the crash took before they were synced, or the
//! ones the restore went back past. After a restore the epoch starts on the
//! time line a backup-point restore uses, since offsets given out after the
//! copy was taken are in no file it holds. See `docs/per-shard-raft.md`, "A
//! standalone broker re-forms itself".

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::epoch_file::{self, EpochBase};
use super::judge::{CopyRoot, kept_copies, lock_dir, parent_of, reached_before};
use super::start::{building, start_due, started_in};
use super::vouch::{BrokerReport, DEFAULT_OFFSET_GAP, GroupRecord, RestoreAt};
use super::wire::{
    PlanView, SurveyStep, WireBase, WireReport, WireStart, first_probes, plan_starts_alone,
    survey_step_alone,
};
use super::{GapFold, copy_file, stop_file};
use crate::disk_log::crash_point;
use crate::disk_log::dir_lock::DirLock;
use crate::log::LogConfig;
use crate::raft_log::state_file;

/// A plan finishes in at most this many survey rounds, as the control
/// plane's does.
const ROUNDS: usize = 8;

/// What the group of one decided, kept until the start finished so a crash
/// part way resumes it instead of planning again over a half-built copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlonePlan {
    /// The copy id the plan made the voter.
    pub copy: u64,
    /// The storage id the copy was minted under. A plan for a copy of
    /// another id belongs to a crash that has since been followed by
    /// another, and is not run.
    pub minted: String,
    /// Every re-form's base, the new epoch's last.
    pub bases: Vec<WireBase>,
    pub start: WireStart,
    /// The copy's last entry when the plan was made, for the stop marker.
    pub last: Option<u64>,
}

/// Where a group of one keeps its [`AlonePlan`]. A standalone broker keeps
/// it beside the copy ([`LocalRecord`]); a cluster's RF 1 shard would write
/// the epoch to the control plane first.
pub trait EpochRecord: Send + Sync {
    /// The plan recorded for `dir`'s copy, if any.
    fn load(&self, dir: &Path) -> io::Result<Option<AlonePlan>>;
    /// Record `plan` durably, before the copy is stopped.
    fn store(&self, dir: &Path, plan: &AlonePlan) -> io::Result<()>;
}

/// The plan as a synced file in the shard directory.
pub struct LocalRecord;

const PLAN_FILE: &str = "raft-reform-alone";

impl EpochRecord for LocalRecord {
    fn load(&self, dir: &Path) -> io::Result<Option<AlonePlan>> {
        state_file::load(dir, PLAN_FILE)?
            .map(|bytes| {
                serde_json::from_slice(&bytes).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{}: {err}", dir.join(PLAN_FILE).display()),
                    )
                })
            })
            .transpose()
    }

    fn store(&self, dir: &Path, plan: &AlonePlan) -> io::Result<()> {
        let bytes = serde_json::to_vec(plan).map_err(io::Error::other)?;
        state_file::store(dir, PLAN_FILE, &[&bytes])
    }
}

/// Reads the copy for a plan: the report at `probes` for a group at the
/// given epoch, as the broker's survey answers.
pub type ReadCopy<'a> = &'a (dyn Fn(&BTreeSet<u64>, u64) -> io::Result<WireReport> + Send + Sync);

/// Everything [`CopyRoot::reform_alone`] needs from the broker.
pub struct Alone<'a> {
    pub shard: &'a str,
    pub dir: &'a Path,
    pub label: &'a str,
    pub incarnation: u64,
    /// The broker's name in its group's membership.
    pub broker: &'a str,
    pub config: &'a LogConfig,
    pub read: ReadCopy<'a>,
    /// The fold for a voter with this copy id.
    pub fold: &'a (dyn Fn(u64) -> Box<dyn GapFold> + Send + Sync),
    /// This copy's member id as its log stores a vote for it.
    pub member: &'a (dyn Fn(u64) -> String + Send + Sync),
    pub record: &'a dyn EpochRecord,
}

/// What [`CopyRoot::reform_alone`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aloned {
    /// No re-form is owed: the copy was not rotated by a relaxed crash, or
    /// it is already in its new epoch.
    NotOwed,
    /// The copy is started in this epoch and opens as its own voter.
    Reformed { epoch: u64 },
}

impl CopyRoot {
    /// Re-form the group of one in `dir` if a relaxed crash rotated its
    /// copy and no re-form has followed. Call before the member opens.
    ///
    /// The steps, each synced before the next: rotate the stale copy as an
    /// open would, plan from its kept copies with the loss accepted, record
    /// the plan, stop, start. A run again after a crash resumes the
    /// recorded plan. A plan for a copy minted under an older storage id,
    /// which a second crash left, is dropped with the copy it was made for
    /// and the group is planned again above everything the first reserved.
    pub async fn reform_alone(&self, a: Alone<'_>) -> io::Result<Aloned> {
        let parent = parent_of(a.dir)?;
        let step = || crash_point::reached(&parent);
        match self.planned_alone(&a, &step).await? {
            None => Ok(Aloned::NotOwed),
            Some((plan, lock)) => self.finish(&a, &plan, &step, lock).await,
        }
    }

    /// The cluster's half of [`Self::reform_alone`]: plan and stop the copy,
    /// and leave the epoch to the control plane. The plan is kept only so
    /// the request can be sent again after a crash; nothing here starts the
    /// copy or counts the epoch as written. Run again, it returns the same
    /// plan while the copy is stopped for it, so the control plane's
    /// idempotent write is never bumped twice.
    pub async fn stop_alone(&self, a: Alone<'_>) -> io::Result<Option<AlonePlan>> {
        let parent = parent_of(a.dir)?;
        let step = || crash_point::reached(&parent);
        let Some((plan, lock)) = self.planned_alone(&a, &step).await? else {
            return Ok(None);
        };
        stop_for(&a, &plan, &step, &lock)?;
        Ok(Some(plan))
    }

    /// The plan this copy owes, recorded, with the directory lock held; or
    /// `None` when no re-form is owed or it already started.
    async fn planned_alone(
        &self,
        a: &Alone<'_>,
        step: &(dyn Fn() -> io::Result<()> + Sync),
    ) -> io::Result<Option<(AlonePlan, DirLock)>> {
        let sources = crate::run_state::reform_evidence(self.root()).map_err(io::Error::other)?;
        if sources.is_empty() {
            return Ok(None);
        }
        let parent = parent_of(a.dir)?;
        // Held from the plan's load to the start's last write, through the
        // rotation's open, so no other open of the copy runs in between.
        let mut lock = lock_dir(a.dir)?;
        if let Some(plan) = a.record.load(a.dir)? {
            let current = copy_file::load(a.dir)?;
            let fresh = current
                .as_ref()
                .is_some_and(|c| c.copy == plan.copy && c.minted == self.storage_id());
            if fresh {
                let epoch = plan.bases.len() as u64;
                if started_in(a.dir, epoch)? {
                    return Ok(None);
                }
                return Ok(Some((plan, lock)));
            }
            if building(a.dir)? {
                // Derived from a kept copy, which a start only reads, and
                // never served: set it aside so the rotation below does not
                // keep a half-built log as evidence.
                discard(a.dir, &parent)?;
                // The lock went with it; the directory the rotation opens
                // is locked at once, as a rotation locks its new one.
                crate::io::create_dir_all_durable(a.dir)?;
                let _discarded = lock.follow(a.dir)?;
            }
        }
        // Rotates a copy minted under another storage id, as an open does.
        let opened = self
            .open_copy_holding(a.shard, a.dir, a.label.to_owned(), a.config.clone(), lock)
            .await?;
        let lock = opened
            .log
            .close_keeping_lock()
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| {
                io::Error::other(format!("{}: closed under the re-form", a.dir.display()))
            })?;
        let Some(restored) = self.owed(a.dir, a.config.index_spacing_bytes, &sources)? else {
            return Ok(None);
        };
        let plan = plan(a, self.storage_id().to_hex(), restored)?;
        step()?;
        a.record.store(a.dir, &plan)?;
        step()?;
        Ok(Some((plan, lock)))
    }

    /// Whether the copy in `dir` was rotated by a relaxed crash or a restore
    /// and is not yet in an epoch past the copies that rotation kept: `None`
    /// if not, else whether a restore is among the rotations since the
    /// newest kept copies were minted. A kept copy minted under an id no
    /// such rotation took away was rotated for another reason, which this
    /// does not re-form.
    fn owed(
        &self,
        dir: &Path,
        spacing: u64,
        sources: &[crate::run_state::Rotated],
    ) -> io::Result<Option<bool>> {
        if !reached_before(dir, spacing)? {
            return Ok(None);
        }
        let epoch_of =
            |dir: &Path| Ok::<_, io::Error>(epoch_file::load(dir)?.map_or(0, |file| file.epoch()));
        let kept = kept_copies(dir)?;
        let mut newest = 0;
        for copy in &kept {
            newest = newest.max(epoch_of(&copy.path)?);
        }
        if epoch_of(dir)? > newest {
            return Ok(None);
        }
        // Newest first, so every rotation since a copy was minted is at or
        // before its own id.
        let mut deepest = 0;
        for copy in kept
            .iter()
            .filter(|copy| epoch_of(&copy.path).is_ok_and(|e| e == newest))
        {
            let minted = copy_file::load(&copy.path)?.map(|file| file.minted);
            let Some(at) = minted.and_then(|minted| sources.iter().position(|s| s.id == minted))
            else {
                return Ok(None);
            };
            deepest = deepest.max(at);
        }
        Ok(Some(sources[..=deepest].iter().any(|s| s.restore)))
    }

    async fn finish(
        &self,
        a: &Alone<'_>,
        plan: &AlonePlan,
        step: &(dyn Fn() -> io::Result<()> + Sync),
        lock: DirLock,
    ) -> io::Result<Aloned> {
        let bases: Vec<EpochBase> = plan.bases.iter().copied().map(EpochBase::from).collect();
        let epoch = bases.len() as u64;
        stop_for(a, plan, step, &lock)?;
        if !start_due(a.dir, epoch)? {
            return Err(io::Error::other(format!(
                "{}: not a copy stopped for epoch {epoch}",
                a.dir.display()
            )));
        }
        let fold = (a.fold)(plan.copy);
        self.start_stopped_holding(
            a.shard,
            a.dir,
            a.incarnation,
            bases,
            &plan.start,
            fold.as_ref(),
            &(a.member)(plan.copy),
            a.config,
            lock,
        )
        .await?;
        Ok(Aloned::Reformed { epoch })
    }
}

/// Stop the copy for `plan`'s epoch unless it is stopped, building or past it.
fn stop_for(
    a: &Alone<'_>,
    plan: &AlonePlan,
    step: &(dyn Fn() -> io::Result<()> + Sync),
    lock: &DirLock,
) -> io::Result<()> {
    let epoch = plan.bases.len() as u64;
    let at = super::group_epoch(a.dir)?.unwrap_or(0);
    if !building(a.dir)? && stop_file::stopped(a.dir)?.is_none() && at + 1 == epoch {
        stop_file::stop_holding(a.dir, plan.last, lock)?;
        step()?;
    }
    Ok(())
}

/// Plan the next epoch of the group of one from `a.dir`'s kept copies. After
/// a restore the epoch starts no lower than the backup-point restore's time
/// line at this broker's clock.
fn plan(a: &Alone<'_>, minted: String, restored: bool) -> io::Result<AlonePlan> {
    let me = a.broker.to_owned();
    let copy = copy_file::load(a.dir)?
        .ok_or_else(|| io::Error::other(format!("{}: no copy id", a.dir.display())))?
        .copy;
    let bases = epoch_file::load(a.dir)?.map_or_else(Vec::new, |file| file.bases);
    let epoch = bases.len() as u64;
    let me_voter = BTreeSet::from([(me.clone(), copy)]);
    let record = GroupRecord {
        incarnation: a.incarnation,
        bases: bases.clone(),
        floor: None,
        offset_floor: None,
        placed: Some(BTreeSet::from([me.clone()])),
        dead: BTreeSet::new(),
        seed: Some(me_voter.clone()),
        ordered: Some(vec![me_voter]),
        restore: None,
    };
    let refuse = |why: String| io::Error::other(format!("{}: {why}", a.dir.display()));
    let mut asked = first_probes(&record);
    for _ in 0..ROUNDS {
        let report = BrokerReport::<String>::try_from((a.read)(&asked, epoch)?)
            .map_err(|err| refuse(err.to_string()))?;
        let reports = BTreeMap::from([(me.clone(), report)]);
        match survey_step_alone(&record, &reports, &asked) {
            SurveyStep::Ask(more) => asked = more,
            SurveyStep::Planned(view) => {
                // Accepted: the loss is the unsynced tail the crash took.
                let start = plan_starts_alone(&record, &reports)
                    .and_then(|mut starts| starts.remove(&me))
                    .ok_or_else(|| refuse("no plan for this broker".into()))?;
                let last = reports[&me].copies.current.last.map(|last| last.index);
                let mut base = new_base(&view);
                if restored {
                    let line = time_line()
                        .ok_or_else(|| refuse("the clock is past the time line".into()))?;
                    base.first_index = base.first_index.max(line);
                    tracing::warn!(
                        shard = a.label,
                        first_index = base.first_index,
                        "a restore rotated this copy; the next epoch starts on the restore time \
                         line, so no offset given out after the restored copy was taken is \
                         given out again"
                    );
                }
                return Ok(AlonePlan {
                    copy,
                    minted,
                    bases: bases
                        .iter()
                        .copied()
                        .map(WireBase::from)
                        .chain([base])
                        .collect(),
                    start,
                    last,
                });
            }
            SurveyStep::Blocked(why) => return Err(refuse(format!("cannot re-form: {why}"))),
            SurveyStep::Leads(_) => return Err(refuse("the member leads".into())),
        }
    }
    Err(refuse("the plan needed more reads than allowed".into()))
}

/// `RESTORE_OFFSETS_PER_MILLI` times this broker's clock in Unix
/// milliseconds, plus the default gap: the first index a backup-point
/// restore planned now takes. `None` past the last index.
fn time_line() -> Option<u64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    RestoreAt {
        end: 0,
        gap: DEFAULT_OFFSET_GAP,
        planned_at_millis: u64::try_from(now.as_millis()).ok()?,
    }
    .first_index()
}

fn new_base(view: &PlanView) -> WireBase {
    WireBase {
        base_last: view.base_last,
        first_index: view.first_index,
    }
}

/// Move a half-built directory out of the shard's way, to `.discarded`.
fn discard(dir: &Path, parent: &Path) -> io::Result<()> {
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::other("a shard directory needs a name"))?;
    let aside = parent.join(".discarded");
    crate::io::create_dir_all_durable(&aside)?;
    let mut n = 0u32;
    let target = loop {
        let candidate = aside.join(format!("{name}.{n}"));
        if !crate::fs::try_exists(&candidate)? {
            break candidate;
        }
        n += 1;
    };
    crate::fs::rename(dir, &target)?;
    crate::io::sync_dir(&aside)?;
    crate::io::sync_dir(parent)
}

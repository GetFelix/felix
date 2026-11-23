//! A group of one that re-forms itself past an offset gap.
//!
//! A standalone broker has no other copy and no control plane, so after a
//! relaxed crash rotated its copies there is nobody to ask. The one voter
//! plans its own next epoch from its kept copies with the rule every
//! re-form uses, records the plan, stops, and starts in the new epoch. It
//! acts only on durable evidence that a relaxed crash rotated the copy
//! ([`crate::run_state::relaxed_crash_sources`]), and accepts the loss that
//! allows: the records the crash took before they were synced. See
//! `docs/per-shard-raft.md`, "A standalone broker re-forms itself".

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::epoch_file::{self, EpochBase};
use super::judge::{CopyRoot, kept_copies, parent_of, reached_before};
use super::start::{building, start_due, started_in};
use super::vouch::{BrokerReport, GroupRecord};
use super::wire::{
    PlanView, SurveyStep, WireBase, WireReport, WireStart, first_probes, plan_starts, survey_step,
};
use super::{GapFold, copy_file, stop_file};
use crate::disk_log::crash_point;
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
        let sources =
            crate::run_state::relaxed_crash_sources(self.root()).map_err(io::Error::other)?;
        if sources.is_empty() {
            return Ok(Aloned::NotOwed);
        }
        let parent = parent_of(a.dir)?;
        let step = || crash_point::reached(&parent);
        if let Some(plan) = a.record.load(a.dir)? {
            let current = copy_file::load(a.dir)?;
            let fresh = current
                .as_ref()
                .is_some_and(|c| c.copy == plan.copy && c.minted == self.storage_id());
            if fresh {
                let epoch = plan.bases.len() as u64;
                if started_in(a.dir, epoch)? {
                    return Ok(Aloned::NotOwed);
                }
                return self.finish(&a, &plan, &step).await;
            }
            if building(a.dir)? {
                // Derived from a kept copy, which a start only reads, and
                // never served: set it aside so the rotation below does not
                // keep a half-built log as evidence.
                discard(a.dir, &parent)?;
            }
        }
        // Rotates a copy minted under another storage id, as an open does.
        let opened = self
            .open_copy(a.shard, a.dir, a.label, a.config.clone())
            .await?;
        opened.log.close().await.map_err(io::Error::other)?;
        if !self.owed(a.dir, a.config.index_spacing_bytes, &sources)? {
            return Ok(Aloned::NotOwed);
        }
        let plan = plan(&a, self.storage_id().to_hex())?;
        step()?;
        a.record.store(a.dir, &plan)?;
        step()?;
        self.finish(&a, &plan, &step).await
    }

    /// Whether the copy in `dir` was rotated by a relaxed crash and is not
    /// yet in an epoch past the copies that rotation kept. A kept copy minted
    /// under an id no relaxed crash rotated away was rotated for another
    /// reason, which this does not re-form.
    fn owed(&self, dir: &Path, spacing: u64, sources: &[crate::StorageId]) -> io::Result<bool> {
        if !reached_before(dir, spacing)? {
            return Ok(false);
        }
        let epoch_of =
            |dir: &Path| Ok::<_, io::Error>(epoch_file::load(dir)?.map_or(0, |file| file.epoch()));
        let kept = kept_copies(dir)?;
        let mut newest = 0;
        for copy in &kept {
            newest = newest.max(epoch_of(&copy.path)?);
        }
        if epoch_of(dir)? > newest {
            return Ok(false);
        }
        for copy in kept
            .iter()
            .filter(|copy| epoch_of(&copy.path).is_ok_and(|e| e == newest))
        {
            let minted = copy_file::load(&copy.path)?.map(|file| file.minted);
            if !minted.is_some_and(|minted| sources.contains(&minted)) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn finish(
        &self,
        a: &Alone<'_>,
        plan: &AlonePlan,
        step: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<Aloned> {
        let bases: Vec<EpochBase> = plan.bases.iter().copied().map(EpochBase::from).collect();
        let epoch = bases.len() as u64;
        let at = super::group_epoch(a.dir)?.unwrap_or(0);
        if !building(a.dir)? && stop_file::stopped(a.dir)?.is_none() && at + 1 == epoch {
            stop_file::stop(a.dir, plan.last)?;
            step()?;
        }
        if !start_due(a.dir, epoch)? {
            return Err(io::Error::other(format!(
                "{}: not a copy stopped for epoch {epoch}",
                a.dir.display()
            )));
        }
        let fold = (a.fold)(plan.copy);
        self.start_stopped(
            a.shard,
            a.dir,
            a.incarnation,
            bases,
            &plan.start,
            fold.as_ref(),
            &(a.member)(plan.copy),
            a.config,
        )
        .await?;
        Ok(Aloned::Reformed { epoch })
    }
}

/// Plan the next epoch of the group of one from `a.dir`'s kept copies.
fn plan(a: &Alone<'_>, minted: String) -> io::Result<AlonePlan> {
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
        match survey_step(&record, &reports, &asked) {
            SurveyStep::Ask(more) => asked = more,
            SurveyStep::Planned(view) => {
                // Accepted: the loss is what the relaxed crash took.
                let start = plan_starts(&record, &reports, true)
                    .and_then(|mut starts| starts.remove(&me))
                    .ok_or_else(|| refuse("no plan for this broker".into()))?;
                let last = reports[&me].copies.current.last.map(|last| last.index);
                return Ok(AlonePlan {
                    copy,
                    minted,
                    bases: bases
                        .iter()
                        .copied()
                        .map(WireBase::from)
                        .chain([new_base(&view)])
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

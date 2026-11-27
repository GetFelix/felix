//! The re-form survey's wire forms: a broker's report as it travels to the
//! control plane, and the plan the control plane shows an operator.
//!
//! These are separate from the planner's own types so either side can
//! change without changing the bytes. `docs/per-shard-raft.md`,
//! "Re-forming a group".
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::epoch_file::{EpochBase, EpochFile, EpochLogId};
use super::reform::{BrokerCopies, CopyRead};
use super::rotated_by::RotatedBy;
use super::vouch::{
    BrokerReport, GroupRecord, Marked, ReformPlan, Refused, Source, VoterSource, plan_reform_as,
    probes,
};
use crate::disk_log::CarriedFloor;
use crate::raft_log::LogPoint;

/// The report layout this build writes and reads. A report of any other
/// version is refused rather than read as this one.
pub const REPORT_VERSION: u32 = 1;

/// One broker's report for one shard, on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireReport {
    pub version: u32,
    pub leads: bool,
    pub names: BTreeSet<String>,
    #[serde(default)]
    pub memberships: Option<Vec<BTreeSet<(String, u64)>>>,
    pub current: WireCopy,
    #[serde(default)]
    pub kept: Vec<WireCopy>,
}

/// One copy as a broker read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireCopy {
    pub path: String,
    pub copy: Option<u64>,
    /// The copy's `raft-epoch`: its incarnation and the base of each
    /// re-form it went through.
    pub epoch: Option<(u64, Vec<WireBase>)>,
    /// The last entry, as `(term, index)`.
    pub last: Option<(u64, u64)>,
    pub purged: Option<u64>,
    pub committed: Option<u64>,
    pub vote: Option<u64>,
    pub reserved: Option<u64>,
    pub floor: Option<WireFloor>,
    /// `"restore"`, or the copy id a power cut rotated into.
    pub rotated_by: Option<WireRotated>,
    /// The term at each probed index, `None` where the copy has no entry.
    #[serde(default)]
    pub terms: BTreeMap<u64, Option<u64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireBase {
    /// `(index, epoch, term)`; `None` for an empty base.
    pub base_last: Option<(u64, u64, u64)>,
    pub first_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireFloor {
    pub offset: u64,
    pub floor: u64,
    pub headers: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireRotated {
    PowerCut { to: u64 },
    Restore,
}

/// Why a report was not taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionMismatch(pub u32);

impl std::fmt::Display for VersionMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "report version {} is not {REPORT_VERSION}", self.0)
    }
}

impl std::error::Error for VersionMismatch {}

impl From<&BrokerReport<std::sync::Arc<str>>> for WireReport {
    fn from(report: &BrokerReport<std::sync::Arc<str>>) -> Self {
        Self {
            version: REPORT_VERSION,
            leads: report.leads,
            names: report.names.iter().map(|b| b.to_string()).collect(),
            memberships: report.memberships.as_ref().map(|sets| {
                sets.iter()
                    .map(|set| set.iter().map(|(b, c)| (b.to_string(), *c)).collect())
                    .collect()
            }),
            current: WireCopy::from(&report.copies.current),
            kept: report.copies.kept.iter().map(WireCopy::from).collect(),
        }
    }
}

impl TryFrom<WireReport> for BrokerReport<String> {
    type Error = VersionMismatch;

    fn try_from(wire: WireReport) -> Result<Self, VersionMismatch> {
        if wire.version != REPORT_VERSION {
            return Err(VersionMismatch(wire.version));
        }
        Ok(Self {
            leads: wire.leads,
            names: wire.names,
            memberships: wire.memberships,
            copies: BrokerCopies {
                current: wire.current.into(),
                kept: wire.kept.into_iter().map(CopyRead::from).collect(),
            },
        })
    }
}

impl From<&CopyRead> for WireCopy {
    fn from(read: &CopyRead) -> Self {
        Self {
            path: read.path.display().to_string(),
            copy: read.copy,
            epoch: read.epoch.as_ref().map(|file| {
                (
                    file.incarnation,
                    file.bases.iter().copied().map(WireBase::from).collect(),
                )
            }),
            last: read.last.map(|last| (last.term, last.index)),
            purged: read.purged,
            committed: read.committed,
            vote: read.vote,
            reserved: read.reserved,
            floor: read.floor.as_ref().map(WireFloor::from),
            rotated_by: read.rotated_by.map(|by| match by {
                RotatedBy::PowerCut { to } => WireRotated::PowerCut { to },
                RotatedBy::Restore => WireRotated::Restore,
            }),
            terms: read.terms.clone(),
        }
    }
}

impl From<WireCopy> for CopyRead {
    fn from(wire: WireCopy) -> Self {
        Self {
            path: PathBuf::from(wire.path),
            copy: wire.copy,
            epoch: wire.epoch.map(|(incarnation, bases)| EpochFile {
                incarnation,
                bases: bases.into_iter().map(EpochBase::from).collect(),
            }),
            last: wire.last.map(|(term, index)| LogPoint { term, index }),
            purged: wire.purged,
            committed: wire.committed,
            vote: wire.vote,
            reserved: wire.reserved,
            floor: wire.floor.as_ref().map(CarriedFloor::from),
            rotated_by: wire.rotated_by.map(|by| match by {
                WireRotated::PowerCut { to } => RotatedBy::PowerCut { to },
                WireRotated::Restore => RotatedBy::Restore,
            }),
            terms: wire.terms,
        }
    }
}

impl From<EpochBase> for WireBase {
    fn from(base: EpochBase) -> Self {
        Self {
            base_last: base.base_last.map(|l| (l.index, l.epoch, l.term)),
            first_index: base.first_index,
        }
    }
}

impl From<WireBase> for EpochBase {
    fn from(wire: WireBase) -> Self {
        Self {
            base_last: wire
                .base_last
                .map(|(index, epoch, term)| EpochLogId { index, epoch, term }),
            first_index: wire.first_index,
        }
    }
}

/// What one survey round says about a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurveyStep {
    /// Some copy has to be read at more indexes: ask every broker again
    /// with these probes.
    Ask(BTreeSet<u64>),
    /// The next epoch.
    Planned(PlanView),
    /// No plan can be made.
    Blocked(String),
    /// A broker leads the current epoch, so the group needs no re-form.
    Leads(String),
}

/// The probes the first round asks, before any broker answered.
pub fn first_probes(record: &GroupRecord<String>) -> BTreeSet<u64> {
    probes(record, &BTreeMap::new())
}

/// Plan from one round's reports, all read at `asked`. A refusal for an
/// index nobody was asked about yet is a new round, never a block: the
/// rounds only grow the probes, so they stop once a round adds none.
pub fn survey_step(
    record: &GroupRecord<String>,
    reports: &BTreeMap<String, BrokerReport<String>>,
    asked: &BTreeSet<u64>,
) -> SurveyStep {
    survey_step_as(record, reports, asked, false)
}

/// [`survey_step`] for a group of one that re-forms itself: every copy
/// vouches for its whole log (`plan_reform_as`).
pub(crate) fn survey_step_alone(
    record: &GroupRecord<String>,
    reports: &BTreeMap<String, BrokerReport<String>>,
    asked: &BTreeSet<u64>,
) -> SurveyStep {
    survey_step_as(record, reports, asked, true)
}

fn survey_step_as(
    record: &GroupRecord<String>,
    reports: &BTreeMap<String, BrokerReport<String>>,
    asked: &BTreeSet<u64>,
    alone: bool,
) -> SurveyStep {
    match plan_reform_as(record, reports, alone) {
        Ok(plan) => SurveyStep::Planned(PlanView::of(&plan, reports)),
        Err(Refused::Leads(broker)) => SurveyStep::Leads(broker),
        Err(Refused::NotProbed { broker, index }) => {
            let next: BTreeSet<u64> = probes(record, reports).union(asked).copied().collect();
            if next.len() > asked.len() {
                SurveyStep::Ask(next)
            } else {
                SurveyStep::Blocked(format!(
                    "{broker} was not read at index {index}, which no probe covers"
                ))
            }
        }
        Err(refused) => SurveyStep::Blocked(refused_reason(&refused)),
    }
}

fn refused_reason(refused: &Refused<String>) -> String {
    match refused {
        Refused::NoPlacedRecord => {
            "the shard has no record of the brokers placed in its epoch".into()
        }
        Refused::Unaccounted(b) => format!("broker {b} did not answer and is not declared dead"),
        Refused::Leads(b) => format!("broker {b} leads the current epoch"),
        Refused::OffRecord { broker, names } => {
            format!("{broker}'s membership names {names}, which is neither placed nor dead")
        }
        Refused::EpochAhead { broker, epoch } => {
            format!("{broker} holds a copy at epoch {epoch}, past the record")
        }
        Refused::BasesDiffer(b) => format!("{b}'s recorded bases differ from the control plane's"),
        Refused::RecordInGap { broker, index } => {
            format!("{broker} holds a record at index {index}, inside a re-form's gap")
        }
        Refused::NotProbed { broker, index } => format!("{broker} was not read at index {index}"),
        Refused::Purged { broker, index } => {
            format!("the base ends at index {index}, which {broker} purged")
        }
        Refused::NotAtPoint { base_end, point } => format!(
            "the base ends at offset {base_end}, not at the backup point's {point}; run \
             `felix-broker restore-point` with this point on the copied leader's directory"
        ),
        Refused::NoIndexLeft => "every index is reserved".into(),
        Refused::RestoreTooLate { planned_at_millis } => format!(
            "the restore's first index at plan time {planned_at_millis} ms is past the last index"
        ),
        Refused::LineBehind { line, given } => format!(
            "the restore's time line plus gap at the plan time ({line}) is not far enough above \
             index {given}, which the floor or an earlier epoch reached, to clear what that epoch \
             can have given out; plan again once about 4.5 minutes have passed since it began, \
             with the default gap, and check the control-plane clocks if it is still refused"
        ),
        Refused::NothingRead => "no copy was read".into(),
        Refused::NoVoters => "no broker with a copy id holds the base".into(),
        Refused::NotAlone => "another broker is or was in the group".into(),
    }
}

/// A plan as the operator sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanView {
    pub epoch: u64,
    pub source_broker: String,
    /// `None` for the broker's current copy, else the kept copy's directory.
    pub source_kept: Option<String>,
    /// `(index, epoch, term)` of the base's last entry; `None` when empty.
    pub base_last: Option<(u64, u64, u64)>,
    pub first_index: u64,
    /// For a restore, the offsets past the point the new epoch leaves
    /// unused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_gap: Option<u64>,
    /// `(broker, copy id)`.
    pub voters: Vec<(String, u64)>,
    pub learners: Vec<String>,
    /// Why the plan cannot vouch past saved commits, when it cannot.
    pub marked: Option<String>,
    pub cut_by_vote: bool,
    /// Whether applying it needs the operator to accept the loss.
    pub needs_accept_loss: bool,
    /// Records some copy holds past the base, which the next epoch drops,
    /// as `(broker, first index, last index)`.
    pub dropped: Vec<(String, u64, u64)>,
}

impl PlanView {
    fn of(plan: &ReformPlan<String>, reports: &BTreeMap<String, BrokerReport<String>>) -> Self {
        let base_end = plan.base_end();
        let dropped = reports
            .iter()
            .filter_map(|(broker, report)| {
                let last = std::iter::once(&report.copies.current)
                    .chain(&report.copies.kept)
                    .filter_map(|read| read.last.map(|l| l.index))
                    .max()?;
                (last >= base_end).then(|| (broker.clone(), base_end, last))
            })
            .collect();
        Self {
            epoch: plan.epoch,
            source_broker: plan.source.0.clone(),
            source_kept: match &plan.source.1 {
                Source::Current => None,
                Source::Kept(path) => Some(path.display().to_string()),
            },
            base_last: plan.base_last.map(|l| (l.index, l.epoch, l.term)),
            first_index: plan.first_index,
            offset_gap: plan.offset_gap,
            voters: plan
                .voters
                .iter()
                .map(|v| (v.broker.clone(), v.copy))
                .collect(),
            learners: plan.learners.clone(),
            marked: plan.marked.as_ref().map(marked_reason),
            cut_by_vote: plan.cut_by_vote,
            needs_accept_loss: plan.needs_accept_loss(),
            dropped,
        }
    }
}

fn marked_reason(marked: &Marked<String>) -> String {
    let from = |source: &VoterSource<String>| match source {
        VoterSource::Seed => "the epoch's first voters".to_string(),
        VoterSource::Ordered => "the targets ordered in the epoch".to_string(),
        VoterSource::Membership(b) => format!("a membership on {b}"),
    };
    let names = |set: &BTreeSet<(String, u64)>| {
        set.iter()
            .map(|(b, _)| b.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    match marked {
        Marked::Unknown(source) => format!("{} are not recorded", from(source)),
        Marked::Majority {
            from: source,
            set,
            unknown,
        } => format!(
            "the votes of {} are not known, a majority of {} ({})",
            names(unknown),
            from(source),
            names(set)
        ),
    }
}

/// One broker's part of an applied plan, sent with its `start` survey:
/// what its copy is in the new epoch. See `docs/per-shard-raft.md`, "What a
/// start needs".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireStart {
    /// The copy id the plan made a voter; `None` for a learner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voter: Option<u64>,
    /// The kept directory a voter's base is read from, as its broker
    /// reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept: Option<String>,
    /// The plan's highest floor, raised into every copy's `raft-floor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor: Option<WireFloor>,
    #[serde(default)]
    pub needs_accept_loss: bool,
    /// Whether the operator named the shard with `--accept-loss`.
    #[serde(default)]
    pub accepted: bool,
}

/// Each placed broker's [`WireStart`] under the plan `reports` make, or
/// `None` when they make none. `accepted` is the operator's `--accept-loss`.
pub fn plan_starts(
    record: &GroupRecord<String>,
    reports: &BTreeMap<String, BrokerReport<String>>,
    accepted: bool,
) -> Option<BTreeMap<String, WireStart>> {
    plan_starts_as(record, reports, accepted, false)
}

/// [`plan_starts`] under [`survey_step_alone`]'s plan.
pub(crate) fn plan_starts_alone(
    record: &GroupRecord<String>,
    reports: &BTreeMap<String, BrokerReport<String>>,
) -> Option<BTreeMap<String, WireStart>> {
    plan_starts_as(record, reports, true, true)
}

fn plan_starts_as(
    record: &GroupRecord<String>,
    reports: &BTreeMap<String, BrokerReport<String>>,
    accepted: bool,
    alone: bool,
) -> Option<BTreeMap<String, WireStart>> {
    let plan = plan_reform_as(record, reports, alone).ok()?;
    let floor = plan.floor.as_ref().map(WireFloor::from);
    let needs_accept_loss = plan.needs_accept_loss();
    let start = |voter: Option<u64>, kept: Option<String>| WireStart {
        voter,
        kept,
        floor: floor.clone(),
        needs_accept_loss,
        accepted,
    };
    let mut starts: BTreeMap<String, WireStart> = plan
        .voters
        .iter()
        .map(|voter| {
            let kept = match &voter.source {
                Source::Current => None,
                Source::Kept(path) => Some(path.display().to_string()),
            };
            (voter.broker.clone(), start(Some(voter.copy), kept))
        })
        .collect();
    for learner in &plan.learners {
        starts
            .entry(learner.clone())
            .or_insert_with(|| start(None, None));
    }
    Some(starts)
}

impl From<&CarriedFloor> for WireFloor {
    fn from(floor: &CarriedFloor) -> Self {
        Self {
            offset: floor.offset,
            floor: floor.floor,
            headers: floor.headers.to_vec(),
        }
    }
}

impl From<&WireFloor> for CarriedFloor {
    fn from(wire: &WireFloor) -> Self {
        Self {
            offset: wire.offset,
            floor: wire.floor,
            headers: Bytes::from(wire.headers.clone()),
        }
    }
}

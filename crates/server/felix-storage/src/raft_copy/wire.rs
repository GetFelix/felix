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
    BrokerReport, GroupRecord, Marked, ReformPlan, Refused, Source, VoterSource, plan_reform,
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
            floor: read.floor.as_ref().map(|floor| WireFloor {
                offset: floor.offset,
                floor: floor.floor,
                headers: floor.headers.to_vec(),
            }),
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
            floor: wire.floor.map(|floor| CarriedFloor {
                offset: floor.offset,
                floor: floor.floor,
                headers: Bytes::from(floor.headers),
            }),
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
    match plan_reform(record, reports) {
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
        Refused::NoIndexLeft => "every index is reserved".into(),
        Refused::NothingRead => "no copy was read".into(),
        Refused::NoVoters => "no broker with a copy id holds the base".into(),
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

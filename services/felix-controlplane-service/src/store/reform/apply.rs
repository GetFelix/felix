//! Applying a re-form plan: a stop round, a compare against the plan the
//! operator read, the epoch write, then a start round. Every step is asked
//! on the survey channel, so the control plane still never calls a broker.
//! `docs/per-shard-raft.md`, "Applying a plan".
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use felix_storage::raft_copy::{
    BrokerReport, PlanView, SurveyStep, WireStart, plan_starts, survey_step,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{ROUND_TIMEOUT, ReformAnswer, ReformPlans, ReformSurvey, ShardSurvey, SurveyKind};
use crate::model::{RaftEpochBase, RaftLogId, ReformEpochStart, ShardKey};
use crate::store::ControlPlaneStore;

/// One shard's line of an applied plan.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformApplyView {
    /// `stopping`, `writing`, `starting`, `started`, `skipped`, `changed`,
    /// `left_alone` or `failed`.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The epoch written, once it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// Brokers the current round still waits on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waiting_on: Vec<String>,
}

pub(super) struct ShardApply {
    /// The plan the operator read, which the post-stop plan must equal.
    planned: PlanView,
    /// Whether the operator named the shard with `--accept-loss`.
    accepted: bool,
    /// Each broker's part of the plan, sent with its `start` survey. Made
    /// with the compare, from the same post-stop reads.
    starts: BTreeMap<String, WireStart>,
    phase: Phase,
}

enum Phase {
    Stopping {
        reports: BTreeMap<String, BrokerReport<String>>,
        started: Instant,
    },
    /// The compare passed; the next request writes it.
    Due(ReformEpochStart),
    /// Taken by one request's [`InFlight`]; back to `Due` if that request
    /// is dropped before the store answers.
    Writing(ReformEpochStart),
    /// The epoch is written; waiting for each broker to start it. There is
    /// no timeout: a broker starts from its feed whether or not it answers.
    Starting {
        epoch: u64,
        started: BTreeSet<String>,
    },
    Ended {
        state: &'static str,
        reason: Option<String>,
        epoch: Option<u64>,
    },
}

impl ShardApply {
    pub(super) fn ended(&self) -> bool {
        matches!(self.phase, Phase::Ended { .. })
    }

    pub(super) fn survey_for(
        &self,
        plan_id: &str,
        shard: &ShardSurvey,
        node: &str,
    ) -> Option<ReformSurvey> {
        if !shard.asked_brokers.contains(node) {
            return None;
        }
        let (kind, epoch, start) = match &self.phase {
            Phase::Stopping { reports, .. } if !reports.contains_key(node) => {
                (SurveyKind::Stop, shard.epoch, None)
            }
            Phase::Starting { epoch, started } if !started.contains(node) => {
                (SurveyKind::Start, *epoch, self.starts.get(node).cloned())
            }
            _ => return None,
        };
        Some(ReformSurvey {
            plan_id: plan_id.to_string(),
            kind,
            round: shard.round,
            key: shard.key.clone(),
            incarnation: shard.incarnation,
            epoch,
            probes: shard.probes.clone(),
            start,
        })
    }

    pub(super) fn view(&self, shard: &ShardSurvey) -> ReformApplyView {
        let waiting = |done: &dyn Fn(&str) -> bool| {
            shard
                .asked_brokers
                .iter()
                .filter(|b| !done(b))
                .cloned()
                .collect()
        };
        let (state, reason, epoch, waiting_on) = match &self.phase {
            Phase::Stopping { reports, .. } => (
                "stopping",
                None,
                None,
                waiting(&|b| reports.contains_key(b)),
            ),
            Phase::Due(_) | Phase::Writing(_) => ("writing", None, None, Vec::new()),
            Phase::Starting { epoch, started } => (
                "starting",
                None,
                Some(*epoch),
                waiting(&|b| started.contains(b)),
            ),
            Phase::Ended {
                state,
                reason,
                epoch,
            } => (*state, reason.clone(), *epoch, Vec::new()),
        };
        ReformApplyView {
            state: state.to_string(),
            reason,
            epoch,
            waiting_on,
        }
    }
}

fn ended(state: &'static str, reason: impl Into<String>) -> Phase {
    Phase::Ended {
        state,
        reason: Some(reason.into()),
        epoch: None,
    }
}

impl ReformPlans {
    /// Apply plan `id`: every shard it marked `reform` starts its stop
    /// round, except one whose plan needs the loss accepted and is not in
    /// `accept_loss`. Returns how many started. A shard already being
    /// applied, or started, is left as it is; one whose apply ended without
    /// a write is applied again, since its stopped copies stay stopped.
    pub(crate) fn apply(
        &self,
        id: &str,
        accept_loss: &[ShardKey],
        now: Instant,
    ) -> Result<usize, String> {
        let mut plans = self.plans.lock();
        let Some(plan) = plans.iter_mut().find(|p| p.id == id) else {
            return Err("no such plan on the metadata leader; plan again".to_string());
        };
        plan.shards.iter_mut().for_each(|s| s.settle(now));
        if plan.shards.iter().any(|s| s.outcome.is_none()) {
            return Err("the plan is still surveying".to_string());
        }
        let mut applying = 0;
        for shard in &mut plan.shards {
            let Some(SurveyStep::Planned(planned)) = &shard.outcome else {
                continue;
            };
            let again = shard
                .apply
                .as_ref()
                .is_none_or(|a| matches!(a.phase, Phase::Ended { epoch: None, .. }));
            if !again {
                continue;
            }
            let accepted = accept_loss.contains(&shard.key);
            let phase = if planned.needs_accept_loss && !accepted {
                ended(
                    "skipped",
                    "the plan may lose acknowledged records; name the shard with --accept-loss",
                )
            } else {
                applying += 1;
                shard.round += 1;
                Phase::Stopping {
                    reports: BTreeMap::new(),
                    started: now,
                }
            };
            shard.apply = Some(ShardApply {
                planned: planned.clone(),
                accepted,
                starts: BTreeMap::new(),
                phase,
            });
        }
        Ok(applying)
    }

    /// Take every epoch write a compare made due, marking each in flight.
    fn take_due(&self) -> Vec<InFlight<'_>> {
        let mut plans = self.plans.lock();
        let mut due = Vec::new();
        for plan in plans.iter_mut() {
            for shard in &mut plan.shards {
                let Some(apply) = &mut shard.apply else {
                    continue;
                };
                if let Phase::Due(start) = &apply.phase {
                    let start = start.clone();
                    apply.phase = Phase::Writing(start.clone());
                    due.push(InFlight {
                        plans: self,
                        id: plan.id.clone(),
                        key: shard.key.clone(),
                        start,
                        answered: false,
                    });
                }
            }
        }
        due
    }

    /// Put a write whose request was dropped back to `Due`, so the next
    /// request writes it again. The write is a compare and swap and
    /// [`write_due`] reads a refusal that finds this start already written
    /// as written, so doing it twice is safe.
    fn unwrite(&self, id: &str, key: &ShardKey) {
        let mut plans = self.plans.lock();
        let apply = plans
            .iter_mut()
            .find(|p| p.id == id)
            .and_then(|p| p.shards.iter_mut().find(|s| &s.key == key))
            .and_then(|s| s.apply.as_mut());
        if let Some(apply) = apply
            && let Phase::Writing(start) = &apply.phase
        {
            apply.phase = Phase::Due(start.clone());
        }
    }

    fn written(&self, id: &str, key: &ShardKey, written: Result<u64, String>) {
        let mut plans = self.plans.lock();
        let Some(shard) = plans
            .iter_mut()
            .find(|p| p.id == id)
            .and_then(|p| p.shards.iter_mut().find(|s| &s.key == key))
        else {
            return;
        };
        let Some(apply) = &mut shard.apply else {
            return;
        };
        apply.phase = match written {
            Ok(epoch) => {
                shard.round += 1;
                Phase::Starting {
                    epoch,
                    started: BTreeSet::new(),
                }
            }
            Err(why) => ended("failed", format!("the epoch write was refused: {why}")),
        };
    }
}

/// An epoch write one request took. Dropped unanswered, it goes back to
/// `Due`; otherwise a request cancelled mid-write would leave its shard at
/// `writing` for good.
pub(super) struct InFlight<'a> {
    plans: &'a ReformPlans,
    pub(super) id: String,
    pub(super) key: ShardKey,
    pub(super) start: ReformEpochStart,
    answered: bool,
}

impl InFlight<'_> {
    fn answer(mut self, written: Result<u64, String>) {
        self.answered = true;
        self.plans.written(&self.id, &self.key, written);
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if !self.answered {
            self.plans.unwrite(&self.id, &self.key);
        }
    }
}

/// Take `node`'s answer to an apply round. False when it is not for the
/// round asked now.
pub(super) fn record(
    shard: &mut ShardSurvey,
    node: &str,
    answer: ReformAnswer,
    now: Instant,
) -> bool {
    let round = shard.round;
    let asked = shard.asked_brokers.contains(node);
    let Some(apply) = &mut shard.apply else {
        return false;
    };
    if !asked || answer.round != round {
        return false;
    }
    let taken = match (&mut apply.phase, answer.kind) {
        (Phase::Stopping { reports, .. }, SurveyKind::Stop) if !reports.contains_key(node) => {
            match BrokerReport::try_from(answer.report) {
                Ok(report) => {
                    reports.insert(node.to_string(), report);
                    true
                }
                Err(_) => false,
            }
        }
        (Phase::Starting { started, .. }, SurveyKind::Start) => started.insert(node.to_string()),
        _ => false,
    };
    if taken {
        settle(shard, now);
    }
    taken
}

/// Move a shard's apply on once its round has every answer, or a stop round
/// timed out.
pub(super) fn settle(shard: &mut ShardSurvey, now: Instant) {
    let Some(apply) = &shard.apply else { return };
    let mut starts = None;
    let next = match &apply.phase {
        Phase::Stopping { reports, started } => {
            let missing: Vec<&str> = shard
                .asked_brokers
                .iter()
                .filter(|b| !reports.contains_key(*b))
                .map(String::as_str)
                .collect();
            if missing.is_empty() {
                let next = compare(shard, &apply.planned, reports);
                if matches!(next, Phase::Due(_))
                    && let Some(record) = &shard.record
                {
                    starts = plan_starts(record, reports, apply.accepted);
                }
                Some(next)
            } else if now.duration_since(*started) >= ROUND_TIMEOUT {
                Some(ended(
                    "failed",
                    format!(
                        "{} did not stop within {} s; nothing was written, and the copies \
                         that stopped stay stopped until the plan is applied again",
                        missing.join(", "),
                        ROUND_TIMEOUT.as_secs()
                    ),
                ))
            } else {
                None
            }
        }
        Phase::Starting { epoch, started } if shard.asked_brokers.is_subset(started) => {
            Some(Phase::Ended {
                state: "started",
                reason: None,
                epoch: Some(*epoch),
            })
        }
        _ => None,
    };
    if let (Some(next), Some(apply)) = (next, &mut shard.apply) {
        apply.phase = next;
        if let Some(starts) = starts {
            apply.starts = starts;
        }
    }
}

/// Plan again from the post-stop reads. Only a plan equal to the one the
/// operator read is written: nothing is done that the operator did not see.
fn compare(
    shard: &ShardSurvey,
    planned: &PlanView,
    reports: &BTreeMap<String, BrokerReport<String>>,
) -> Phase {
    let Some(record) = &shard.record else {
        return ended("failed", "the shard has no record to re-form from");
    };
    match survey_step(record, reports, &shard.probes) {
        SurveyStep::Planned(now) if &now == planned => {
            let planned_floor = [record.floor.map(|f| f.index), record.offset_floor]
                .into_iter()
                .flatten()
                .max();
            Phase::Due(ReformEpochStart {
                incarnation: shard.incarnation,
                expected_epoch: shard.epoch,
                planned_floor,
                base: RaftEpochBase {
                    base_last: now.base_last.map(|(index, epoch, term)| RaftLogId {
                        index,
                        epoch,
                        term,
                    }),
                    first_index: now.first_index,
                },
                voters: now.voters.iter().cloned().collect(),
                dead: record.dead.clone(),
            })
        }
        SurveyStep::Planned(_) => ended(
            "changed",
            "the plan from the post-stop reads differs from the one applied; plan again",
        ),
        SurveyStep::Leads(broker) => Phase::Ended {
            state: "left_alone",
            reason: Some(format!("broker {broker} leads the current epoch")),
            epoch: None,
        },
        SurveyStep::Blocked(why) => ended("changed", format!("after the stop: {why}")),
        SurveyStep::Ask(_) => ended(
            "changed",
            "the post-stop reads need probes the plan did not read; plan again",
        ),
    }
}

/// Commit every due epoch write and move its shard to the start round.
pub(super) async fn write_due(plans: &ReformPlans, store: &dyn ControlPlaneStore) {
    for due in plans.take_due() {
        let written = match store
            .start_reformed_epoch(&due.key, due.start.clone())
            .await
        {
            Ok(shard) => Ok(shard.group_epoch()),
            // A write committed for a request that was dropped before it
            // answered is refused the second time: the group moved on.
            Err(err) => match store.get_shard_assignment(&due.key).await {
                Ok(shard) if shard.started_by(&due.start) => Ok(shard.group_epoch()),
                _ => Err(err.to_string()),
            },
        };
        if let Err(why) = &written {
            tracing::warn!(shard = ?due.key, error = %why, "a re-form's epoch write was refused");
        }
        due.answer(written);
    }
}

#[cfg(test)]
mod tests;

//! Re-form plans: what a re-form of each leaderless Raft group would do,
//! surveyed from the brokers that hold its copies. Read-only: a plan
//! changes nothing on a broker or in the store.
//!
//! The control plane never calls a broker, so brokers poll for surveys and
//! post their reports. Plans live on the metadata Raft leader, so a poll or
//! answer that lands on any member reaches the plan; a leader change or a
//! restart loses them, and the operator plans again.
//!
//! Applying a plan runs on the same channel: a stop round, a compare, the
//! epoch write and a start round ([`apply`]). `docs/per-shard-raft.md`,
//! "Re-forming a group" and "Applying a plan".
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use felix_storage::raft_copy::{
    BrokerReport, DEFAULT_OFFSET_GAP, GroupRecord, MAX_OFFSET_GAP, MIN_OFFSET_GAP, PlanView,
    RestoreAt, SurveyStep, WireReport, WireStart, first_probes, survey_step,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::model::{ShardAssignment, ShardKey};

use crate::store::{ControlPlaneStore, StoreResult};
pub use apply::ReformApplyView;

/// How long a round waits for every broker before planning without the
/// ones that did not answer, which blocks their shards.
pub(crate) const ROUND_TIMEOUT: Duration = Duration::from_secs(30);

/// Rounds a shard may take. Each only adds probes, so the planner
/// converges in two or three; more means something keeps moving.
const MAX_ROUNDS: u32 = 8;

/// Plans kept at once; the oldest goes first.
const MAX_PLANS: usize = 16;

/// Most surveys or reports in one request.
pub(crate) const MAX_BATCH: usize = 1024;

/// The plans one leader holds in one term.
#[derive(Default)]
pub struct ReformPlans {
    plans: Mutex<Vec<Plan>>,
    /// Per shard, the plan time, id and written epoch of the newest plan
    /// whose epoch write went through. An older plan is not applied after it: its first index
    /// is lower on the time line, below offsets the newer epoch gave out.
    written: Mutex<HashMap<ShardKey, (u64, String, u64)>>,
}

impl ReformPlans {
    /// Start a plan over `shards`, the Raft shards in scope.
    pub(crate) fn create(
        &self,
        scope: ReformScope,
        restore: Option<ReformRestore>,
        shards: &[ShardAssignment],
        dead: &BTreeSet<String>,
        now: Instant,
    ) -> String {
        let id = uuid::Uuid::new_v4().simple().to_string();
        // Stamped once: the post-stop compare re-plans with this restore
        // and must reach the same first index, and apply orders plans by it.
        let planned_at_millis = felix_common::clock::wall_millis();
        let restore = restore.map(|request| PlannedRestore {
            gap: request.offset_gap.unwrap_or(DEFAULT_OFFSET_GAP),
            planned_at_millis,
            request,
        });
        let shards = shards
            .iter()
            .map(|s| ShardSurvey::new(s, dead, restore.as_ref(), now))
            .collect();
        let mut plans = self.plans.lock();
        if plans.len() >= MAX_PLANS {
            plans.remove(0);
        }
        plans.push(Plan {
            id: id.clone(),
            scope,
            restore: restore.map(|planned| ReformRestoreView {
                point: planned.request.point,
                offset_gap: planned.gap,
                taken_at_millis: planned.request.taken_at_millis,
                planned_at_millis: planned.planned_at_millis,
            }),
            planned_at_millis,
            shards,
        });
        id
    }

    pub(crate) fn view(&self, id: &str, now: Instant) -> Option<ReformPlanView> {
        let mut plans = self.plans.lock();
        let plan = plans.iter_mut().find(|p| p.id == id)?;
        plan.shards.iter_mut().for_each(|s| s.settle(now));
        let shards: Vec<ReformShardView> = plan.shards.iter().map(ShardSurvey::view).collect();
        let surveying = shards.iter().any(|s| s.state == "surveying");
        let applied = shards.iter().any(|s| s.apply.is_some());
        let applying = plan
            .shards
            .iter()
            .any(|s| s.apply.as_ref().is_some_and(|a| !a.ended()));
        let state = match (surveying, applied, applying) {
            (true, _, _) => "surveying",
            (_, _, true) => "applying",
            (_, true, false) => "applied",
            _ => "done",
        };
        Some(ReformPlanView {
            plan_id: plan.id.clone(),
            scope: plan.scope.clone(),
            restore: plan.restore.clone(),
            state: state.to_string(),
            shards,
        })
    }

    /// The surveys `node` has not answered in each shard's current round.
    pub(crate) fn surveys_for(&self, node: &str, now: Instant) -> Vec<ReformSurvey> {
        let mut plans = self.plans.lock();
        let mut out = Vec::new();
        for plan in plans.iter_mut() {
            for shard in &mut plan.shards {
                shard.settle(now);
                if let Some(apply) = &shard.apply {
                    if out.len() < MAX_BATCH
                        && let Some(survey) = apply.survey_for(&plan.id, shard, node)
                    {
                        out.push(survey);
                    }
                    continue;
                }
                if shard.outcome.is_none()
                    && shard.asked_brokers.contains(node)
                    && !shard.reports.contains_key(node)
                    && out.len() < MAX_BATCH
                {
                    out.push(ReformSurvey {
                        plan_id: plan.id.clone(),
                        kind: SurveyKind::Survey,
                        round: shard.round,
                        key: shard.key.clone(),
                        incarnation: shard.incarnation,
                        epoch: shard.epoch,
                        probes: shard.probes.clone(),
                        start: None,
                    });
                }
            }
        }
        out
    }

    /// Take `node`'s answers that match a current round. The node is the
    /// authenticated one, so an answer stands only for the broker that
    /// read the copies.
    pub(crate) fn record(
        &self,
        node: &str,
        answers: Vec<ReformAnswer>,
        now: Instant,
    ) -> (usize, usize) {
        let mut plans = self.plans.lock();
        let (mut accepted, mut ignored) = (0, 0);
        for answer in answers {
            let shard = plans
                .iter_mut()
                .find(|p| p.id == answer.plan_id)
                .and_then(|p| p.shards.iter_mut().find(|s| s.key == answer.key));
            let Some(shard) = shard else {
                ignored += 1;
                continue;
            };
            if shard.apply.is_some() {
                if apply::record(shard, node, answer, now) {
                    accepted += 1;
                } else {
                    ignored += 1;
                }
                continue;
            }
            let current = shard.outcome.is_none()
                && answer.kind == SurveyKind::Survey
                && shard.round == answer.round
                && shard.asked_brokers.contains(node)
                && !shard.reports.contains_key(node);
            match (current, BrokerReport::try_from(answer.report)) {
                (true, Ok(report)) => {
                    shard.reports.insert(node.to_string(), report);
                    shard.settle(now);
                    accepted += 1;
                }
                _ => ignored += 1,
            }
        }
        (accepted, ignored)
    }
}

/// What a plan covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReformScope {
    /// Every Raft shard.
    Cluster,
    /// The Raft shards placed on this node.
    Node(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformPlanView {
    pub plan_id: String,
    pub scope: ReformScope,
    /// Set when the plan re-forms from a backup point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore: Option<ReformRestoreView>,
    /// `surveying` until every shard has an answer, then `done`; once
    /// applied, `applying` until every applied shard ended, then `applied`.
    pub state: String,
    pub shards: Vec<ReformShardView>,
}

/// A plan that re-forms each shard from a backup point: the restored copy
/// is the base, cut back to the point, and the next epoch starts at least
/// `offset_gap` past 2^14 times the plan's time in Unix milliseconds.
/// `docs/per-shard-raft.md`, "A restore from a
/// backup point is a re-form".
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformRestore {
    /// The point's name, as `felixctl backup-point` wrote it.
    pub point: String,
    /// The point's barrier in UTC milliseconds (`taken_at_millis` in the
    /// point's manifest), by the clock of the host that took it. Shown, not
    /// used for the first index.
    pub taken_at_millis: u64,
    /// The point's offset for each shard's log.
    pub shards: Vec<RestoreShard>,
    /// How far past the time line the next epoch starts
    /// (`RestoreAt::first_index`); 2^40 when not given, and never above it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_gap: Option<u64>,
    /// Take an `offset_gap` below 2^32, or a plan whose time line plus gap
    /// is below the point's end (a control-plane clock far behind).
    #[serde(default)]
    pub accept_short_gap: bool,
}

impl ReformRestore {
    /// Why the request is refused, if it is.
    pub(crate) fn refused(&self) -> Option<String> {
        match self.offset_gap {
            // A later restore or abandon assumes no epoch started further
            // above the time line than this.
            Some(gap) if gap > MAX_OFFSET_GAP => Some(format!(
                "an offset gap of {gap} is above {MAX_OFFSET_GAP}, the largest a restore takes"
            )),
            Some(gap) if gap < MIN_OFFSET_GAP && !self.accept_short_gap => Some(format!(
                "an offset gap of {gap} is below {MIN_OFFSET_GAP}; accept it with \
                 accept_short_gap"
            )),
            _ => None,
        }
    }
}

/// One shard in a backup point.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RestoreShard {
    pub key: ShardKey,
    /// The incarnation the point read the shard at.
    #[serde(default)]
    pub incarnation: u64,
    /// Where the shard's restored log ends.
    pub end: u64,
}

/// The restore a plan was made for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformRestoreView {
    pub point: String,
    pub offset_gap: u64,
    pub taken_at_millis: u64,
    /// The control plane's wall clock when the plan was made, in Unix
    /// milliseconds. The first index is at least 2^14 times it plus the gap
    /// (`RestoreAt::first_index`).
    pub planned_at_millis: u64,
}

/// A plan's restore, with the gap and the time it was planned at.
struct PlannedRestore {
    request: ReformRestore,
    gap: u64,
    planned_at_millis: u64,
}

/// One shard's line in a plan.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformShardView {
    pub key: ShardKey,
    pub incarnation: u64,
    /// The group epoch the record holds.
    pub epoch: u64,
    /// `surveying`, `reform`, `blocked` or `left_alone`.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub round: u32,
    /// Brokers this round still waits on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waiting_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub plan: Option<PlanView>,
    /// How far `apply` got, once the plan is applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply: Option<ReformApplyView>,
}

/// What a survey asks a broker to do. A broker that predates a kind reads
/// every survey as a read-only `survey` and answers without the kind, so
/// its answer to a stop or start is ignored and the round times out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SurveyKind {
    /// Read the copy, changing nothing.
    #[default]
    Survey,
    /// Stop the member unless it leads the epoch, then read the copy.
    Stop,
    /// Answer once the copy has started the survey's epoch.
    Start,
}

/// One survey a broker is asked to answer.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformSurvey {
    pub plan_id: String,
    #[serde(default)]
    pub kind: SurveyKind,
    pub round: u32,
    pub key: ShardKey,
    pub incarnation: u64,
    /// The group epoch surveyed or stopped; for a `start`, the epoch the
    /// copy is to start.
    pub epoch: u64,
    /// Indexes to read each copy's term at.
    pub probes: BTreeSet<u64>,
    /// For a `start`, this broker's part of the plan. Left out of every
    /// other kind, so those read as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub start: Option<WireStart>,
}

/// One broker's answer to one survey.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformAnswer {
    pub plan_id: String,
    #[serde(default)]
    pub kind: SurveyKind,
    pub round: u32,
    pub key: ShardKey,
    #[schema(value_type = Object)]
    pub report: WireReport,
}

/// A re-form request, answered where the plans live.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ReformRequest {
    Create {
        scope: ReformScope,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        restore: Option<ReformRestore>,
    },
    View {
        plan_id: String,
    },
    Surveys {
        node_id: String,
    },
    Reports {
        node_id: String,
        answers: Vec<ReformAnswer>,
    },
    Apply {
        plan_id: String,
        /// Shards whose plan needs the loss accepted that the operator named.
        #[serde(default)]
        accept_loss: Vec<ShardKey>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReformReply {
    Created {
        plan_id: String,
    },
    /// The plan was not made; the reason says why.
    NotCreated {
        reason: String,
    },
    Plan {
        plan: Option<ReformPlanView>,
    },
    Surveys {
        surveys: Vec<ReformSurvey>,
    },
    Recorded {
        accepted: usize,
        ignored: usize,
    },
    /// `applying` shards started their stop round.
    Applied {
        applying: usize,
    },
    /// The plan cannot be applied; the reason says why.
    NotApplied {
        reason: String,
    },
}

struct Plan {
    id: String,
    scope: ReformScope,
    restore: Option<ReformRestoreView>,
    /// The control plane's wall clock at creation, in Unix milliseconds.
    planned_at_millis: u64,
    shards: Vec<ShardSurvey>,
}

struct ShardSurvey {
    key: ShardKey,
    incarnation: u64,
    epoch: u64,
    /// `None` when the record refused; `outcome` then says why.
    record: Option<GroupRecord<String>>,
    /// Placed brokers not declared dead: the ones a round asks.
    asked_brokers: BTreeSet<String>,
    round: u32,
    probes: BTreeSet<u64>,
    started: Instant,
    reports: BTreeMap<String, BrokerReport<String>>,
    outcome: Option<SurveyStep>,
    apply: Option<apply::ShardApply>,
}

impl ShardSurvey {
    fn new(
        shard: &ShardAssignment,
        dead: &BTreeSet<String>,
        restore: Option<&PlannedRestore>,
        now: Instant,
    ) -> Self {
        let record = shard.reform_record(dead).map_err(|err| err.to_string());
        let record = match restore {
            None => record,
            Some(restore) => record.and_then(|mut record| {
                record.restore = Some(restore_at(restore, shard)?);
                Ok(record)
            }),
        };
        let asked_brokers = shard
            .placed
            .as_ref()
            .map(|placed| placed.brokers.difference(dead).cloned().collect())
            .unwrap_or_default();
        let (record, outcome, probes) = match record {
            Ok(record) => {
                let probes = first_probes(&record);
                (Some(record), None, probes)
            }
            Err(err) => (None, Some(SurveyStep::Blocked(err)), BTreeSet::new()),
        };
        Self {
            key: shard.key.clone(),
            incarnation: shard.incarnation,
            epoch: shard.group_epoch(),
            record,
            asked_brokers,
            round: 1,
            probes,
            started: now,
            reports: BTreeMap::new(),
            outcome,
            apply: None,
        }
    }

    /// Plan once every asked broker answered, or the round timed out.
    fn settle(&mut self, now: Instant) {
        if self.apply.is_some() {
            apply::settle(self, now);
            return;
        }
        let Some(record) = &self.record else { return };
        if self.outcome.is_some() {
            return;
        }
        let all = self
            .asked_brokers
            .iter()
            .all(|b| self.reports.contains_key(b));
        if !all && now.duration_since(self.started) < ROUND_TIMEOUT {
            return;
        }
        match survey_step(record, &self.reports, &self.probes) {
            SurveyStep::Ask(next) if self.round < MAX_ROUNDS => {
                // A new round asks everyone again: an answer read at the
                // old probes is never planned with.
                self.round += 1;
                self.probes = next;
                self.reports.clear();
                self.started = now;
            }
            SurveyStep::Ask(_) => {
                self.outcome = Some(SurveyStep::Blocked(format!(
                    "the survey did not settle in {MAX_ROUNDS} rounds"
                )));
            }
            other => self.outcome = Some(other),
        }
    }

    fn view(&self) -> ReformShardView {
        let (state, reason, plan) = match &self.outcome {
            None => ("surveying", None, None),
            Some(SurveyStep::Planned(plan)) => ("reform", None, Some(plan.clone())),
            Some(SurveyStep::Blocked(why)) => ("blocked", Some(why.clone()), None),
            Some(SurveyStep::Leads(broker)) => (
                "left_alone",
                Some(format!("broker {broker} leads the current epoch")),
                None,
            ),
            Some(SurveyStep::Ask(_)) => ("surveying", None, None),
        };
        let waiting_on = if self.outcome.is_none() {
            self.asked_brokers
                .iter()
                .filter(|b| !self.reports.contains_key(*b))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        ReformShardView {
            key: self.key.clone(),
            incarnation: self.incarnation,
            epoch: self.epoch,
            state: state.to_string(),
            reason,
            round: self.round,
            waiting_on,
            plan,
            apply: self.apply.as_ref().map(|apply| apply.view(self)),
        }
    }
}

/// Where the point puts `shard`, refused unless it names the shard at its
/// incarnation: restoring a shard the point does not hold would cut it to
/// nothing.
fn restore_at(planned: &PlannedRestore, shard: &ShardAssignment) -> Result<RestoreAt, String> {
    let restore = &planned.request;
    let Some(point) = restore.shards.iter().find(|p| p.key == shard.key) else {
        return Err(format!(
            "the backup point {} does not name this shard",
            restore.point
        ));
    };
    if point.incarnation != shard.incarnation {
        return Err(format!(
            "the backup point {} read this shard at incarnation {}; the shard is at {}",
            restore.point, point.incarnation, shard.incarnation
        ));
    }
    let line = RestoreAt {
        end: point.end,
        gap: planned.gap,
        planned_at_millis: planned.planned_at_millis,
    };
    // Below the point's end the gap is silently dropped, so offsets past
    // the point could be reused. Only an explicit accept takes that.
    if !restore.accept_short_gap && line.first_index().is_some_and(|first| first < point.end) {
        tracing::warn!(
            shard = ?shard.key,
            end = point.end,
            planned_at_millis = planned.planned_at_millis,
            "the control plane's clock puts the restore's time line below the point's end"
        );
        return Err(format!(
            "the control plane's clock ({} ms) puts the time line plus gap below the point's              end {}; fix the clock, or accept it with --accept-short-gap",
            planned.planned_at_millis, point.end
        ));
    }
    Ok(RestoreAt {
        end: point.end,
        gap: planned.gap,
        planned_at_millis: planned.planned_at_millis,
    })
}

/// Answer `request` from `plans`, reading shards and dead nodes from
/// `store`. Nothing is written to `store`.
pub(crate) async fn answer(
    plans: &ReformPlans,
    store: &dyn ControlPlaneStore,
    request: ReformRequest,
) -> StoreResult<ReformReply> {
    let now = Instant::now();
    let reply = match request {
        ReformRequest::Create { scope, restore } => {
            if let Some(reason) = restore.as_ref().and_then(ReformRestore::refused) {
                return Ok(ReformReply::NotCreated { reason });
            }
            let shards = in_scope(store.list_shard_assignments().await?, &scope);
            let dead = store.dead_nodes().await?;
            ReformReply::Created {
                plan_id: plans.create(scope, restore, &shards, &dead, now),
            }
        }
        ReformRequest::View { plan_id } => ReformReply::Plan {
            plan: plans.view(&plan_id, now),
        },
        ReformRequest::Surveys { node_id } => ReformReply::Surveys {
            surveys: plans.surveys_for(&node_id, now),
        },
        ReformRequest::Reports { node_id, answers } => {
            let (accepted, ignored) = plans.record(&node_id, answers, now);
            ReformReply::Recorded { accepted, ignored }
        }
        ReformRequest::Apply {
            plan_id,
            accept_loss,
        } => match plans.apply(&plan_id, &accept_loss, now) {
            Ok(applying) => ReformReply::Applied { applying },
            Err(reason) => ReformReply::NotApplied { reason },
        },
    };
    // Any request can be the one that finished a stop round, so each
    // writes the epochs it made due.
    apply::write_due(plans, store).await;
    Ok(reply)
}

/// The Raft shards `scope` covers: stream and cache shards with a record of
/// the brokers placed in their epoch, not switched from the hybrid. A cache
/// shard on a Raft cluster runs a group too, and loses its majority the same
/// way (#2978).
fn in_scope(shards: Vec<ShardAssignment>, scope: &ReformScope) -> Vec<ShardAssignment> {
    shards
        .into_iter()
        .filter(|s| !s.switched())
        .filter(|s| match (&s.placed, scope) {
            (None, _) => false,
            (Some(_), ReformScope::Cluster) => true,
            (Some(placed), ReformScope::Node(node)) => placed.brokers.contains(node),
        })
        .collect()
}

mod apply;
#[cfg(test)]
pub(crate) mod tests;

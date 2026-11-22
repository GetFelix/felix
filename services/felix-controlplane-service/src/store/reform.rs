//! Re-form plans: what a re-form of each leaderless Raft group would do,
//! surveyed from the brokers that hold its copies. Read-only: a plan
//! changes nothing on a broker or in the store.
//!
//! The control plane never calls a broker, so brokers poll for surveys and
//! post their reports. Plans live on the metadata Raft leader, so a poll or
//! answer that lands on any member reaches the plan; a leader change or a
//! restart loses them, and the operator plans again.
//! `docs/per-shard-raft.md`, "Re-forming a group".
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use felix_storage::raft_copy::{
    BrokerReport, GroupRecord, PlanView, SurveyStep, WireReport, first_probes, survey_step,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::model::{ShardAssignment, ShardKey, ShardKind};
use crate::store::{ControlPlaneStore, StoreResult};

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
}

impl ReformPlans {
    /// Start a plan over `shards`, the Raft shards in scope.
    pub(crate) fn create(
        &self,
        scope: ReformScope,
        shards: &[ShardAssignment],
        dead: &BTreeSet<String>,
        now: Instant,
    ) -> String {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let shards = shards
            .iter()
            .map(|s| ShardSurvey::new(s, dead, now))
            .collect();
        let mut plans = self.plans.lock();
        if plans.len() >= MAX_PLANS {
            plans.remove(0);
        }
        plans.push(Plan {
            id: id.clone(),
            scope,
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
        Some(ReformPlanView {
            plan_id: plan.id.clone(),
            scope: plan.scope.clone(),
            state: if surveying { "surveying" } else { "done" }.to_string(),
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
                if shard.outcome.is_none()
                    && shard.asked_brokers.contains(node)
                    && !shard.reports.contains_key(node)
                    && out.len() < MAX_BATCH
                {
                    out.push(ReformSurvey {
                        plan_id: plan.id.clone(),
                        round: shard.round,
                        key: shard.key.clone(),
                        incarnation: shard.incarnation,
                        epoch: shard.epoch,
                        probes: shard.probes.clone(),
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
            let current = shard.outcome.is_none()
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
    /// `surveying` until every shard has an answer, then `done`.
    pub state: String,
    pub shards: Vec<ReformShardView>,
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
}

/// One survey a broker is asked to answer.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformSurvey {
    pub plan_id: String,
    pub round: u32,
    pub key: ShardKey,
    pub incarnation: u64,
    pub epoch: u64,
    /// Indexes to read each copy's term at.
    pub probes: BTreeSet<u64>,
}

/// One broker's answer to one survey.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReformAnswer {
    pub plan_id: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReformReply {
    Created { plan_id: String },
    Plan { plan: Option<ReformPlanView> },
    Surveys { surveys: Vec<ReformSurvey> },
    Recorded { accepted: usize, ignored: usize },
}

struct Plan {
    id: String,
    scope: ReformScope,
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
}

impl ShardSurvey {
    fn new(shard: &ShardAssignment, dead: &BTreeSet<String>, now: Instant) -> Self {
        let record = shard.reform_record(dead);
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
            Err(err) => (
                None,
                Some(SurveyStep::Blocked(err.to_string())),
                BTreeSet::new(),
            ),
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
        }
    }

    /// Plan once every asked broker answered, or the round timed out.
    fn settle(&mut self, now: Instant) {
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
        }
    }
}

/// Answer `request` from `plans`, reading shards and dead nodes from
/// `store`. Nothing is written to `store`.
pub(crate) async fn answer(
    plans: &ReformPlans,
    store: &dyn ControlPlaneStore,
    request: ReformRequest,
) -> StoreResult<ReformReply> {
    let now = Instant::now();
    Ok(match request {
        ReformRequest::Create { scope } => {
            let shards = in_scope(store.list_shard_assignments().await?, &scope);
            let dead = store.dead_nodes().await?;
            ReformReply::Created {
                plan_id: plans.create(scope, &shards, &dead, now),
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
    })
}

/// The Raft shards `scope` covers: stream shards with a record of the
/// brokers placed in their epoch, not switched from the hybrid.
fn in_scope(shards: Vec<ShardAssignment>, scope: &ReformScope) -> Vec<ShardAssignment> {
    shards
        .into_iter()
        .filter(|s| s.key.kind == ShardKind::Stream && !s.switched())
        .filter(|s| match (&s.placed, scope) {
            (None, _) => false,
            (Some(_), ReformScope::Cluster) => true,
            (Some(placed), ReformScope::Node(node)) => placed.brokers.contains(node),
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests;

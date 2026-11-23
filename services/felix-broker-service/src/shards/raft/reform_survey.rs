//! Re-form surveys: the control plane's questions about this broker's Raft
//! copies, polled and answered here because the control plane never calls a
//! broker (`docs/per-shard-raft.md`, "Re-forming a group").
//!
//! A `survey` is read-only: a copy is read from its directory without
//! opening, claiming or stopping it. A `stop`, sent once an operator applies
//! a plan, stops the member unless it leads and answers with a read taken
//! after the stop; a `start` is answered once the copy started the new
//! epoch ("Applying a plan"). Each answer carries the plan id and
//! round it was read for, so the control plane never plans with a read made
//! at older probes. A survey this broker cannot read is left unanswered,
//! which blocks the shard once its round times out.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use felix_replication::controlplane_link::ControlPlaneLink;
use felix_replication::reporter::Credential;
use felix_storage::raft_copy::{WireReport, WireStart};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{RaftShards, ShardKey, ShardKind};

/// How often the control plane is asked for surveys.
pub(crate) const INTERVAL: Duration = Duration::from_secs(2);

/// How long a poll or an answer may take before the tick gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// What a survey asks. A kind this build does not know is skipped, never
/// read as a plain survey.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SurveyKind {
    #[default]
    Survey,
    Stop,
    Start,
    #[serde(other)]
    Unknown,
}

/// One survey, as the control plane asks it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Survey {
    pub(crate) plan_id: String,
    #[serde(default)]
    pub(crate) kind: SurveyKind,
    pub(crate) round: u32,
    pub(crate) key: ShardKey,
    pub(crate) incarnation: u64,
    pub(crate) epoch: u64,
    pub(crate) probes: BTreeSet<u64>,
    /// For a `start`, this broker's part of the plan.
    #[serde(default)]
    pub(crate) start: Option<WireStart>,
}

/// This broker's part of an applied plan for one shard, from a `start`
/// survey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedStart {
    pub(crate) incarnation: u64,
    /// The epoch the start begins.
    pub(crate) epoch: u64,
    pub(crate) start: WireStart,
}

#[derive(Debug, Deserialize)]
struct Surveys {
    surveys: Vec<Survey>,
}

#[derive(Debug, Serialize)]
struct Answer<'a> {
    plan_id: &'a str,
    kind: SurveyKind,
    round: u32,
    key: &'a ShardKey,
    report: WireReport,
}

/// Reads this broker's report for a survey; `None` when it cannot speak
/// for the shard. Blocking: run off the async threads.
pub(crate) type Read = Arc<dyn Fn(&Survey) -> Option<WireReport> + Send + Sync>;

/// Stops this broker's copy for a `stop` survey and reads it after; `None`
/// when it cannot speak for the shard.
pub(crate) type Stop =
    Arc<dyn Fn(Survey) -> Pin<Box<dyn Future<Output = Option<WireReport>> + Send>> + Send + Sync>;

/// One tick: fetch the surveys owed and post every answer read, in one
/// request each way.
pub(crate) async fn tick<L: ControlPlaneLink>(
    link: &L,
    node_id: &str,
    bearer: Option<&str>,
    read: Read,
    stop: Stop,
) {
    let asked =
        match tokio::time::timeout(REQUEST_TIMEOUT, link.reform_surveys(bearer, node_id)).await {
            // A control plane that predates surveys answers 404: nothing to do.
            Ok(Ok(answer)) if answer.status == reqwest::StatusCode::OK => {
                match answer.json::<Surveys>() {
                    Ok(asked) => asked.surveys,
                    Err(err) => {
                        tracing::debug!(error = %err, "unreadable re-form surveys");
                        return;
                    }
                }
            }
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => return,
        };
    let (stops, reads): (Vec<Survey>, Vec<Survey>) = asked
        .into_iter()
        .filter(|survey| survey.kind != SurveyKind::Unknown)
        .partition(|survey| survey.kind == SurveyKind::Stop);
    if stops.is_empty() && reads.is_empty() {
        return;
    }
    let mut answers = Vec::new();
    // One at a time: each stop closes a member and syncs a marker.
    for survey in stops {
        if let Some(report) = stop(survey.clone()).await {
            answers.push((survey, report));
        }
    }
    if let Ok(read) = tokio::task::spawn_blocking(move || {
        reads
            .into_iter()
            .filter_map(|survey| read(&survey).map(|report| (survey, report)))
            .collect::<Vec<_>>()
    })
    .await
    {
        answers.extend(read);
    }
    if answers.is_empty() {
        return;
    }
    let reports: Vec<Answer<'_>> = answers
        .iter()
        .map(|(survey, report)| Answer {
            plan_id: &survey.plan_id,
            kind: survey.kind,
            round: survey.round,
            key: &survey.key,
            report: report.clone(),
        })
        .collect();
    let body = serde_json::json!({ "reports": reports });
    match tokio::time::timeout(REQUEST_TIMEOUT, link.reform_reports(bearer, node_id, &body)).await {
        Ok(Ok(answer)) if answer.status == reqwest::StatusCode::OK => {}
        // The survey is asked again on the next tick while its round lasts.
        Ok(Ok(answer)) => tracing::debug!(
            status = %answer.status,
            "the control plane did not take the re-form reports"
        ),
        Ok(Err(err)) => tracing::debug!(error = %err, "could not send the re-form reports"),
        Err(_) => tracing::debug!("the re-form report timed out"),
    }
}

impl RaftShards {
    /// How this broker answers surveys: its copy of each surveyed stream or
    /// cache shard, read from disk at the survey's probes, and whether its member
    /// leads the group at the surveyed incarnation and epoch.
    pub(crate) fn reform_reader(&self) -> Read {
        let broker = Arc::clone(&self.broker);
        let led: BTreeSet<(ShardKey, u64, u64)> = self
            .led_floors()
            .into_iter()
            .map(|led| (led.key, led.incarnation, led.epoch))
            .collect();
        let starts = Arc::clone(&self.reform_starts);
        Arc::new(move |survey: &Survey| {
            if survey.kind == SurveyKind::Start
                && let Some(start) = &survey.start
            {
                starts.lock().insert(
                    survey.key.clone(),
                    PlannedStart {
                        incarnation: survey.incarnation,
                        epoch: survey.epoch,
                        start: start.clone(),
                    },
                );
            }
            let dir = reform_dir(&broker, &survey.key, survey.incarnation)?;
            // A start is answered once the copy holds the new epoch, which
            // the start writes before the member opens.
            if survey.kind == SurveyKind::Start
                && felix_storage::raft_copy::group_epoch(&dir).ok().flatten() != Some(survey.epoch)
            {
                return None;
            }
            let leads = led.contains(&(survey.key.clone(), survey.incarnation, survey.epoch));
            read_report(&broker, &dir, survey, leads)
        })
    }

    /// How this broker answers `stop` surveys: [`Self::stop_for_reform`].
    pub(crate) fn reform_stopper(&self) -> Stop {
        let me = self.me.clone();
        Arc::new(move |survey: Survey| {
            let me = me.clone();
            Box::pin(async move { me.upgrade()?.stop_for_reform(&survey).await })
        })
    }

    /// Stop this broker's copy of the surveyed shard in the surveyed epoch,
    /// then answer with a read taken after the stop. A member that leads
    /// the epoch is left running and answers that it leads, which leaves
    /// the group alone. The `open` lock is held throughout, so no feed pass
    /// opens the member between the close and the marker; once the marker
    /// is synced an open is refused ([`OpenError::Stopped`]).
    ///
    /// [`OpenError::Stopped`]: felix_replication::raft::OpenError::Stopped
    pub(crate) async fn stop_for_reform(&self, survey: &Survey) -> Option<WireReport> {
        let key = &survey.key;
        let mut open = self.open.lock().await;
        let dir = reform_dir(&self.broker, key, survey.incarnation)?;
        let epoch = felix_storage::raft_copy::group_epoch(&dir)
            .ok()?
            .unwrap_or(0);
        if epoch != survey.epoch {
            // Not the epoch asked about: stopping it could stop a group a
            // re-form already started.
            return None;
        }
        let member_here = open
            .get(key)
            .is_some_and(|member| member.incarnation == survey.incarnation);
        if member_here {
            if self.runtime.led_term(key).is_some() {
                return read_report(&self.broker, &dir, survey, true);
            }
            let member = open.remove(key)?;
            self.close_member(key, member).await;
            if key.kind == ShardKind::Stream
                && let Err(err) = self
                    .broker
                    .close_stream_shard(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        None,
                    )
                    .await
            {
                tracing::warn!(shard = ?key, error = %err, "could not close a stopped Raft shard's stream");
            }
        }
        let spacing = self.broker.durable_storage()?.config().index_spacing_bytes;
        let stopped = {
            let dir = dir.clone();
            tokio::task::spawn_blocking(move || {
                let read = felix_replication::raft::reform_report(
                    &dir,
                    spacing,
                    &BTreeSet::new(),
                    epoch,
                    false,
                )?;
                // `(term, index)` of the copy's last entry.
                let last = WireReport::from(&read).current.last.map(|(_, index)| index);
                felix_storage::raft_copy::stop(&dir, last)
            })
            .await
        };
        match stopped {
            Ok(Ok(())) => {
                tracing::info!(shard = ?key, epoch, "stopped a Raft copy for a re-form");
                drop(open);
                read_report(&self.broker, &dir, survey, false)
            }
            Ok(Err(err)) => {
                tracing::warn!(shard = ?key, error = %err, "could not stop a Raft copy for a re-form");
                None
            }
            Err(_) => None,
        }
    }
}

/// The directory of `key`'s copy at `incarnation`. A cache shard's copy
/// lives under the cache store's root, as `open_member` claims it.
fn reform_dir(broker: &felix_broker::Broker, key: &ShardKey, incarnation: u64) -> Option<PathBuf> {
    let (tenant, namespace, name, shard) = (&key.tenant_id, &key.namespace, &key.stream, key.shard);
    match key.kind {
        ShardKind::Stream => broker
            .durable_storage()?
            .raft_dir(tenant, namespace, name, shard, incarnation)
            .ok(),
        ShardKind::Cache => broker
            .cache()
            .as_log_cache()?
            .raft_dir(tenant, namespace, name, shard, incarnation)
            .ok(),
    }
    .flatten()
}

/// Read `dir`'s report at the survey's probes.
fn read_report(
    broker: &felix_broker::Broker,
    dir: &std::path::Path,
    survey: &Survey,
    leads: bool,
) -> Option<WireReport> {
    let spacing = broker.durable_storage()?.config().index_spacing_bytes;
    match felix_replication::raft::reform_report(dir, spacing, &survey.probes, survey.epoch, leads)
    {
        Ok(report) => Some(WireReport::from(&report)),
        Err(err) => {
            tracing::warn!(error = %err, shard = ?survey.key, "could not read the copy a re-form survey asks about");
            None
        }
    }
}

/// Answer the control plane's re-form surveys every [`INTERVAL`] until
/// `stop`.
pub(crate) async fn run<L: ControlPlaneLink>(
    shards: Arc<RaftShards>,
    link: L,
    node_id: String,
    credential: Option<Arc<dyn Credential>>,
    stop: CancellationToken,
) {
    let mut every = tokio::time::interval(INTERVAL);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = every.tick() => {}
        }
        let bearer = credential.as_ref().map(|credential| credential.bearer());
        let answer = tick(
            &link,
            &node_id,
            bearer.as_deref().map(String::as_str),
            shards.reform_reader(),
            shards.reform_stopper(),
        );
        tokio::select! {
            () = stop.cancelled() => return,
            () = answer => {}
        }
    }
}

#[cfg(test)]
mod tests;

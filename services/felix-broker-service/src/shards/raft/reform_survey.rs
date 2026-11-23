//! Re-form surveys: the control plane's questions about this broker's Raft
//! copies, polled and answered here because the control plane never calls a
//! broker (`docs/per-shard-raft.md`, "Re-forming a group").
//!
//! Answering is read-only: a copy is read from its directory without
//! opening, claiming or stopping it. Each answer carries the plan id and
//! round it was read for, so the control plane never plans with a read made
//! at older probes. A survey this broker cannot read is left unanswered,
//! which blocks the shard once its round times out.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use felix_replication::controlplane_link::ControlPlaneLink;
use felix_replication::reporter::Credential;
use felix_storage::raft_copy::WireReport;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{RaftShards, ShardKey, ShardKind};

/// How often the control plane is asked for surveys.
pub(crate) const INTERVAL: Duration = Duration::from_secs(2);

/// How long a poll or an answer may take before the tick gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// One survey, as the control plane asks it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Survey {
    pub(crate) plan_id: String,
    pub(crate) round: u32,
    pub(crate) key: ShardKey,
    pub(crate) incarnation: u64,
    pub(crate) epoch: u64,
    pub(crate) probes: BTreeSet<u64>,
}

#[derive(Debug, Deserialize)]
struct Surveys {
    surveys: Vec<Survey>,
}

#[derive(Debug, Serialize)]
struct Answer<'a> {
    plan_id: &'a str,
    round: u32,
    key: &'a ShardKey,
    report: WireReport,
}

/// Reads this broker's report for a survey; `None` when it cannot speak
/// for the shard. Blocking: run off the async threads.
pub(crate) type Read = Arc<dyn Fn(&Survey) -> Option<WireReport> + Send + Sync>;

/// One tick: fetch the surveys owed and post every answer read, in one
/// request each way.
pub(crate) async fn tick<L: ControlPlaneLink>(
    link: &L,
    node_id: &str,
    bearer: Option<&str>,
    read: Read,
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
    if asked.is_empty() {
        return;
    }
    let read = tokio::task::spawn_blocking(move || {
        asked
            .into_iter()
            .filter_map(|survey| read(&survey).map(|report| (survey, report)))
            .collect::<Vec<_>>()
    })
    .await;
    let answers = match read {
        Ok(answers) if !answers.is_empty() => answers,
        _ => return,
    };
    let reports: Vec<Answer<'_>> = answers
        .iter()
        .map(|(survey, report)| Answer {
            plan_id: &survey.plan_id,
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
        Arc::new(move |survey: &Survey| {
            let storage = broker.durable_storage()?;
            let key = &survey.key;
            // A cache shard's copy lives under the cache store's root, as
            // `open_member` claims it.
            let dir = match key.kind {
                ShardKind::Stream => storage
                    .raft_dir(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        survey.incarnation,
                    )
                    .ok(),
                ShardKind::Cache => broker
                    .cache()
                    .as_log_cache()?
                    .raft_dir(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        survey.incarnation,
                    )
                    .ok(),
            }
            .flatten()?;
            let leads = led.contains(&(key.clone(), survey.incarnation, survey.epoch));
            let spacing = storage.config().index_spacing_bytes;
            match felix_replication::raft::reform_report(
                &dir,
                spacing,
                &survey.probes,
                survey.epoch,
                leads,
            ) {
                Ok(report) => Some(WireReport::from(&report)),
                Err(err) => {
                    tracing::warn!(error = %err, shard = ?key, "could not read the copy a re-form survey asks about");
                    None
                }
            }
        })
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
        );
        tokio::select! {
            () = stop.cancelled() => return,
            () = answer => {}
        }
    }
}

#[cfg(test)]
mod tests;

//! Replica reports, batched the way the storage layer batches fsyncs.
//!
//! A report has to reach the control plane before the shard's quorum mark
//! moves, so it sits on the path of every `Quorum` publish. One POST per shard
//! per pass meant a broker leading many shards spent that path on round trips
//! that differ only in which shard they name — and the endpoint has always
//! taken a list, so they were single-element batches by habit rather than by
//! need.
//!
//! **Group commit, not a timer.** A window would add its own wait to a pass
//! that has only one shard to report, which is the common small deployment and
//! the one least able to spare it. Instead a flush takes everything queued at
//! that moment and sends it; reports arriving while that request is in flight
//! queue behind it and go together in the next one. Batches grow under load,
//! which is when they are worth having, and an idle broker waits for nothing.
//! `disk_log/sync.rs` makes the same trade for the same reason.
use felix_common::membership::{
    ReplicaOffset, ReplicaStatusRequest, ReplicaStatusResponse, ShardKind as WireShardKind,
    ShardReplicaStatus,
};
use felix_router::ShardKey;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{FollowerCursor, caught_up};

/// Most reports in one request.
///
/// The bound is the control plane's request size, not the broker's appetite: a
/// pass ships at most `SHARD_CONCURRENCY` shards at once, so this is only
/// reached when passes overlap or a broker leads a great many shards.
const MAX_BATCH: usize = 256;

/// How many reports may wait for a flush before submitting blocks.
///
/// Blocking is the right answer when it is reached — the caller is a shard
/// waiting to publish its mark, and dropping its report would release a
/// `Quorum` publish on a report nobody sent.
const QUEUE_DEPTH: usize = 1024;

struct Pending {
    report: ShardReport,
    landed: oneshot::Sender<bool>,
}

/// Submits replica reports and tells each caller whether its own landed.
#[derive(Clone)]
pub struct Reporter {
    tx: mpsc::Sender<Pending>,
}

impl Reporter {
    /// Start the flushing task. It runs until `shutdown`, or until every
    /// `Reporter` handle is dropped.
    pub fn spawn(to: ReportTo, shutdown: CancellationToken) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(QUEUE_DEPTH);
        let task = tokio::spawn(flush_loop(to, rx, shutdown));
        (Self { tx }, task)
    }

    /// Submit `report` and wait until it has reached the control plane.
    ///
    /// `false` means it did not, and the caller must leave its quorum mark
    /// where it was: releasing a publish on a report the control plane never
    /// saw is the window the report-before-mark ordering exists to close.
    pub async fn send(&self, report: ShardReport) -> bool {
        let (landed, answer) = oneshot::channel();
        if self.tx.send(Pending { report, landed }).await.is_err() {
            // The flushing task is gone, which means the broker is shutting
            // down. Nothing will carry this report, so it did not land.
            return false;
        }
        answer.await.unwrap_or(false)
    }

    /// Submit `report` without waiting for it. Dropped if the queue is full:
    /// the next pass sends a fresher one, and no mark waits on this.
    pub(crate) fn submit(&self, report: ShardReport) {
        let (landed, _) = oneshot::channel();
        let _ = self.tx.try_send(Pending { report, landed });
    }
}

/// Where a leader sends its replica reports.
///
/// Optional: a broker with no control plane has nobody to tell, and the reports
/// are only ever read by one.
pub struct ReportTo {
    pub client: reqwest::Client,
    pub base_url: String,
    pub node_id: String,
    /// Held, not copied: this reports for the life of the process, across
    /// however many access tokens that spans.
    pub token: Option<std::sync::Arc<dyn Credential>>,
    pub incarnation: u64,
}

/// The token a report is sent with.
///
/// Read on every request rather than copied, because the broker refreshes it
/// for the life of the process.
pub trait Credential: Send + Sync {
    fn bearer(&self) -> std::sync::Arc<String>;
}

/// One shard's replicas, as this leader currently sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardReport {
    pub key: ShardKey,
    pub generation: u64,
    pub caught_up: Vec<String>,
    /// How far each follower had got. Sent as well as `caught_up` because
    /// "caught up" is only true of the tail it was measured against, and a
    /// leader that reports and then writes more before dying leaves a report
    /// that says every replica was level without saying level with what.
    pub offsets: Vec<(String, u64)>,
    /// This leader's tail, which `offsets` are measured against.
    pub tail: u64,
    /// This broker has stopped serving the shard and its log will not grow,
    /// so `caught_up` is measured against the final tail.
    pub drained: bool,
}

/// Who could take this shard over, as of `tail`.
///
/// A follower is offered when it holds everything below `acknowledged`: the
/// records a client may already have been told are stored.
pub(super) fn shard_report(
    key: &ShardKey,
    generation: u64,
    tail: u64,
    acknowledged: u64,
    followers: &[FollowerCursor],
    drained: bool,
) -> ShardReport {
    ShardReport {
        key: key.clone(),
        generation,
        caught_up: caught_up(acknowledged, followers),
        // Only followers whose position is moving: placement fences a move
        // on how far behind its destination is, and one it cannot reach is
        // not going to close any gap.
        offsets: followers
            .iter()
            .filter(|follower| follower.halted.is_none() && !follower.stalled)
            .map(|follower| (follower.node_id.clone(), follower.next_offset))
            .collect(),
        tail,
        drained,
    }
}

async fn flush_loop(to: ReportTo, mut rx: mpsc::Receiver<Pending>, shutdown: CancellationToken) {
    loop {
        let first = tokio::select! {
            _ = shutdown.cancelled() => break,
            received = rx.recv() => match received {
                Some(pending) => pending,
                // Every handle dropped.
                None => break,
            },
        };

        // Everything already waiting joins this request. Nothing is waited
        // *for*: what queues while the request is in flight goes in the next.
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(pending) => batch.push(pending),
                Err(_) => break,
            }
        }

        metrics::histogram!(super::metrics::REPORTS_PER_REQUEST).record(batch.len() as f64);
        let reports: Vec<ShardReport> =
            batch.iter().map(|pending| pending.report.clone()).collect();
        let landed = send_reports(&to, &reports).await;

        // Each caller hears about its own shard: a report the control plane
        // refused must not move that shard's mark, whatever happened to the
        // rest of the batch.
        for (pending, landed) in batch.into_iter().zip(landed) {
            let _ = pending.landed.send(landed);
        }
    }

    // Anything still queued will never be sent, and a caller blocked on an
    // answer must not wait out its publish timeout for one that is not coming.
    rx.close();
    while let Ok(pending) = rx.try_recv() {
        let _ = pending.landed.send(false);
    }
}

/// Tell the control plane which replicas could take each shard over, and say
/// for each whether it stored the report.
///
/// Not retried here: the next pass sends a fresher one, and a queue of stale
/// reports is worse than none, since promotion is gated on *recent* positions.
/// The answer instead gates the quorum mark, so "could not tell" reads as
/// false — see the caller.
async fn send_reports(to: &ReportTo, reports: &[ShardReport]) -> Vec<bool> {
    if reports.is_empty() {
        return Vec::new();
    }
    let refused = || vec![false; reports.len()];
    // The shared type, not a `json!` literal: the control plane parses this
    // same definition, so a field renamed on one side stops compiling instead
    // of quietly arriving as a missing one.
    let body = ReplicaStatusRequest {
        incarnation: to.incarnation,
        shards: reports
            .iter()
            .map(|report| ShardReplicaStatus {
                tenant_id: report.key.tenant_id.clone(),
                namespace: report.key.namespace.clone(),
                stream: report.key.stream.clone(),
                shard: report.key.shard,
                // Without the kind the control plane files a cache's report
                // under the stream of the same name, so placement finds no
                // caught-up replica for the cache and its shard is never
                // promoted -- the contents are unreachable after a failover.
                kind: wire_kind(report.key.kind),
                generation: report.generation,
                caught_up: report.caught_up.to_vec(),
                drained: report.drained,
                leader_offset: Some(report.tail),
                replica_offsets: report
                    .offsets
                    .iter()
                    .map(|(node_id, durable_offset)| ReplicaOffset {
                        node_id: node_id.clone(),
                        durable_offset: *durable_offset,
                    })
                    .collect(),
            })
            .collect(),
    };
    let url = format!("{}/v1/nodes/{}/replica-status", to.base_url, to.node_id);
    let mut request = to.client.post(&url).json(&body);
    if let Some(token) = &to.token {
        request = request.bearer_auth(token.bearer());
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(err) => {
            tracing::warn!(error = %err, "could not send a replica report");
            return refused();
        }
    };
    let status = response.status();
    // A control plane that predates per-shard answers says 204 and nothing
    // else. It stored what it accepted and skipped the rest without saying,
    // which is all this broker can go on; see "Replica reports" in
    // `docs/replication-design.md` for what that costs during an upgrade.
    if status == reqwest::StatusCode::NO_CONTENT {
        return vec![true; reports.len()];
    }
    if status != reqwest::StatusCode::OK && status != reqwest::StatusCode::CONFLICT {
        tracing::warn!(%status, "the control plane refused a replica report");
        return refused();
    }
    match response.json::<ReplicaStatusResponse>().await {
        Ok(answer) => landed_per_shard(reports, &answer),
        Err(err) => {
            tracing::warn!(%status, error = %err, "unreadable answer to a replica report");
            refused()
        }
    }
}

/// Which of `reports` the control plane says it stored.
///
/// Matched by position and checked by key: an answer that does not line up
/// with what was sent is not evidence that anything landed.
fn landed_per_shard(reports: &[ShardReport], answer: &ReplicaStatusResponse) -> Vec<bool> {
    if answer.shards.len() != reports.len() {
        tracing::warn!(
            sent = reports.len(),
            answered = answer.shards.len(),
            "the control plane answered a different number of replica reports than were sent",
        );
        return vec![false; reports.len()];
    }
    reports
        .iter()
        .zip(&answer.shards)
        .map(|(report, outcome)| {
            let same = outcome.tenant_id == report.key.tenant_id
                && outcome.namespace == report.key.namespace
                && outcome.stream == report.key.stream
                && outcome.shard == report.key.shard
                && outcome.kind == wire_kind(report.key.kind)
                && outcome.generation == report.generation;
            if same && !outcome.outcome.accepted() {
                tracing::warn!(
                    stream = %report.key.stream,
                    shard = report.key.shard,
                    generation = report.generation,
                    outcome = ?outcome.outcome,
                    "the control plane did not store a replica report",
                );
            }
            same && outcome.outcome.accepted()
        })
        .collect()
}

fn wire_kind(kind: felix_router::ShardKind) -> WireShardKind {
    match kind {
        felix_router::ShardKind::Cache => WireShardKind::Cache,
        felix_router::ShardKind::Stream => WireShardKind::Stream,
    }
}

#[cfg(test)]
mod tests;

//! Confirmed reads of one shard's assignment, for a copy with no promise on
//! disk. See [`felix_replication::confirmed`].
use std::time::Duration;

use felix_replication::confirmed::{Confirmed, ConfirmedRoutes};
use serde::Deserialize;

use crate::cluster::credential::NodeCredential;
use crate::shards::watch::ShardAssignment;
use felix_router::{ShardKey, ShardKind};

/// How long a fence waits on the control plane. The leader retries a
/// refusal, so a slow read costs a round, not the shard.
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// At most one warning per this long that the control plane serves no
/// confirmed read.
const NOT_SERVED_WARN_EVERY: Duration = Duration::from_secs(60);

/// Asks the control plane over its REST API.
pub(crate) struct ControlPlaneRoutes {
    client: reqwest::Client,
    base_url: String,
    bearer: Option<NodeCredential>,
    /// When the last not-served warning was logged.
    warned: parking_lot::Mutex<Option<tokio::time::Instant>>,
}

#[derive(Deserialize)]
struct Response {
    assignment: Option<ShardAssignment>,
}

impl ControlPlaneRoutes {
    pub(crate) fn new(
        client: reqwest::Client,
        base_url: &str,
        bearer: Option<NodeCredential>,
    ) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            bearer,
            warned: parking_lot::Mutex::new(None),
        }
    }

    /// Count the outcome, and say so when the check is off: without a
    /// confirmed read a lagging view can still back a deposed leader.
    fn record(&self, key: &ShardKey, confirmed: &Confirmed) {
        let outcome = match confirmed {
            Confirmed::Assigned { .. } => "assigned",
            Confirmed::Unassigned => "unassigned",
            Confirmed::NotServed => "not_served",
            Confirmed::Unavailable => "unavailable",
        };
        crate::shards::watch::metrics::record_confirmed_read(outcome);
        if *confirmed != Confirmed::NotServed {
            return;
        }
        let now = tokio::time::Instant::now();
        let mut warned = self.warned.lock();
        if warned.is_some_and(|at| now.duration_since(at) < NOT_SERVED_WARN_EVERY) {
            return;
        }
        *warned = Some(now);
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            "the control plane serves no confirmed shard assignment read; a copy with no \
             promise is checked against the routing view alone, which can lag",
        );
    }
}

#[async_trait::async_trait]
impl ConfirmedRoutes for ControlPlaneRoutes {
    async fn confirmed(&self, key: &ShardKey) -> Confirmed {
        let kind = match key.kind {
            ShardKind::Stream => "stream",
            ShardKind::Cache => "cache",
        };
        let shard = key.shard.to_string();
        let mut request = self
            .client
            .get(format!("{}/v1/shard-assignments/confirmed", self.base_url))
            .query(&[
                ("tenant_id", key.tenant_id.as_str()),
                ("namespace", key.namespace.as_str()),
                ("stream", key.stream.as_str()),
                ("shard", shard.as_str()),
                ("kind", kind),
            ])
            .timeout(READ_TIMEOUT);
        if let Some(bearer) = &self.bearer {
            request = request.bearer_auth(bearer.bearer().as_str());
        }
        let confirmed = match request.send().await {
            Ok(response) => answer(response.status(), response.json::<Response>().await.ok()),
            Err(err) => {
                tracing::debug!(error = %err, "confirmed shard assignment read failed");
                Confirmed::Unavailable
            }
        };
        self.record(key, &confirmed);
        confirmed
    }
}

/// Only a control plane that says it has no such read leaves the view to
/// answer alone: 404 from a build without the route, 501 from a Raft leader
/// that predates it. Anything else unreadable is unavailable.
fn answer(status: reqwest::StatusCode, body: Option<Response>) -> Confirmed {
    match status {
        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::NOT_IMPLEMENTED => {
            Confirmed::NotServed
        }
        status if status.is_success() => match body {
            Some(Response {
                assignment: Some(assignment),
            }) => Confirmed::Assigned {
                generation: assignment.generation,
                leader: assignment.leader,
            },
            Some(Response { assignment: None }) => Confirmed::Unassigned,
            None => Confirmed::Unavailable,
        },
        _ => Confirmed::Unavailable,
    }
}

#[cfg(test)]
mod tests;

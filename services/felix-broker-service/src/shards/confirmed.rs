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

/// Asks the control plane over its REST API.
pub(crate) struct ControlPlaneRoutes {
    client: reqwest::Client,
    base_url: String,
    bearer: Option<NodeCredential>,
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
        }
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
        let response = match request.send().await {
            Ok(response) => response,
            Err(err) => {
                tracing::debug!(error = %err, "confirmed shard assignment read failed");
                return Confirmed::Unavailable;
            }
        };
        answer(response.status(), response.json::<Response>().await.ok())
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

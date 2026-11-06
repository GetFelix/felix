//! Confirmed reads of one shard's assignment, for a copy with no promise on
//! disk. See [`felix_replication::confirmed`].
use std::time::Duration;

use felix_replication::confirmed::{Confirmed, ConfirmedRoutes};
use felix_replication::controlplane_link::{ControlPlaneLink, LinkResponse};
use serde::Deserialize;

use crate::cluster::credential::NodeCredential;
use crate::shards::watch::ShardAssignment;
use felix_router::ShardKey;

/// How long a fence waits on the control plane. The leader retries a
/// refusal, so a slow read costs a round, not the shard.
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// At most one warning per this long that the control plane serves no
/// confirmed read.
const NOT_SERVED_WARN_EVERY: Duration = Duration::from_secs(60);

/// Asks the control plane over `link`.
pub(crate) struct ControlPlaneRoutes<L> {
    link: L,
    bearer: Option<NodeCredential>,
    /// When the last not-served warning was logged.
    warned: parking_lot::Mutex<Option<tokio::time::Instant>>,
}

#[derive(Deserialize)]
struct Response {
    assignment: Option<ShardAssignment>,
}

impl<L: ControlPlaneLink> ControlPlaneRoutes<L> {
    pub(crate) fn new(link: L, bearer: Option<NodeCredential>) -> Self {
        Self {
            link,
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
impl<L: ControlPlaneLink> ConfirmedRoutes for ControlPlaneRoutes<L> {
    async fn confirmed(&self, key: &ShardKey) -> Confirmed {
        let bearer = self.bearer.as_ref().map(NodeCredential::bearer);
        let confirmed = match self
            .link
            .confirmed_assignment(bearer.as_deref().map(String::as_str), key, READ_TIMEOUT)
            .await
        {
            Ok(response) => answer(&response),
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
fn answer(response: &LinkResponse) -> Confirmed {
    match response.status {
        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::NOT_IMPLEMENTED => {
            Confirmed::NotServed
        }
        status if status.is_success() => match response.json::<Response>() {
            Ok(Response {
                assignment: Some(assignment),
            }) => Confirmed::Assigned {
                generation: assignment.generation,
                leader: assignment.leader,
            },
            Ok(Response { assignment: None }) => Confirmed::Unassigned,
            Err(_) => Confirmed::Unavailable,
        },
        _ => Confirmed::Unavailable,
    }
}

#[cfg(test)]
mod tests;

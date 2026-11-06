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

/// Asks the control plane through the broker's link to it.
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

#[derive(Deserialize)]
struct ManyResponse {
    items: Vec<Response>,
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
        let bearer = self.bearer.as_ref().map(|bearer| bearer.bearer());
        let answered = self
            .link
            .confirmed_assignment(bearer.as_deref().map(String::as_str), key, READ_TIMEOUT)
            .await;
        let confirmed = match answered {
            Ok(response) => answer(response.status, response.json::<Response>().ok()),
            Err(err) => {
                tracing::debug!(error = %err, "confirmed shard assignment read failed");
                Confirmed::Unavailable
            }
        };
        self.record(key, &confirmed);
        confirmed
    }

    async fn confirmed_many(&self, keys: &[ShardKey]) -> Option<Vec<Confirmed>> {
        let bearer = self.bearer.as_ref().map(|bearer| bearer.bearer());
        let answered = self
            .link
            .confirmed_assignments(bearer.as_deref().map(String::as_str), keys, READ_TIMEOUT)
            .await;
        let answers = match answered {
            Ok(response) => answer_many(&response, keys.len())?,
            Err(err) => {
                tracing::debug!(error = %err, "confirmed shard assignment read failed");
                vec![Confirmed::Unavailable; keys.len()]
            }
        };
        for (key, confirmed) in keys.iter().zip(&answers) {
            self.record(key, confirmed);
        }
        Some(answers)
    }
}

/// `None` from a control plane without the batched read: 404 from one
/// without confirmed reads at all, 405 from one that serves only `GET`.
fn answer_many(response: &LinkResponse, keys: usize) -> Option<Vec<Confirmed>> {
    match response.status {
        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::METHOD_NOT_ALLOWED => None,
        status if status.is_success() => Some(match response.json::<ManyResponse>() {
            Ok(many) if many.items.len() == keys => many
                .items
                .into_iter()
                .map(|item| answer(status, Some(item)))
                .collect(),
            _ => vec![Confirmed::Unavailable; keys],
        }),
        status => Some(vec![answer(status, None); keys]),
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

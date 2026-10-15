//! Following the control plane's principal revocations.
//!
//! Polled on the catalog's interval, so a revocation reaches every broker,
//! and ends the streams it covers, within about one sync interval. A failed
//! poll keeps the cursor and is retried; applying a revocation twice changes
//! nothing, so a cursor the control plane no longer recognizes just replays
//! the feed.
//!
//! While the feed is unreachable, revocations made meanwhile are not enforced
//! here: a revoked token is still accepted until it expires, and a stream it
//! already opened stays open until the feed is back. `docs/auth.md`
//! ("Revoking a principal") states the bound.
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::cluster::credential::NodeCredential;
use crate::serving::revocation::Revocations;

/// Unix time, in seconds, of the last poll that reached the feed.
const LAST_SUCCESS_GAUGE: &str = "felix_revocation_feed_last_success_seconds";
/// Polls that failed, for any reason.
const POLL_ERRORS_TOTAL: &str = "felix_revocation_feed_poll_errors_total";

/// How often a feed that keeps failing is warned about again.
const WARN_EVERY: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
struct RevocationChanges {
    items: Vec<RevocationItem>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct RevocationItem {
    tenant_id: String,
    principal_id: String,
    revoked_at: i64,
}

/// Poll until `shutdown`, applying each revocation to `revocations`.
pub(crate) async fn follow_revocations(
    revocations: Arc<Revocations>,
    base_url: String,
    interval: Duration,
    credential: Option<NodeCredential>,
    shutdown: CancellationToken,
) {
    let client = match crate::cluster::controlplane_client::build() {
        Ok(client) => client,
        Err(err) => {
            tracing::warn!(error = %err, "cannot follow principal revocations");
            return;
        }
    };
    // Started at now, so a feed that is down from the start still shows an
    // age that grows, rather than no series to alert on.
    mark_polled();
    let mut since = 0;
    let mut outage = Outage::default();
    loop {
        let bearer = credential.as_ref().map(NodeCredential::bearer);
        // Drained page by page, so a burst is applied in one round.
        loop {
            match fetch(
                &client,
                &base_url,
                bearer.as_deref().map(String::as_str),
                since,
            )
            .await
            {
                Ok(changes) => {
                    outage.ended();
                    let more = !changes.items.is_empty();
                    for item in changes.items {
                        revocations.revoke(&item.tenant_id, &item.principal_id, item.revoked_at);
                    }
                    since = changes.next_seq;
                    if !more {
                        break;
                    }
                }
                Err(err) => {
                    metrics::counter!(POLL_ERRORS_TOTAL).increment(1);
                    if outage.should_warn(Instant::now()) {
                        tracing::warn!(
                            error = %err,
                            "cannot reach the principal revocation feed; revocations \
                             made meanwhile are not enforced here until it is back \
                             (new sessions: until their tokens expire)",
                        );
                    } else {
                        tracing::debug!(error = %err, "principal revocation poll failed");
                    }
                    break;
                }
            }
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(interval) => {}
        }
    }
}

/// A run of failed polls, warned about once and then every [`WARN_EVERY`],
/// so a control plane that is down for long is visible without flooding the
/// log.
#[derive(Debug, Default)]
struct Outage {
    last_warned: Option<Instant>,
}

impl Outage {
    fn should_warn(&mut self, now: Instant) -> bool {
        let due = self
            .last_warned
            .is_none_or(|warned| now.duration_since(warned) >= WARN_EVERY);
        if due {
            self.last_warned = Some(now);
        }
        due
    }

    fn ended(&mut self) {
        if self.last_warned.take().is_some() {
            tracing::info!("the principal revocation feed is reachable again");
        }
        mark_polled();
    }
}

fn mark_polled() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    metrics::gauge!(LAST_SUCCESS_GAUGE).set(now);
}

async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    since: u64,
) -> Result<RevocationChanges> {
    let url = format!(
        "{}/v1/principal-revocations/changes?since={since}",
        base_url.trim_end_matches('/')
    );
    let request = match bearer {
        Some(bearer) => client.get(url).bearer_auth(bearer),
        None => client.get(url),
    };
    request
        .send()
        .await
        .context("revocations request")?
        .error_for_status()
        .context("revocations status")?
        .json()
        .await
        .context("revocations body")
}

#[cfg(test)]
mod tests;

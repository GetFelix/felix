//! Following the control plane's principal revocations.
//!
//! Polled on the catalog's interval, so a revocation reaches every broker,
//! and ends the streams it covers, within about one sync interval. A failed
//! poll keeps the cursor and is retried; applying a revocation twice changes
//! nothing, so a cursor the control plane no longer recognizes just replays
//! the feed.
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::cluster::credential::NodeCredential;
use crate::serving::revocation::Revocations;

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
    let mut since = 0;
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
                    // A control plane that predates the feed answers 404 on
                    // every poll; debug keeps that from flooding the log.
                    tracing::debug!(error = %err, "principal revocation poll failed");
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

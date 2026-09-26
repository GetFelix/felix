//! The loop that keeps [`NodeCredential`] current.
//!
//! Wakes before the access token expires, exchanges the refresh token for a new
//! pair, persists the replacement, and swaps the access token in. Failure is
//! never fatal — a control plane that is briefly unreachable must not take down
//! a broker that is otherwise serving — so it retries with backoff and says
//! loudly when the credential it is running on is about to stop working.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::{NodeCredential, now_secs, read_claims, refresh_delay};
use crate::cluster::membership::metrics as mm;

/// What `/token/refresh` answers with. Both fields are live credentials, so
/// `Debug` shows neither.
#[derive(Deserialize)]
struct RefreshResponse {
    felix_token: String,
    refresh_token: String,
}

impl std::fmt::Debug for RefreshResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshResponse")
            .field("felix_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .finish()
    }
}

/// Everything the loop needs.
pub struct RefreshConfig {
    pub client: reqwest::Client,
    pub base_url: String,
    /// The holder every control-plane caller reads through.
    pub credential: NodeCredential,
    /// Where the refresh token is read from and written back to.
    pub refresh_token_file: PathBuf,
}

/// Keep `credential` current until `shutdown`.
///
/// Returns immediately, with a warning, if the access token does not say when
/// it expires or which tenant it belongs to — there is nothing to schedule
/// against and nowhere to send the refresh, and running a loop that cannot
/// succeed is worse than saying so once.
pub async fn run(config: RefreshConfig, shutdown: CancellationToken) {
    let RefreshConfig {
        client,
        base_url,
        credential,
        refresh_token_file,
    } = config;
    let base_url = base_url.trim_end_matches('/').to_string();

    let Some(claims) = read_claims(&credential.bearer()) else {
        mm::record_credential_expiry_unknown();
        tracing::warn!(
            "the node credential is not a Felix token, so it cannot be \
             refreshed; this broker will fall out of the cluster when the \
             credential expires",
        );
        return;
    };
    let mut expires_at = claims.exp;
    let url = format!("{}/v1/tenants/{}/token/refresh", base_url, claims.tid);

    let mut failures: u32 = 0;
    loop {
        // Published every pass, including the failing ones. A broker whose
        // refreshes are failing looks healthy right up to expiry, and this is
        // the number that says how long that has left to run.
        mm::record_credential_expiry(expires_at - now_secs());
        let delay = if failures == 0 {
            refresh_delay(now_secs(), expires_at)
        } else {
            // Backed off, but never past the deadline: once the token is close
            // to expiry every attempt matters, and a backoff that sails past
            // the moment it stops working turns a recoverable outage into a
            // deregistration.
            backoff(failures).min(refresh_delay(now_secs(), expires_at))
        };

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }

        match refresh_once(&client, &url, &refresh_token_file).await {
            Ok((access, next_refresh)) => {
                // Persist the replacement *before* adopting the new access
                // token. A crash between them costs one access token, which
                // the next startup refreshes past. The other order costs the
                // chain: the broker would be running on a token whose refresh
                // half it never wrote down, and its restart would present a
                // spent one.
                if let Err(err) = persist(&refresh_token_file, &next_refresh) {
                    mm::record_credential_refresh(mm::KIND_UNAVAILABLE);
                    failures += 1;
                    tracing::error!(
                        path = %refresh_token_file.display(),
                        error = %err,
                        "refreshed the node credential but could not save the \
                         replacement refresh token; not adopting it, because a \
                         restart would then present a spent one and the control \
                         plane would revoke the chain",
                    );
                    continue;
                }

                expires_at = read_claims(&access)
                    .map(|claims| claims.exp)
                    .unwrap_or_else(|| {
                        // Accepted anyway: the control plane minted it, and the
                        // only thing lost is knowing when to refresh next, which
                        // the floor below covers.
                        tracing::warn!(
                            "the refreshed credential does not say when it expires; \
                         refreshing on a fixed short interval instead",
                        );
                        now_secs() + 60
                    });
                credential.replace(access);
                failures = 0;
                mm::record_credential_refresh(mm::KIND_OK);
                mm::record_credential_expiry(expires_at - now_secs());
                tracing::info!(expires_at, "refreshed the node credential");
            }
            Err(err) => {
                failures += 1;
                mm::record_credential_refresh(mm::KIND_UNAVAILABLE);
                let remaining = expires_at.saturating_sub(now_secs());
                if remaining <= 0 {
                    tracing::error!(
                        error = %err,
                        failures,
                        "the node credential has expired and cannot be refreshed; \
                         every control-plane call will be refused until this \
                         succeeds",
                    );
                } else {
                    tracing::warn!(
                        error = %err,
                        failures,
                        remaining_secs = remaining,
                        "could not refresh the node credential; retrying",
                    );
                }
            }
        }
    }
}

async fn refresh_once(
    client: &reqwest::Client,
    url: &str,
    refresh_token_file: &Path,
) -> anyhow::Result<(String, String)> {
    use anyhow::Context;

    // Read every time rather than held in memory, so an operator who
    // re-provisions the file by hand is picked up without a restart.
    let refresh_token = std::fs::read_to_string(refresh_token_file)
        .with_context(|| format!("read {}", refresh_token_file.display()))?
        .trim()
        .to_string();
    if refresh_token.is_empty() {
        anyhow::bail!("{} is empty", refresh_token_file.display());
    }

    let response = client
        .post(url)
        // The node credential is for the control plane's API, which refuses
        // broker-audience tokens. A control plane that predates audiences
        // ignores the field.
        .json(&serde_json::json!({
            "refresh_token": refresh_token,
            "audience": "felix-controlplane",
        }))
        .send()
        .await
        .context("send refresh request")?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("refresh rejected ({status}): {body}");
    }

    let refreshed: RefreshResponse = response.json().await.context("decode refresh response")?;
    Ok((refreshed.felix_token, refreshed.refresh_token))
}

/// Write the replacement refresh token, atomically.
///
/// Through a temporary and a rename, because a half-written token file is
/// unrecoverable: the old one is already spent, so there is no going back to
/// it. A rename is atomic on the same filesystem, so the file either holds the
/// old token or the new one.
fn persist(path: &Path, token: &str) -> std::io::Result<()> {
    use std::io::Write;

    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(token.as_bytes())?;
        file.write_all(b"\n")?;
        // Durable before the rename, or a crash can leave the file present and
        // empty — which reads as "no credential" rather than as the old one.
        file.sync_all()?;
    }
    std::fs::rename(&temporary, path)
}

fn backoff(failures: u32) -> Duration {
    const BASE: Duration = Duration::from_secs(1);
    const CEILING: Duration = Duration::from_secs(60);
    BASE.saturating_mul(1u32 << failures.min(6)).min(CEILING)
}

#[cfg(test)]
mod tests;

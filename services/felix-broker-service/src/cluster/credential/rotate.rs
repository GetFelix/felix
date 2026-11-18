//! Picking up a credential something outside the broker rewrote.
//!
//! The refresh loop covers the case where the broker renews its own token. It
//! does not cover the other one: a Vault agent, SPIRE, or a sidecar that mints
//! the credential and writes it to `FELIX_NODE_TOKEN_FILE`. That file used to be
//! read once at startup and never again, so rotation took effect on the next
//! restart and not before — while the *refresh* token file was deliberately
//! re-read every time, "so an operator who re-provisions the file by hand is
//! picked up without a restart". The asymmetry was the surprising half.
//!
//! So this watches the file and adopts what it finds, through the same
//! `ArcSwap` a refresh uses. Polling rather than inotify: the file is usually a
//! projected secret or a bind mount, where the write the broker cares about is
//! a rename or a symlink swap that filesystem notifications report
//! inconsistently across platforms and container runtimes. A poll is a `read`
//! of a small file on a slow timer, and it behaves the same everywhere.

use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{NodeCredential, now_secs, read_claims};
use crate::cluster::membership::metrics as mm;

/// How often the token file is re-read.
///
/// Slow on purpose. Rotation happens on the scale of a token's lifetime, and
/// the cost of noticing a minute late is a minute of a credential that is still
/// valid — the outgoing token overlaps the incoming one by design.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Adopt `path` whenever its contents change, until `shutdown`.
pub async fn run(
    path: PathBuf,
    credential: NodeCredential,
    interval: Duration,
    shutdown: CancellationToken,
) {
    // What the broker is already running on, so an unchanged file is not
    // repeatedly "adopted" and logged.
    let mut current = credential.bearer().as_str().to_string();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }

        let token = match tokio::fs::read_to_string(&path).await {
            Ok(contents) => contents.trim().to_string(),
            Err(err) => {
                // Not fatal, and not even necessarily wrong: a rotation that
                // writes through a temporary can leave the path missing for an
                // instant. The credential in hand keeps working.
                tracing::debug!(
                    path = %path.display(),
                    error = %err,
                    "could not read the node token file; keeping the current credential",
                );
                continue;
            }
        };

        if token.is_empty() || token == current {
            continue;
        }

        // Refuse a replacement that is already dead rather than adopting it:
        // swapping a working credential for an expired one turns a rotation
        // bug into an outage this broker caused.
        match read_claims(&token) {
            Some(claims) if claims.exp <= now_secs() => {
                tracing::warn!(
                    path = %path.display(),
                    exp = claims.exp,
                    "the node token file holds an already-expired credential; \
                     keeping the current one",
                );
                mm::record_credential_rotation(mm::KIND_REJECTED);
                continue;
            }
            claims => {
                credential.replace(token.clone());
                current = token;
                mm::record_credential_rotation(mm::KIND_OK);
                match claims {
                    Some(claims) => {
                        mm::record_credential_expiry(claims.exp - now_secs());
                        tracing::info!(
                            path = %path.display(),
                            expires_at = claims.exp,
                            "adopted a rotated node credential",
                        );
                    }
                    // Someone else's token format. Adopted anyway -- the broker
                    // is not the authority on what the control plane accepts --
                    // but there is no expiry to count down from.
                    None => tracing::info!(
                        path = %path.display(),
                        "adopted a rotated node credential that does not say when it expires",
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;

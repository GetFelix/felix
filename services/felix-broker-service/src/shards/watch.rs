//! Broker-side view of which shards this node owns.
//!
//! Snapshot once, then follow changes from where the snapshot left off. The
//! control plane guarantees those two together describe every committed change
//! exactly once, so the interesting work here is what to do when that guarantee
//! stops holding: the broker fell behind the retention window, or the sequence
//! it was following reset under it.
//!
//! Both are recoverable, and both recover the same way — take a fresh snapshot.
//! What must never happen is carrying on from a checkpoint the control plane can
//! no longer honour, because that silently drops ownership changes and leaves
//! this broker serving shards it no longer owns.
//!
//! Changes are long-polled: the control plane holds the request until one
//! lands, so a fence or a cut-over reaches this broker as it is written rather
//! than at its next poll. A control plane that predates long-polling answers at
//! once, and the loop then waits out the interval as it always did.
pub mod metrics;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;

use crate::shards::ShardKey;
use crate::shards::watch::metrics as mm;

/// Ceiling on poll backoff after a failure.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How long the control plane may hold a changes request open. Under its own
/// 25 s cap, which sits under the idle timeout of common proxies.
const LONG_POLL_WAIT: Duration = Duration::from_secs(20);

/// Slack on top of [`LONG_POLL_WAIT`] before a held request counts as failed.
/// Without a bound, a connection that died silently would stop the watch.
const LONG_POLL_SLACK: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ShardAssignment {
    #[serde(flatten)]
    pub key: ShardKey,
    pub leader: String,
    #[serde(default)]
    pub replicas: Vec<String>,
    pub generation: u64,
    pub state: String,
    /// Where a move in progress is taking the shard, if it has a destination.
    #[serde(default)]
    pub successor: Option<String>,
    /// How the stream maps keys to shards, stamped by the control plane from
    /// the stream. Absent is modulo, which is all an older control plane knows.
    #[serde(default)]
    pub routing: felix_wire::routing::ShardRouting,
}

impl ShardAssignment {
    /// The control plane has told the leader to stop serving at this
    /// generation so the shard can move.
    pub fn is_draining(&self) -> bool {
        self.state == "draining"
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ShardAssignmentChange {
    seq: u64,
    key: ShardKey,
    assignment: Option<ShardAssignment>,
}

#[derive(Debug, Deserialize)]
struct SnapshotResponse {
    items: Vec<ShardAssignment>,
    next_seq: u64,
}

#[derive(Debug, Deserialize)]
struct ChangesResponse {
    items: Vec<ShardAssignmentChange>,
    next_seq: u64,
}

/// What this broker currently believes about shard ownership.
///
/// Shared, so the ingress path can ask "do I own this shard?" without touching
/// the control plane.
#[derive(Debug, Default)]
pub struct ShardOwnership {
    assignments: HashMap<ShardKey, ShardAssignment>,
}

impl ShardOwnership {
    /// Apply a change, ignoring one that would move a shard backwards.
    ///
    /// Returns whether anything changed. A duplicate delivery is therefore
    /// harmless, and a stale generation — from a retry, a reordered poll, or a
    /// control plane that was behind — cannot roll ownership back.
    pub fn apply(&mut self, key: &ShardKey, assignment: Option<ShardAssignment>) -> bool {
        match assignment {
            Some(next) => {
                if let Some(current) = self.assignments.get(key)
                    && next.generation <= current.generation
                {
                    mm::record_stale_change();
                    return false;
                }
                self.assignments.insert(key.clone(), next);
                true
            }
            None => self.assignments.remove(key).is_some(),
        }
    }

    /// Replace everything with a snapshot.
    pub fn reset(&mut self, items: Vec<ShardAssignment>) {
        self.assignments = items
            .into_iter()
            .map(|assignment| (assignment.key.clone(), assignment))
            .collect();
    }

    /// Everything this broker currently believes about ownership.
    pub fn assignments(&self) -> &HashMap<ShardKey, ShardAssignment> {
        &self.assignments
    }

    pub fn get(&self, key: &ShardKey) -> Option<&ShardAssignment> {
        self.assignments.get(key)
    }

    /// Whether `node_id` leads this shard.
    pub fn is_leader(&self, key: &ShardKey, node_id: &str) -> bool {
        self.assignments
            .get(key)
            .is_some_and(|assignment| assignment.leader == node_id)
    }

    pub fn len(&self) -> usize {
        self.assignments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assignments.is_empty()
    }
}

/// Why the broker must take a fresh snapshot rather than keep polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resync {
    /// Changes before the first returned one were evicted from the window.
    GapInHistory,
    /// The control plane's sequence is behind our checkpoint, so it restarted
    /// onto a store that does not persist the sequence.
    SequenceReset,
}

/// Decide whether a changes response can be applied as-is.
///
/// Split out from the polling loop because this is the part that is easy to get
/// wrong and easy to test: every branch is a way to silently lose an ownership
/// change.
pub fn check_continuity(since: u64, first_seq: Option<u64>, next_seq: u64) -> Option<Resync> {
    if next_seq < since {
        return Some(Resync::SequenceReset);
    }
    match first_seq {
        // A first change above where we asked means the ones between were
        // evicted while we were away.
        Some(seq) if seq > since => Some(Resync::GapInHistory),
        // Nothing returned, but the log has moved past us: the whole span was
        // evicted rather than being empty.
        None if next_seq > since => Some(Resync::GapInHistory),
        _ => None,
    }
}

/// Follow shard ownership until `shutdown` fires.
///
/// Never returns an error: the control plane being unreachable is not a reason
/// for a broker to stop serving what it already owns. Failures back off, and are
/// visible through `felix_broker_shard_watch_failures_total`.
///
/// `changed` is notified after every snapshot and every batch of changes that
/// moved something, so the routing feed acts on it now rather than at its
/// next tick.
pub async fn run(
    client: reqwest::Client,
    base_url: String,
    // Held rather than copied: this task outlives many token lifetimes, and a
    // copy taken at startup is the exact bug that drops a broker out of the
    // cluster when its first token expires.
    bearer: Option<crate::cluster::credential::NodeCredential>,
    ownership: Arc<RwLock<ShardOwnership>>,
    changed: Arc<Notify>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let base_url = base_url.trim_end_matches('/').to_string();
    // `None` means "no valid checkpoint": take a snapshot before polling.
    let mut checkpoint: Option<u64> = None;
    let mut failures: u32 = 0;
    // The first snapshot is taken at once: nothing is gained by starting a
    // broker's ownership view an interval late.
    let mut delay = Duration::ZERO;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }

        // Read per poll, not once: a refreshed token has to reach this loop
        // without it knowing refresh exists.
        let token = bearer.as_ref().map(|credential| credential.bearer());
        let token = token.as_deref().map(String::as_str);
        let started = tokio::time::Instant::now();
        // Raced against shutdown: a long-poll can be held for tens of seconds,
        // and shutdown must not wait for it.
        let result = tokio::select! {
            _ = shutdown.cancelled() => return,
            result = async {
                match checkpoint {
                    None => snapshot(&client, &base_url, token, &ownership).await,
                    Some(since) => poll(&client, &base_url, token, &ownership, since).await,
                }
            } => result,
        };

        delay = match result {
            Ok(Progress::At {
                seq,
                changed: moved,
            }) => {
                failures = 0;
                checkpoint = Some(seq);
                mm::record_checkpoint(seq);
                if moved {
                    changed.notify_one();
                }
                next_delay(interval, moved, started.elapsed())
            }
            Ok(Progress::MustResync(reason)) => {
                failures = 0;
                // Deliberately not an error: falling behind the window is an
                // expected consequence of a broker being away long enough.
                tracing::warn!(?reason, "shard watch must resnapshot");
                mm::record_resync(reason);
                checkpoint = None;
                interval
            }
            Err(err) => {
                failures = failures.saturating_add(1);
                mm::record_failure();
                tracing::warn!(
                    consecutive_failures = failures,
                    error = %err,
                    "shard watch poll failed; retrying with backoff",
                );
                backoff(interval, failures)
            }
        };
    }
}

/// How long to wait before the next request.
///
/// None after a change, since the next request long-polls anyway. After an
/// empty answer, whatever is left of `interval`: a long-poll that waited is
/// already past it, and a control plane that ignores `wait_ms` answers at once
/// and is polled on the interval as before rather than in a tight loop.
fn next_delay(interval: Duration, changed: bool, took: Duration) -> Duration {
    if changed {
        Duration::ZERO
    } else {
        interval.saturating_sub(took)
    }
}

enum Progress {
    /// Caught up to `seq`; `changed` if anything was applied on the way.
    At {
        seq: u64,
        changed: bool,
    },
    MustResync(Resync),
}

async fn snapshot(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    ownership: &Arc<RwLock<ShardOwnership>>,
) -> Result<Progress> {
    let response: SnapshotResponse = get(
        client,
        &format!("{base_url}/v1/shard-assignments/snapshot"),
        bearer,
    )
    .await
    .context("shard assignment snapshot")?;

    let count = response.items.len();
    ownership.write().await.reset(response.items);
    mm::record_snapshot(count);
    tracing::info!(
        assignments = count,
        next_seq = response.next_seq,
        "shard ownership seeded from snapshot",
    );
    Ok(Progress::At {
        seq: response.next_seq,
        changed: true,
    })
}

async fn poll(
    client: &reqwest::Client,
    base_url: &str,
    bearer: Option<&str>,
    ownership: &Arc<RwLock<ShardOwnership>>,
    since: u64,
) -> Result<Progress> {
    let response: ChangesResponse = get(
        client,
        &format!(
            "{base_url}/v1/shard-assignments/changes?since={since}&wait_ms={}",
            LONG_POLL_WAIT.as_millis()
        ),
        bearer,
    )
    .await
    .context("shard assignment changes")?;

    let first_seq = response.items.first().map(|change| change.seq);
    if let Some(reason) = check_continuity(since, first_seq, response.next_seq) {
        return Ok(Progress::MustResync(reason));
    }

    let mut changed = false;
    if !response.items.is_empty() {
        let mut owned = ownership.write().await;
        for change in &response.items {
            changed |= owned.apply(&change.key, change.assignment.clone());
        }
        mm::record_applied(response.items.len());
    }
    Ok(Progress::At {
        seq: response.next_seq,
        changed,
    })
}

async fn get<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    bearer: Option<&str>,
) -> Result<T> {
    let mut request = client.get(url).timeout(LONG_POLL_WAIT + LONG_POLL_SLACK);
    if let Some(bearer) = bearer {
        request = request.bearer_auth(bearer);
    }
    let response = request.send().await.context("send request")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("{status}: {body}"));
    }
    response.json().await.context("decode response")
}

/// Exponential backoff, capped, so a long control-plane outage does not turn
/// into an hours-long gap before the broker notices it came back.
fn backoff(interval: Duration, failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(16);
    interval
        .saturating_mul(2u32.saturating_pow(shift))
        .min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests;

//! Broker-side control-plane sync client.
//!
//! This module runs **inside the broker service** and keeps the broker's local registries
//! (tenants / namespaces / streams / caches) aligned with the control-plane.
//!
//! Design goals
//! - Keep the **hot data path** (publish / subscribe / cache ops) free of control-plane I/O.
//! - Provide **eventual consistency**: brokers converge to the control-plane state over time.
//! - Allow a broker to cold-start with a full snapshot, then poll incremental change feeds.
//!
//! What this code assumes about the control-plane HTTP API
//! - For each resource type there is a `snapshot` endpoint that returns the full current set
//!   plus a monotonically increasing `next_seq` cursor.
//! - For each resource type there is a `changes?since=<seq>` endpoint that returns all changes
//!   after `since`, plus the new `next_seq` cursor.
//! - `next_seq` is **monotonic** per resource type (not necessarily contiguous).
//! - Change feeds are allowed to be empty; the broker will simply advance `next_seq`.
//!
//! Failure mode philosophy
//! - Snapshot/changes fetch failures are **non-fatal**: we log a warning and keep going.
//! - Local application failures (e.g., broker rejects a registration) are treated as real errors
//!   and bubble up, because they indicate an invariant mismatch or corrupted input.
//!
//! NOTE: This file is a *client* of the control-plane. Persisting control-plane data
//! (e.g., in Postgres) is implemented on the **control-plane service**, not here.

mod apply;
mod fetch;
mod pass;
mod wire;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_broker::Broker;

use crate::cluster::credential::NodeCredential;
use pass::sync_once;

/// Polls the control plane forever, applying changes as they arrive.
///
/// Kept as the plain entry point because it is part of this crate's public API
/// and has consumers outside the workspace — `demos/rbac-live` is a standalone
/// crate, so a workspace-scoped lint cannot see that it is used. `dead_code`
/// reports it unused and `unreachable_pub` wants it demoted; both are wrong
/// here, and demoting it breaks `task demo:check`.
#[allow(dead_code, unreachable_pub)]
pub async fn start_sync(
    broker: Arc<Broker>,
    base_url: String,
    interval: Duration,
    credential: Option<NodeCredential>,
) -> Result<()> {
    start_sync_with_signal(broker, base_url, interval, None, credential).await
}

/// Starts the control-plane sync loop as a background task.
///
/// This function runs forever, periodically fetching control-plane snapshots and change feeds,
/// and applying them to the broker's local registries.
///
/// - It is safe to run as a background task (spawns no additional tasks).
/// - It intentionally sleeps for `interval` between change feed polls.
/// - Apply-time errors are logged and retried on the next iteration so transient
///   ordering races (for example namespace before tenant) do not stop sync.
///
/// Fires `seeded` after the first full sync.
///
/// Most callers want [`start_sync`], which is this with no signal.
///
/// A broker with durable storage uses that signal to hold readiness until its
/// streams exist. Until the first sync lands the catalog has not been applied
/// and no durable stream has been recovered — reporting ready before then
/// invites an orchestrator to route traffic at an instance whose durable
/// streams are, as far as any client can tell, missing. Pass `None` to start
/// syncing without gating anything on it.
///
/// `credential` is what the feeds are read with. They require
/// `node.view:cluster:*`, so without one every poll is refused; the holder is
/// read on each iteration, so a refresh is picked up without a restart.
pub(crate) async fn start_sync_with_signal(
    broker: Arc<Broker>,
    base_url: String,
    interval: Duration,
    mut seeded: Option<tokio::sync::oneshot::Sender<()>>,
    credential: Option<NodeCredential>,
) -> Result<()> {
    let client = crate::cluster::controlplane_http::client();
    // Sequence cursors for each change feed; 0 means "not yet seeded".
    let mut state = SyncState::new();
    loop {
        let bearer = credential.as_ref().map(NodeCredential::bearer);
        match sync_once(
            &broker,
            &client,
            &base_url,
            bearer.as_deref().map(String::as_str),
            state,
        )
        .await
        {
            Ok(next_state) => {
                state = next_state;
                // Only signal once the catalog is genuinely in place. An
                // iteration can succeed having applied nothing at all, and
                // reporting ready off the back of that would advertise a broker
                // whose streams — durable ones included — do not exist.
                if state.is_seeded()
                    && let Some(signal) = seeded.take()
                {
                    let _ = signal.send(());
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "control plane sync iteration failed; retrying")
            }
        }
        // Polling interval between change feed reads.
        tokio::time::sleep(interval).await;
    }
}

/// Per-resource cursors into the control-plane change feeds.
///
/// Each resource type (tenants, namespaces, caches, streams) has an independent cursor.
/// A value of `0` is treated as "unseeded" and triggers an initial snapshot fetch.
///
/// Why separate cursors?
/// - Each feed can advance independently.
/// - It keeps payload sizes small and avoids cross-resource coupling.
///
/// NOTE: These cursors are **in-memory** in the broker process. On broker restart,
/// we re-seed from snapshots (safe but more expensive than resuming from a stored cursor).
#[derive(Debug, Clone, Copy)]
struct SyncState {
    /// Next sequence cursor for the tenants change feed.
    next_tenant_seq: u64,
    /// Next sequence cursor for the namespaces change feed.
    next_namespace_seq: u64,
    /// Next sequence cursor for the caches change feed.
    next_cache_seq: u64,
    /// Next sequence cursor for the streams change feed.
    next_stream_seq: u64,
    /// Which cold-start snapshots have actually been fetched and applied.
    ///
    /// Deliberately separate from the cursors. A cursor cannot answer "was this
    /// resource seeded?" for two independent reasons: a *successful* snapshot of
    /// an empty catalog legitimately returns `next_seq == 0`, which reads as
    /// "never seeded" and would leave such a deployment unready forever; and a
    /// *failed* snapshot leaves the cursor at 0, after which the change feed in
    /// the same iteration polls from zero and can advance the cursor straight
    /// to the global tail — making a resource look seeded when nothing was
    /// applied. Only an explicit flag set where the snapshot is applied says
    /// what actually happened.
    seeded: SeededSnapshots,
}

impl SyncState {
    /// Whether every cold-start snapshot has been fetched and applied.
    ///
    /// `sync_once` returns `Ok` even when every snapshot fetch failed — the
    /// failures are logged and retried, which is right for a background
    /// refresh, but means "the iteration completed" says nothing about whether
    /// any tenant, namespace, cache or stream was restored. Readiness needs the
    /// stronger statement.
    fn is_seeded(&self) -> bool {
        self.seeded.all()
    }

    fn new() -> Self {
        Self {
            next_tenant_seq: 0,
            next_namespace_seq: 0,
            next_cache_seq: 0,
            next_stream_seq: 0,
            seeded: SeededSnapshots::default(),
        }
    }
}

/// Per-resource record of a completed cold-start snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SeededSnapshots {
    tenants: bool,
    namespaces: bool,
    caches: bool,
    streams: bool,
}

impl SeededSnapshots {
    fn all(&self) -> bool {
        self.tenants && self.namespaces && self.caches && self.streams
    }
}

#[cfg(test)]
mod tests;

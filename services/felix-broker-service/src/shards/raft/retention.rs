//! Retention on a Raft shard.
//!
//! A Raft log takes no retention bounds: its entries leave only through a
//! purge, which stream shards do not run yet. Instead the shard's leader
//! reads the stream's bounds on the storage's retention timer and, when they
//! would start the log past the current floor, proposes a head trim to the
//! segment boundary they name. Followers never evaluate the bounds, whose
//! clocks and sizes differ; they apply the floor at the trim's entry like any
//! other. The records are hidden on every member, and no disk is freed. See
//! `docs/per-shard-raft.md`, "Retention".

use std::sync::Weak;
use std::time::Duration;

use bytes::Bytes;
use felix_broker::{StreamHandle, TrimFailed};
use felix_replication::raft::AckAt;
use felix_storage::DiskLog;

use super::{RaftShards, ShardKey};

/// How long one retention trim may take before the pass gives up on it. The
/// next tick proposes again.
const TRIM_WITHIN: Duration = Duration::from_secs(30);

/// Retention passes on a Raft shard's leader that could not trim: the trim
/// was refused or failed. Logged when the reason changes.
pub(crate) const RETENTION_TRIMS_REFUSED: &str = "felix_broker_raft_retention_trims_refused_total";

/// What one retention pass did.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Pass {
    /// Not the leader, no bounds, or nothing past the floor to hide.
    Nothing,
    /// A trim moved the floor here.
    Trimmed(u64),
    /// The cut or the trim failed; the next tick tries again.
    Refused(String),
}

impl RaftShards {
    /// Run [`Self::enforce_retention`] for `key` every `interval` until the
    /// member closes, which aborts the task, or the shards are dropped.
    pub(super) fn spawn_retention(
        me: Weak<Self>,
        key: ShardKey,
        log: DiskLog,
        stream: StreamHandle,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Not on open: a member that just won its term has not moved its
            // stream to the committed tail yet.
            ticker.tick().await;
            // Logged when it changes, not on every tick.
            let mut refused: Option<String> = None;
            loop {
                ticker.tick().await;
                let Some(shards) = me.upgrade() else { return };
                match shards.enforce_retention(&key, &log, &stream).await {
                    Pass::Nothing => {}
                    Pass::Trimmed(floor) => {
                        refused = None;
                        tracing::info!(shard = ?key, floor, "retention hid the shard below a new floor");
                    }
                    Pass::Refused(why) => {
                        metrics::counter!(RETENTION_TRIMS_REFUSED).increment(1);
                        if refused.as_ref() != Some(&why) {
                            tracing::warn!(shard = ?key, reason = %why, "retention could not trim a Raft shard");
                        }
                        refused = Some(why);
                    }
                }
            }
        })
    }

    /// One retention pass on `key`, whose member's log is `log`: when this
    /// broker leads it and the stream's bounds start the log past its floor,
    /// trim to where they start it.
    ///
    /// The trim is not conditional on the tail, as a client's is: the cut is
    /// a sealed segment boundary, at or below the tail and so at or below
    /// any index the trim's entry can take ([`Broker::propose_trim_below`]).
    /// Conditional, a busy shard's publishes would land first and refuse it
    /// on every pass.
    ///
    /// [`Broker::propose_trim_below`]: felix_broker::Broker::propose_trim_below
    pub(super) async fn enforce_retention(
        &self,
        key: &ShardKey,
        log: &DiskLog,
        stream: &StreamHandle,
    ) -> Pass {
        let Some(term) = self.leading(key) else {
            return Pass::Nothing;
        };
        let Some(storage) = self.broker.durable_storage() else {
            return Pass::Nothing;
        };
        let bounds = storage.stream_retention(&key.tenant_id, &key.namespace, &key.stream);
        let cut = match log.retention_cut(bounds).await {
            Ok(Some(cut)) => cut,
            Ok(None) => return Pass::Nothing,
            Err(err) => return Pass::Refused(err.to_string()),
        };
        if cut <= log.head_trim().map_or(0, |trim| trim.floor) {
            return Pass::Nothing;
        }
        let ack = AckAt::for_fsync(&storage.config().fsync_mode);
        let outcome = match self
            .engine
            .trim_below(
                &self.broker,
                stream,
                key,
                Some(term),
                cut,
                Bytes::new(),
                ack,
                TRIM_WITHIN,
            )
            .await
        {
            Ok(proposed) => proposed.complete().await.map_err(|failed| match failed {
                TrimFailed::Engine(err) => err.to_string(),
                TrimFailed::Broker(err) => err.to_string(),
            }),
            Err(err) => Err(err.to_string()),
        };
        match outcome {
            Ok(outcome) => Pass::Trimmed(outcome.oldest),
            // Deposed mid-pass: the next leader runs its own.
            Err(_) if self.leading(key) != Some(term) => Pass::Nothing,
            Err(why) => Pass::Refused(why),
        }
    }
}

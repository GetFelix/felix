//! Whether this member is fit to serve, and the gauges that show it.
use openraft::async_runtime::WatchReceiver;
use std::time::Duration;

use super::RaftHandle;

impl RaftHandle {
    /// Entries this member may trail its own log by before readiness calls
    /// it unfit to serve. Small on purpose: metadata writes are rare, the
    /// only healthy lag is the handful of entries in flight between append
    /// and apply, and a member further behind is serving a past that
    /// callers cannot detect. (Startup replay is handled separately — a
    /// node does not finish starting until it has re-applied everything it
    /// had committed.)
    const READY_APPLY_LAG_MAX: u64 = 32;

    /// How stale a leader's last quorum acknowledgement may be before
    /// readiness stops trusting it. A leader that has not heard a quorum in
    /// this long is a leader in name only — likely partitioned with the
    /// minority — and must leave rotation before it serves stale reads or
    /// queues writes that cannot commit.
    const READY_QUORUM_ACK_MAX: Duration = Duration::from_secs(5);

    /// Whether this member is fit to serve, and if not, why — the store's
    /// readiness probe under the raft backend.
    ///
    /// Three questions, in order: does this member know a leader (an
    /// instance mid-election or partitioned off does not); is it applying
    /// what its log holds; and, when it *is* the leader, has a quorum
    /// acknowledged it recently. All answered from local metrics — a
    /// readiness probe must never cost a consensus round trip.
    pub fn readiness(&self) -> Result<(), String> {
        if !self.may_vote.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("started without raft state; catching up before serving".to_string());
        }
        let metrics = self.raft.metrics().borrow_watched().clone();
        let Some(leader) = metrics.current_leader else {
            return Err("no raft leader is known to this instance".to_string());
        };
        let last_log = metrics.last_log_index.unwrap_or(0);
        let applied = metrics.last_applied.map(|id| id.index).unwrap_or(0);
        // A follower with a leader and an *empty* log has just joined an
        // established group — a wiped volume, a brand-new member — and
        // holds none of the group's state. Its apply-lag reads zero because
        // the lag is measured against its own log, which is exactly the
        // blind spot: until the first replication batch lands, it would
        // serve an empty world as ready. (A leader is exempt — it holds the
        // head by definition — and a genuinely new cluster is leaderless,
        // so this clause never blocks group formation.)
        if leader != self.id && last_log == 0 {
            return Err(
                "joined an established group; nothing replicated into this member yet".to_string(),
            );
        }
        let lag = last_log.saturating_sub(applied);
        if lag > Self::READY_APPLY_LAG_MAX {
            return Err(format!(
                "applied state trails the log by {lag} entries (bound {})",
                Self::READY_APPLY_LAG_MAX
            ));
        }
        if leader == self.id
            && let Some(acked) = metrics.last_quorum_acked
            && let millis = openraft::Instant::elapsed(&*acked).as_millis() as u64
            && millis > Self::READY_QUORUM_ACK_MAX.as_millis() as u64
        {
            return Err(format!(
                "leader without a quorum acknowledgement for {millis}ms (bound {}ms)",
                Self::READY_QUORUM_ACK_MAX.as_millis()
            ));
        }
        Ok(())
    }

    /// Publish this member's consensus position as gauges, once a second,
    /// until `shutdown`. The names an operator's dashboard needs to answer
    /// "who leads, and is everyone keeping up" — per-instance series, no
    /// unbounded labels.
    pub fn spawn_metrics(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let raft = self.raft.clone();
        let id = self.id;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = ticker.tick() => {
                        let metrics = raft.metrics().borrow_watched().clone();
                        metrics::gauge!("felix_meta_raft_term")
                            .set(shown_term(metrics.current_term) as f64);
                        metrics::gauge!("felix_meta_raft_leader_known")
                            .set(if metrics.current_leader.is_some() { 1.0 } else { 0.0 });
                        metrics::gauge!("felix_meta_raft_is_leader")
                            .set(if metrics.current_leader == Some(id) { 1.0 } else { 0.0 });
                        metrics::gauge!("felix_meta_raft_last_log_index")
                            .set(metrics.last_log_index.unwrap_or(0) as f64);
                        metrics::gauge!("felix_meta_raft_last_applied_index")
                            .set(metrics.last_applied.map(|id| id.index).unwrap_or(0) as f64);
                        metrics::gauge!("felix_meta_raft_snapshot_index")
                            .set(metrics.snapshot.map(|id| id.index).unwrap_or(0) as f64);
                    }
                }
            }
        })
    }
}

/// The first term of a store converted from redb. Terms it copied are below
/// this and every term after the conversion is at or above it
/// (`docs/metadata-raft-design.md`, "Upgrading an existing cluster").
const CONVERTED_TERM_BASE: u64 = 1 << 62;

/// A term as it is shown: one at or past [`CONVERTED_TERM_BASE`] with that
/// taken off. The gauge is a float, exact only below 2^53, and a status
/// reader may hold a signed 64-bit number. The term on the wire and on disk
/// is unchanged.
pub(super) fn shown_term(term: u64) -> u64 {
    term.checked_sub(CONVERTED_TERM_BASE).unwrap_or(term)
}

#[cfg(test)]
mod tests;

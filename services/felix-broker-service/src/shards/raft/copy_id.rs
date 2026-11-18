//! Raft copy ids: the copy each member open here votes under, reported to
//! the control plane so it can write the shard's target from them
//! (`docs/per-shard-raft.md`, "Where a target comes from").
//!
//! Only a member whose copy is open is reported, and a copy is open only
//! once its copy file is synced, so the control plane never names a copy a
//! crash could take back. A report is idempotent. One is owed until the
//! control plane's target names the copy, and a report it took is sent
//! again after [`RESEND`] while no target does: the control plane drops
//! the ids of a shard whose set changes, and a restore brings back older
//! ones.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use felix_replication::controlplane_link::{ControlPlaneLink, RaftCopyId};
use felix_replication::reporter::Credential;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{RaftShards, ShardKey};
use crate::cluster::membership::RootSequenceSlot;

/// How often owed copy ids are sent.
pub(crate) const INTERVAL: Duration = Duration::from_secs(1);

/// How long a report the control plane refused or never answered waits
/// before it is sent again. After an answered refusal the wait doubles with
/// each one in a row, up to [`MAX_RETRY`]: each is still an entry in the
/// control plane's log. An unanswered report wrote nothing and keeps this.
const RETRY: Duration = Duration::from_secs(5);

/// The longest a refused report waits before it is sent again. Kept short:
/// nothing tells the broker when the cause of a refusal clears.
const MAX_RETRY: Duration = Duration::from_secs(60);

/// How long a report the control plane took waits before it is sent again
/// while its target still does not name the copy.
pub(crate) const RESEND: Duration = Duration::from_secs(30);

/// Most reports one tick sends. The rest wait for later ticks.
const MAX_PER_TICK: usize = 32;

/// How long one report may take before the tick gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A member open here, as the last placement pass saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Held {
    pub(crate) key: ShardKey,
    /// The stream incarnation the group runs.
    pub(crate) incarnation: u64,
    /// The copy id the member votes under.
    pub(crate) copy: u64,
    /// The brokers placement names for the shard. The control plane keeps
    /// copy ids only while this stays the same.
    pub(crate) set: BTreeSet<String>,
    /// The copy id the control plane's target names for this broker, if
    /// the view carried a target that names it.
    pub(crate) in_target: Option<u64>,
}

/// What was last sent for one member, and when.
#[derive(Debug, Clone)]
struct Sent {
    copy: u64,
    set: BTreeSet<String>,
    at: Instant,
    taken: bool,
    /// Answered refusals in a row for this copy and set.
    refused: u32,
    /// The last report got no answer.
    unanswered: bool,
}

/// The reports owed to the control plane, by shard and stream incarnation.
#[derive(Debug, Default)]
pub(crate) struct CopyIds {
    sent: BTreeMap<(ShardKey, u64), Sent>,
}

impl CopyIds {
    /// The members owed a report now, oldest attempt first, at most
    /// [`MAX_PER_TICK`]. Forgets members no longer held.
    fn owed(&mut self, held: &[Held], now: Instant) -> Vec<Held> {
        let keys: BTreeSet<(ShardKey, u64)> = held
            .iter()
            .map(|held| (held.key.clone(), held.incarnation))
            .collect();
        self.sent.retain(|key, _| keys.contains(key));
        let mut owed: Vec<(Option<Instant>, &Held)> = held
            .iter()
            .filter(|held| held.in_target != Some(held.copy))
            .filter_map(|held| {
                let last = self
                    .sent
                    .get(&(held.key.clone(), held.incarnation))
                    .filter(|sent| sent.copy == held.copy && sent.set == held.set);
                let wait = match last {
                    None => return Some((None, held)),
                    Some(sent) if sent.taken => RESEND,
                    Some(sent) if sent.unanswered => RETRY,
                    Some(sent) => retry_after(sent.refused),
                };
                let at = last.map(|sent| sent.at)?;
                (now.duration_since(at) >= wait).then_some((Some(at), held))
            })
            .collect();
        owed.sort_by_key(|(at, _)| *at);
        owed.into_iter()
            .take(MAX_PER_TICK)
            .map(|(_, held)| held.clone())
            .collect()
    }

    /// Records a report sent for `held`. Returns how many answered
    /// refusals in a row this copy and set have now had.
    fn record(&mut self, held: &Held, now: Instant, outcome: Outcome) -> u32 {
        let key = (held.key.clone(), held.incarnation);
        let before = self
            .sent
            .get(&key)
            .filter(|sent| sent.copy == held.copy && sent.set == held.set)
            .map_or(0, |sent| sent.refused);
        let refused = match outcome {
            Outcome::Taken => 0,
            Outcome::Refused => before.saturating_add(1),
            Outcome::Unanswered => before,
        };
        self.sent.insert(
            key,
            Sent {
                copy: held.copy,
                set: held.set.clone(),
                at: now,
                taken: outcome == Outcome::Taken,
                refused,
                unanswered: outcome == Outcome::Unanswered,
            },
        );
        refused
    }
}

/// How a report went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Taken,
    Refused,
    Unanswered,
}

/// The wait after `refused` answered refusals in a row.
fn retry_after(refused: u32) -> Duration {
    RETRY
        .saturating_mul(1 << refused.saturating_sub(1).min(16))
        .min(MAX_RETRY)
}

/// The registration a report is sent under: the process's incarnation and
/// `registered_at_millis`.
pub(crate) type Registration = (u64, u64);

/// One tick: send each owed report. Anything but a 200 leaves it owed, to
/// be sent again after a wait that grows from [`RETRY`]. The first refusal
/// for a copy and set is logged; the rest are debug.
pub(crate) async fn tick<L: ControlPlaneLink>(
    ids: &mut CopyIds,
    held: &[Held],
    registration: Registration,
    link: &L,
    node_id: &str,
    bearer: Option<&str>,
) {
    let (incarnation, registered_at_millis) = registration;
    for held in ids.owed(held, Instant::now()) {
        let report = RaftCopyId {
            key: held.key.clone(),
            copy_id: held.copy,
            incarnation,
            registered_at_millis,
            shard_incarnation: held.incarnation,
        };
        let sent = tokio::time::timeout(
            REQUEST_TIMEOUT,
            link.report_raft_copy_id(bearer, node_id, &report),
        )
        .await;
        let (outcome, reason) = match sent {
            Ok(Ok(answer)) if answer.status == reqwest::StatusCode::OK => (Outcome::Taken, None),
            // A 409 is a shard not on Raft, a placement not yet acknowledged
            // by every broker, or a control plane below the level that takes
            // these; a 404 one that predates them. All are tried again.
            Ok(Ok(answer)) => (Outcome::Refused, Some(format!("status {}", answer.status))),
            Ok(Err(err)) => (Outcome::Unanswered, Some(format!("{err:#}"))),
            Err(_) => (Outcome::Unanswered, Some("timed out".to_string())),
        };
        let refused = ids.record(&held, Instant::now(), outcome);
        if let Some(reason) = reason {
            if outcome == Outcome::Refused && refused == 1 {
                tracing::info!(shard = ?held.key, %reason, "the control plane did not take a Raft copy id; retrying with backoff");
            } else {
                tracing::debug!(shard = ?held.key, %reason, refused, "a Raft copy id was not taken; retrying");
            }
        }
    }
}

/// Report `shards`' copy ids every [`INTERVAL`] until `stop`. Nothing is
/// sent before this process has registered.
pub(crate) async fn run<L: ControlPlaneLink>(
    shards: Arc<RaftShards>,
    link: L,
    node_id: String,
    credential: Option<Arc<dyn Credential>>,
    registration: RootSequenceSlot,
    stop: CancellationToken,
) {
    let mut ids = CopyIds::default();
    let mut warned_untimed = false;
    let mut every = tokio::time::interval(INTERVAL);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = every.tick() => {}
        }
        let Some(registered) = registration.registration() else {
            // Copy id reports need a control plane that says when it
            // registered the node; an older one never does.
            if !warned_untimed && registration.registered_untimed() {
                warned_untimed = true;
                tracing::warn!(
                    "the control plane did not say when it registered this broker; Raft copy ids are not reported"
                );
            }
            continue;
        };
        let bearer = credential.as_ref().map(|credential| credential.bearer());
        let held = shards.held_copies();
        let report = tick(
            &mut ids,
            &held,
            registered,
            &link,
            &node_id,
            bearer.as_deref().map(String::as_str),
        );
        tokio::select! {
            () = stop.cancelled() => return,
            () = report => {}
        }
    }
}

#[cfg(test)]
mod tests;

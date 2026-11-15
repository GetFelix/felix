//! Raft floors: the committed log id of every group this broker leads,
//! reported to the control plane so a later start of the group can be held
//! to it.
//!
//! One request per tick carries every led group whose commit moved since the
//! control plane last accepted it. A group stays owed until a 200 answers a
//! request that carried it, so a control plane that refused the writes (409
//! before its metadata level allows them) gets every led group, idle ones
//! included, on the first tick it accepts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use felix_replication::controlplane_link::{ControlPlaneLink, RaftFloor};
use felix_replication::raft::LogPoint;
use felix_replication::reporter::Credential;
use tokio_util::sync::CancellationToken;

use super::{RaftShards, ShardKey};

/// How often the floors are reported.
pub(crate) const INTERVAL: Duration = Duration::from_secs(1);

/// Most entries in one request. More wait for the next tick, so the first
/// report of a broker leading many groups stays within the control plane's
/// request size.
const MAX_BATCH: usize = 1024;

/// How long a report may take before the tick gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A led group's committed log id and the last one the control plane took.
#[derive(Debug, Clone, Copy)]
struct Entry {
    at: LogPoint,
    acked: Option<LogPoint>,
}

/// The floors owed to the control plane, by shard and incarnation: a group
/// of a new incarnation is a different group with a floor of its own.
#[derive(Debug, Default)]
pub(crate) struct Floors {
    entries: BTreeMap<(ShardKey, u64), Entry>,
}

impl Floors {
    /// Take this tick's led groups. A group no longer in `led` is dropped,
    /// with whatever it still owed: it stopped here or stopped leading, and
    /// whoever leads it now reports. If it leads here again it starts owed.
    pub(crate) fn observe(&mut self, led: impl IntoIterator<Item = (ShardKey, u64, LogPoint)>) {
        let mut seen = BTreeMap::new();
        for (key, incarnation, at) in led {
            let acked = self
                .entries
                .remove(&(key.clone(), incarnation))
                .and_then(|entry| entry.acked);
            seen.insert((key, incarnation), Entry { at, acked });
        }
        self.entries = seen;
    }

    /// The groups whose commit the control plane has not taken yet, up to
    /// [`MAX_BATCH`]. Empty when nothing moved.
    pub(crate) fn batch(&self) -> Vec<RaftFloor> {
        self.entries
            .iter()
            .filter(|(_, entry)| entry.acked != Some(entry.at))
            .take(MAX_BATCH)
            .map(|((key, incarnation), entry)| RaftFloor {
                key: key.clone(),
                incarnation: *incarnation,
                // No group is re-formed yet.
                epoch: 0,
                index: entry.at.index,
                term: entry.at.term,
            })
            .collect()
    }

    /// The control plane took `batch`. An entry that moved since stays owed.
    pub(crate) fn accepted(&mut self, batch: &[RaftFloor]) {
        for floor in batch {
            if let Some(entry) = self
                .entries
                .get_mut(&(floor.key.clone(), floor.incarnation))
            {
                entry.acked = Some(LogPoint {
                    term: floor.term,
                    index: floor.index,
                });
            }
        }
    }
}

/// One tick: observe the led groups and send what is owed. Anything but a
/// 200 leaves it owed for the next tick.
pub(crate) async fn tick<L: ControlPlaneLink>(
    floors: &mut Floors,
    led: Vec<(ShardKey, u64, LogPoint)>,
    link: &L,
    node_id: &str,
    bearer: Option<&str>,
) {
    floors.observe(led);
    let batch = floors.batch();
    if batch.is_empty() {
        return;
    }
    let sent = tokio::time::timeout(
        REQUEST_TIMEOUT,
        link.report_raft_floors(bearer, node_id, &batch),
    )
    .await;
    match sent {
        Ok(Ok(answer)) if answer.status == reqwest::StatusCode::OK => floors.accepted(&batch),
        // A 409 is a control plane below the level that stores floors, and
        // a 404 one that predates them: both are retried until it takes them.
        Ok(Ok(answer)) => tracing::debug!(
            status = %answer.status,
            floors = batch.len(),
            "the control plane did not take the Raft floors; retrying"
        ),
        Ok(Err(err)) => {
            tracing::debug!(error = %err, "could not report the Raft floors; retrying")
        }
        Err(_) => tracing::debug!("the Raft floor report timed out; retrying"),
    }
}

/// Report `shards`' floors every [`INTERVAL`] until `stop`.
pub(crate) async fn run<L: ControlPlaneLink>(
    shards: Arc<RaftShards>,
    link: L,
    node_id: String,
    credential: Option<Arc<dyn Credential>>,
    stop: CancellationToken,
) {
    let mut floors = Floors::default();
    let mut every = tokio::time::interval(INTERVAL);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = every.tick() => {}
        }
        let bearer = credential.as_ref().map(|credential| credential.bearer());
        let report = tick(
            &mut floors,
            shards.led_floors(),
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

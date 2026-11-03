//! Leaderships elected by the replicas, at generations the control plane has
//! not named.
//!
//! "Electing without the control plane" in `docs/replication-design.md`;
//! `Elect` and `PreVote` in `docs/formal/FelixShard.tla`. A candidate records
//! its own standing here once its ballot is on disk, and a replica records
//! the candidate it answered. The routing feed lays both over the control
//! plane's routes, so the candidate opens the shard as a promotion and the
//! followers take its batches and send clients to it, until the control plane
//! names that generation or a newer one.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use felix_common::membership::ElectedLeadership;
use felix_router::ShardKey;
use felix_storage::disk_log::Membership;
use felix_wire::internal::ReplicaSetView;
use parking_lot::Mutex;
use tokio::sync::Notify;

mod candidate;

pub use candidate::spawn;

/// Elected leaderships this broker knows of, by shard.
#[derive(Debug)]
pub struct Elections {
    local_node_id: String,
    entries: Mutex<HashMap<ShardKey, Elected>>,
    /// The control plane's set for each shard with an entry, which the
    /// overlay hides from the router.
    recorded: Mutex<HashMap<ShardKey, Recorded>>,
    /// Woken on every change, for the routing feed to lay the new view over
    /// the control plane's.
    changed: Arc<Notify>,
}

/// One leadership a candidate stood for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elected {
    pub leader: String,
    /// The generation it stood at, above any the control plane named.
    pub generation: u64,
    /// The set it was fenced on. Standing changes no set, so this keeps the
    /// generation of the assignment that wrote it.
    pub set: Membership,
    /// While a set change is pending, the set being left, fenced too.
    pub leaving: Option<Membership>,
}

/// The set the control plane records for a shard, as its route names it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recorded {
    /// Each member, with the storage id the route names for it, in hex.
    pub members: BTreeMap<String, Option<String>>,
    /// A set change is pending.
    pub pending: bool,
}

impl Recorded {
    pub(crate) fn of_route(route: &felix_router::Route) -> Self {
        Self {
            members: crate::replica_set::view(route, None)
                .members
                .into_iter()
                .map(|member| {
                    let storage = member
                        .storage_id
                        .map(|id| felix_storage::StorageId::from_bytes(id).to_hex());
                    (member.node_id, storage)
                })
                .collect(),
            pending: route.pending.is_some(),
        }
    }

    /// Whether `kept` is this set: the same members, on the same copies
    /// where both name one, with no change pending.
    pub(crate) fn is(&self, kept: &Membership) -> bool {
        !self.pending
            && kept.members.len() == self.members.len()
            && kept.members.iter().all(|member| {
                self.members
                    .get(&member.node)
                    .is_some_and(|named| match (named, member.storage) {
                        (Some(named), Some(kept)) => *named == kept.to_hex(),
                        _ => true,
                    })
            })
    }
}

/// What a candidate's fence carries in place of the route's own set.
#[derive(Debug, Clone)]
pub(crate) struct Candidacy {
    pub(crate) set: ReplicaSetView,
    pub(crate) leaving: Option<Membership>,
}

impl Elections {
    pub fn new(local_node_id: impl Into<String>, changed: Arc<Notify>) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            entries: Mutex::new(HashMap::new()),
            recorded: Mutex::new(HashMap::new()),
            changed,
        }
    }

    /// Record `elected` for `key`, replacing one at an older generation. An
    /// equal or older one changes nothing: a generation has one leader.
    pub fn record(&self, key: ShardKey, elected: Elected) -> bool {
        let recorded = {
            let mut entries = self.entries.lock();
            match entries.get(&key) {
                Some(known) if known.generation >= elected.generation => false,
                _ => {
                    entries.insert(key, elected);
                    true
                }
            }
        };
        if recorded {
            self.changed.notify_one();
        }
        recorded
    }

    /// Forget `key`'s leadership at `generation`, if it is still the one
    /// recorded.
    pub fn withdraw(&self, key: &ShardKey, generation: u64) {
        let removed = {
            let mut entries = self.entries.lock();
            let current = entries
                .get(key)
                .is_some_and(|known| known.generation == generation);
            current && entries.remove(key).is_some()
        };
        if removed {
            self.changed.notify_one();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }

    pub fn get(&self, key: &ShardKey) -> Option<Elected> {
        self.entries.lock().get(key).cloned()
    }

    /// The leaderships above the generation `named` says the control plane's
    /// route has. One at or below it is dropped: the control plane adopted it
    /// or named a newer leader, and its route says which.
    pub fn overlay(&self, named: impl Fn(&ShardKey) -> Option<u64>) -> Vec<(ShardKey, Elected)> {
        let mut entries = self.entries.lock();
        entries.retain(|key, elected| named(key).is_none_or(|named| named < elected.generation));
        self.recorded
            .lock()
            .retain(|key, _| entries.contains_key(key));
        entries
            .iter()
            .map(|(key, elected)| (key.clone(), elected.clone()))
            .collect()
    }

    /// Note the control plane's set for `key`, which has an entry here.
    pub fn note_recorded(&self, key: ShardKey, recorded: Recorded) {
        self.recorded.lock().insert(key, recorded);
    }

    /// The control plane's set for `key` while an entry hides its route.
    pub(crate) fn recorded(&self, key: &ShardKey) -> Option<Recorded> {
        self.recorded.lock().get(key).cloned()
    }

    /// The shards this broker stands for or leads by election, with the
    /// generation of each.
    pub(crate) fn standing(&self) -> Vec<(ShardKey, u64)> {
        self.entries
            .lock()
            .iter()
            .filter(|(_, elected)| elected.leader == self.local_node_id)
            .map(|(key, elected)| (key.clone(), elected.generation))
            .collect()
    }

    /// This broker's own standing for `key` at `generation`, if that is what
    /// the route being fenced is.
    pub(crate) fn candidacy(&self, key: &ShardKey, generation: u64) -> Option<Candidacy> {
        let entries = self.entries.lock();
        let elected = entries.get(key)?;
        (elected.leader == self.local_node_id && elected.generation == generation).then(|| {
            Candidacy {
                set: crate::promotion::joint::view(&elected.set),
                leaving: elected.leaving.clone(),
            }
        })
    }

    /// What this broker's report says of `key` at `generation` when it
    /// leads it by election, for the control plane to adopt (rule 10).
    pub(crate) fn reported(&self, key: &ShardKey, generation: u64) -> Option<ElectedLeadership> {
        let entries = self.entries.lock();
        let elected = entries.get(key)?;
        (elected.leader == self.local_node_id && elected.generation == generation).then(|| {
            ElectedLeadership {
                leader: elected.leader.clone(),
                generation,
                replicas: elected
                    .set
                    .members
                    .iter()
                    .filter(|member| member.node != elected.leader)
                    .map(|member| member.node.clone())
                    .collect(),
                set_generation: elected.set.generation,
            }
        })
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
}

#[cfg(test)]
mod tests;

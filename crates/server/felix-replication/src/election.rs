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
    /// The generation each of this broker's standings is being fenced or
    /// caught up at, while the driver works on it.
    fencing: Mutex<HashMap<ShardKey, u64>>,
    /// The generation and incarnation each shard last opened at here once
    /// this broker's standing fenced it. Outlives the entry, which the
    /// overlay drops when the control plane adopts the leadership.
    opened: Mutex<HashMap<ShardKey, (u64, u64)>>,
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
    /// The set kept beside it, which the fence covers too. A candidate stands
    /// only when it names the same copies as `set`; a follower records what
    /// the candidate's fence carried.
    pub leaving: Option<Membership>,
    /// The stream or cache incarnation it stood on. A recreated stream is
    /// another log, which this leadership says nothing about.
    pub incarnation: u64,
}

/// The set the control plane records for a shard, as its route names it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recorded {
    /// The generation the control plane named.
    pub generation: u64,
    /// Each member, with the storage id the route names for it, in hex.
    pub members: BTreeMap<String, Option<String>>,
    /// A set change is pending.
    pub pending: bool,
}

impl Recorded {
    pub(crate) fn of_route(route: &felix_router::Route) -> Self {
        Self {
            generation: route.generation,
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

/// What the control plane's route names for a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Named {
    pub generation: u64,
    pub incarnation: u64,
    pub leader: String,
}

/// What a candidate's fence carries in place of the route's own set.
#[derive(Debug, Clone)]
pub(crate) struct Candidacy {
    pub(crate) set: ReplicaSetView,
    /// The set being left, fenced jointly with `set`.
    pub(crate) leaving: Option<Membership>,
}

impl Elections {
    pub fn new(local_node_id: impl Into<String>, changed: Arc<Notify>) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            entries: Mutex::new(HashMap::new()),
            recorded: Mutex::new(HashMap::new()),
            fencing: Mutex::new(HashMap::new()),
            opened: Mutex::new(HashMap::new()),
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
    /// or named a newer leader, and its route says which. So is one for a
    /// shard with no route, whose stream was deleted, or for another
    /// incarnation, whose stream was recreated.
    ///
    /// Except this broker's own standing when the route names another leader
    /// at that same generation. The two collided, and ballots let at most one
    /// of them open there. If this broker did, the other never can, so it
    /// keeps leading and reports the collision until the control plane names
    /// a newer generation. If it did not, the candidate withdraws it.
    pub fn overlay(&self, named: impl Fn(&ShardKey) -> Option<Named>) -> Vec<(ShardKey, Elected)> {
        let mut entries = self.entries.lock();
        entries.retain(|key, elected| {
            named(key).is_some_and(|named| {
                let collided = named.generation == elected.generation
                    && named.leader != elected.leader
                    && elected.leader == self.local_node_id;
                (named.generation < elected.generation || collided)
                    && named.incarnation == elected.incarnation
            })
        });
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
    pub fn recorded(&self, key: &ShardKey) -> Option<Recorded> {
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

    /// `key` opened here at `generation` on this broker's standing.
    pub(crate) fn opened(&self, key: &ShardKey, generation: u64) {
        let Some(incarnation) = self
            .entries
            .lock()
            .get(key)
            .filter(|elected| elected.generation == generation)
            .map(|elected| elected.incarnation)
        else {
            return;
        };
        self.opened
            .lock()
            .insert(key.clone(), (generation, incarnation));
    }

    /// Whether `key` opened here at `generation` and `incarnation` on this
    /// broker's standing. Its fence left the set at the generation it stood
    /// on, and no fence since kept a newer one.
    pub(crate) fn opened_elected(&self, key: &ShardKey, generation: u64, incarnation: u64) -> bool {
        self.opened.lock().get(key) == Some(&(generation, incarnation))
    }

    /// Whether `key` opened here at `generation` on this broker's standing,
    /// in any incarnation.
    pub(crate) fn opened_at(&self, key: &ShardKey, generation: u64) -> bool {
        self.opened
            .lock()
            .get(key)
            .is_some_and(|(opened, _)| *opened == generation)
    }

    /// Mark `key`'s standing at `generation` as being fenced until the
    /// returned guard drops, so the candidate does not give it up meanwhile.
    pub(crate) fn fencing(&self, key: &ShardKey, generation: u64) -> Fencing<'_> {
        self.fencing.lock().insert(key.clone(), generation);
        Fencing {
            elections: self,
            key: key.clone(),
            generation,
        }
    }

    /// Whether the driver is fencing or catching up `key` at `generation`.
    pub(crate) fn is_fencing(&self, key: &ShardKey, generation: u64) -> bool {
        self.fencing.lock().get(key) == Some(&generation)
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

/// A fence for a standing in progress; see [`Elections::fencing`].
pub(crate) struct Fencing<'a> {
    elections: &'a Elections,
    key: ShardKey,
    generation: u64,
}

impl Drop for Fencing<'_> {
    fn drop(&mut self) {
        let mut fencing = self.elections.fencing.lock();
        if fencing.get(&self.key) == Some(&self.generation) {
            fencing.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests;

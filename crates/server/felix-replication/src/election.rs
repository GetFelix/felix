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
use std::time::Duration;

use felix_common::membership::ElectedLeadership;
use felix_router::ShardKey;
use felix_storage::disk_log::Membership;
use felix_wire::internal::ReplicaSetView;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

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
    /// When each of this broker's standings was recorded or first fenced, by
    /// generation. Outlives the fence guard, so a fence retried pass after
    /// pass keeps its first time.
    fenced_since: Mutex<HashMap<ShardKey, (u64, Instant)>>,
    /// When this copy last stored a batch or took a fence from each shard's
    /// leader, for per-shard suspicion. A candidate's fence does not count.
    heard: Mutex<HashMap<ShardKey, Instant>>,
    /// Since when the control plane's route names another leader at the
    /// generation of this broker's own standing, by shard.
    collided: Mutex<HashMap<ShardKey, (u64, Instant)>>,
    /// When the driver last saw a copy refuse this broker's elected
    /// leadership at a generation while the rest of the set answered.
    refused_copy: Mutex<HashMap<ShardKey, (u64, Instant)>>,
    /// How long a copy defers to an elected leader that has not started its
    /// generation. See [`Self::with_defer_for`].
    defer_for: Duration,
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
            fenced_since: Mutex::new(HashMap::new()),
            heard: Mutex::new(HashMap::new()),
            collided: Mutex::new(HashMap::new()),
            refused_copy: Mutex::new(HashMap::new()),
            defer_for: crate::suspicion::DEFAULT_DEFER_FOR,
            changed,
        }
    }

    /// Bound how long an elected leader that never starts its generation
    /// holds the shard: a fencing leader refuses pre-votes, and a follower
    /// that took its fence does not stand, only this long. Without a bound a
    /// leader stuck fencing, or one that dropped the shard, holds the shard
    /// until the control plane names a new generation.
    pub fn with_defer_for(mut self, defer_for: Duration) -> Self {
        self.defer_for = defer_for;
        self
    }

    pub(crate) fn defer_for(&self) -> Duration {
        self.defer_for
    }

    /// Record `elected` for `key`, replacing one at an older generation. An
    /// equal or older one changes nothing: a generation has one leader.
    pub fn record(&self, key: ShardKey, elected: Elected) -> bool {
        let recorded = {
            let mut entries = self.entries.lock();
            match entries.get(&key) {
                Some(known) if known.generation >= elected.generation => false,
                _ => {
                    if elected.leader == self.local_node_id {
                        self.note_standing(&key, elected.generation);
                    }
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
        let mut collisions = Vec::new();
        entries.retain(|key, elected| {
            named(key).is_some_and(|named| {
                let collided = named.generation == elected.generation
                    && named.leader != elected.leader
                    && elected.leader == self.local_node_id;
                let keep = (named.generation < elected.generation || collided)
                    && named.incarnation == elected.incarnation;
                if keep && collided {
                    collisions.push((key.clone(), elected.generation));
                }
                keep
            })
        });
        self.recorded
            .lock()
            .retain(|key, _| entries.contains_key(key));
        {
            let mut since = self.collided.lock();
            since.retain(|key, (generation, _)| {
                collisions.iter().any(|(k, g)| k == key && g == generation)
            });
            let now = Instant::now();
            for (key, generation) in collisions {
                since.entry(key).or_insert((generation, now));
            }
        }
        self.refused_copy.lock().retain(|key, (generation, _)| {
            entries
                .get(key)
                .is_some_and(|elected| elected.generation == *generation)
        });
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
        self.note_standing(key, generation);
        Fencing {
            elections: self,
            key: key.clone(),
            generation,
        }
    }

    fn note_standing(&self, key: &ShardKey, generation: u64) {
        let mut since = self.fenced_since.lock();
        if since.get(key).is_none_or(|(at, _)| *at != generation) {
            since.insert(key.clone(), (generation, Instant::now()));
        }
    }

    /// Whether this broker stands for `key` at `generation`, recorded and
    /// not yet withdrawn, whether or not the driver has reached its fence.
    pub(crate) fn stands_at(&self, key: &ShardKey, generation: u64) -> bool {
        self.entries.lock().get(key).is_some_and(|elected| {
            elected.leader == self.local_node_id && elected.generation == generation
        })
    }

    /// Whether this broker recorded or first fenced its standing for `key` at
    /// `generation` within [`Self::defer_for`].
    pub(crate) fn fenced_within_defer(&self, key: &ShardKey, generation: u64) -> bool {
        self.fenced_since
            .lock()
            .get(key)
            .is_some_and(|(at, since)| *at == generation && since.elapsed() < self.defer_for)
    }

    /// Note that this copy heard `key`'s leader: a batch stored or a
    /// leader's fence taken.
    pub(crate) fn heard_leader(&self, key: &ShardKey) {
        let now = Instant::now();
        let mut heard = self.heard.lock();
        // On every stamped batch: no key clone once the shard is known.
        match heard.get_mut(key) {
            Some(at) => *at = now,
            None => {
                heard.insert(key.clone(), now);
            }
        }
    }

    /// Drop what was heard for shards this copy no longer follows.
    pub(crate) fn forget_heard_except(&self, followed: &std::collections::HashSet<ShardKey>) {
        self.heard.lock().retain(|key, _| followed.contains(key));
    }

    /// Note that a copy of `key`'s set refuses this broker's elected
    /// leadership at `generation` while the rest of the set answers it.
    pub(crate) fn note_refused_copy(&self, key: &ShardKey, generation: u64) {
        let now = Instant::now();
        let mut refused = self.refused_copy.lock();
        match refused.get_mut(key) {
            Some(seen) => *seen = (generation, now),
            None => {
                refused.insert(key.clone(), (generation, now));
            }
        }
    }

    /// This broker's open standings a copy has refused within `bound`, while
    /// the control plane's route has named another leader at the same
    /// generation for at least `bound`: that copy can follow neither, and
    /// nothing renames the shard. Each with its generation.
    pub(crate) fn collided_and_refused(&self, bound: Duration) -> Vec<(ShardKey, u64)> {
        // Collected before `entries` is locked: the overlay takes that first.
        let due: Vec<(ShardKey, u64)> = {
            let collided = self.collided.lock();
            let refused = self.refused_copy.lock();
            collided
                .iter()
                .filter(|(key, (generation, since))| {
                    since.elapsed() >= bound
                        && refused
                            .get(*key)
                            .is_some_and(|(at, seen)| at == generation && seen.elapsed() < bound)
                })
                .map(|(key, (generation, _))| (key.clone(), *generation))
                .collect()
        };
        due.into_iter()
            .filter(|(key, generation)| {
                self.opened_at(key, *generation) && self.stands_at(key, *generation)
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn refused_copy_at(&self, key: &ShardKey) -> Option<u64> {
        self.refused_copy
            .lock()
            .get(key)
            .map(|(generation, _)| *generation)
    }

    pub(crate) fn last_heard(&self, key: &ShardKey) -> Option<Instant> {
        self.heard.lock().get(key).copied()
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

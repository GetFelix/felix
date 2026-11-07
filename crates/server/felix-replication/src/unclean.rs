//! A broker that restarted after a crash that may have cost it records it
//! acknowledged: its storage id rotated at startup (`run_state::begin`).
//!
//! A copy of a shard this broker led before the crash may be short of what
//! it acknowledged, so it must not lead that shard again from it, and must
//! not answer a promotion fence for it: either counts the short copy as a
//! vote, and the records only the lost tail held can be overwritten. See
//! "A leader's own crash" in `docs/replication-design.md` (#1295).

use felix_common::random::{HashMap, HashSet};
use std::sync::Arc;

use crate::ShardKey;

/// What this process knows about the copies it led before the crash.
#[derive(Debug)]
pub struct UncleanStart {
    node_id: Arc<str>,
    /// The generation this process took leadership of each shard at. A ballot
    /// at that generation naming this broker is this process's own.
    led: parking_lot::Mutex<HashMap<ShardKey, u64>>,
    /// Shards a leader has shipped to this process: their copies have been
    /// compared with a leader's since the crash.
    vouched: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards whose copy a leader has shipped level through both its
    /// generation's start and the commit offset it carried: the copy holds
    /// everything that leader acknowledged ([`Self::trust`]).
    trusted: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Copies this broker led before the crash that took a newer leader's
    /// ballot from a batch, which has not yet brought them a record of that
    /// leader's generation: the ballot no longer says what they lost.
    doubted: parking_lot::Mutex<HashSet<ShardKey>>,
    /// The last ballot this process wrote standing for election on each
    /// shard, with what the copy said of the crash before its first standing.
    stood: parking_lot::Mutex<HashMap<ShardKey, Stood>>,
    /// The crash lost no ballot: the directory was neither restored nor one
    /// that already replaced a lost copy, and ballots are always synced.
    kept_promises: bool,
}

/// A ballot this process wrote for its own candidacy. It replaced the ballot
/// the crash left, so what that ballot said is kept here.
#[derive(Debug, Clone, Copy)]
struct Stood {
    generation: u64,
    /// The copy was the one this broker led before the crash, or had been
    /// given up.
    led_before: bool,
    /// The generation the copy had accepted from another leader before this
    /// process stood on it, if any.
    followed: Option<u64>,
}

impl UncleanStart {
    pub fn new(node_id: impl Into<Arc<str>>) -> Self {
        Self {
            node_id: node_id.into(),
            led: parking_lot::Mutex::new(HashMap::default()),
            vouched: parking_lot::Mutex::new(HashSet::default()),
            trusted: parking_lot::Mutex::new(HashSet::default()),
            stood: parking_lot::Mutex::new(HashMap::default()),
            doubted: parking_lot::Mutex::new(HashSet::default()),
            kept_promises: false,
        }
    }

    /// Note that the crash lost no ballot this directory took.
    pub fn with_kept_promises(mut self, kept: bool) -> Self {
        self.kept_promises = kept;
        self
    }

    /// Whether every ballot this directory took survived the crash.
    pub fn kept_promises(&self) -> bool {
        self.kept_promises
    }

    /// A leader shipped `key` to this process, and its copy now holds a
    /// record of that leader's generation, so it has been compared with a
    /// leader's since the crash. The leader wrote that record after every
    /// record it held, everything acknowledged before the crash included, so
    /// a batch that brought back only part of what the crash lost does not
    /// count (#2029).
    pub fn vouch(&self, key: &ShardKey) {
        self.doubted.lock().remove(key);
        self.vouched.lock().insert(key.clone());
    }

    /// A leader shipped `key` a batch that left this copy level through both
    /// the start of the leader's generation and the batch's commit offset,
    /// which is above zero. Stricter than [`Self::vouch`]: right after an open
    /// the commit offset can sit below records an earlier generation
    /// acknowledged, and the start alone is short of what the leader
    /// acknowledged since (`VouchBelowStart`, `StandInNoCatchUp` in
    /// `docs/formal/FelixShardUnified.tla`).
    pub(crate) fn trust(&self, key: &ShardKey) {
        self.trusted.lock().insert(key.clone());
    }

    /// Whether [`Self::trust`] has been called for `key` in this process.
    pub(crate) fn trusted(&self, key: &ShardKey) -> bool {
        self.trusted.lock().contains(key)
    }

    /// `log`, `key`'s copy, is about to take a leader's ballot at
    /// `generation` from a batch. A copy this broker led before the crash
    /// stays one until [`Self::vouch`], though the new ballot names another
    /// leader.
    pub(crate) fn accepting(&self, key: &ShardKey, log: &felix_broker::StreamLog, generation: u64) {
        let accepted = log.accepted_generation();
        let raises = generation > accepted || (generation == accepted && log.gave_up());
        if raises && self.led_before(key, log, generation) {
            self.doubted.lock().insert(key.clone());
        }
    }

    /// Whether `log`, `key`'s copy, may be short of what this broker
    /// acknowledged before the crash and has not been compared with a
    /// leader's since, while this broker is asked to lead it at `generation`:
    /// the copy it led ([`Self::led_before`]), or one it followed at an
    /// older generation, as a follower named leader that crashed before it
    /// took the generation would.
    pub fn unvouched(
        &self,
        key: &ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
    ) -> bool {
        if self.led_before(key, log, generation) {
            return true;
        }
        let accepted = log.accepted_generation();
        if self.leads_at(key, accepted) || self.vouched.lock().contains(key) {
            return false;
        }
        let followed = match self.own_ballot(key, log) {
            Some(stood) => stood.followed,
            None => held(log).then_some(accepted),
        };
        followed.is_some_and(|followed| followed < generation)
    }

    /// This process is about to write its own ballot for `key` at
    /// `generation`, standing for election on `log`. Called before the
    /// ballot, and only while `generation` is above the one `log` accepted,
    /// so a ballot naming this broker at `generation` is this process's own.
    ///
    /// What the copy said of the crash before the first such ballot is kept:
    /// a copy this broker led before the crash stays one after it stands.
    pub(crate) fn stand(&self, key: &ShardKey, log: &felix_broker::StreamLog, generation: u64) {
        let accepted = log.accepted_generation();
        if held(log) && generation <= accepted {
            // The ballot write fails too: a newer one landed meanwhile.
            return;
        }
        let before = self.own_ballot(key, log).unwrap_or_else(|| Stood {
            generation,
            led_before: log.gave_up() || self.led_before(key, log, generation),
            followed: (held(log) && !self.leads_at(key, accepted)).then_some(accepted),
        });
        self.stood.lock().insert(
            key.clone(),
            Stood {
                generation,
                ..before
            },
        );
    }

    /// The candidacy that wrote `log`'s ballot, if this process wrote it.
    fn own_ballot(&self, key: &ShardKey, log: &felix_broker::StreamLog) -> Option<Stood> {
        let stood = *self.stood.lock().get(key)?;
        let own = stood.generation == log.accepted_generation()
            && log.accepted_leader().as_deref() == Some(&*self.node_id);
        own.then_some(stood)
    }

    /// This process leads `key` at `generation`.
    pub fn lead(&self, key: &ShardKey, generation: u64) {
        self.led.lock().insert(key.clone(), generation);
    }

    /// Whether this process took the lead of `key` at `generation`.
    pub fn leads_at(&self, key: &ShardKey, generation: u64) -> bool {
        self.led.lock().get(key) == Some(&generation)
    }

    /// Whether `log` is `key`'s copy as this broker led it before the crash:
    /// its accepted generation was this broker's own, and no newer leader has
    /// been accepted here since. `generation` is the one being asked about.
    ///
    /// Without a ballot naming a leader only an assignment at the accepted
    /// generation itself says so: the control plane names one leader per
    /// generation, so a broker named at a generation it accepted led it.
    ///
    /// A ballot this process wrote standing for election ([`Self::stand`])
    /// says nothing of the crash; what the copy said before it does. Nor does
    /// a newer leader's ballot taken from a batch before the copy is vouched
    /// for ([`Self::accepting`]).
    pub fn led_before(
        &self,
        key: &ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
    ) -> bool {
        let accepted = log.accepted_generation();
        if !held(log) || self.leads_at(key, accepted) {
            return false;
        }
        if self.doubted.lock().contains(key) {
            return true;
        }
        if let Some(stood) = self.own_ballot(key, log) {
            return stood.led_before;
        }
        match log.accepted_leader() {
            // Its own ballot, or one it already gave up.
            Some(leader) => *leader == *self.node_id || leader.is_empty(),
            None => generation == accepted,
        }
    }

    /// This broker's node id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Whether `log` ever took a generation: a ballot, a recorded generation, or
/// one above 0. Generation 0 is a real one (a shard placed before
/// incarnations starts there), so the number alone cannot say.
pub(crate) fn held(log: &felix_broker::StreamLog) -> bool {
    log.accepted_generation() > 0
        || log.accepted_leader().is_some()
        || !log.generations().is_empty()
}

#[cfg(test)]
mod tests;

//! The seal leader's round in a shard's switch from the hybrid to Raft: who
//! answered the seal's fence and from which log start, when the record at
//! `N` may be claimed, who acknowledged it, and the one report to the
//! control plane.
//!
//! The steps on disk are `felix_storage::disk_log::switch::seal`'s. This is
//! the counting around them, `SealClaim`, `FollowerAck` and `Report` in
//! `docs/formal/FelixRaftSwitch.tla`. See `docs/per-shard-raft.md`,
//! "Moving a shard from the hybrid to Raft".

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use felix_storage::StorageId;
use felix_storage::disk_log::switch::SwitchBase;
use felix_storage::log::Offset;

/// Where a copy's three logs start: the shard's, its dead letters' and its
/// cursors'.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogStarts {
    pub log: Offset,
    pub dead_letters: Offset,
    pub cursors: Offset,
}

impl LogStarts {
    fn max(self, other: Self) -> Self {
        Self {
            log: self.log.max(other.log),
            dead_letters: self.dead_letters.max(other.dead_letters),
            cursors: self.cursors.max(other.cursors),
        }
    }
}

/// What the seal leader reports once a majority holds the record at `N`:
/// the base and the copies that may vote from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealReport {
    pub base: SwitchBase,
    /// Each copy that acknowledged, by node and copy id, the leader's own
    /// included.
    pub voters: BTreeMap<String, StorageId>,
}

/// One seal at one generation, as its leader counts it.
#[derive(Debug, Clone)]
pub struct SealRound {
    generation: u64,
    set: BTreeSet<String>,
    own: (String, StorageId),
    starts: BTreeMap<String, LogStarts>,
    base: Option<SwitchBase>,
    acks: BTreeMap<String, StorageId>,
    reported: bool,
    abandoned: bool,
}

impl SealRound {
    /// A round for the seal at `generation` over the replica set `set`, led
    /// by `own` (this node and its copy id), whose logs start at
    /// `own_starts`.
    pub fn new(
        generation: u64,
        set: impl IntoIterator<Item = String>,
        own: (String, StorageId),
        own_starts: LogStarts,
    ) -> Self {
        let mut set: BTreeSet<String> = set.into_iter().collect();
        set.insert(own.0.clone());
        Self {
            generation,
            set,
            starts: BTreeMap::from([(own.0.clone(), own_starts)]),
            own,
            base: None,
            acks: BTreeMap::new(),
            reported: false,
            abandoned: false,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// `node` took the seal's fence, and its logs start at `starts`. Only
    /// before the claim: the start is fixed once the record names it.
    pub fn answered(&mut self, node: &str, starts: LogStarts) {
        if self.base.is_none() && self.set.contains(node) {
            self.starts.insert(node.to_string(), starts);
        }
    }

    /// Where the digests start: the highest start among the copies that
    /// answered, so each of them holds every record the digests cover. A
    /// copy that starts higher cannot acknowledge.
    pub fn starts(&self) -> LogStarts {
        self.starts
            .values()
            .fold(LogStarts::default(), |all, starts| all.max(*starts))
    }

    /// Whether the leader may append the record at `N` now: a majority has
    /// taken the fence, and `quiesced`, no write admitted before the seal
    /// is still inside the shard's fence. A write in flight would take an
    /// offset above `N`, or be acknowledged after the seal and then lost.
    pub fn may_claim(&self, quiesced: bool) -> bool {
        !self.abandoned && self.base.is_none() && quiesced && self.majority(self.starts.len())
    }

    /// The leader appended the record naming `base`, which is on its disk.
    pub fn claimed(&mut self, base: SwitchBase) {
        if self.base.is_none() && base.generation == self.generation {
            self.base = Some(base);
            self.acks.insert(self.own.0.clone(), self.own.1);
        }
    }

    /// `node`, as the copy `copy`, acknowledged the record at `N`, which it
    /// holds on disk. A node outside the set, or before the claim, counts
    /// for nothing.
    pub fn acked(&mut self, node: &str, copy: StorageId) {
        if self.base.is_some() && !self.reported && self.set.contains(node) {
            self.acks.insert(node.to_string(), copy);
        }
    }

    /// The report, once a majority of the set holds the record at `N`.
    /// Given once per round: the control plane's write changes the set's
    /// version, so a second report of the same seal would fail its compare
    /// anyway, and no later acknowledgement changes who was reported.
    pub fn report(&mut self) -> Option<SealReport> {
        let base = self.base?;
        if self.abandoned || self.reported || !self.majority(self.acks.len()) {
            return None;
        }
        self.reported = true;
        Some(SealReport {
            base,
            voters: self.acks.clone(),
        })
    }

    /// Give the seal up: this leader restarted or lost power before it
    /// reported, so its count is gone. Nothing is reported from this round;
    /// the control plane seals again at a later generation.
    pub fn abandon(&mut self) {
        if !self.reported {
            self.abandoned = true;
        }
    }

    fn majority(&self, count: usize) -> bool {
        2 * count > self.set.len()
    }
}

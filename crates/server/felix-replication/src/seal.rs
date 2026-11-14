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
use std::net::SocketAddr;

use felix_storage::StorageId;
use felix_storage::disk_log::DiskLog;
use felix_storage::disk_log::GenerationCheck;
use felix_storage::disk_log::switch::SwitchBase;
use felix_storage::disk_log::switch::seal::SealRefused;
use felix_storage::log::{AppendOnlyLog, Offset, ReadRange};
use felix_wire::internal::{
    Fence, InternalMessage, ReplicaLog, SealAck, SealFence, SealStarts, ShardRef,
};

use crate::peer::PeerRequester;

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
    /// A record naming other starts than the answers did is not this
    /// round's and counts for nothing.
    pub fn claimed(&mut self, base: SwitchBase) {
        let starts = self.starts();
        let names_starts = base.start == starts.log
            && base.dead_letters.start == starts.dead_letters.min(base.dead_letters.end)
            && base.cursors.start == starts.cursors.min(base.cursors.end);
        if self.base.is_none() && base.generation == self.generation && names_starts {
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

    /// One pass of the round as its leader runs it: promise the seal on its
    /// own copy, fence each copy of `peers` that has not answered, claim the
    /// record at `N` once [`Self::may_claim`] allows, then send the record
    /// to each copy that has not acknowledged it, and report once a
    /// majority holds it. `shard` is the stream shard at the seal's
    /// generation. Called again until it reports or gives up; a follower
    /// still catching up to `N` acknowledges on a later pass.
    ///
    /// A leader that finds its own claim already made, by an earlier run
    /// whose count is gone, abandons the round: one record per generation,
    /// so the control plane seals again at a later one.
    pub async fn step<R: PeerRequester>(
        &mut self,
        requester: &R,
        shard: &ShardRef,
        peers: &[(String, SocketAddr)],
        logs: SealLogs<'_>,
        quiesced: bool,
    ) -> felix_storage::Result<SealProgress> {
        if self.abandoned {
            return Ok(SealProgress::Abandoned);
        }
        if self.base.is_none() {
            match Self::promise(logs.log, self.generation, &self.own).await? {
                Ok(()) => {}
                Err(why) => return Ok(self.give_up(why)),
            }
            for (node, addr) in peers {
                if self.starts.contains_key(node) || !self.set.contains(node) {
                    continue;
                }
                let fence = InternalMessage::SealFence(SealFence {
                    fence: Fence {
                        correlation_id: 0,
                        shard: shard.clone(),
                        log: ReplicaLog::Stream,
                        election: None,
                    },
                    sealer: self.own.1.to_bytes(),
                });
                if let Ok(InternalMessage::SealFenceOk(ok)) =
                    requester.request(node, *addr, fence).await
                {
                    self.answered(node, ok.starts.into());
                }
            }
            if !self.may_claim(quiesced) {
                return Ok(SealProgress::Waiting);
            }
            let starts = self.starts();
            match logs
                .log
                .claim_seal(
                    self.own.1,
                    starts.log,
                    (logs.dead_letters, starts.dead_letters),
                    (logs.cursors, starts.cursors),
                )
                .await?
            {
                Ok(base) => self.claimed(base),
                // Any refusal may come after the claim's mark, which uses
                // up the generation.
                Err(why) => return Ok(self.give_up(why)),
            }
        }
        let Some(base) = self.base else {
            return Ok(SealProgress::Waiting);
        };
        let record = logs
            .log
            .read_range_with_trims(ReadRange {
                start: base.offset,
                max_bytes: 1,
            })
            .await?
            .into_iter()
            .find(|record| record.offset == base.offset);
        let Some(record) = record else {
            return Ok(self.give_up(SealRefused::NotASeal));
        };
        for (node, addr) in peers {
            if self.acks.contains_key(node) || !self.set.contains(node) {
                continue;
            }
            let ack = InternalMessage::SealAck(SealAck {
                correlation_id: 0,
                shard: shard.clone(),
                sealer: self.own.1.to_bytes(),
                offset: record.offset,
                timestamp_micros: record.timestamp_micros,
                payload: record.payload.clone(),
            });
            if let Ok(InternalMessage::SealAckOk(ok)) = requester.request(node, *addr, ack).await {
                self.acked(node, StorageId::from_bytes(ok.copy));
            }
        }
        Ok(self
            .report()
            .map_or(SealProgress::Waiting, SealProgress::Reported))
    }

    /// The leader's own promise to the seal at `generation`, on its copy
    /// `own` of `log`. Taken before the round is built, so the starts the
    /// round begins from are read under it, and again by every pass until
    /// the claim. A claim already made at this generation is an earlier
    /// run's, and refuses.
    pub async fn promise(
        log: &DiskLog,
        generation: u64,
        own: &(String, StorageId),
    ) -> felix_storage::Result<std::result::Result<(), SealRefused>> {
        if let Some(promise) = log.seal_promise()
            && promise.generation == generation
        {
            if promise.claimed {
                return Ok(Err(SealRefused::Claimed { generation }));
            }
            if promise.sealer == own.1 {
                return Ok(Ok(()));
            }
        }
        match log.promise_seal(generation, &own.0, own.1).await? {
            Ok(GenerationCheck::Raised | GenerationCheck::Current) => Ok(Ok(())),
            Ok(_) => Ok(Err(SealRefused::NoPromise)),
            Err(why) => Ok(Err(why)),
        }
    }

    fn give_up(&mut self, why: SealRefused) -> SealProgress {
        self.abandon();
        SealProgress::Refused(why)
    }
}

/// The seal leader's three logs for the shard.
#[derive(Clone, Copy)]
pub struct SealLogs<'a> {
    pub log: &'a DiskLog,
    pub dead_letters: &'a DiskLog,
    pub cursors: &'a DiskLog,
}

/// Where a pass of [`SealRound::step`] left the round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealProgress {
    /// Not yet: too few answers, a write still in flight, or too few
    /// copies holding the record. Run another pass.
    Waiting,
    /// A majority holds the record: send this to the control plane.
    Reported(SealReport),
    /// The round is given up, for this reason. Nothing is reported from
    /// it; the control plane seals again at a later generation, which ends
    /// every copy's promise.
    Refused(SealRefused),
    /// Given up earlier.
    Abandoned,
}

impl From<SealStarts> for LogStarts {
    fn from(starts: SealStarts) -> Self {
        Self {
            log: starts.log,
            dead_letters: starts.dead_letters,
            cursors: starts.cursors,
        }
    }
}

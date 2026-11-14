//! A shard's switch from the hybrid to Raft, as the control plane records
//! it: the seal it ordered, and the one report it accepted.
//!
//! `docs/per-shard-raft.md`, "Moving a shard from the hybrid to Raft", and
//! `CpWrite`, `Reseal` and `Failover` in `docs/formal/FelixRaftSwitch.tla`.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::ShardAssignment;
use crate::model::node::validate_storage_id;

/// The seal the control plane ordered for a shard, and its outcome.
///
/// Store-owned, like `generation`: only the seal, report and give-up writes
/// set it, and whatever a caller passes on another write is replaced.
///
/// While `report` is absent the shard is a hybrid shard that is closed. The
/// order stands only at `generation`: any write that moves the assignment to
/// a later generation drops it, which is how a seal is given up. Once
/// `report` is set the shard's engine is Raft, and no hybrid write is taken
/// for it again.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct ShardSwitch {
    /// The seal's generation: the assignment's generation when it was
    /// ordered.
    pub generation: u64,
    /// The node that seals, the shard's leader at `generation`.
    pub sealer: String,
    /// The sealer's storage identity (32 lowercase hex) when the seal was
    /// ordered, if it registered one. A report must name the sealer under
    /// it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealer_copy: Option<String>,
    /// The seal report the control plane accepted. One per generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<SealReport>,
}

impl ShardSwitch {
    /// Whether the engine is Raft: a report was accepted.
    pub fn switched(&self) -> bool {
        self.report.is_some()
    }
}

/// What a seal leader reports once a majority holds the record at `offset`:
/// the base of the shard's Raft log, and the copies that may vote from it.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct SealReport {
    /// The seal's generation, which the record at `offset` carries.
    pub generation: u64,
    /// The offset of the seal's generation-start record. The first Raft
    /// entry is the next one.
    pub offset: u64,
    /// Where the digest starts: the highest log start among the copies that
    /// answered the seal.
    pub start: u64,
    /// SHA-256 of the records in `start..offset` (64 lowercase hex).
    pub digest: String,
    /// The shard's dead-letter log as the seal leader held it.
    pub dead_letters: SealedSideLog,
    /// The shard's group-cursor log as the seal leader held it.
    pub cursors: SealedSideLog,
    /// Each copy that acknowledged the record, by node id, with its storage
    /// identity (32 lowercase hex). These are the shard's voters.
    pub voters: BTreeMap<String, String>,
}

/// One side log's range at the seal, and its digest (64 lowercase hex).
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct SealedSideLog {
    pub start: u64,
    pub end: u64,
    pub digest: String,
}

/// Why a seal report cannot be the one the control plane records.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SealReportError {
    #[error("generation 0 is never a seal's")]
    NoGeneration,
    #[error("the seal starts at {start}, above its record at {offset}")]
    StartAboveRecord { start: u64, offset: u64 },
    #[error("a side log's range starts at {start}, above its end {end}")]
    SideStartAboveEnd { start: u64, end: u64 },
    #[error("a digest must be 64 lowercase hex digits")]
    Digest,
    #[error("voter {0}: a copy id must be 32 lowercase hex digits")]
    CopyId(String),
    #[error("voter {0} is not in the shard's replica set")]
    NotInSet(String),
    #[error("{voters} of {set} copies acknowledged, which is not a majority")]
    NoMajority { voters: usize, set: usize },
    #[error("the sealing copy on {0} is not among the voters")]
    SealerMissing(String),
    #[error("the seal was ordered from another copy on {0}")]
    AnotherSealer(String),
}

impl SealReport {
    /// What a report must say whatever shard it is for.
    pub fn validate(&self) -> Result<(), SealReportError> {
        if self.generation == 0 {
            return Err(SealReportError::NoGeneration);
        }
        if self.start > self.offset {
            return Err(SealReportError::StartAboveRecord {
                start: self.start,
                offset: self.offset,
            });
        }
        for side in [&self.dead_letters, &self.cursors] {
            if side.start > side.end {
                return Err(SealReportError::SideStartAboveEnd {
                    start: side.start,
                    end: side.end,
                });
            }
        }
        let digests = [
            &self.digest,
            &self.dead_letters.digest,
            &self.cursors.digest,
        ];
        if !digests.into_iter().all(|digest| is_digest(digest)) {
            return Err(SealReportError::Digest);
        }
        for (node, copy) in &self.voters {
            if validate_storage_id(copy).is_err() {
                return Err(SealReportError::CopyId(node.clone()));
            }
        }
        Ok(())
    }

    /// Whether this report can be recorded for `assignment`, whose seal
    /// `switch` ordered. The voters become the only copies that can elect a
    /// Raft leader, so they must be a majority of the set the hybrid's fence
    /// counted, and the copy that sealed must be one of them: it is the one
    /// copy known to hold every acknowledged record.
    pub fn names_voters_of(
        &self,
        assignment: &ShardAssignment,
        switch: &ShardSwitch,
    ) -> Result<(), SealReportError> {
        self.validate()?;
        let set: Vec<&String> = assignment.nodes().collect();
        if let Some(stranger) = self.voters.keys().find(|node| !set.contains(node)) {
            return Err(SealReportError::NotInSet(stranger.clone()));
        }
        if 2 * self.voters.len() <= set.len() {
            return Err(SealReportError::NoMajority {
                voters: self.voters.len(),
                set: set.len(),
            });
        }
        let Some(copy) = self.voters.get(&switch.sealer) else {
            return Err(SealReportError::SealerMissing(switch.sealer.clone()));
        };
        if switch
            .sealer_copy
            .as_ref()
            .is_some_and(|ordered| ordered != copy)
        {
            return Err(SealReportError::AnotherSealer(switch.sealer.clone()));
        }
        Ok(())
    }
}

fn is_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Why a shard cannot be sealed now.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SealOrderError {
    #[error("only a stream shard can switch to Raft")]
    NotAStream,
    #[error("the shard is not active")]
    NotActive,
    #[error("a move or a replica set change is in progress")]
    Moving,
    #[error("the shard already switched to Raft")]
    Switched,
}

impl ShardAssignment {
    /// Whether a seal can be ordered for the shard as it stands. A seal
    /// counts the replica set as it is, so it waits for any change to it.
    pub fn can_seal(&self) -> Result<(), SealOrderError> {
        if self.key.kind != super::ShardKind::Stream {
            return Err(SealOrderError::NotAStream);
        }
        if self.switched() {
            return Err(SealOrderError::Switched);
        }
        if self.state != super::ShardState::Active {
            return Err(SealOrderError::NotActive);
        }
        if self.successor.is_some()
            || self.joining.is_some()
            || self.pending_set.is_some()
            || !self.learners.is_empty()
        {
            return Err(SealOrderError::Moving);
        }
        Ok(())
    }

    /// The seal order standing at this assignment's generation, if any.
    pub fn sealing(&self) -> Option<&ShardSwitch> {
        self.switch
            .as_ref()
            .filter(|switch| !switch.switched() && switch.generation == self.generation)
    }

    /// Whether the shard's engine is Raft.
    pub fn switched(&self) -> bool {
        self.switch.as_ref().is_some_and(ShardSwitch::switched)
    }

    /// The order to seal this assignment at `generation`, led by its leader.
    pub(crate) fn seal_order(&self, generation: u64) -> ShardSwitch {
        ShardSwitch {
            generation,
            sealer: self.leader.clone(),
            sealer_copy: self.member_storage_ids.get(&self.leader).cloned(),
            report: None,
        }
    }
}

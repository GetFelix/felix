//! The broker a Raft shard's group elected, as its leaders' floor reports
//! show it. Placement reads it when it renames a leader that is down, so it
//! names the member already leading instead of one the leader cannot hand
//! over to while a voter is down (#3041). Advisory: the group alone decides
//! who leads.
//!
//! `docs/per-shard-raft.md`, "Leaders follow placement".
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{RaftFloorReport, ShardAssignment};

/// The broker that reported leading a Raft shard's group at the highest
/// term, in the group the record runs.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct RaftLeader {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// The reform epoch the group runs. 0 until a re-form.
    pub epoch: u64,
    /// The term of the committed entry the report carried.
    pub term: u64,
    /// The reporting broker.
    pub node: String,
}

impl ShardAssignment {
    /// Note that `node_id` reported leading the group, from its floor
    /// report. Kept only for this record's incarnation and epoch, from a
    /// broker in the set that its reported voters (if any) include, and only
    /// at a higher term than the one kept. Within a term only the first
    /// reporter counts: a leader reports a committed id from an earlier term
    /// until its own first entry commits, so a later report at that term
    /// can be a deposed leader's. Returns whether this changed the record.
    pub(crate) fn note_raft_leader(&mut self, node_id: &str, report: &RaftFloorReport) -> bool {
        let (incarnation, epoch) = (self.incarnation, self.group_epoch());
        if report.incarnation != incarnation || report.epoch != epoch {
            return false;
        }
        if !self.raft_set().contains(node_id)
            || report
                .voters
                .as_ref()
                .is_some_and(|voters| !voters.contains_key(node_id))
        {
            return false;
        }
        let newer = self.raft_leader.as_ref().is_none_or(|held| {
            held.incarnation != incarnation || held.epoch != epoch || report.term > held.term
        });
        if !newer {
            return false;
        }
        self.raft_leader = Some(RaftLeader {
            incarnation,
            epoch,
            term: report.term,
            node: node_id.to_string(),
        });
        true
    }

    /// The broker the group last reported electing, in the group this
    /// record runs. `None` before any report from it.
    pub(crate) fn reported_leader(&self) -> Option<&str> {
        self.raft_leader
            .as_ref()
            .filter(|held| held.incarnation == self.incarnation && held.epoch == self.group_epoch())
            .map(|held| held.node.as_str())
    }

    /// Keep `previous`'s reported leader when this write is at the same
    /// stream incarnation: a placement write must never drop it.
    pub(crate) fn carry_raft_leader(&mut self, previous: Option<&ShardAssignment>) {
        self.raft_leader = previous
            .and_then(|previous| previous.raft_leader.clone())
            .filter(|held| held.incarnation == self.incarnation);
    }
}

#[cfg(test)]
mod tests;

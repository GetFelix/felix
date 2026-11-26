//! A broker's answer to a re-form survey of one shard group, with the
//! membership format this crate stores. The rule that plans from the answers
//! is `felix_storage::raft_copy`'s; see `docs/per-shard-raft.md`,
//! "Re-forming a group".

use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use felix_storage::DiskLog;
use felix_storage::raft_copy::{BrokerReport, GapFold, MembershipReader, VoterSets, build_report};
use felix_storage::raft_log::{EntryCodec, LogPoint, StoredSnapshot};

use super::codec::ShardCodec;
use super::types::{MemberId, ShardTypes};

/// Reads [`super::codec::StoredMembership`] payloads and snapshot metadata.
pub(super) struct Memberships;

impl MembershipReader<Arc<str>> for Memberships {
    fn voter_sets(&self, payload: &[u8]) -> io::Result<VoterSets<Arc<str>>> {
        let membership = EntryCodec::<ShardTypes>::decode_membership(&ShardCodec, payload)?;
        Ok(sets(membership.get_joint_config()))
    }

    fn snapshot_voter_sets(&self, meta: &[u8]) -> io::Result<Option<(u64, VoterSets<Arc<str>>)>> {
        Ok(super::machine::snapshot_membership(meta)?.map(|(index, joint)| (index, sets(&joint))))
    }
}

fn sets(joint: &[BTreeSet<MemberId>]) -> VoterSets<Arc<str>> {
    joint
        .iter()
        .map(|set| {
            set.iter()
                .map(|member| (member.broker.clone(), member.copy))
                .collect::<BTreeSet<_>>()
        })
        .collect()
}

/// This broker's report for the shard in `dir`, whose group the control
/// plane records at epoch `epoch`: its copies read at `probes`, and the
/// voter sets of every membership of that epoch they hold, from the log and
/// from snapshots. `leads` is whether this broker's member leads the epoch.
pub fn reform_report(
    dir: &Path,
    index_spacing_bytes: u64,
    probes: &BTreeSet<u64>,
    epoch: u64,
    leads: bool,
) -> io::Result<BrokerReport<Arc<str>>> {
    build_report(dir, index_spacing_bytes, probes, epoch, leads, &Memberships)
}

/// The snapshot a voter of a re-formed epoch stores at the gap's end, for
/// [`felix_storage::raft_copy::CopyRoot::start_stopped`]: its copy's
/// state folded through the base, with the epoch's voters as the
/// membership. See `docs/per-shard-raft.md`, "A voter's snapshot at the
/// gap's end is a fold".
pub struct GapSnapshot {
    kind: crate::ShardKind,
    voters: BTreeSet<MemberId>,
}

impl GapSnapshot {
    /// The fold for a shard of `kind` whose new epoch's voters are `voters`.
    pub fn new(kind: crate::ShardKind, voters: BTreeSet<MemberId>) -> Self {
        Self { kind, voters }
    }
}

impl GapFold for GapSnapshot {
    fn fold<'a>(
        &'a self,
        dir: &'a Path,
        log: &'a DiskLog,
        base: Option<LogPoint>,
        gap_end: LogPoint,
    ) -> Pin<Box<dyn Future<Output = io::Result<StoredSnapshot>> + Send + 'a>> {
        Box::pin(super::machine::gap_snapshot(
            dir,
            log,
            self.kind,
            &self.voters,
            base,
            gap_end,
        ))
    }
}

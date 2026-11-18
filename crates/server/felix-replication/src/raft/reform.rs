//! A broker's answer to a re-form survey of one shard group, with the
//! membership format this crate stores. The rule that plans from the answers
//! is `felix_storage::raft_copy`'s; see `docs/per-shard-raft.md`,
//! "Re-forming a group".

use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::sync::Arc;

use felix_storage::raft_copy::{BrokerReport, MembershipReader, VoterSets, build_report};
use felix_storage::raft_log::EntryCodec;

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

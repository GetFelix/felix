//! The membership format as a re-form survey reads it.

use std::collections::BTreeSet;
use std::sync::Arc;

use felix_storage::raft_copy::MembershipReader;
use felix_storage::raft_log::{EntryCodec, LogPoint};

use super::super::codec::ShardCodec;
use super::super::machine::base_snapshot;
use super::super::membership::TargetVersion;
use super::super::reform::Memberships;
use super::super::types::{MemberId, Membership, ShardTypes};

fn members(ids: &[(&str, u64)]) -> BTreeSet<MemberId> {
    ids.iter()
        .map(|(broker, copy)| MemberId::new(*broker, *copy))
        .collect()
}

fn ids(ids: &[(&str, u64)]) -> BTreeSet<(Arc<str>, u64)> {
    ids.iter()
        .map(|(broker, copy)| (Arc::from(*broker), *copy))
        .collect()
}

/// A joint membership reads as both halves, by broker and copy id;
/// learners are no voter set.
#[test]
fn a_joint_membership_reads_as_both_halves() {
    let old = [("a", 1), ("b", 2), ("c", 3)];
    let new = [("a", 1), ("d", 4), ("e", 5)];
    let membership = Membership::new(
        vec![members(&old), members(&new)],
        members(&[("f", 6)]),
        TargetVersion::default(),
    );
    let payload =
        EntryCodec::<ShardTypes>::encode_membership(&ShardCodec, &membership).expect("encode");
    let sets = Memberships.voter_sets(&payload).expect("decode");
    assert_eq!(sets, vec![ids(&old), ids(&new)]);
    assert!(Memberships.voter_sets(b"{}").is_err());
}

/// A stored snapshot's metadata gives its membership and the index of the
/// entry it came from.
#[test]
fn a_snapshots_membership_reads_with_its_index() {
    let voters = members(&[("a", 1), ("b", 2)]);
    let stored = base_snapshot(LogPoint { term: 3, index: 41 }, &voters).expect("snapshot");
    let read = Memberships
        .snapshot_voter_sets(&stored.meta)
        .expect("decode");
    assert_eq!(read, Some((41, vec![ids(&[("a", 1), ("b", 2)])])));
    assert!(Memberships.snapshot_voter_sets(b"not json").is_err());
}

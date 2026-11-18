//! A broker's re-form report: the voter sets of every membership of the
//! group's epoch, from log entries and from the snapshot.

use std::collections::BTreeSet;
use std::io;
use std::path::Path;

use bytes::Bytes;
use openraft::entry::RaftEntry;
use openraft::storage::RaftLogStorage;
use tempfile::TempDir;

use super::reclaim::{kept_dir, rotated};
use super::*;
use crate::DiskLog;
use crate::raft_log::snapshot::{self, SnapshotId, StoredSnapshot};
use crate::raft_log::tests::writes::append_flushed;
use crate::raft_log::tests::{Codec, TestEntry, Types, log_id, normal};
use crate::raft_log::{EntryCodec, RaftLog};

type Log = RaftLog<Types, Codec>;
type Sets = Vec<BTreeSet<(u64, u64)>>;

const SPACING: u64 = 4096;

/// Node `n` runs copy `10 * n`.
struct Reader;

impl MembershipReader<u64> for Reader {
    fn voter_sets(&self, payload: &[u8]) -> io::Result<Sets> {
        let membership = EntryCodec::<Types>::decode_membership(&Codec, payload)?;
        Ok(ids(membership.get_joint_config()))
    }

    fn snapshot_voter_sets(&self, meta: &[u8]) -> io::Result<Option<(u64, Sets)>> {
        let (index, joint): (u64, Vec<BTreeSet<u64>>) =
            serde_json::from_slice(meta).map_err(io::Error::other)?;
        Ok(Some((index, ids(&joint))))
    }
}

fn ids(joint: &[BTreeSet<u64>]) -> Sets {
    joint
        .iter()
        .map(|set| set.iter().map(|node| (*node, node * 10)).collect())
        .collect()
}

fn sets(joint: &[&[u64]]) -> Sets {
    ids(&joint
        .iter()
        .map(|set| set.iter().copied().collect())
        .collect::<Vec<_>>())
}

fn open(dir: &Path) -> Log {
    let log = DiskLog::open(dir, "t/ns/raft/0", config()).expect("open");
    Log::new(log, Codec).expect("a Raft log")
}

fn membership(term: u64, index: u64, joint: &[&[u64]]) -> TestEntry {
    let config = joint
        .iter()
        .map(|set| set.iter().copied().collect())
        .collect();
    let membership = openraft::impls::Membership::new_with_defaults(config, BTreeSet::<u64>::new());
    TestEntry::new_membership(log_id(term, index), membership)
}

/// Entries 0..=9 of term 1, with a membership at each index in `at`.
async fn write(dir: &Path, at: &[(u64, &[&[u64]])]) -> Log {
    let mut raft = open(dir);
    let entries: Vec<_> = (0..10)
        .map(|index| match at.iter().find(|(i, _)| *i == index) {
            Some((_, joint)) => membership(1, index, joint),
            None => normal(1, index, "x"),
        })
        .collect();
    append_flushed(&mut raft, &entries).await;
    raft
}

/// A snapshot covering `last` whose membership came from the entry at
/// `index`.
fn snapshot_holding(dir: &Path, last: u64, index: u64, joint: &[&[u64]]) {
    let joint: Vec<Vec<u64>> = joint.iter().map(|set| set.to_vec()).collect();
    let stored = StoredSnapshot {
        last: Some(SnapshotId {
            term: 1,
            index: last,
        }),
        meta: Bytes::from(serde_json::to_vec(&(index, joint)).expect("meta")),
        data: Bytes::new(),
    };
    snapshot::store_in(dir, &stored).expect("snapshot");
}

fn report(dir: &Path, epoch: u64) -> BrokerReport<u64> {
    build_report(dir, SPACING, &BTreeSet::new(), epoch, false, &Reader).expect("report")
}

/// Both halves of a joint entry count, uncommitted ones too, and the
/// newest entry names the brokers.
#[tokio::test]
async fn every_membership_entry_is_reported_with_both_halves() {
    let dir: TempDir = tempfile::tempdir().expect("dir");
    let raft = write(
        dir.path(),
        &[(2, &[&[1, 2, 3]]), (5, &[&[1, 2, 3], &[1, 4, 5]])],
    )
    .await;
    raft.disk_log().close().await.expect("close");

    let report = report(dir.path(), 0);
    assert_eq!(
        report.memberships,
        Some(sets(&[&[1, 2, 3], &[1, 2, 3], &[1, 4, 5]]))
    );
    assert_eq!(report.names, BTreeSet::from([1, 2, 3, 4, 5]));
    assert!(!report.leads);
}

/// A purge drops the entry, so the voters live only in the snapshot.
#[tokio::test]
async fn a_membership_purged_from_the_log_is_read_from_the_snapshot() {
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = write(dir.path(), &[(2, &[&[1, 2], &[3, 4]])]).await;
    raft.purge(log_id(1, 6)).await.expect("purge");
    raft.disk_log().close().await.expect("close");
    snapshot_holding(dir.path(), 6, 2, &[&[1, 2], &[3, 4]]);

    let report = report(dir.path(), 0);
    assert_eq!(report.memberships, Some(sets(&[&[1, 2], &[3, 4]])));
    assert_eq!(report.names, BTreeSet::from([1, 2, 3, 4]));
}

/// Below its epoch's first index a copy holds its base, whose voters the
/// record keeps as the seed; a copy of another epoch holds no voter set of
/// this one.
#[tokio::test]
async fn only_the_epochs_own_memberships_are_reported() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = write(dir.path(), &[(1, &[&[7, 8, 9]]), (7, &[&[1, 2, 3]])]).await;
    raft.disk_log().close().await.expect("close");
    snapshot_holding(dir.path(), 1, 1, &[&[7, 8, 9]]);
    let file = EpochFile {
        incarnation: 3,
        bases: vec![EpochBase {
            base_last: Some(EpochLogId {
                index: 1,
                epoch: 0,
                term: 1,
            }),
            first_index: 6,
        }],
    };
    epoch_file::store(dir.path(), &file).expect("epoch");

    let at_one = report(dir.path(), 1);
    assert_eq!(at_one.memberships, Some(sets(&[&[1, 2, 3]])));
    assert_eq!(at_one.names, BTreeSet::from([1, 2, 3]));
    let at_two = report(dir.path(), 2);
    assert_eq!(at_two.memberships, Some(Vec::new()));
    assert!(at_two.names.is_empty());
}

/// A kept copy's memberships count as the current one's do, and its newest
/// membership names brokers when the current copy holds none.
#[tokio::test]
async fn a_kept_copys_memberships_are_reported() {
    let root = Root::new();
    let dir = root.shard_dir();
    let (_copies, old, new) = rotated(&root, 3).await;
    new.log.close().await.expect("close");
    let kept = kept_dir(&root).join(format!("0.{old:016x}"));
    snapshot_holding(&kept, 1, 1, &[&[4, 5, 6]]);

    let report = report(&dir, 0);
    assert_eq!(report.memberships, Some(sets(&[&[4, 5, 6]])));
    assert_eq!(report.names, BTreeSet::from([4, 5, 6]));
    assert_eq!(report.copies.kept.len(), 1);
}

/// Fail closed: a membership that does not decode fails the report.
#[tokio::test]
async fn a_membership_that_does_not_decode_fails_the_report() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = write(dir.path(), &[]).await;
    raft.disk_log().close().await.expect("close");
    let stored = StoredSnapshot {
        last: Some(SnapshotId { term: 1, index: 1 }),
        meta: Bytes::from_static(b"not json"),
        data: Bytes::new(),
    };
    snapshot::store_in(dir.path(), &stored).expect("snapshot");
    build_report(dir.path(), SPACING, &BTreeSet::new(), 0, false, &Reader).expect_err("refused");
}

/// A purge past the snapshot would hide the newest purged membership, so
/// the report refuses instead of reading it as none.
#[tokio::test]
async fn a_purge_past_the_snapshot_fails_the_report() {
    let dir = tempfile::tempdir().expect("dir");
    let mut raft = write(dir.path(), &[(2, &[&[1, 2, 3]])]).await;
    raft.purge(log_id(1, 6)).await.expect("purge");
    raft.disk_log().close().await.expect("close");
    build_report(dir.path(), SPACING, &BTreeSet::new(), 0, false, &Reader)
        .expect_err("no snapshot");
    snapshot_holding(dir.path(), 5, 2, &[&[1, 2, 3]]);
    build_report(dir.path(), SPACING, &BTreeSet::new(), 0, false, &Reader).expect_err("behind");
    snapshot_holding(dir.path(), 6, 2, &[&[1, 2, 3]]);
    let report = report(dir.path(), 0);
    assert_eq!(report.memberships, Some(sets(&[&[1, 2, 3]])));
}

/// A copy with no `raft-epoch` past epoch 0 could be of the epoch (a
/// learner placed fresh), so every membership it holds counts. It names no
/// broker: it may be of an older epoch.
#[tokio::test]
async fn a_copy_with_no_epoch_file_past_epoch_zero_reports_every_membership() {
    let dir = tempfile::tempdir().expect("dir");
    let raft = write(dir.path(), &[(1, &[&[7, 8, 9]]), (7, &[&[1, 2, 3]])]).await;
    raft.disk_log().close().await.expect("close");

    let report = report(dir.path(), 1);
    assert_eq!(report.memberships, Some(sets(&[&[7, 8, 9], &[1, 2, 3]])));
    assert!(report.names.is_empty());
}

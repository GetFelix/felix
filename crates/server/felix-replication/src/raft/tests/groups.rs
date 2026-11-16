//! A stream shard's consumer groups: every member folds the shard's group
//! entries into the same table, keeps it across a restart, and takes it
//! from a snapshot it installs.

use felix_broker::{CreatedBy, CreationToken, GroupEntry, GroupOp, GroupTable};
use openraft::entry::RaftEntry;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{EntryOf, LogIdOf};

use super::super::machine::{Machine, Opened};
use super::commit::led;
use super::*;

/// The token of a tracker hydrated at the start of term 0's log.
const CRE: CreationToken = CreationToken {
    term: 0,
    index: 0,
    by: CreatedBy::Hydrate,
};

fn entry(group: &str, op: GroupOp) -> GroupEntry {
    GroupEntry {
        group: group.to_owned(),
        op,
    }
}

/// Writes of two groups: `a` advances with a dead letter behind it and one
/// redriven, `b` is created and deleted, `c` is created by a seek.
fn writes() -> Vec<GroupEntry> {
    vec![
        entry("a", GroupOp::Advance { cre: CRE, to: 2 }),
        entry(
            "a",
            GroupOp::DeadLetter {
                cre: CRE,
                offset: 2,
            },
        ),
        entry(
            "a",
            GroupOp::DeadLetter {
                cre: CRE,
                offset: 3,
            },
        ),
        entry("a", GroupOp::Advance { cre: CRE, to: 5 }),
        entry(
            "a",
            GroupOp::Redrive {
                cre: CRE,
                offset: 2,
            },
        ),
        entry("b", GroupOp::Advance { cre: CRE, to: 1 }),
        entry("b", GroupOp::Delete),
        entry(
            "c",
            GroupOp::Seek {
                to: 4,
                if_new: false,
            },
        ),
    ]
}

/// What `broker`'s member of shard 0 has folded.
fn table_on(cluster: &Cluster, broker: &str) -> GroupTable {
    let engine = RaftEngine::new(cluster.runtime(broker));
    let groups = engine.groups(&shard(0)).expect("a stream shard's groups");
    groups.read().table().clone()
}

/// **Every member folds the shard's group writes into the same table, and
/// a restart, from the log alone or from a snapshot and the log above it,
/// leaves each member's table as it was.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_member_folds_the_same_group_table_and_keeps_it_across_a_restart() {
    let led = led().await;
    let writes = writes();
    let mut last = 0;
    for write in &writes {
        let written = led
            .engine
            .propose_group(&shard(0), led.term, 1_700_000_000, write)
            .await
            .expect("written");
        led.engine
            .await_commit(&shard(0), written, PATIENCE)
            .await
            .expect("committed");
        last = written.end - 1;
    }
    let mut cluster = led.cluster;
    let folded = eventually(|| {
        let tables: Vec<_> = BROKERS
            .iter()
            .map(|broker| table_on(&cluster, broker))
            .collect();
        let all_applied = tables.iter().all(|table| table.applied() == last);
        (all_applied && tables.windows(2).all(|pair| pair[0] == pair[1])).then(|| tables[0].clone())
    })
    .await;
    let a = folded.group("a").expect("a");
    assert_eq!(a.cursor, 5);
    assert_eq!(a.dead.iter().copied().collect::<Vec<_>>(), [3]);
    assert_eq!(a.redriven.iter().copied().collect::<Vec<_>>(), [2]);
    assert!(folded.group("b").is_none());
    assert_eq!(folded.group("c").expect("c").cursor, 4);
    assert_eq!(folded.last_delete(), last - 1);

    // One member snapshots above some of the writes, so its restart loads
    // those from the snapshot and folds only the entries above it.
    cluster.snapshot("b", 0, last - 3).await;
    cluster.stop().await;
    cluster.start(true);
    cluster.open(0).await;
    cluster.elected(0).await;
    let next = cluster.write(0, b"after").await;
    cluster.committed_everywhere(0, next).await;
    for broker in BROKERS {
        let table = eventually(|| {
            let table = table_on(&cluster, broker);
            (table.applied() == last).then_some(table)
        })
        .await;
        assert_eq!(table, folded, "{broker}");
    }
    cluster.stop().await;
}

/// A stream shard's state machine over a fresh log and snapshot store in
/// `dir`.
async fn stream_machine(dir: &std::path::Path) -> (Machine, super::super::machine::Groups) {
    let log = open_log(dir, "m", 0, log_config());
    let snapshots = felix_storage::raft_log::SnapshotStore::open(dir).expect("store");
    let opened = Machine::open(snapshots, ShardKind::Stream, Some(&log)).await;
    let Opened {
        machine, groups, ..
    } = opened.expect("open");
    (machine, groups.expect("a stream shard's groups"))
}

/// `writes` as entries of term 1 from `first`, with a record between each
/// pair so the group entries are not the only ones.
fn group_entries(
    first: u64,
    writes: &[GroupEntry],
) -> impl futures::Stream<Item = std::io::Result<openraft::storage::EntryResponder<ShardTypes>>> + Unpin
{
    let entries: Vec<_> = (first..)
        .step_by(2)
        .zip(writes)
        .flat_map(|(index, write)| {
            let command = |payload, group| Command {
                timestamp_micros: index,
                payload,
                producer: None,
                expected: None,
                trim: None,
                commit: false,
                group,
                publisher: None,
                headers: None,
                written: None,
            };
            [
                (index, command(write.encode(), true)),
                (
                    index + 1,
                    command(bytes::Bytes::from_static(b"record"), false),
                ),
            ]
        })
        .map(|(index, command)| {
            let id = LogIdOf::<ShardTypes>::new(1.into(), index);
            Ok((EntryOf::<ShardTypes>::new_normal(id, command), None))
        })
        .collect();
    futures::stream::iter(entries)
}

/// The table every write folds to, applied in order at the indexes
/// [`group_entries`] gives them from 1.
fn full_fold(writes: &[GroupEntry]) -> GroupTable {
    let mut table = GroupTable::default();
    for (index, write) in (1..).step_by(2).zip(writes) {
        table.apply(1, index, write);
    }
    table
}

/// **A member that installs a snapshot taken above group entries holds the
/// table a member that applied every entry holds.** It has no entry below
/// the snapshot to fold, so the table comes from the snapshot, and the
/// entries above it fold on top.
#[tokio::test]
async fn an_installed_snapshot_carries_the_group_table() {
    let writes = writes();
    let (below, above) = writes.split_at(5);
    let sender_dir = tempdir().expect("dir");
    let (mut sender, sender_groups) = stream_machine(sender_dir.path()).await;
    sender.apply(group_entries(1, below)).await.expect("apply");
    let built = sender
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect("build");
    assert_eq!(
        built.meta.last_log_id.map(|id| id.index),
        Some(below.len() as u64 * 2)
    );

    let receiver_dir = tempdir().expect("dir");
    let (mut receiver, receiver_groups) = stream_machine(receiver_dir.path()).await;
    receiver
        .install_snapshot(&built.meta, built.snapshot)
        .await
        .expect("install");
    let first_above = below.len() as u64 * 2 + 1;
    for machine in [&mut sender, &mut receiver] {
        machine
            .apply(group_entries(first_above, above))
            .await
            .expect("apply");
    }
    let full = full_fold(&writes);
    assert_eq!(receiver_groups.read().table(), &full);
    assert_eq!(sender_groups.read().table(), &full);

    // And a restart of the receiver loads it from the stored snapshot.
    drop(receiver);
    let (_, reopened) = stream_machine(receiver_dir.path()).await;
    assert_eq!(reopened.read().table(), &full_fold(below));
}

/// **A group entry this build cannot read stops the member** rather than
/// leave its table unlike the others'.
#[tokio::test]
async fn a_group_entry_that_does_not_decode_stops_the_apply() {
    let dir = tempdir().expect("dir");
    let (mut machine, groups) = stream_machine(dir.path()).await;
    let mut damaged = entry("a", GroupOp::Advance { cre: CRE, to: 1 })
        .encode()
        .to_vec();
    damaged[0] = 9;
    let command = Command {
        timestamp_micros: 1,
        payload: damaged.into(),
        producer: None,
        expected: None,
        trim: None,
        commit: false,
        group: true,
        publisher: None,
        headers: None,
        written: None,
    };
    let id = LogIdOf::<ShardTypes>::new(1.into(), 1);
    let entries = [Ok((EntryOf::<ShardTypes>::new_normal(id, command), None))];
    let applied = machine.apply(futures::stream::iter(entries)).await;
    assert!(applied.is_err());
    assert_eq!(groups.read().table(), &GroupTable::default());
}

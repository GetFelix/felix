//! A standalone broker after a crash: under `Periodic` a machine crash
//! rotates the storage id, and the group of one re-forms itself from its
//! own copy. A clean stop, an `OnCommit` crash and an operator's rotation
//! leave it alone.

use std::path::Path;
use std::time::Duration;

use felix_broker::ConsistencyLevel;
use felix_storage::log::FsyncMode;
use felix_storage::raft_copy::{EpochRecord, LocalRecord, group_epoch, highest_reserved};
use felix_storage::run_state::{self, BootId};

use super::super::super::tests::{INCARNATION, broker_at};
use super::*;

const PERIODIC: FsyncMode = FsyncMode::Periodic {
    interval: Duration::from_millis(50),
};

fn boot(n: u8) -> Option<BootId> {
    Some(BootId::from_bytes([n; 16]))
}

/// Start a run on `root` in boot `n`. Relaxed runs rotate on a crash into
/// another boot.
fn begin(root: &Path, relaxed: bool, n: u8) -> bool {
    let mut fresh = [n; 16];
    fresh[0] = 0xF0;
    run_state::begin(root, relaxed, None, boot(n), || false, || fresh)
        .expect("begin")
        .rotated
}

async fn broker_in(root: &Path, fsync: FsyncMode) -> Arc<Broker> {
    broker_at(root, 1, INCARNATION, ConsistencyLevel::Leader, fsync).await
}

fn orders_dir(broker: &Arc<Broker>) -> std::path::PathBuf {
    broker
        .durable_storage()
        .expect("storage")
        .claim_for_raft("t1", "ns", "orders", 0, INCARNATION)
        .expect("directory")
        .0
}

/// What the first run left: the shard's directory, the end of what it
/// committed, and the highest index it reserved.
struct Written {
    dir: std::path::PathBuf,
    end: u64,
    reserved: Option<u64>,
}

/// Write `one two three`, then `four five` with the saved commit kept from
/// before them, as a crash leaves a commit saved at most once a second.
async fn write(root: &Path, fsync: FsyncMode) -> Written {
    let broker = broker_in(root, fsync).await;
    let (shards, engine, stop) = standalone(&broker);
    let orders = key("orders", 0);
    led(&shards).await;
    let dir = orders_dir(&broker);
    let first = publish(&broker, &engine, &[b"one", b"two", b"three"]).await;
    until("the batch commits", || {
        engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(first + 3)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let saved = std::fs::read(dir.join("replica")).ok();
    let second = publish(&broker, &engine, &[b"four", b"five"]).await;
    until("the second batch commits", || {
        engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(second + 2)
    })
    .await;
    stop.cancel();
    assert!(EngineShards::shutdown(&*shards).await);
    if let Some(saved) = saved {
        std::fs::write(dir.join("replica"), saved).expect("restore the older commit");
    }
    Written {
        reserved: highest_reserved(&dir).expect("reserved"),
        dir,
        end: second + 2,
    }
}

/// Reopen and wait until the shard is led and has every record read back.
async fn reopen(
    root: &Path,
    fsync: FsyncMode,
    end: u64,
) -> (
    Arc<Broker>,
    Arc<RaftShards>,
    Arc<StreamEngine>,
    CancellationToken,
) {
    let broker = broker_in(root, fsync).await;
    let (shards, engine, stop) = standalone(&broker);
    led(&shards).await;
    let orders = key("orders", 0);
    until("the records are readable again", || {
        matches!(
            engine.committed_bound(&orders),
            felix_broker::ReadBound::Committed(bound) if bound >= end
        )
    })
    .await;
    (broker, shards, engine, stop)
}

async fn payloads(broker: &Broker) -> Vec<Vec<u8>> {
    broker
        .read_committed("t1", "ns", "orders", 0, 0, 1 << 20)
        .await
        .expect("history")
        .iter()
        .map(|record| record.payload.to_vec())
        .collect()
}

fn written() -> Vec<Vec<u8>> {
    [&b"one"[..], b"two", b"three", b"four", b"five"]
        .iter()
        .map(|p| p.to_vec())
        .collect()
}

/// **A Periodic crash re-forms the group of one and keeps every synced
/// record**, including ones past the saved commit, and the next offset lands
/// above everything the crashed run reserved.
#[tokio::test]
async fn a_periodic_crash_re_forms_and_keeps_every_synced_record() {
    let root = tempfile::tempdir().expect("tempdir");
    begin(root.path(), true, 1);
    let w = write(root.path(), PERIODIC).await;
    assert!(begin(root.path(), true, 2), "a relaxed crash rotates");

    let (broker, shards, engine, stop) = reopen(root.path(), PERIODIC, w.end).await;
    assert_eq!(group_epoch(&w.dir).expect("epoch"), Some(1), "re-formed");
    let plan = LocalRecord
        .load(&w.dir)
        .expect("plan")
        .expect("a recorded plan");
    let first = plan.bases.last().expect("a base").first_index;
    let reserved = w.reserved.expect("a reservation");
    assert!(
        first > reserved,
        "the epoch starts at {first}, not above the reservation {reserved}"
    );
    assert_eq!(payloads(&broker).await, written());
    let next = publish(&broker, &engine, &[b"six"]).await;
    assert!(next >= w.end, "offset {next} reused below {}", w.end);
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **A clean restart does not re-form**, under the same fsync mode.
#[tokio::test]
async fn a_clean_restart_does_not_re_form() {
    let root = tempfile::tempdir().expect("tempdir");
    begin(root.path(), true, 1);
    let w = write(root.path(), PERIODIC).await;
    let id = felix_storage::storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    run_state::mark_clean(root.path(), id).expect("clean");
    assert!(!begin(root.path(), true, 2), "a clean stop keeps the id");

    let (broker, shards, _engine, stop) = reopen(root.path(), PERIODIC, w.end).await;
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None);
    assert_eq!(payloads(&broker).await, written());
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **An `OnCommit` crash does not re-form**: it rotates nothing.
#[tokio::test]
async fn an_on_commit_crash_does_not_re_form() {
    let root = tempfile::tempdir().expect("tempdir");
    begin(root.path(), false, 1);
    let w = write(root.path(), FsyncMode::OnCommit).await;
    assert!(!begin(root.path(), false, 2), "OnCommit keeps the id");

    let (broker, shards, _engine, stop) = reopen(root.path(), FsyncMode::OnCommit, w.end).await;
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None);
    assert_eq!(payloads(&broker).await, written());
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **An operator's rotation after a relaxed crash does not re-form**: it
/// voids the crash's evidence, and the copy is not this broker's to vouch
/// for.
#[tokio::test]
async fn an_operator_rotation_does_not_re_form() {
    let root = tempfile::tempdir().expect("tempdir");
    begin(root.path(), true, 1);
    let w = write(root.path(), PERIODIC).await;
    assert!(begin(root.path(), true, 2));
    run_state::copy_trust::rotate_storage_id(root.path(), [0x77; 16]).expect("rotate");

    let broker = broker_in(root.path(), PERIODIC).await;
    let (shards, _engine, stop) = standalone(&broker);
    let orders = key("orders", 0);
    until("the member opens", || shards.engine_for(&orders).is_some()).await;
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None);
    assert!(!w.dir.join("raft-reform-alone").exists());
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **A relaxed crash right after an operator's rotation does not re-form**:
/// the crash's evidence is fresh, but the copy was minted under an id the
/// operator rotated away, not one the crash did.
#[tokio::test]
async fn a_crash_after_an_operator_rotation_does_not_re_form() {
    let root = tempfile::tempdir().expect("tempdir");
    begin(root.path(), true, 1);
    let w = write(root.path(), PERIODIC).await;
    run_state::copy_trust::rotate_storage_id(root.path(), [0x77; 16]).expect("rotate");
    assert!(begin(root.path(), true, 2), "a relaxed crash rotates");
    assert!(
        !run_state::relaxed_crash_sources(root.path())
            .expect("sources")
            .is_empty(),
        "the crash left its evidence"
    );

    let broker = broker_in(root.path(), PERIODIC).await;
    let (shards, _engine, stop) = standalone(&broker);
    let orders = key("orders", 0);
    until("the member opens", || shards.engine_for(&orders).is_some()).await;
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None);
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

/// **A second Periodic crash re-forms again above the first epoch's
/// reservation**: publishing in epoch 1 raises the reservation past the
/// first plan, and the second plan must clear it, not only the records it
/// holds. Every synced record of both epochs reads back.
#[tokio::test]
async fn a_second_periodic_crash_re_forms_above_the_first_epochs_reservation() {
    let root = tempfile::tempdir().expect("tempdir");
    begin(root.path(), true, 1);
    let w = write(root.path(), PERIODIC).await;
    assert!(begin(root.path(), true, 2), "a relaxed crash rotates");

    let (broker, shards, engine, stop) = reopen(root.path(), PERIODIC, w.end).await;
    let first_plan = LocalRecord
        .load(&w.dir)
        .expect("plan")
        .expect("a recorded plan")
        .bases
        .last()
        .expect("a base")
        .first_index;
    let orders = key("orders", 0);
    let next = publish(&broker, &engine, &[b"six", b"seven"]).await;
    until("the epoch 1 batch commits", || {
        engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(next + 2)
    })
    .await;
    // Let the periodic sync cover the batch, as a crash keeps only synced
    // records.
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.cancel();
    assert!(EngineShards::shutdown(&*shards).await);
    drop(broker);
    let epoch_one = highest_reserved(&w.dir)
        .expect("reserved")
        .expect("a reservation");
    assert!(
        epoch_one > first_plan,
        "epoch 1 reserved {epoch_one}, not past its first index {first_plan}"
    );

    assert!(
        begin(root.path(), true, 3),
        "a second relaxed crash rotates"
    );
    let (broker, shards, engine, stop) = reopen(root.path(), PERIODIC, next + 2).await;
    assert_eq!(
        group_epoch(&w.dir).expect("epoch"),
        Some(2),
        "re-formed again"
    );
    let second_plan = LocalRecord
        .load(&w.dir)
        .expect("plan")
        .expect("a recorded plan")
        .bases
        .last()
        .expect("a base")
        .first_index;
    assert!(
        second_plan > epoch_one,
        "epoch 2 starts at {second_plan}, not above epoch 1's reservation {epoch_one}"
    );
    assert!(second_plan > first_plan);
    let mut expected = written();
    expected.extend([b"six".to_vec(), b"seven".to_vec()]);
    assert_eq!(payloads(&broker).await, expected);
    let last = publish(&broker, &engine, &[b"eight"]).await;
    assert!(last >= next + 2, "offset {last} reused below {}", next + 2);
    stop.cancel();
    EngineShards::shutdown(&*shards).await;
}

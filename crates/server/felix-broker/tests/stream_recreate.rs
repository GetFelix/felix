//! A durable stream deleted and created again under the same name starts
//! empty: no records, offsets from zero, and no producer state from the stream
//! that held the name before it.

use std::sync::Arc;

use bytes::Bytes;
use felix_broker::{
    Broker, ConsumerGroups, DeadLetters, DurableStorage, GroupKey, SequenceReuse, StreamMetadata,
};
use felix_storage::EphemeralCache;
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::{TempDir, tempdir};

fn log_config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 4 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

async fn broker(dir: &TempDir) -> (Arc<Broker>, DurableStorage) {
    let storage = DurableStorage::open(dir.path(), log_config()).expect("storage");
    let groups =
        Arc::new(ConsumerGroups::open(dir.path().join("groups"), log_config()).expect("groups"));
    let dead_letters = Arc::new(
        DeadLetters::open(dir.path().join("dead-letters"), log_config()).expect("dead letters"),
    );
    let broker = Broker::new(EphemeralCache::new().into())
        .with_durable_storage(storage.clone())
        .with_consumer_groups(groups, dead_letters, std::time::Duration::from_secs(30), 5);
    // As a broker following the control plane does.
    broker.require_registered_streams();
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    (Arc::new(broker), storage)
}

fn durable(shards: u32) -> StreamMetadata {
    incarnation(shards, 0)
}

fn incarnation(shards: u32, incarnation: u64) -> StreamMetadata {
    StreamMetadata {
        durable: true,
        shards,
        incarnation,
        ..Default::default()
    }
}

fn group(shard: u32) -> GroupKey {
    GroupKey {
        tenant_id: "t1".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard,
        group: "workers".into(),
    }
}

/// Commit a group position and a dead letter on `shard`.
async fn group_state(broker: &Broker, shard: u32) {
    broker
        .consumer_groups()
        .expect("groups")
        .commit("t1", "default", "orders", shard, "workers", 2)
        .await
        .expect("commit");
    broker
        .group_reader()
        .expect("reader")
        .dead_letters()
        .record(&group(shard), 1)
        .await
        .expect("dead letter");
}

async fn assert_no_group_state(broker: &Broker, shard: u32) {
    let committed = broker
        .consumer_groups()
        .expect("groups")
        .committed("t1", "default", "orders", shard, "workers")
        .await
        .expect("committed");
    assert_eq!(committed, None, "group position inherited");
    let dead = broker
        .group_reader()
        .expect("reader")
        .dead_letters()
        .list(&group(shard))
        .await
        .expect("list");
    assert!(dead.is_empty(), "dead letters inherited: {dead:?}");
}

fn payload(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

async fn publish_three(broker: &Broker, stream: &str) {
    for value in ["a", "b", "c"] {
        broker
            .publish("t1", "default", stream, payload(value))
            .await
            .expect("publish");
    }
}

/// Offset the next record published to shard 0 lands at.
async fn next_offset(broker: &Broker, stream: &str) -> u64 {
    let handle = broker
        .resolve_stream_handle("t1", "default", stream, 0)
        .await
        .expect("handle");
    let outcome = broker
        .publish_batch_with_outcome(&handle, &[payload("probe")], None)
        .await
        .expect("publish");
    outcome.offsets.expect("durable").0
}

#[tokio::test]
async fn a_recreated_stream_starts_empty() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    publish_three(&broker, "orders").await;

    assert!(
        broker
            .remove_stream("t1", "default", "orders")
            .await
            .expect("remove")
    );
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register again");

    assert_eq!(
        next_offset(&broker, "orders").await,
        0,
        "offsets restart at zero"
    );
}

#[tokio::test]
async fn a_recreated_stream_has_no_producer_state() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    let producer = broker.new_producer_id();
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await
        .expect("handle");
    broker
        .publish_batch_idempotent(
            &handle,
            producer,
            0,
            &[payload("old")],
            SequenceReuse::Refuse,
        )
        .await
        .expect("publish");

    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register again");

    // The same producer's first batch is new to the new stream, not a
    // re-send of the old one's.
    let handle = broker
        .resolve_stream_handle("t1", "default", "orders", 0)
        .await
        .expect("handle");
    let outcome = broker
        .publish_batch_idempotent(
            &handle,
            producer,
            0,
            &[payload("new")],
            SequenceReuse::Refuse,
        )
        .await
        .expect("publish to the new stream");
    assert!(!outcome.duplicate, "answered from the old stream's state");
}

#[tokio::test]
async fn a_recreated_stream_survives_a_restart_empty() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        broker
            .remove_stream("t1", "default", "orders")
            .await
            .expect("remove");
        storage.shutdown().await.expect("shutdown");
    }
    // A broker started after the delete is told only that the stream exists.
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    assert_eq!(next_offset(&broker, "orders").await, 0);
}

#[tokio::test]
async fn a_recreated_stream_has_no_group_state() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    group_state(&broker, 0).await;

    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register again");
    assert_no_group_state(&broker, 0).await;
}

/// The broker never saw the delete: the next thing it hears is the new
/// stream, under a new incarnation. The old one stops serving but its data
/// stays until the catalog's delete names it: a registration is no evidence
/// of a delete, since it may come from a catalog behind the one read before.
#[tokio::test]
async fn a_new_incarnation_replaces_the_registered_one() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 5))
        .await
        .expect("register");
    publish_three(&broker, "orders").await;
    group_state(&broker, 0).await;

    broker
        .register_stream("t1", "default", "orders", incarnation(1, 6))
        .await
        .expect("register the new stream");
    assert_eq!(next_offset(&broker, "orders").await, 0);
    assert_no_group_state(&broker, 0).await;
    let old = felix_storage::disk_log::layout::incarnation_dir(dir.path(), &shard_key(0), 5);
    assert!(
        old.exists(),
        "a registration deleted the incarnation it replaced"
    );

    broker
        .remove_stream_from_catalog("t1", "default", "orders", Some(5))
        .await
        .expect("the catalog's delete");
    assert!(!old.exists(), "the delete naming it kept it");
    assert_eq!(
        next_offset(&broker, "orders").await,
        1,
        "the live stream changed"
    );
}

/// Deleted and created again while the broker was down, at a different width.
#[tokio::test]
async fn a_restart_into_a_new_incarnation_starts_empty() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(4, 5))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        group_state(&broker, 3).await;
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(2, 6))
        .await
        .expect("register");
    assert_eq!(next_offset(&broker, "orders").await, 0);
    assert_no_group_state(&broker, 3).await;
}

/// A stream from before incarnations, deleted while the broker was down and
/// created again by a control plane that gives incarnations. The new stream
/// starts empty, and the old one's data stays until a tombstone covers it: the
/// registration alone is no word that incarnation 0 is deleted.
#[tokio::test]
async fn an_incarnated_stream_keeps_data_from_before_incarnations_until_a_tombstone() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        group_state(&broker, 0).await;
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 9))
        .await
        .expect("register");
    assert_eq!(next_offset(&broker, "orders").await, 0);
    assert_no_group_state(&broker, 0).await;
    let old = felix_storage::disk_log::layout::shard_dir(dir.path(), &shard_key(0));
    assert!(old.exists(), "a registration took incarnation 0's data");

    broker
        .delete_stream_through("t1", "default", "orders", 0)
        .await
        .expect("the tombstone");
    assert!(!old.exists(), "the tombstone kept incarnation 0's data");
    assert_eq!(
        next_offset(&broker, "orders").await,
        1,
        "the live stream changed"
    );
}

/// Upgrading keeps a stream from before incarnations exactly as it was.
#[tokio::test]
async fn a_stream_from_before_incarnations_keeps_its_records() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    assert_eq!(next_offset(&broker, "orders").await, 3);
}

/// A namespace deleted before its streams, as a cascading delete arrives.
#[tokio::test]
async fn a_stream_is_removed_after_its_namespace() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    publish_three(&broker, "orders").await;

    broker
        .remove_namespace("t1", "default")
        .await
        .expect("remove namespace");
    assert!(
        broker
            .remove_stream("t1", "default", "orders")
            .await
            .expect("remove stream")
    );
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace again");
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register again");
    assert_eq!(next_offset(&broker, "orders").await, 0);
}

#[tokio::test]
async fn a_removed_streams_logs_cannot_be_opened_for_replication() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    assert!(
        broker
            .shard_log(felix_broker::LogKind::Stream, "t1", "default", "orders", 0)
            .await
            .is_none(),
        "opened before registration"
    );
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 5))
        .await
        .expect("register");
    assert!(
        broker
            .shard_log(felix_broker::LogKind::Stream, "t1", "default", "orders", 0)
            .await
            .is_some()
    );
    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");
    for kind in [
        felix_broker::LogKind::Stream,
        felix_broker::LogKind::GroupCursors,
        felix_broker::LogKind::GroupDeadLetters,
    ] {
        assert!(
            broker
                .shard_log(kind, "t1", "default", "orders", 0)
                .await
                .is_none(),
            "{kind:?} opened after removal"
        );
    }
}

fn shard_key(shard: u32) -> felix_storage::log::ShardKey {
    felix_storage::log::ShardKey {
        tenant: "t1".into(),
        namespace: "default".into(),
        stream: "orders".into(),
        shard,
    }
}

/// A delete replayed after the create that followed it names the deleted
/// incarnation, and leaves the new stream alone.
#[tokio::test]
async fn a_replayed_delete_keeps_the_stream_created_after_it() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 5))
        .await
        .expect("register");
    broker
        .remove_stream_from_catalog("t1", "default", "orders", Some(5))
        .await
        .expect("remove");
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 6))
        .await
        .expect("register again");
    publish_three(&broker, "orders").await;

    assert!(
        !broker
            .remove_stream_from_catalog("t1", "default", "orders", Some(5))
            .await
            .expect("replayed remove")
    );
    assert_eq!(next_offset(&broker, "orders").await, 3);
}

/// A catalog record without the incarnation, from a control plane before
/// incarnations, is not a new stream, and does not say which incarnation it
/// means: it is refused, before and after a restart, and the data stays.
#[tokio::test]
async fn a_record_without_the_incarnation_keeps_the_streams_data() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 5))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        // An update from an older control plane.
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("update without the incarnation");
        assert_eq!(next_offset(&broker, "orders").await, 3);
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register without the incarnation");
    assert!(!broker.stream_exists("t1", "default", "orders").await);
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 5))
        .await
        .expect("register with it");
    assert_eq!(next_offset(&broker, "orders").await, 4);
}

/// The broker was down for the delete: the incarnation it holds on disk is
/// kept when the next one registers, and goes, every shard of it, when the
/// catalog's delete naming it arrives.
#[tokio::test]
async fn a_delete_missed_while_down_waits_for_the_delete_naming_it() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(4, 5))
            .await
            .expect("register");
        group_state(&broker, 3).await;
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 6))
        .await
        .expect("register the new stream");
    let groups = dir.path().join("groups");
    let old = felix_storage::disk_log::layout::incarnation_dir(&groups, &shard_key(3), 5);
    assert!(
        old.exists(),
        "a registration deleted the incarnation it replaced"
    );
    broker
        .remove_stream_from_catalog("t1", "default", "orders", Some(5))
        .await
        .expect("the catalog's delete");
    assert!(!old.exists(), "the deleted stream's shard 3 was kept");
}

/// A delete that fails part-way is finished when it is delivered again.
#[tokio::test]
async fn a_failed_delete_is_finished_when_replayed() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 5))
        .await
        .expect("register");
    group_state(&broker, 0).await;
    // The groups store cannot set anything aside.
    let blocker = dir.path().join("groups").join(".deleted");
    std::fs::write(&blocker, b"").expect("block");

    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect_err("the groups store failed");
    let cursors = felix_storage::disk_log::layout::incarnation_dir(
        &dir.path().join("groups"),
        &shard_key(0),
        5,
    );
    assert!(cursors.exists());

    std::fs::remove_file(&blocker).expect("unblock");
    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("delivered again");
    assert!(!cursors.exists(), "the delete was not finished");
}

/// A crash between marking a delete and finishing it: the next start
/// finishes it before anything is registered.
#[tokio::test]
async fn a_delete_cut_short_by_a_crash_is_finished_at_start() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        storage.shutdown().await.expect("shutdown");
        // As the delete leaves it the moment before it moves anything.
        let provider =
            felix_storage::DiskLogProvider::new(dir.path(), log_config()).expect("provider");
        provider
            .write_stream_marker(&felix_storage::StreamMarker {
                tenant: "t1".into(),
                namespace: "default".into(),
                stream: "orders".into(),
                incarnation: 0,
                retiring: true,
                shards: None,
                deleted: None,
            })
            .expect("marker");
    }
    let (broker, _storage) = broker(&dir).await;
    broker.finish_pending_deletes().await.expect("finish");
    assert!(!felix_storage::disk_log::layout::shard_dir(dir.path(), &shard_key(0)).exists());
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register again");
    assert_eq!(next_offset(&broker, "orders").await, 0);
}

/// The layout before 0.6 kept dead letters per group; deleting the stream
/// deletes those too.
#[tokio::test]
async fn a_deleted_stream_takes_its_legacy_dead_letters() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    group_state(&broker, 0).await;
    let legacy = felix_storage::disk_log::layout::shard_dir(
        &dir.path().join("dead-letters"),
        &felix_storage::log::ShardKey {
            stream: "orders\u{1f}workers".into(),
            ..shard_key(0)
        },
    );
    std::fs::create_dir_all(&legacy).expect("legacy dir");

    broker
        .remove_stream("t1", "default", "orders")
        .await
        .expect("remove");
    assert!(!legacy.exists(), "the legacy dead letters were kept");
}

/// A marker that no longer parses says nothing, so the broker starts and
/// keeps every incarnation on disk. Told the incarnation, it serves it; told
/// none, it serves nothing rather than guess, and still keeps the data.
#[tokio::test]
async fn a_damaged_marker_keeps_the_streams_data() {
    for damage in [&b""[..], &b"\0\0\0"[..], &b"felix-stream"[..]] {
        let dir = tempdir().expect("dir");
        {
            let (broker, storage) = broker(&dir).await;
            broker
                .register_stream("t1", "default", "orders", incarnation(1, 5))
                .await
                .expect("register");
            publish_three(&broker, "orders").await;
            storage.shutdown().await.expect("shutdown");
        }
        let markers = dir.path().join(".streams");
        for entry in std::fs::read_dir(&markers).expect("markers") {
            std::fs::write(entry.expect("entry").path(), damage).expect("damage");
        }

        let (broker, _storage) = broker(&dir).await;
        broker.finish_pending_deletes().await.expect("start");
        // A record without the incarnation: not served.
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("register");
        assert!(
            !broker.stream_exists("t1", "default", "orders").await,
            "served a stream it could not place"
        );
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 5))
            .await
            .expect("register");
        assert_eq!(
            next_offset(&broker, "orders").await,
            3,
            "lost the stream's records with marker {damage:?}"
        );
    }
}

/// A marker that never reached the disk (it is written without a flush) is
/// no reason to open the stream at incarnation 0 when an incarnated stream of
/// the name is on disk.
#[tokio::test]
async fn a_lost_marker_and_a_record_without_the_incarnation_serve_nothing() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 5))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        storage.shutdown().await.expect("shutdown");
    }
    std::fs::remove_dir_all(dir.path().join(".streams")).expect("lose the marker");

    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    assert!(!broker.stream_exists("t1", "default", "orders").await);
    assert!(!felix_storage::disk_log::layout::shard_dir(dir.path(), &shard_key(0)).exists());
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 5))
        .await
        .expect("register");
    assert_eq!(next_offset(&broker, "orders").await, 3);
}

/// Deleted and created again as an ephemeral stream while this broker was
/// down: the deleted durable stream's data stays until the catalog's delete
/// names it, then goes, and the marker keeps serving the ephemeral one.
#[tokio::test]
async fn a_stream_recreated_ephemeral_still_deletes_the_durable_one() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 5))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            StreamMetadata {
                durable: false,
                incarnation: 6,
                ..Default::default()
            },
        )
        .await
        .expect("register");
    let old = felix_storage::disk_log::layout::incarnation_dir(dir.path(), &shard_key(0), 5);
    assert!(
        old.exists(),
        "a registration deleted the incarnation it replaced"
    );
    broker
        .remove_stream_from_catalog("t1", "default", "orders", Some(5))
        .await
        .expect("the catalog's delete");
    assert!(!old.exists(), "the deleted stream's records were kept");
    let provider = felix_storage::DiskLogProvider::new(dir.path(), log_config()).expect("provider");
    match provider
        .stream_marker("t1", "default", "orders")
        .expect("marker")
    {
        felix_storage::MarkerRead::Valid(marker) => {
            assert_eq!(marker.incarnation, 6);
            assert_eq!(marker.deleted, Some(5));
            assert!(!marker.retiring, "the finished delete left it retiring");
        }
        other => panic!("marker {other:?}"),
    }
    assert_eq!(
        broker.stream_incarnation("t1", "default", "orders"),
        Some(6)
    );
}

/// An ephemeral stream's consumer-group state is on disk too, so its delete
/// is recorded and a delete cut short finishes at the next start.
#[tokio::test]
async fn an_ephemeral_streams_cut_short_delete_finishes_at_start() {
    let dir = tempdir().expect("dir");
    {
        let (broker, _storage) = broker(&dir).await;
        broker
            .register_stream(
                "t1",
                "default",
                "orders",
                StreamMetadata {
                    durable: false,
                    incarnation: 5,
                    ..Default::default()
                },
            )
            .await
            .expect("register");
        group_state(&broker, 0).await;
        // The groups store cannot set anything aside, so the delete stops.
        std::fs::write(dir.path().join("groups").join(".deleted"), b"").expect("block");
        broker
            .remove_stream("t1", "default", "orders")
            .await
            .expect_err("the groups store failed");
    }
    std::fs::remove_file(dir.path().join("groups").join(".deleted")).expect("unblock");
    let cursors = felix_storage::disk_log::layout::incarnation_dir(
        &dir.path().join("groups"),
        &shard_key(0),
        5,
    );
    assert!(cursors.exists());
    let (broker, _storage) = broker(&dir).await;
    broker.finish_pending_deletes().await.expect("start");
    assert!(!cursors.exists(), "the delete was not finished");
}

/// Before the fleet finalizes incarnations the control plane's delete names
/// none. The data stays and a stream created again at 0 reopens it, as before
/// incarnations: a new, empty log would be judged by the quorum marks and
/// follower positions kept for the old one, which share its generations. A
/// delete replayed after that create removes nothing either.
#[tokio::test]
async fn a_delete_naming_no_incarnation_keeps_the_data_for_a_create_at_0() {
    let dir = tempdir().expect("dir");
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("register");
    publish_three(&broker, "orders").await;

    assert!(
        broker
            .remove_stream_from_catalog("t1", "default", "orders", None)
            .await
            .expect("the catalog's delete")
    );
    assert!(!broker.stream_exists("t1", "default", "orders").await);
    assert!(
        broker
            .shard_log(felix_broker::LogKind::Stream, "t1", "default", "orders", 0)
            .await
            .is_none(),
        "a deleted stream's log opened"
    );
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("created again at 0");
    // The delete delivered again, after the create.
    broker
        .remove_stream_from_catalog("t1", "default", "orders", None)
        .await
        .expect("replayed");
    broker
        .register_stream("t1", "default", "orders", durable(1))
        .await
        .expect("created again at 0");
    assert_eq!(next_offset(&broker, "orders").await, 3, "the data went");
}

/// Once incarnations are finalized, deleting a stream from before them
/// writes a tombstone for incarnation 0, and that deletes everything kept
/// for it: records, group positions and the per-group dead letters of the
/// layout before 0.6, whether the stream is registered, was kept after an
/// earlier delete, was left by a broker that never wrote a marker, or sits
/// under a later incarnation registered since.
#[tokio::test]
async fn a_tombstone_for_0_takes_kept_data_and_legacy_dead_letters() {
    for case in ["registered", "kept", "no marker", "under a later one"] {
        let dir = tempdir().expect("dir");
        let shard = felix_storage::disk_log::layout::shard_dir(dir.path(), &shard_key(0));
        let legacy = felix_storage::disk_log::layout::shard_dir(
            &dir.path().join("dead-letters"),
            &felix_storage::log::ShardKey {
                stream: "orders\u{1f}workers".into(),
                ..shard_key(0)
            },
        );
        {
            let (broker, storage) = broker(&dir).await;
            broker
                .register_stream("t1", "default", "orders", durable(1))
                .await
                .expect("register");
            publish_three(&broker, "orders").await;
            group_state(&broker, 0).await;
            std::fs::create_dir_all(&legacy).expect("legacy dir");
            if case == "kept" {
                broker
                    .remove_stream_from_catalog("t1", "default", "orders", None)
                    .await
                    .expect("delete naming none");
            }
            storage.shutdown().await.expect("shutdown");
        }
        if case == "no marker" {
            std::fs::remove_dir_all(dir.path().join(".streams")).expect("no marker");
        }
        let (broker, _storage) = broker(&dir).await;
        if case == "registered" {
            broker
                .register_stream("t1", "default", "orders", durable(1))
                .await
                .expect("register");
        }
        if case == "under a later one" {
            broker
                .register_stream("t1", "default", "orders", incarnation(1, 9))
                .await
                .expect("register");
            publish_three(&broker, "orders").await;
        }
        assert!(shard.exists() && legacy.exists(), "{case}: went early");
        broker
            .remove_stream_from_catalog("t1", "default", "orders", Some(0))
            .await
            .expect("the tombstone");
        assert!(!shard.exists(), "{case}: the records were kept");
        assert!(
            !legacy.exists(),
            "{case}: the legacy dead letters were kept"
        );
        assert_no_group_state_on_disk(&dir, case);
        if case == "under a later one" {
            assert_eq!(
                next_offset(&broker, "orders").await,
                3,
                "the live one changed"
            );
            continue;
        }
        // Nothing comes back at 0.
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("register at 0");
        assert!(
            !broker.stream_exists("t1", "default", "orders").await,
            "{case}: a deleted incarnation was registered again"
        );
    }
}

fn assert_no_group_state_on_disk(dir: &TempDir, case: &str) {
    for store in ["groups", "dead-letters"] {
        let cursors =
            felix_storage::disk_log::layout::shard_dir(&dir.path().join(store), &shard_key(0));
        assert!(!cursors.exists(), "{case}: {store} kept");
    }
}

/// A delete cut short by a crash still finds the per-group dead letters of
/// the layout before 0.6: the retiring marker keeps the shard count.
#[tokio::test]
async fn a_delete_cut_short_still_takes_the_legacy_dead_letters() {
    let dir = tempdir().expect("dir");
    let legacy = felix_storage::disk_log::layout::shard_dir(
        &dir.path().join("dead-letters"),
        &felix_storage::log::ShardKey {
            stream: "orders\u{1f}workers".into(),
            ..shard_key(0)
        },
    );
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", durable(1))
            .await
            .expect("register");
        group_state(&broker, 0).await;
        std::fs::create_dir_all(&legacy).expect("legacy dir");
        storage.shutdown().await.expect("shutdown");
        // As a delete leaves it the moment before it moves anything.
        let provider =
            felix_storage::DiskLogProvider::new(dir.path(), log_config()).expect("provider");
        provider
            .write_stream_marker(&felix_storage::StreamMarker {
                tenant: "t1".into(),
                namespace: "default".into(),
                stream: "orders".into(),
                incarnation: 0,
                retiring: true,
                shards: Some(1),
                deleted: None,
            })
            .expect("marker");
    }
    let (broker, _storage) = broker(&dir).await;
    broker.finish_pending_deletes().await.expect("finish");
    assert!(!legacy.exists(), "the legacy dead letters were kept");
}

/// A catalog view older than one already applied (a control-plane member
/// behind, or a restart seeded from one) names a lower incarnation than the
/// one held here. It does not replace it, before or after a restart, and
/// nothing is deleted.
#[tokio::test]
async fn an_older_catalog_never_replaces_the_live_incarnation() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 8))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 7))
            .await
            .expect("an older create");
        assert_eq!(
            broker.stream_incarnation("t1", "default", "orders"),
            Some(8)
        );
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 7))
        .await
        .expect("an older snapshot");
    assert_eq!(
        broker.stream_incarnation("t1", "default", "orders"),
        None,
        "a catalog naming 7 replaced the live incarnation"
    );
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 8))
        .await
        .expect("the live one");
    assert_eq!(
        next_offset(&broker, "orders").await,
        3,
        "the live data went"
    );
}

/// An incarnation the catalog deleted is never registered again, even by a
/// broker that holds nothing of the name any more: a restart seeded by a
/// member that has not applied the delete yet would otherwise serve a deleted
/// stream, and take writes the delete then throws away.
#[tokio::test]
async fn a_deleted_incarnation_is_never_registered_again() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 7))
            .await
            .expect("register");
        broker
            .remove_stream_from_catalog("t1", "default", "orders", Some(7))
            .await
            .expect("delete");
        storage.shutdown().await.expect("shutdown");
    }
    for older in [7, 6] {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, older))
            .await
            .expect("an older snapshot");
        assert_eq!(
            broker.stream_incarnation("t1", "default", "orders"),
            None,
            "the deleted incarnation {older} was registered again"
        );
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 9))
        .await
        .expect("a later one");
    assert_eq!(
        broker.stream_incarnation("t1", "default", "orders"),
        Some(9)
    );
}

/// A restore to a backup point puts the control plane back, and with it the
/// incarnation the point names, below one this broker registered or saw
/// deleted since. `restore-point` records the point's incarnation, so the
/// broker takes it rather than refusing it for good, and deletes nothing.
#[tokio::test]
async fn a_restore_to_a_backup_point_takes_the_points_incarnation() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 5))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        // After the point: deleted and created again.
        broker
            .remove_stream_from_catalog("t1", "default", "orders", Some(4))
            .await
            .expect("an earlier tombstone");
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 8))
            .await
            .expect("register");
        storage.shutdown().await.expect("shutdown");
    }
    {
        // `restore-point`, on the stopped broker.
        let (broker, storage) = broker(&dir).await;
        broker
            .restore_stream_marker("t1", "default", "orders", 5)
            .await
            .expect("restore the marker");
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    broker
        .register_stream("t1", "default", "orders", incarnation(1, 5))
        .await
        .expect("the point's incarnation");
    assert_eq!(
        broker.stream_incarnation("t1", "default", "orders"),
        Some(5)
    );
    assert_eq!(
        next_offset(&broker, "orders").await,
        3,
        "the point's data went"
    );
    let later = felix_storage::disk_log::layout::incarnation_dir(dir.path(), &shard_key(0), 8);
    assert!(later.exists(), "the restore deleted data");
}

/// A snapshot's tombstones, as a broker joining the fleet reads them: most
/// name streams it never had, whose floors it records together, and one names
/// the stream it holds, which goes. Both hold across a restart.
#[tokio::test]
async fn a_snapshots_tombstones_record_floors_and_delete_held_data() {
    let dir = tempdir().expect("dir");
    let names: Vec<String> = (0..50).map(|i| format!("gone-{i}")).collect();
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 5))
            .await
            .expect("register");
        publish_three(&broker, "orders").await;
        let mut tombstones = vec![("t1", "default", "orders", 5)];
        tombstones.extend(names.iter().map(|name| ("t1", "default", name.as_str(), 3)));
        broker
            .apply_tombstones(&tombstones)
            .await
            .expect("the tombstones");
        assert_eq!(broker.stream_incarnation("t1", "default", "orders"), None);
        // Applied again, as a resnapshot does: nothing left to do.
        broker
            .apply_tombstones(&tombstones)
            .await
            .expect("the tombstones again");
        storage.shutdown().await.expect("shutdown");
    }
    let (broker, _storage) = broker(&dir).await;
    for (name, deleted) in [("orders", 5), ("gone-0", 3), ("gone-49", 3)] {
        broker
            .register_stream("t1", "default", name, incarnation(1, deleted))
            .await
            .expect("an older snapshot");
        assert_eq!(
            broker.stream_incarnation("t1", "default", name),
            None,
            "the deleted incarnation of {name} was registered again"
        );
        broker
            .register_stream("t1", "default", name, incarnation(1, deleted + 1))
            .await
            .expect("a later one");
        assert_eq!(next_offset(&broker, name).await, 0, "{name} kept data");
    }
}

/// A tombstone's floor survives losing the stream markers, as a power loss can
/// leave a marker that was not flushed. A member that has not applied the
/// delete, seeding the deleted incarnation after the restart, is refused.
#[tokio::test]
async fn a_tombstone_floor_outlives_its_markers() {
    let dir = tempdir().expect("dir");
    {
        let (broker, storage) = broker(&dir).await;
        broker
            .register_stream("t1", "default", "orders", incarnation(1, 9))
            .await
            .expect("register");
        broker
            .apply_tombstones(&[("t1", "default", "orders", 5), ("t1", "default", "gone", 3)])
            .await
            .expect("the tombstones");
        storage.shutdown().await.expect("shutdown");
    }
    for entry in std::fs::read_dir(dir.path().join(".streams")).expect("markers") {
        let path = entry.expect("entry").path();
        if !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with('.'))
        {
            std::fs::remove_file(path).expect("lose a marker");
        }
    }
    let (broker, _storage) = broker(&dir).await;
    for (name, deleted) in [("orders", 5), ("gone", 3)] {
        broker
            .register_stream("t1", "default", name, incarnation(1, deleted))
            .await
            .expect("a lagging snapshot");
        assert_eq!(
            broker.stream_incarnation("t1", "default", name),
            None,
            "the deleted incarnation of {name} was registered"
        );
    }
}

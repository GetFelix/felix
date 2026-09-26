//! What a group gave up on, and whose it was.

use felix_storage::log::FsyncMode;

use super::*;

fn config() -> LogConfig {
    LogConfig {
        segment_size_bytes: 64 * 1024,
        index_spacing_bytes: 256,
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    }
}

fn key(stream: &str, group: &str) -> GroupKey {
    GroupKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        group: group.to_string(),
    }
}

#[tokio::test]
async fn nothing_is_dead_lettered_to_begin_with() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");

    assert_eq!(
        dead.list(&key("jobs", "workers")).await.expect("list"),
        Vec::<u64>::new(),
    );
}

/// A redrive is one durable state change on the entry: off the list, onto the
/// redriven set, and back to dead if the record is given up on again.
#[tokio::test]
async fn a_redrive_moves_an_entry_between_states() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key("jobs", "workers");
    {
        let dead = DeadLetters::open(dir.path(), config()).expect("open");
        dead.record(&key, 3).await.expect("record");
        dead.record(&key, 7).await.expect("record");
        assert!(dead.redrive(&key, 3).await.expect("redrive"));
        assert!(
            !dead.redrive(&key, 3).await.expect("again"),
            "redriven twice"
        );
        assert!(!dead.redrive(&key, 99).await.expect("unknown"));
        assert!(
            !dead.discard(&key, 3).await.expect("discard"),
            "owed and dropped"
        );
    }

    // Reopened, as a new leader would find it.
    let dead = DeadLetters::open(dir.path(), config()).expect("reopen");
    assert_eq!(dead.list(&key).await.expect("list"), vec![7]);
    assert_eq!(dead.redriven(&key).await.expect("redriven"), vec![3]);

    dead.record(&key, 3).await.expect("given up again");
    assert_eq!(dead.list(&key).await.expect("list"), vec![3, 7]);
    assert!(dead.redriven(&key).await.expect("redriven").is_empty());
    // Finishing a redrive leaves an entry that went back to dead alone.
    dead.finish_redrive(&key, 3).await.expect("finish");
    assert_eq!(dead.list(&key).await.expect("list"), vec![3, 7]);

    assert!(dead.redrive(&key, 7).await.expect("redrive"));
    dead.finish_redrive(&key, 7).await.expect("finish");
    assert!(dead.redriven(&key).await.expect("redriven").is_empty());
    assert_eq!(dead.list(&key).await.expect("list"), vec![3]);
}

#[tokio::test]
async fn recorded_offsets_come_back_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");
    let key = key("jobs", "workers");

    for offset in [12u64, 3, 40] {
        dead.record(&key, offset).await.expect("record");
    }

    assert_eq!(dead.list(&key).await.expect("list"), vec![3, 12, 40]);
}

/// Recording the same offset twice is one dead letter, not two. A retry of the
/// pass that records it must not double-count.
#[tokio::test]
async fn recording_the_same_offset_twice_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");
    let key = key("jobs", "workers");

    dead.record(&key, 7).await.expect("record");
    dead.record(&key, 7).await.expect("record");

    assert_eq!(dead.list(&key).await.expect("list"), vec![7]);
}

/// Two groups reading one stream fail on different records. Merging their dead
/// letters would have each answering for work the other gave up on.
#[tokio::test]
async fn two_groups_on_one_stream_keep_separate_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");

    dead.record(&key("jobs", "alpha"), 1).await.expect("record");
    dead.record(&key("jobs", "beta"), 2).await.expect("record");

    assert_eq!(
        dead.list(&key("jobs", "alpha")).await.expect("list"),
        vec![1]
    );
    assert_eq!(
        dead.list(&key("jobs", "beta")).await.expect("list"),
        vec![2]
    );
}

/// And two streams read by a group of the same name stay separate too.
#[tokio::test]
async fn two_streams_keep_separate_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");

    dead.record(&key("jobs", "workers"), 1)
        .await
        .expect("record");
    dead.record(&key("orders", "workers"), 2)
        .await
        .expect("record");

    assert_eq!(
        dead.list(&key("jobs", "workers")).await.expect("list"),
        vec![1]
    );
    assert_eq!(
        dead.list(&key("orders", "workers")).await.expect("list"),
        vec![2]
    );
}

/// A dead letter that vanished on restart would leave the record skipped with
/// nothing recording that it was ever tried.
#[tokio::test]
async fn dead_letters_survive_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key("jobs", "workers");

    {
        let dead = DeadLetters::open(dir.path(), config()).expect("open");
        dead.record(&key, 99).await.expect("record");
        dead.shutdown().await.expect("shutdown");
    }

    let reopened = DeadLetters::open(dir.path(), config()).expect("open");
    assert_eq!(reopened.list(&key).await.expect("list"), vec![99]);
}

/// A discarded offset is gone from the list, and discarding what was never
/// there is an answer.
#[tokio::test]
async fn a_discarded_offset_leaves_the_list() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dead = DeadLetters::open(dir.path(), config()).expect("open");
    let key = key("jobs", "workers");

    dead.record(&key, 5).await.expect("record");
    assert!(dead.discard(&key, 5).await.expect("discard"));
    assert_eq!(dead.list(&key).await.expect("list"), Vec::<u64>::new());
    assert!(
        !dead.discard(&key, 5).await.expect("discard again"),
        "discarding what is not there is an answer, not a repeat",
    );
}

/// Dead letters recorded under the earlier one-log-per-(stream, group) layout
/// are still listed and can still be discarded. Losing them on upgrade would
/// silently drop exactly the records an operator was told to look at.
#[tokio::test]
async fn a_legacy_layout_is_read_and_discarded_but_never_written() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key("jobs", "workers");

    // What the old layout wrote: one log per (stream, group), the offset as
    // the whole key. Written through a plain LogCache exactly as the old code
    // did, so this is the bytes an upgraded broker actually finds.
    {
        let legacy = LogCache::open(dir.path(), config()).expect("open legacy");
        for offset in [4u64, 9] {
            legacy
                .put_checked(
                    &key.tenant_id,
                    &key.namespace,
                    &format!("{}\u{1f}{}", key.stream, key.group),
                    key.shard,
                    &offset.to_string(),
                    bytes::Bytes::new(),
                    None,
                )
                .await
                .expect("legacy record");
        }
    }

    let dead = DeadLetters::open(dir.path(), config()).expect("open");
    // New entries land in the per-shard log; both layouts list as one.
    dead.record(&key, 12).await.expect("record");
    assert_eq!(dead.list(&key).await.expect("list"), vec![4, 9, 12]);

    // A legacy entry can be discarded, and stays discarded.
    assert!(dead.discard(&key, 9).await.expect("discard legacy"));
    assert_eq!(dead.list(&key).await.expect("list"), vec![4, 12]);

    // And redriven: it moves to the per-shard log as redriven.
    assert!(dead.redrive(&key, 4).await.expect("redrive legacy"));
    assert_eq!(dead.list(&key).await.expect("list"), vec![12]);
    assert_eq!(dead.redriven(&key).await.expect("redriven"), vec![4]);

    // A group with no legacy directory costs a path check and creates
    // nothing: the listing for a fresh group is clean, not littered with an
    // empty legacy log.
    assert_eq!(
        dead.list(&super::tests::key("jobs", "fresh"))
            .await
            .expect("list"),
        Vec::<u64>::new(),
    );
}

//! What the generation history promises, and where it refuses to answer.
//!
//! Its answers become truncation points, so a wrong one costs records. Every
//! case below that ends in `None` is there because guessing would be worse.
use super::*;

fn map(entries: &[(u64, Offset)]) -> EpochMap {
    let mut map = EpochMap::default();
    for (generation, start) in entries {
        map.record(*generation, *start);
    }
    map
}

#[test]
fn a_generation_ends_where_the_next_one_begins() {
    let map = map(&[(1, 0), (2, 100), (3, 250)]);
    assert_eq!(map.end_of(1, 400), Some(100));
    assert_eq!(map.end_of(2, 400), Some(250));
}

#[test]
fn the_newest_generation_ends_at_the_tail() {
    // It has not ended yet, so the tail is as far as it goes.
    let map = map(&[(1, 0), (2, 100)]);
    assert_eq!(map.end_of(2, 400), Some(400));
}

#[test]
fn a_generation_never_seen_has_no_end() {
    // The case that must refuse. Interpolating between neighbours would be a
    // plausible-looking truncation point with nothing behind it.
    let map = map(&[(1, 0), (3, 250)]);
    assert_eq!(map.end_of(2, 400), None);
    assert_eq!(map.end_of(9, 400), None);
    assert_eq!(EpochMap::default().end_of(1, 400), None);
}

#[test]
fn an_older_generation_does_not_rewrite_history() {
    // A stale message from a deposed leader must not move where a later
    // generation is recorded as starting.
    let mut map = map(&[(5, 100)]);
    assert!(!map.record(4, 50), "an older generation was accepted");
    assert!(!map.record(5, 999), "the same generation was re-recorded");
    assert_eq!(
        map.newest(),
        Some(Epoch {
            generation: 5,
            start_offset: 100
        })
    );
}

#[test]
fn the_newest_shared_generation_is_the_last_one_they_cannot_disagree_within() {
    let mine = map(&[(1, 0), (2, 100), (4, 300)]);
    let theirs = [
        Epoch {
            generation: 1,
            start_offset: 0,
        },
        Epoch {
            generation: 2,
            start_offset: 100,
        },
        Epoch {
            generation: 3,
            start_offset: 220,
        },
    ];

    // Not 4 (they have never heard of it) and not 3 (this broker has not).
    assert_eq!(
        mine.newest_shared(&theirs),
        Some(Epoch {
            generation: 2,
            start_offset: 100
        }),
    );
}

#[test]
fn two_histories_with_nothing_in_common_share_nothing() {
    // Which is a refusal to repair, not a repair to offset zero.
    let mine = map(&[(7, 0)]);
    let theirs = [Epoch {
        generation: 2,
        start_offset: 0,
    }];
    assert_eq!(mine.newest_shared(&theirs), None);
    assert_eq!(mine.newest_shared(&[]), None);
}

#[test]
fn truncating_forgets_generations_that_start_at_or_after_the_cut() {
    let mut map = map(&[(1, 0), (2, 100), (3, 250)]);
    map.truncate_from(250);
    assert_eq!(
        map.newest(),
        Some(Epoch {
            generation: 2,
            start_offset: 100
        })
    );

    // The history must not outlive the records it describes: a generation
    // starting at an offset the log no longer holds would answer `end_of` with
    // a point that cannot be read.
    map.truncate_from(100);
    assert_eq!(
        map.newest(),
        Some(Epoch {
            generation: 1,
            start_offset: 0
        })
    );
}

#[test]
fn history_round_trips_through_a_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let written = map(&[(1, 0), (2, 100), (7, 4096)]);

    store(dir.path(), &written).expect("store");
    assert_eq!(load(dir.path()), written);
    // The temporary is gone rather than left beside the real file.
    assert!(!dir.path().join("epochs.tmp").exists());
}

#[test]
fn an_absent_history_reads_as_empty_rather_than_failing() {
    // Every shard written before this existed has none, and a broker that
    // refused to open them would trade an outage for a convenience.
    let dir = tempfile::tempdir().expect("tempdir");
    assert!(load(dir.path()).is_empty());
}

#[test]
fn a_corrupt_history_reads_as_empty_rather_than_as_a_wrong_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    store(dir.path(), &map(&[(1, 0), (2, 100)])).expect("store");

    // Flip a byte in the body. The checksum is what stops this being read back
    // as a confident answer about where generation 2 began.
    let path = dir.path().join(epochs_file_name());
    let mut bytes = std::fs::read(&path).expect("read");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, &bytes).expect("write");

    assert!(
        load(dir.path()).is_empty(),
        "a corrupt history was read as real, so a truncation point could come \
         from flipped bytes",
    );
}

#[test]
fn a_file_from_another_format_is_not_read_as_ours() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join(epochs_file_name()), b"not ours at all").expect("write");
    assert!(load(dir.path()).is_empty());
}

#[test]
fn the_history_is_bounded() {
    // One entry per leadership change is small, but not bounded on its own —
    // and every open reads the whole file.
    let mut map = EpochMap::default();
    for generation in 0..(MAX_ENTRIES as u64 * 2) {
        map.record(generation, generation * 10);
    }
    assert_eq!(map.entries().len(), MAX_ENTRIES);
    // The newest are what a divergence can be within; the oldest are gone.
    assert_eq!(
        map.newest(),
        Some(Epoch {
            generation: MAX_ENTRIES as u64 * 2 - 1,
            start_offset: (MAX_ENTRIES as u64 * 2 - 1) * 10,
        }),
    );
}

/// The history survives a reopen, and a truncation takes it with it.
///
/// These go through `DiskLog` rather than the map alone, because the failure
/// they guard against is the wiring: a history held only in memory answers
/// confidently until a restart, and one that outlives a truncation answers with
/// offsets the log no longer holds.
mod through_the_log {
    use crate::disk_log::DiskLog;
    use crate::log::AppendOnlyLog;
    use crate::log::{FsyncMode, LogConfig};
    use bytes::Bytes;

    fn config() -> LogConfig {
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        }
    }

    async fn append(log: &DiskLog, values: &[&str]) {
        for value in values {
            log.append(&[crate::log::AppendRecord {
                payload: Bytes::from((*value).to_string()),
                timestamp_micros: 0,
                mark: Default::default(),
                publisher: None,
            }])
            .await
            .expect("append");
        }
    }

    #[tokio::test]
    async fn a_generation_history_survives_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let log = DiskLog::open(dir.path(), "shard", config()).expect("open");
            append(&log, &["a", "b"]).await;
            assert!(log.record_generation(4, 0).expect("record"));
            append(&log, &["c"]).await;
            assert!(log.record_generation(5, 2).expect("record"));
        }

        let reopened = DiskLog::open(dir.path(), "shard", config()).expect("reopen");
        assert_eq!(reopened.generations().len(), 2);
        assert_eq!(reopened.generation_end(4, 3), Some(2));
        assert_eq!(
            reopened.generation_end(5, 3),
            Some(3),
            "the newest generation ends at the tail",
        );
    }

    #[tokio::test]
    async fn truncating_forgets_the_generations_it_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = DiskLog::open(dir.path(), "shard", config()).expect("open");
        append(&log, &["a", "b"]).await;
        log.record_generation(4, 0).expect("record");
        append(&log, &["c", "d"]).await;
        log.record_generation(5, 2).expect("record");

        log.truncate(2).await.expect("truncate");

        assert_eq!(
            log.generation_end(5, 2),
            None,
            "a generation starting at the cut still answers, so a later repair \
             would truncate to an offset the log no longer holds",
        );
        assert_eq!(log.generation_end(4, 2), Some(2));

        // And the forgetting is durable, not just in memory.
        drop(log);
        let reopened = DiskLog::open(dir.path(), "shard", config()).expect("reopen");
        assert_eq!(reopened.generations().len(), 1);
    }
}

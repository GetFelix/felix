//! Moving a log onto segment format v9 once its gate allows it: only new
//! segments move, and every record in one carries the generation that wrote
//! it. A leader writes the generation it recorded as its own, a copied batch
//! the generations it came with, and a record below the one before it is
//! refused, across a roll, a reopen and a truncation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::log::{Epoch, UNKNOWN_WRITER, WrittenBy};
use crate::segment::format::{decode_record_in, encode_record_at};
use crate::segment::{
    GENERATION_VERSION, HEADERS_VERSION, OLDEST_READABLE_VERSION, SEGMENT_HEADER_LEN,
    SegmentHeader, segment_file_name,
};

const LEADER: &str = "a";

fn gated(open: &Arc<AtomicBool>) -> LogConfig {
    let config = config(FsyncMode::OnCommit);
    let open = Arc::clone(open);
    config
        .generations
        .open_when(move || open.load(Ordering::Acquire));
    config
}

fn open_gated(dir: &TempDir) -> DiskLog {
    DiskLog::open(
        dir.path(),
        "t/ns/s/0",
        gated(&Arc::new(AtomicBool::new(true))),
    )
    .expect("open")
}

/// Each segment's version and the generation of every record in it.
fn generations(dir: &TempDir, log: &DiskLog) -> Vec<(u16, Vec<Option<u64>>)> {
    log.segments()
        .iter()
        .map(|segment| {
            let bytes =
                std::fs::read(dir.path().join(segment_file_name(segment.id))).expect("read");
            let version = SegmentHeader::decode(&bytes).expect("header").version;
            let mut at = SEGMENT_HEADER_LEN as usize;
            let mut found = Vec::new();
            while let Ok((record, len)) = decode_record_in(&bytes[at..], version) {
                found.push(record.header.generation);
                at += len as usize;
            }
            (version, found)
        })
        .collect()
}

/// The generation of every record, oldest first, `None` for a pre-v9 one.
fn flat(dir: &TempDir, log: &DiskLog) -> Vec<Option<u64>> {
    generations(dir, log)
        .into_iter()
        .flat_map(|(_, found)| found)
        .collect()
}

/// Make `generation` this log's leader: accepted, and recorded as starting at
/// the tail, as a leader does before it serves.
async fn lead(log: &DiskLog, generation: u64) {
    log.accept_generation(generation, Some(LEADER))
        .await
        .expect("accept");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("record");
    log.lead_at(generation);
}

/// A log holding one older record, at offset 0, whose active segment is v9.
async fn v9_log(dir: &TempDir) -> DiskLog {
    let log = open_gated(dir);
    log.append(&records(&["old"])).await.expect("append");
    log.roll_now().await.expect("roll");
    assert_eq!(
        log.segments().last().expect("active").version,
        GENERATION_VERSION
    );
    log
}

/// The generation of every record from `start`, as a reader sees it.
async fn read_generations(log: &DiskLog, start: Offset) -> Vec<Option<u64>> {
    log.read_range(ReadRange {
        start,
        max_bytes: usize::MAX,
    })
    .await
    .expect("read")
    .records
    .into_iter()
    .map(|record| record.generation)
    .collect()
}

fn run(generation: u64, start_offset: Offset) -> Epoch {
    Epoch {
        generation,
        start_offset,
    }
}

fn names(from: usize, count: usize) -> Vec<AppendRecord> {
    let payloads: Vec<String> = (from..from + count).map(|n| format!("r{n}")).collect();
    let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
    records(&refs)
}

/// Copy `batch` in at `first` as `writers` wrote it.
async fn copy_in(
    log: &DiskLog,
    first: Offset,
    batch: &[AppendRecord],
    writers: WrittenBy,
) -> Result<()> {
    log.append_pending_at(first, batch, writers)
        .await
        .map(|written| assert!(written.is_some(), "the tail moved"))
}

/// Appending below the floor `floor` as a leader at `generation` is refused.
fn assert_below(error: Option<StorageError>, generation: u64, floor: u64) {
    match error {
        Some(StorageError::GenerationBelowFloor {
            generation: found,
            floor: held,
            ..
        }) => assert_eq!((found, held), (generation, floor)),
        other => panic!("expected GenerationBelowFloor, got {other:?}"),
    }
}

async fn reopen(dir: &TempDir, log: DiskLog) -> DiskLog {
    log.shutdown().await.expect("shutdown");
    drop(log);
    open_gated(dir)
}

/// Rewrite the first record of `segment` at `generation`, with its checksums
/// fitted, as a writer that ignored the floor would have.
fn restamp_first(dir: &TempDir, segment: u64, generation: u64) {
    let path = dir.path().join(segment_file_name(segment));
    let mut bytes = std::fs::read(&path).expect("read");
    let at = SEGMENT_HEADER_LEN as usize;
    let (record, len) = decode_record_in(&bytes[at..], GENERATION_VERSION).expect("record");
    let mut encoded = Vec::new();
    encode_record_at(
        &mut encoded,
        Some(generation),
        record.header.offset,
        record.header.timestamp_micros,
        &record.payload,
        record.headers.as_deref(),
        record.publisher.as_deref(),
        &record.mark,
    );
    assert_eq!(encoded.len() as u64, len);
    bytes[at..at + encoded.len()].copy_from_slice(&encoded);
    std::fs::write(&path, bytes).expect("write");
}

/// Until the gate opens nothing is written at v9. Once it does, the segment
/// already active is left as it is and the next one is v9, with every record
/// at the leader's generation.
#[tokio::test]
async fn only_a_new_segment_moves_onto_v9_once_the_gate_opens() {
    let dir = tempdir().expect("dir");
    let open = Arc::new(AtomicBool::new(false));
    let log = DiskLog::open(dir.path(), "t/ns/s/0", gated(&open)).expect("open");
    lead(&log, 3).await;
    for payload in ["a", "b", "c", "d", "e", "f"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    let closed = generations(&dir, &log);
    assert!(closed.len() > 1, "never rolled");
    assert!(
        closed
            .iter()
            .all(|(version, _)| *version < GENERATION_VERSION)
    );

    open.store(true, Ordering::Release);
    log.append(&records(&["g"])).await.expect("append");
    let active = closed.last().expect("active").0;
    assert_eq!(
        generations(&dir, &log).last().expect("active").0,
        active,
        "the active segment was rewritten"
    );
    log.roll_now().await.expect("roll");
    for payload in ["h", "i", "j", "k"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    let opened = generations(&dir, &log);
    let moved = &opened[closed.len()..];
    assert!(moved.len() > 1, "never rolled after the gate");
    for (version, found) in moved {
        assert_eq!(*version, GENERATION_VERSION);
        assert!(found.iter().all(|generation| *generation == Some(3)));
    }
    let log = reopen(&dir, log).await;
    assert_eq!(
        read_all(&log, 0).await,
        ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"]
    );
}

/// A leader's record takes the generation it recorded as its own, and a newer
/// one takes effect from the next append.
#[tokio::test]
async fn a_leader_stamps_the_generation_it_recorded() {
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append(&records(&["v3"])).await.expect("append");
    log.roll_now().await.expect("roll");
    lead(&log, 4).await;
    log.append(&records(&["a", "b"])).await.expect("append");
    lead(&log, 6).await;
    log.append(&records(&["c"])).await.expect("append");
    assert_eq!(flat(&dir, &log), [None, Some(4), Some(4), Some(6)]);
}

/// A record written above the leader's generation (here by the test hook)
/// sets a floor that holds after a reopen: a leader below it is refused,
/// and one at it writes at its own generation. The floor comes from the
/// active segment's full scan.
#[tokio::test]
async fn the_floor_survives_a_reopen() {
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append_v9_for_tests(&records(&["a"]), 7)
        .await
        .expect("append");
    let log = reopen(&dir, log).await;
    lead(&log, 3).await;
    assert_below(log.append(&records(&["b"])).await.err(), 3, 7);
    assert_eq!(log.tail_offset().await.expect("tail"), 1);
    lead(&log, 7).await;
    log.append(&records(&["b"])).await.expect("append");
    let log = reopen(&dir, log).await;
    assert_eq!(flat(&dir, &log), [Some(7), Some(7)]);
}

/// The floor follows the log onto a new segment, whether the roll is inline
/// (the segment is full) or in the background. A reopen with the active
/// segment empty takes it from the sealed one before.
#[tokio::test]
async fn the_floor_survives_a_roll() {
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append_v9_for_tests(&records(&["a"]), 7)
        .await
        .expect("append");
    log.roll_now().await.expect("background roll");
    lead(&log, 4).await;
    assert_below(log.append(&records(&["b"])).await.err(), 4, 7);
    lead(&log, 7).await;
    log.append(&records(&["b"])).await.expect("append");
    for payload in ["c", "d", "e", "f"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    log.roll_now().await.expect("background roll");
    let log = reopen(&dir, log).await;
    // Copied from a generation below the floor, on the empty active segment.
    let tail = log.tail_offset().await.expect("tail");
    let below = copy_in(
        &log,
        tail,
        &names(0, 1),
        WrittenBy::Writers(vec![run(3, 0)]),
    )
    .await;
    assert!(matches!(
        below,
        Err(StorageError::GenerationBelowFloor { floor: 7, .. })
    ));
    // Leading is not kept across a reopen; a broker opens the shard to lead it.
    lead(&log, 7).await;
    log.append(&records(&["g"])).await.expect("append");
    let log = reopen(&dir, log).await;
    let found = generations(&dir, &log);
    assert!(found.len() > 3, "never rolled inline: {found:?}");
    assert!(
        flat(&dir, &log)
            .iter()
            .all(|generation| *generation == Some(7)),
        "{found:?}"
    );
}

/// A cut leaves the newest kept record's generation as the floor: inside the
/// active segment, at its base (the floor is the sealed segment's last), and
/// inside a sealed segment that becomes active again.
#[tokio::test]
async fn the_floor_survives_a_truncation() {
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append(&records(&["v3"])).await.expect("append");
    log.roll_now().await.expect("roll");
    // Offsets 1 and 2 at 5, then 3 and 4 at 9, all in one segment.
    log.append_v9_for_tests(&records(&["a", "b"]), 5)
        .await
        .expect("append");
    log.append_v9_for_tests(&records(&["c", "d"]), 9)
        .await
        .expect("append");

    // Inside the active segment, below the records at 9: the floor is 5.
    log.truncate(3).await.expect("truncate");
    lead(&log, 2).await;
    assert_below(log.append(&records(&["e"])).await.err(), 2, 5);
    lead(&log, 5).await;
    log.append(&records(&["e"])).await.expect("append");
    let log = reopen(&dir, log).await;
    assert_eq!(flat(&dir, &log), [None, Some(5), Some(5), Some(5)]);

    // At the active segment's base, which drops that segment: the floor
    // comes from the full scan of the one before it.
    log.roll_now().await.expect("roll");
    log.append_v9_for_tests(&records(&["f"]), 9)
        .await
        .expect("append");
    let base = log.segments().last().expect("active").base_offset;
    log.truncate(base).await.expect("truncate");
    let tail = log.tail_offset().await.expect("tail");
    let below = copy_in(
        &log,
        tail,
        &names(0, 1),
        WrittenBy::Writers(vec![run(4, 0)]),
    )
    .await;
    assert!(matches!(
        below,
        Err(StorageError::GenerationBelowFloor { floor: 5, .. })
    ));
    lead(&log, 5).await;
    log.append(&records(&["g"])).await.expect("append");
    let log = reopen(&dir, log).await;
    assert_eq!(flat(&dir, &log).last(), Some(&Some(5)));

    // Inside a sealed segment, which becomes the active one again.
    log.roll_now().await.expect("roll");
    log.append_v9_for_tests(&records(&["h"]), 9)
        .await
        .expect("append");
    log.truncate(2).await.expect("truncate");
    let tail = log.tail_offset().await.expect("tail");
    let below = copy_in(
        &log,
        tail,
        &names(0, 1),
        WrittenBy::Writers(vec![run(4, 0)]),
    )
    .await;
    assert!(matches!(
        below,
        Err(StorageError::GenerationBelowFloor { floor: 5, .. })
    ));
    // The cut dropped the record of where 5 began.
    lead(&log, 5).await;
    log.append(&records(&["i"])).await.expect("append");
    let log = reopen(&dir, log).await;
    assert_eq!(flat(&dir, &log), [None, Some(5), Some(5)]);
    assert_eq!(read_all(&log, 0).await, ["v3", "a", "i"]);
}

/// Each segment's scan checks generations inside it; the open also checks
/// them across segments. A segment starting below the one before it is
/// damage, whether it is sealed or active.
#[tokio::test]
async fn a_generation_falling_across_segments_refuses_the_open() {
    for damaged in [1, 2] {
        let dir = tempdir().expect("dir");
        let log = open_gated(&dir);
        for _ in 0..3 {
            log.append_v9_for_tests(&records(&["a", "b"]), 9)
                .await
                .expect("append");
            log.roll_now().await.expect("roll");
        }
        log.append_v9_for_tests(&records(&["c"]), 9)
            .await
            .expect("append");
        let segments = log.segments();
        log.shutdown().await.expect("shutdown");
        drop(log);
        // The second segment is sealed; the last one is active.
        let segment = if damaged == 1 {
            segments[1].id
        } else {
            segments.last().expect("active").id
        };
        restamp_first(&dir, segment, 3);
        let refused = DiskLog::open(
            dir.path(),
            "t/ns/s/0",
            gated(&Arc::new(AtomicBool::new(true))),
        )
        .expect_err("a falling generation opened");
        assert!(
            matches!(
                &refused,
                StorageError::Corruption(corruption) if matches!(
                    corruption.kind,
                    crate::CorruptionKind::GenerationFell { previous: 9, found: 3 }
                )
            ),
            "segment {segment}: {refused}"
        );
    }
}

/// A committed vote is written as ballot format v2, which the previous
/// release refuses, so it waits for the same gate as the segment format.
#[tokio::test]
async fn a_committed_vote_waits_for_the_gate() {
    let dir = tempdir().expect("dir");
    let open = Arc::new(AtomicBool::new(false));
    let log = DiskLog::open(dir.path(), "t/ns/s/0", gated(&open)).expect("open");
    log.accept_generation(2, Some(LEADER))
        .await
        .expect("accept");
    let before = std::fs::read(dir.path().join("ballot")).expect("ballot");
    assert!(matches!(
        log.commit_vote(2, LEADER)
            .await
            .expect_err("before the gate"),
        StorageError::Unsupported(_)
    ));
    assert!(!log.accepted_vote_committed());
    assert_eq!(
        std::fs::read(dir.path().join("ballot")).expect("ballot"),
        before
    );

    open.store(true, Ordering::Release);
    assert_eq!(
        log.commit_vote(2, LEADER).await.expect("commit"),
        GenerationCheck::Current
    );
    assert!(log.accepted_vote_committed());
    let ballot = std::fs::read(dir.path().join("ballot")).expect("ballot");
    assert_eq!(u16::from_be_bytes([ballot[4], ballot[5]]), 2);
}

/// The previous release refuses what this build writes past the gate. Its
/// segment check, as of v0.7.1 (`segment/format.rs`, `SegmentHeader::decode`),
/// takes `OLDEST_READABLE_VERSION..=8`, and its ballot check takes version 1
/// only. Neither binary is run here: the check is restated from that tag.
#[tokio::test]
async fn the_previous_release_refuses_a_log_past_the_gate() {
    const PREVIOUS_FORMAT_VERSION: u16 = HEADERS_VERSION;
    const PREVIOUS_BALLOT_VERSION: u16 = 1;
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append(&records(&["a"])).await.expect("append");
    log.roll_now().await.expect("roll");
    lead(&log, 2).await;
    log.append(&records(&["b"])).await.expect("append");
    log.commit_vote(2, LEADER).await.expect("commit");
    let previous_reads = OLDEST_READABLE_VERSION..=PREVIOUS_FORMAT_VERSION;
    let versions: Vec<u16> = generations(&dir, &log)
        .into_iter()
        .map(|(version, _)| version)
        .collect();
    assert!(previous_reads.contains(&versions[0]), "{versions:?}");
    assert!(!previous_reads.contains(&versions[1]), "{versions:?}");
    let ballot = std::fs::read(dir.path().join("ballot")).expect("ballot");
    assert_ne!(
        u16::from_be_bytes([ballot[4], ballot[5]]),
        PREVIOUS_BALLOT_VERSION
    );
}

/// A provider's gate marks the data directory before its first v9 segment,
/// so a broker can refuse a fleet that has not finalized the feature without
/// opening every log.
#[tokio::test]
async fn the_first_v9_segment_marks_the_data_directory() {
    let root = tempdir().expect("root");
    let provider =
        DiskLogProvider::new(root.path(), config(FsyncMode::OnCommit)).expect("provider");
    let shard = ShardKey {
        tenant: "t".into(),
        namespace: "ns".into(),
        stream: "s".into(),
        shard: 0,
    };
    let log = provider.open_shard(&shard).expect("open");
    log.append(&records(&["a"])).await.expect("append");
    log.roll_now().await.expect("roll");
    assert!(!crate::run_state::has_record_generations(root.path()).expect("marker"));

    let open = Arc::new(AtomicBool::new(false));
    let check = Arc::clone(&open);
    provider.record_generations_when(move || check.load(Ordering::Acquire));
    log.append(&records(&["b"])).await.expect("append");
    log.roll_now().await.expect("roll");
    assert!(!crate::run_state::has_record_generations(root.path()).expect("marker"));
    assert!(
        log.segments()
            .iter()
            .all(|s| s.version < GENERATION_VERSION)
    );

    open.store(true, Ordering::Release);
    log.append(&records(&["c"])).await.expect("append");
    log.roll_now().await.expect("roll");
    assert!(crate::run_state::has_record_generations(root.path()).expect("marker"));
    assert_eq!(
        log.segments().last().expect("active").version,
        GENERATION_VERSION
    );
    provider.shutdown().await.expect("shutdown");
}

/// Random appends, new leaders, writes above the leader's generation, rolls, cuts
/// and reopens. After each reopen the log opens, holds what it acknowledged,
/// and its generations never fall.
#[tokio::test]
async fn random_rolls_cuts_and_reopens_keep_generations_from_falling() {
    use crate::io::writeback::SplitMix64;
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let dir = tempdir().expect("dir");
        let mut log = open_gated(&dir);
        let mut accepted = 0;
        let mut highest = 0;
        let mut led = None;
        let mut expected: Vec<String> = Vec::new();
        for step in 0..40 {
            let payload = format!("{seed}-{step}");
            match rng.below(8) {
                0 => {
                    accepted = accepted.max(highest) + 1 + rng.below(3);
                    lead(&log, accepted).await;
                    led = Some(accepted);
                }
                1 => {
                    // Above anything written so far, so never below the floor.
                    highest = highest.max(accepted) + 1 + rng.below(3);
                    let above = highest;
                    log.append_v9_for_tests(&records(&[&payload]), above)
                        .await
                        .expect("append above");
                    expected.push(payload);
                }
                2 => {
                    log.roll_now().await.expect("roll");
                }
                3 if !expected.is_empty() => {
                    let keep = rng.below(expected.len() as u64 + 1) as usize;
                    log.truncate(keep as u64).await.expect("truncate");
                    expected.truncate(keep);
                    led = None;
                }
                4 => {
                    log = reopen(&dir, log).await;
                    led = None;
                }
                _ => {
                    // A cut can drop the leader's record of its start, and
                    // a write above leaves a floor over the accepted one.
                    accepted = accepted.max(highest);
                    if led != Some(accepted) {
                        lead(&log, accepted).await;
                        led = Some(accepted);
                    }
                    log.append(&records(&[&payload])).await.expect("append");
                    expected.push(payload);
                }
            }
        }
        let log = reopen(&dir, log).await;
        assert_eq!(read_all(&log, 0).await, expected, "seed {seed}");
        let found = flat(&dir, &log);
        let stamped: Vec<u64> = found.iter().flatten().copied().collect();
        assert!(
            stamped.windows(2).all(|pair| pair[0] <= pair[1]),
            "seed {seed}: {found:?}"
        );
        log.shutdown().await.expect("shutdown");
    }
}

/// A follower stamps a copied record with the generation that wrote it, not
/// the one it has accepted since, whatever its history says.
#[tokio::test]
async fn a_copied_batch_keeps_the_generations_that_wrote_it() {
    let dir = tempdir().expect("dir");
    let log = v9_log(&dir).await;
    log.accept_generation(4, None).await.expect("accept");
    log.record_generation(4, 1).expect("record");
    copy_in(&log, 1, &names(0, 5), WrittenBy::Writers(vec![run(3, 1)]))
        .await
        .expect("copy");
    assert_eq!(read_generations(&log, 1).await, [Some(3); 5]);

    // Two runs in one batch at 6..10: 6..8 at 3, then 9 and 10 at 4.
    let runs = WrittenBy::Writers(vec![run(3, 6), run(4, 9)]);
    copy_in(&log, 6, &names(5, 5), runs).await.expect("copy");
    assert_eq!(
        read_generations(&log, 6).await,
        [Some(3), Some(3), Some(3), Some(4), Some(4)]
    );

    // Records held at 3, then more arrive after this log accepted 4, with
    // runs that start before the batch: the stamps a leader at 3 would write.
    let follower = tempdir().expect("dir");
    let f = v9_log(&follower).await;
    f.accept_generation(3, None).await.expect("accept");
    copy_in(&f, 1, &names(0, 5), WrittenBy::Writers(vec![run(3, 0)]))
        .await
        .expect("copy");
    f.accept_generation(4, None).await.expect("accept");
    copy_in(&f, 6, &names(5, 6), WrittenBy::Writers(vec![run(3, 0)]))
        .await
        .expect("copy");

    let leader = tempdir().expect("dir");
    let l = v9_log(&leader).await;
    lead(&l, 3).await;
    l.append(&names(0, 11)).await.expect("append");
    let from_follower = read_generations(&f, 1).await;
    assert_eq!(from_follower, [Some(3); 11]);
    assert_eq!(from_follower, read_generations(&l, 1).await);
}

/// A copied record below the generation of the one before it is refused, and
/// nothing is written.
#[tokio::test]
async fn a_copied_record_below_the_floor_is_refused() {
    let dir = tempdir().expect("dir");
    let log = v9_log(&dir).await;
    lead(&log, 5).await;
    log.append(&names(0, 2)).await.expect("append");
    let refused = copy_in(&log, 3, &names(2, 2), WrittenBy::Writers(vec![run(3, 3)])).await;
    assert!(matches!(
        refused,
        Err(StorageError::GenerationBelowFloor {
            offset: 3,
            generation: 3,
            floor: 5
        })
    ));
    assert_eq!(log.tail_offset().await.expect("tail"), 3);
    assert_eq!(read_generations(&log, 1).await, [Some(5), Some(5)]);
}

/// A v9 segment will not take a batch whose writers nobody named; an older
/// one has nowhere to keep them and takes it.
#[tokio::test]
async fn a_v9_log_refuses_a_batch_that_names_no_writer() {
    let dir = tempdir().expect("dir");
    let log = v9_log(&dir).await;
    lead(&log, 3).await;
    let refused = copy_in(&log, 1, &names(0, 2), WrittenBy::Unknown).await;
    assert!(matches!(refused, Err(StorageError::WriterUnknown(_))));
    assert_eq!(log.tail_offset().await.expect("tail"), 1);

    let closed = tempdir().expect("dir");
    let log = DiskLog::open(
        closed.path(),
        "t/ns/s/0",
        gated(&Arc::new(AtomicBool::new(false))),
    )
    .expect("open");
    copy_in(&log, 0, &names(0, 2), WrittenBy::Unknown)
        .await
        .expect("an older segment ignores who wrote it");
    assert_eq!(read_generations(&log, 0).await, [None, None]);
}

/// A leader stamps only the generation it leads at. Once its log accepts a
/// newer one from a fence, that generation belongs to another leader, even
/// after this log's history takes the new leader's labels: L leads 3, takes
/// N's fence at 4 and N's batch written at 4, and its own in-flight append is
/// refused rather than stamped 4.
#[tokio::test]
async fn a_deposed_leader_is_refused_even_with_the_new_leaders_labels() {
    let dir = tempdir().expect("dir");
    let log = v9_log(&dir).await;
    lead(&log, 3).await;
    log.append(&names(0, 1)).await.expect("append");

    log.accept_generation(4, Some("n")).await.expect("accept");
    let refused = log.append(&names(1, 1)).await.err();
    assert!(matches!(refused, Some(StorageError::WriterUnknown(_))));

    log.record_generation(4, 2).expect("label");
    copy_in(&log, 2, &names(1, 1), WrittenBy::Writers(vec![run(4, 2)]))
        .await
        .expect("copy");
    let refused = log.append(&names(2, 1)).await.err();
    assert!(matches!(refused, Some(StorageError::WriterUnknown(_))));
    assert_eq!(log.tail_offset().await.expect("tail"), 3);
    assert_eq!(read_generations(&log, 1).await, [Some(3), Some(4)]);

    // Leading again, at a newer generation, stamps that one.
    lead(&log, 5).await;
    log.append(&names(2, 1)).await.expect("append");
    assert_eq!(read_generations(&log, 1).await, [Some(3), Some(4), Some(5)]);
}

/// A record read back carries its generation when its segment keeps one.
#[tokio::test]
async fn read_back_carries_each_records_generation() {
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append(&records(&["old"])).await.expect("append");
    log.roll_now().await.expect("roll");
    lead(&log, 2).await;
    log.append(&records(&["new"])).await.expect("append");
    assert_eq!(read_generations(&log, 0).await, [None, Some(2)]);
    let log = reopen(&dir, log).await;
    assert_eq!(read_generations(&log, 0).await, [None, Some(2)]);
}

/// Runs that do not cover the batch in order are refused, nothing written.
#[tokio::test]
async fn malformed_runs_are_refused() {
    let dir = tempdir().expect("dir");
    let log = v9_log(&dir).await;
    lead(&log, 3).await;
    log.append(&names(0, 2)).await.expect("append");
    let cases: [Vec<Epoch>; 3] = [vec![], vec![run(3, 6)], vec![run(4, 3), run(3, 4)]];
    for runs in cases {
        let refused = copy_in(&log, 3, &names(2, 2), WrittenBy::Writers(runs.clone())).await;
        assert!(
            matches!(refused, Err(StorageError::WriterUnknown(_))),
            "{runs:?}"
        );
        assert_eq!(log.tail_offset().await.expect("tail"), 3, "{runs:?}");
    }
}

/// A leader's append is stamped only once the history records where its
/// generation starts, at or below the append. Between taking the generation
/// and recording its start, an append admitted under the old one would land
/// below that start yet carry the new stamp, so it is refused.
#[tokio::test]
async fn a_leader_is_refused_until_its_start_is_recorded() {
    let dir = tempdir().expect("dir");
    let log = v9_log(&dir).await;
    lead(&log, 3).await;
    log.append(&names(0, 1)).await.expect("append");

    log.accept_generation(5, Some(LEADER))
        .await
        .expect("accept");
    log.lead_at(5);
    let refused = log.append(&names(1, 1)).await.err();
    assert!(matches!(refused, Some(StorageError::WriterUnknown(_))));

    // Recorded past the tail: an append at the tail would sit below it.
    log.record_generation(5, 3).expect("record");
    let refused = log.append(&names(1, 1)).await.err();
    assert!(matches!(refused, Some(StorageError::WriterUnknown(_))));
    assert_eq!(log.tail_offset().await.expect("tail"), 2);
    assert_eq!(read_generations(&log, 1).await, [Some(3)]);
}

/// A record whose writer the sender did not know is stamped
/// [`UNKNOWN_WRITER`]. It passes under any floor and leaves the floor where
/// it was, after a roll, a reopen (the unknown records at a sealed segment's
/// ends hide its known ones from a resume scan) and a cut just past one.
#[tokio::test]
async fn unknown_writers_leave_the_floor_where_it_was() {
    let dir = tempdir().expect("dir");
    let log = v9_log(&dir).await;
    let unknown = || WrittenBy::Writers(vec![run(UNKNOWN_WRITER, 0)]);
    copy_in(&log, 1, &names(0, 1), unknown())
        .await
        .expect("copy");
    copy_in(&log, 2, &names(1, 1), WrittenBy::Writers(vec![run(5, 2)]))
        .await
        .expect("copy");
    copy_in(&log, 3, &names(2, 2), unknown())
        .await
        .expect("copy");
    let below = || WrittenBy::Writers(vec![run(4, 0)]);
    let refused = copy_in(&log, 5, &names(4, 1), below()).await.err();
    assert_below(refused, 4, 5);

    log.roll_now().await.expect("roll");
    let log = reopen(&dir, log).await;
    let refused = copy_in(&log, 5, &names(4, 1), below()).await.err();
    assert_below(refused, 4, 5);
    assert_eq!(
        read_generations(&log, 1).await,
        [Some(0), Some(5), Some(0), Some(0)]
    );

    // A cut that leaves an unknown writer newest finds the known one before.
    log.roll_now().await.expect("roll");
    copy_in(&log, 5, &names(4, 1), unknown())
        .await
        .expect("copy");
    copy_in(&log, 6, &names(5, 1), WrittenBy::Writers(vec![run(8, 6)]))
        .await
        .expect("copy");
    log.truncate(6).await.expect("truncate");
    let refused = copy_in(&log, 6, &names(5, 1), below()).await.err();
    assert_below(refused, 4, 5);
    copy_in(&log, 6, &names(5, 1), WrittenBy::Writers(vec![run(6, 6)]))
        .await
        .expect("at or above the floor");
    let log = reopen(&dir, log).await;
    assert_eq!(
        read_generations(&log, 1).await,
        [Some(0), Some(5), Some(0), Some(0), Some(0), Some(6)]
    );
}

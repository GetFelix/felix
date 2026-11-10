//! Moving a log onto segment format v9 once its gate allows it: only new
//! segments move, every record in one carries a generation, and the
//! generations never fall across a roll, a reopen or a truncation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
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
/// at the accepted generation.
#[tokio::test]
async fn only_a_new_segment_moves_onto_v9_once_the_gate_opens() {
    let dir = tempdir().expect("dir");
    let open = Arc::new(AtomicBool::new(false));
    let log = DiskLog::open(dir.path(), "t/ns/s/0", gated(&open)).expect("open");
    log.accept_generation(3, Some(LEADER))
        .await
        .expect("accept");
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

/// A record takes the generation the log accepted when it was written, and a
/// raise takes effect from the next append.
#[tokio::test]
async fn a_record_carries_the_accepted_generation() {
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append(&records(&["v3"])).await.expect("append");
    log.roll_now().await.expect("roll");
    log.accept_generation(4, Some(LEADER))
        .await
        .expect("accept");
    log.append(&records(&["a", "b"])).await.expect("append");
    log.accept_generation(6, Some(LEADER))
        .await
        .expect("accept");
    log.append(&records(&["c"])).await.expect("append");
    assert_eq!(flat(&dir, &log), [None, Some(4), Some(4), Some(6)]);
}

/// A record written above the accepted generation (here by the test hook; in
/// a broker, by a raise a crash lost before its file was stored) sets a floor
/// the next append keeps to after a reopen. The floor comes from the active
/// segment's full scan; without it the append goes in at 0 and the next open
/// refuses the log.
#[tokio::test]
async fn the_floor_survives_a_reopen() {
    let dir = tempdir().expect("dir");
    let log = open_gated(&dir);
    log.append_v9_for_tests(&records(&["a"]), 7)
        .await
        .expect("append");
    let log = reopen(&dir, log).await;
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
    log.append(&records(&["b"])).await.expect("append");
    for payload in ["c", "d", "e", "f"] {
        log.append(&records(&[payload])).await.expect("append");
    }
    log.roll_now().await.expect("background roll");
    let log = reopen(&dir, log).await;
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

    // Inside the active segment, below the records at 9.
    log.truncate(3).await.expect("truncate");
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
    log.append(&records(&["g"])).await.expect("append");
    let log = reopen(&dir, log).await;
    assert_eq!(flat(&dir, &log).last(), Some(&Some(5)));

    // Inside a sealed segment, which becomes the active one again.
    log.roll_now().await.expect("roll");
    log.append_v9_for_tests(&records(&["h"]), 9)
        .await
        .expect("append");
    log.truncate(2).await.expect("truncate");
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
    log.append(&records(&["b"])).await.expect("append");
    log.accept_generation(2, Some(LEADER))
        .await
        .expect("accept");
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

/// Random appends, raises, writes above the accepted generation, rolls, cuts
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
        let mut expected: Vec<String> = Vec::new();
        for step in 0..40 {
            let payload = format!("{seed}-{step}");
            match rng.below(8) {
                0 => {
                    accepted += 1 + rng.below(3);
                    log.accept_generation(accepted, Some(LEADER))
                        .await
                        .expect("accept");
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
                }
                4 => {
                    log = reopen(&dir, log).await;
                }
                _ => {
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

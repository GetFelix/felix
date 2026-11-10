//! Power loss on a log moving onto segment format v9, on the simulator's
//! filesystem.
//!
//! A gated log writes v3 segments, rolls onto v9, raises its generation,
//! rolls again, takes records above the leader's generation, is cut back by
//! a truncation and reopened. The power is cut
//! after each of those steps, and during an append into a v9 segment, where
//! the cut tears the 36-byte header, generation field included. Recovery
//! must open what is left, keep every acknowledged record as a gap-free
//! prefix, find generations that never fall, and take appends from the leader.
//!
//! Built only with the `sim` feature; CI runs it in the coverage job
//! (`--all-features`).

use std::path::{Path, PathBuf};

use super::*;
use crate::io::writeback::SplitMix64;
use crate::segment::format::decode_record_in;
use crate::segment::{GENERATION_VERSION, SEGMENT_HEADER_LEN, SegmentHeader, segment_file_name};
use crate::sim_fs;

const KEY: &str = "t/ns/s/0";
const SEEDS: u64 = 12;

fn config() -> LogConfig {
    let config = LogConfig {
        segment_size_bytes: 2 * 4096 + 300,
        index_spacing_bytes: 512,
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: true,
        rollover_threshold_percent: 100,
        max_overshoot_percent: 200,
        ..LogConfig::default()
    };
    config.generations.open_when(|| true);
    config
}

fn payload(offset: Offset) -> Vec<u8> {
    let mut rng = SplitMix64::new(offset.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let len = 1 + rng.below(700) as usize;
    let mut out = format!("{offset}:").into_bytes();
    out.extend((0..len).map(|_| b'a' + rng.below(26) as u8));
    out
}

fn record(offset: Offset) -> AppendRecord {
    AppendRecord {
        payload: Bytes::from(payload(offset)),
        timestamp_micros: 1_700_000_000 + offset,
        mark: Default::default(),
        publisher: None,
        headers: None,
    }
}

struct Sim(PathBuf);

impl Sim {
    fn new(seed: u64) -> Self {
        let root =
            std::env::temp_dir().join(format!("felix-simfs-v9-{}-{seed}", std::process::id()));
        sim_fs::register_root(&root);
        Self(root)
    }

    fn log_dir(&self) -> PathBuf {
        self.0.join("log")
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        sim_fs::unregister_root(&self.0);
    }
}

/// Make `generation` the log's leader: accepted, and recorded at the tail.
async fn lead(log: &DiskLog, generation: u64) {
    log.accept_generation(generation, Some("a"))
        .await
        .expect("accept");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("record");
}

async fn append(log: &DiskLog, from: Offset, to: Offset) {
    for offset in from..to {
        log.append(&[record(offset)]).await.expect("append");
    }
}

/// Where the workload stops for the power cut.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    /// Just rolled onto the first v9 segment.
    FirstV9,
    /// A raised generation, across more rolls.
    Raised,
    /// Cut back into a v9 segment, and appended again.
    Truncated,
    /// Reopened after the cut, and appended again.
    Reopened,
}

const STEPS: [Step; 4] = [Step::FirstV9, Step::Raised, Step::Truncated, Step::Reopened];

/// Run the workload up to `stop`, returning the log and the tail it wrote.
async fn run_to(dir: &Path, stop: Step) -> (DiskLog, Offset) {
    let log = DiskLog::open(dir, KEY, config()).expect("open");
    lead(&log, 2).await;
    // Starts on v3: the gate moves only the next segment.
    append(&log, 0, 6).await;
    log.roll_now().await.expect("roll");
    append(&log, 6, 8).await;
    if stop == Step::FirstV9 {
        return (log, 8);
    }
    append(&log, 8, 20).await;
    lead(&log, 4).await;
    append(&log, 20, 38).await;
    // Above the leader's generation, as after a raise whose `replica` file a
    // crash lost. A leader below this floor is refused until it leads above.
    log.append_v9_for_tests(&[record(38), record(39)], 6)
        .await
        .expect("append above the leader's generation");
    lead(&log, 6).await;
    // A background roll, then enough to roll inline.
    log.roll_now().await.expect("roll");
    append(&log, 40, 70).await;
    assert!(
        log.segments()
            .iter()
            .filter(|s| s.base_offset >= 40)
            .count()
            > 1,
        "never rolled inline above the floor"
    );
    if stop == Step::Raised {
        return (log, 70);
    }
    log.truncate(60).await.expect("truncate");
    append(&log, 60, 62).await;
    if stop == Step::Truncated {
        return (log, 62);
    }
    log.shutdown().await.expect("shutdown");
    drop(log);
    let log = DiskLog::open(dir, KEY, config()).expect("reopen");
    append(&log, 62, 66).await;
    (log, 66)
}

/// Every segment's generations, oldest first, read from the simulated files.
fn generations(dir: &Path, log: &DiskLog) -> Vec<(u16, Vec<Option<u64>>)> {
    log.segments()
        .iter()
        .map(|segment| {
            let bytes = crate::fs::read(dir.join(segment_file_name(segment.id))).expect("read");
            let version = SegmentHeader::decode(&bytes).expect("header").version;
            let mut at = SEGMENT_HEADER_LEN as usize;
            let mut found = Vec::new();
            while at < bytes.len()
                && let Ok((record, len)) = decode_record_in(&bytes[at..], version)
            {
                found.push(record.header.generation);
                at += len as usize;
            }
            (version, found)
        })
        .collect()
}

/// Open what the cut left and check it. Returns how many records came back.
async fn recover(dir: &Path, acknowledged: Offset, written: Offset) -> Offset {
    let log = DiskLog::open(dir, KEY, config()).expect("recovery refused an honest crash");
    let records = log
        .read_range(ReadRange {
            start: 0,
            max_bytes: usize::MAX,
        })
        .await
        .expect("read")
        .records;
    let kept = records.len() as u64;
    assert!(
        kept >= acknowledged,
        "lost acknowledged records: {kept} of {acknowledged}"
    );
    assert!(kept <= written, "invented records: {kept} of {written}");
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.offset, index as u64, "gap or reorder");
        assert_eq!(record.payload.as_ref(), payload(record.offset).as_slice());
    }
    check_generations(dir, &log);

    // The repaired log takes an append at its floor and opens again.
    log.append(&[record(kept)])
        .await
        .expect("append after recovery");
    log.shutdown().await.expect("shutdown");
    drop(log);
    let log = DiskLog::open(dir, KEY, config()).expect("reopen after the append");
    check_generations(dir, &log);
    log.shutdown().await.expect("shutdown");
    kept
}

/// Pre-v9 segments first, then v9 ones, with generations that never fall.
fn check_generations(dir: &Path, log: &DiskLog) {
    let found = generations(dir, log);
    let mut seen_v9 = false;
    let mut newest = 0;
    for (version, records) in &found {
        if *version >= GENERATION_VERSION {
            seen_v9 = true;
            for generation in records {
                let generation = generation.expect("a v9 record without a generation");
                assert!(generation >= newest, "generations fell: {found:?}");
                newest = generation;
            }
        } else {
            assert!(!seen_v9, "an older segment after a v9 one: {found:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_each_step_recovers_the_acknowledged_prefix() {
    for step in STEPS {
        for seed in 0..SEEDS {
            let sim = Sim::new(seed + 100 * step as u64);
            let (log, written) = run_to(&sim.log_dir(), step).await;
            let acknowledged = log.durable_offset();
            assert_eq!(
                acknowledged, written,
                "{step:?}: OnCommit acknowledged early"
            );
            sim_fs::crash(&sim.0, seed).expect("crash");
            let kept = recover(&sim.log_dir(), acknowledged, written).await;
            assert_eq!(kept, written, "{step:?} seed {seed}");
            log.shutdown().await.expect("shutdown the cut-off log");
        }
    }
}

/// The cut lands inside an append to a v9 segment, so its header, the
/// generation in it, or its body comes back short or torn. Recovery repairs
/// that tail and keeps everything before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torn_v9_append_is_repaired_after_each_step() {
    for step in STEPS {
        for seed in 0..SEEDS {
            let sim = Sim::new(5_000 + seed + 100 * step as u64);
            let (log, written) = run_to(&sim.log_dir(), step).await;
            assert_eq!(
                log.segments().last().expect("active").version,
                GENERATION_VERSION
            );
            let acknowledged = log.durable_offset();
            let torn = std::sync::Arc::new(std::sync::Mutex::new(None));
            let slot = std::sync::Arc::clone(&torn);
            sim_fs::arm_power_cut(&sim.0, seed, move |path, _| {
                *slot.lock().unwrap() = Some(path.to_path_buf());
            })
            .expect("arm");
            let _ = log.append(&[record(written)]).await;
            assert!(
                sim_fs::reboot(&sim.0).expect("reboot"),
                "{step:?} seed {seed}: no cut"
            );
            let torn = torn.lock().unwrap().clone().expect("the torn segment");
            let kept = recover(&sim.log_dir(), acknowledged, written).await;
            assert_eq!(kept, written, "{step:?} seed {seed}");
            let repaired = sim_fs::torn_tails_repaired(&sim.0).expect("repairs");
            assert!(
                repaired.contains(&torn),
                "{step:?} seed {seed}: the torn append was not repaired"
            );
            log.shutdown().await.expect("shutdown the cut-off log");
        }
    }
}
